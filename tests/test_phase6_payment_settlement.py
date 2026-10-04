"""Focused tests for provider-neutral payment and settlement persistence."""
from __future__ import annotations

import sys
import unittest
from decimal import Decimal
from pathlib import Path

import pandas as pd
from sqlalchemy import text
from sqlalchemy.exc import IntegrityError

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from config.database import get_db_connection
from connectors.payment import (
    CODReconciliationProvider,
    EasyPaisaPaymentProvider,
    JazzCashPaymentProvider,
    ManualPaymentProvider,
    PaymentRecord,
    PaymentSettlement,
    PaymentSettlementEntry,
    StripePaymentProvider,
    normalize_method,
    normalize_status,
)
from connectors.payment.provider_factory import get_payment_provider
from etl.payment_transformer import PaymentTransformer
from loaders.payment_loader import PaymentLoader
from pipeline.run_payment_sync import run_payment_sync, run_stripe_sync


class TestPhase6PaymentSettlement(unittest.TestCase):
    B1 = 1
    B2 = 2
    ORDER_B1_SHOPIFY = "5589123456701_p6"
    ORDER_B1_DARAZ = "daraz_9900112201_p6"
    ORDER_B2_SHOPIFY = "5589123456702_p6"
    TEST_PREFIX = "p55-test-"

    @classmethod
    def setUpClass(cls):
        with get_db_connection() as conn:
            conn.execute(text("""
                INSERT INTO core.business (business_id, business_name, is_active)
                OVERRIDING SYSTEM VALUE
                VALUES (1, 'Test Business Phase 6 B1', TRUE)
                ON CONFLICT (business_id) DO UPDATE SET is_active = TRUE;
            """))
            conn.execute(text("""
                INSERT INTO core.business (business_id, business_name, is_active)
                OVERRIDING SYSTEM VALUE
                VALUES (2, 'Test Business Phase 6 B2', TRUE)
                ON CONFLICT (business_id) DO UPDATE SET is_active = TRUE;
            """))
            conn.commit()
        cls._cleanup_test_data()

    @classmethod
    def tearDownClass(cls):
        cls._cleanup_test_data()

    @classmethod
    def _cleanup_test_data(cls):
        with get_db_connection() as conn:
            conn.execute(text("""
                DELETE FROM core.payment_settlement_entry
                WHERE business_id IN (1, 2)
                  AND settlement_id IN (
                      SELECT settlement_id
                      FROM core.payment_settlement
                      WHERE business_id IN (1, 2)
                        AND external_settlement_id LIKE :prefix
                  );
            """), {"prefix": f"{cls.TEST_PREFIX}%"})
            conn.execute(text("""
                DELETE FROM core.payment_settlement
                WHERE business_id IN (1, 2)
                  AND external_settlement_id LIKE :prefix;
            """), {"prefix": f"{cls.TEST_PREFIX}%"})
            conn.execute(text("""
                DELETE FROM core.payment
                WHERE business_id IN (1, 2)
                  AND external_payment_id LIKE :prefix;
            """), {"prefix": f"{cls.TEST_PREFIX}%"})
            conn.execute(text("""
                DELETE FROM core.orders
                WHERE business_id IN (1, 2)
                  AND external_order_id IN (:o1, :o2, :o3);
            """), {
                "o1": cls.ORDER_B1_SHOPIFY,
                "o2": cls.ORDER_B1_DARAZ,
                "o3": cls.ORDER_B2_SHOPIFY,
            })
            conn.commit()

    def setUp(self):
        with get_db_connection() as conn:
            for business_id, phone, city in (
                (self.B1, "+923001234571", "Lahore"),
                (self.B2, "+923001234572", "Karachi"),
            ):
                conn.execute(text("""
                    INSERT INTO core.customer (business_id, external_customer_id, full_name, phone, city)
                    VALUES (:business_id, :external_customer_id, :full_name, :phone, :city)
                    ON CONFLICT (business_id, phone) DO NOTHING;
                """), {
                    "business_id": business_id,
                    "external_customer_id": f"cust_p6_b{business_id}",
                    "full_name": f"Customer P6 B{business_id}",
                    "phone": phone,
                    "city": city,
                })

            orders = (
                (self.B1, "+923001234571", self.ORDER_B1_SHOPIFY, "card", 4700.00, "shopify"),
                (self.B1, "+923001234571", self.ORDER_B1_DARAZ, "wallet", 3150.00, "daraz"),
                (self.B2, "+923001234572", self.ORDER_B2_SHOPIFY, "card", 8300.00, "shopify"),
            )
            for business_id, phone, external_id, method, total, platform in orders:
                conn.execute(text("""
                    INSERT INTO core.orders (
                        business_id, customer_id, external_order_id, order_date, order_status,
                        payment_method, subtotal_amount, discount_amount, shipping_charged,
                        total_amount, currency, shipping_city, source_platform
                    )
                    SELECT :business_id, customer_id, :external_id, now(), 'confirmed',
                           :method, :subtotal, 0, 0, :total, 'PKR', 'Lahore', :platform
                    FROM core.customer
                    WHERE business_id = :business_id AND phone = :phone
                    ON CONFLICT (business_id, external_order_id) DO NOTHING;
                """), {
                    "business_id": business_id,
                    "phone": phone,
                    "external_id": external_id,
                    "method": method,
                    "subtotal": total,
                    "total": total,
                    "platform": platform,
                })
            conn.commit()

    def _order_map(self, business_id: int) -> dict[str, int]:
        return PaymentLoader(business_id).get_order_id_map()

    def _payment_frame(
        self,
        business_id: int,
        external_id: str,
        *,
        order_external_id: str | None = None,
        payment_type: str = "payment",
        status: str = "paid",
        amount: str = "100.00",
        fee: str = "2.00",
        provider: str = "manual_test_provider",
        account: str = "merchant-test-account",
        currency: str = "PKR",
        original_payment_id: int | None = None,
    ) -> pd.DataFrame:
        order_map = self._order_map(business_id)
        if order_external_id is None:
            order_external_id = self.ORDER_B1_SHOPIFY if business_id == self.B1 else self.ORDER_B2_SHOPIFY
        return pd.DataFrame([{
            "business_id": business_id,
            "order_id": order_map[order_external_id],
            "external_payment_id": external_id,
            "payment_method": "card",
            "payment_type": payment_type,
            "payment_status": status,
            "amount": Decimal(amount),
            "gateway_fee": Decimal(fee),
            "paid_at": None,
            "provider": provider,
            "provider_account_identifier": account,
            "currency": currency,
            "original_payment_id": original_payment_id,
        }])

    def _create_payment(self, business_id: int, external_id: str) -> int:
        PaymentLoader(business_id).load_payments(
            self._payment_frame(business_id, external_id)
        )
        return PaymentLoader(business_id).get_payment_id_map()[external_id]

    def test_provider_parsers_do_not_infer_canonical_transaction_fields(self):
        stripe = StripePaymentProvider().parse_transactions([{
            "object": "charge",
            "id": f"{self.TEST_PREFIX}stripe",
            "amount": 470000,
            "application_fee_amount": 13630,
            "currency": "pkr",
            "status": "succeeded",
            "metadata": {"order_id": self.ORDER_B1_SHOPIFY},
            "payment_method_details": {"type": "card"},
            "provider_account_identifier": "acct_explicit",
        }])[0]
        self.assertIsNone(stripe.amount)
        self.assertIsNone(stripe.gateway_fee)
        self.assertEqual(stripe.currency, "PKR")
        self.assertEqual(stripe.provider_account_identifier, "acct_explicit")
        self.assertIsNone(stripe.payment_status)
        self.assertIsNone(stripe.net_amount)

        providers = (
            (JazzCashPaymentProvider(), "Txn Ref No,Merchant Order ID,Txn Type,Txn Amount,Txn Status,Commission\n"
             f"{self.TEST_PREFIX}jc,{self.ORDER_B1_DARAZ},payment,3150,paid,47.25\n"),
            (EasyPaisaPaymentProvider(), "Transaction ID,Order ID,Type,Gross Amount,Status,Commission\n"
             f"{self.TEST_PREFIX}ep,{self.ORDER_B1_DARAZ},payment,3150,paid,47.25\n"),
            (ManualPaymentProvider(), '[{"external_payment_id":"' + self.TEST_PREFIX +
             'manual","external_order_id":"' + self.ORDER_B1_SHOPIFY +
             '","amount":"10.00","gateway_fee":"0.00","payment_method":"bank_transfer",'
             '"payment_type":"payment","payment_status":"paid","currency":"PKR",'
             '"provider":"bank_statement","provider_account_identifier":"acct_manual"}]'),
        )
        for provider, payload in providers:
            with self.subTest(provider=provider.PROVIDER_NAME):
                record = provider.parse_transactions(payload)[0]
                if provider.PROVIDER_NAME == "manual":
                    self.assertEqual(record.amount, Decimal("10.00"))
                    self.assertEqual(record.gateway_fee, Decimal("0.00"))
                    self.assertEqual(record.currency, "PKR")
                    self.assertIsNotNone(record.provider_account_identifier)
                else:
                    self.assertIsNone(record.amount)
                    self.assertIsNone(record.gateway_fee)
                    self.assertIsNone(record.currency)
                    self.assertIsNone(record.provider_account_identifier)
                self.assertIsNone(record.net_amount)

    def test_unknown_currency_fee_status_and_missing_ids_remain_raw_only(self):
        parsed = JazzCashPaymentProvider().parse_transactions([{
            "pp_TxnRefNo": f"{self.TEST_PREFIX}incomplete",
            "pp_Amount": "10000",
            "payment_method": "wallet",
            "payment_status": "success",
        }])
        self.assertEqual(len(parsed), 1)
        self.assertIsNone(parsed[0].gateway_fee)
        self.assertIsNone(parsed[0].currency)
        self.assertIsNone(parsed[0].payment_status)
        self.assertIsNone(parsed[0].external_payment_id)
        self.assertIsNone(parsed[0].amount)
        self.assertIn("pp_TxnRefNo", parsed[0].raw_metadata)
        self.assertEqual(normalize_status("some-new-provider-state"), None)
        self.assertIsNone(normalize_status("success"))
        self.assertIsNone(normalize_status("completed"))
        self.assertIsNone(normalize_method("EasyPaisa"))

        transformer = PaymentTransformer(self.B1)
        frame = transformer.transform_payment_records(parsed, self._order_map(self.B1))
        self.assertTrue(frame.empty)
        self.assertEqual(len(transformer.rejected_records), 1)
        for provider_name in ("stripe", "payfast"):
            with self.subTest(provider=provider_name):
                with self.assertRaisesRegex(ValueError, "Unsupported payment provider"):
                    get_payment_provider(provider_name)
        with self.assertRaisesRegex(RuntimeError, "outside the Pakistan payment scope"):
            run_stripe_sync(secret_key="must-not-be-used")
        self.assertIsNone(CODReconciliationProvider().parse_transactions([{
            "tracking_number": "TRACK-ONLY",
            "cod_amount": "100.00",
            "status": "delivered",
        }])[0].external_payment_id)

    def test_jazzcash_documented_api_fields_remain_raw_without_verified_mapping(self):
        provider = JazzCashPaymentProvider()
        transaction_status_response = {
            "pp_TxnRefNo": f"{self.TEST_PREFIX}jazzcash-reference",
            "status": "success",
            "rrn": "sample-rrn",
            "settlementDate": "2026-10-04",
            "settlementExpiryDate": "2026-10-05",
        }
        payment_request = {
            "pp_TxnRefNo": f"{self.TEST_PREFIX}jazzcash-request-reference",
            "pp_Amount": "1000",
            "pp_TxnCurrency": "PKR",
            "pp_TxnDateTime": "20261004120000",
        }
        refund_request = {
            "RefundRequest": {
                "pp_TxnRefNo": f"{self.TEST_PREFIX}jazzcash-original-reference",
                "pp_Amount": "9999",
                "pp_TxnCurrency": "PKR",
            }
        }

        status_record = provider.parse_transactions([transaction_status_response])[0]
        payment_record = provider.parse_transactions([payment_request])[0]
        refund_record = provider.parse_transactions([refund_request])[0]

        for record, expected_raw in (
            (status_record, transaction_status_response),
            (payment_record, payment_request),
            (refund_record, refund_request),
        ):
            with self.subTest(raw_fields=tuple(expected_raw)):
                self.assertIsNone(record.external_payment_id)
                self.assertIsNone(record.external_order_id)
                self.assertIsNone(record.amount)
                self.assertIsNone(record.gateway_fee)
                self.assertIsNone(record.currency)
                self.assertIsNone(record.payment_status)
                self.assertIsNone(record.payment_type)
                self.assertIsNone(record.paid_at)
                self.assertEqual(record.raw_metadata, expected_raw)

        transformer = PaymentTransformer(self.B1)
        frame = transformer.transform_payment_records(
            [status_record, payment_record, refund_record], {}
        )
        self.assertTrue(frame.empty)
        self.assertEqual(len(transformer.rejected_records), 3)

    def test_manual_import_does_not_guess_ambiguous_column_meanings(self):
        record = ManualPaymentProvider().parse_transactions([{
            "transaction_id": f"{self.TEST_PREFIX}ambiguous-transaction",
            "order_ref": self.ORDER_B1_SHOPIFY,
            "amount": "100.00",
            "fee": "2.00",
            "method": "wallet",
            "type": "payment",
            "status": "success",
            "currency": "PKR",
            "provider": "manual_bank_statement",
            "provider_account_identifier": "manual-account",
        }])[0]

        self.assertIsNone(record.external_payment_id)
        self.assertIsNone(record.external_order_id)
        self.assertIsNone(record.gateway_fee)
        self.assertIsNone(record.payment_method)
        self.assertIsNone(record.payment_type)
        self.assertIsNone(record.payment_status)
        self.assertIn("transaction_id", record.raw_metadata)

    def test_transformer_requires_provider_account_currency_and_explicit_fee(self):
        incomplete = PaymentRecord(
            external_order_id=self.ORDER_B1_SHOPIFY,
            external_payment_id=f"{self.TEST_PREFIX}raw-only",
            amount=Decimal("100.00"),
            payment_method="card",
            payment_type="payment",
            payment_status="paid",
            provider="manual_test_provider",
        )
        transformer = PaymentTransformer(self.B1)
        frame = transformer.transform_payment_records(
            [incomplete], self._order_map(self.B1)
        )
        self.assertTrue(frame.empty)
        self.assertGreaterEqual(len(transformer.rejected_records), 1)

    def test_transformer_preserves_refund_and_verified_original_reference(self):
        payment_id = self._create_payment(self.B1, f"{self.TEST_PREFIX}original")
        context = PaymentLoader(self.B1).get_payment_context_map()
        record = PaymentRecord(
            external_order_id=None,
            external_payment_id=f"{self.TEST_PREFIX}refund",
            amount=Decimal("25.00"),
            gateway_fee=Decimal("0.00"),
            payment_method="card",
            payment_type="refund",
            payment_status="paid",
            currency="PKR",
            provider="manual_test_provider",
            provider_account_identifier="merchant-test-account",
            original_external_payment_id=f"{self.TEST_PREFIX}original",
        )
        transformer = PaymentTransformer(self.B1)
        result = transformer.transform_payment_records(
            [record],
            self._order_map(self.B1),
            context,
        )
        self.assertEqual(result.iloc[0]["payment_type"], "refund")
        self.assertEqual(result.iloc[0]["amount"], Decimal("25.00"))
        self.assertEqual(result.iloc[0]["original_payment_id"], payment_id)
        self.assertEqual(PaymentLoader(self.B1).load_payments(result), (1, 0))
        with get_db_connection() as conn:
            stored_original_id = conn.execute(text("""
                SELECT original_payment_id
                FROM core.payment
                WHERE business_id = :business_id
                  AND external_payment_id = :refund_id;
            """), {
                "business_id": self.B1,
                "refund_id": f"{self.TEST_PREFIX}refund",
            }).scalar_one()
        self.assertEqual(stored_original_id, payment_id)

        unresolved = PaymentRecord(
            external_order_id=self.ORDER_B1_SHOPIFY,
            external_payment_id=f"{self.TEST_PREFIX}refund-unmatched",
            amount=Decimal("5.00"),
            gateway_fee=Decimal("0.00"),
            payment_method="card",
            payment_type="refund",
            payment_status="paid",
            currency="PKR",
            provider="manual_test_provider",
            provider_account_identifier="merchant-test-account",
            original_external_payment_id="unknown-original-id",
        )
        result = transformer.transform_payment_records(
            [unresolved], self._order_map(self.B1)
        )
        self.assertIsNone(result.iloc[0]["original_payment_id"])

    def test_payment_upsert_is_idempotent_and_persists_new_fields(self):
        loader = PaymentLoader(self.B1)
        external_id = f"{self.TEST_PREFIX}upsert"
        frame = self._payment_frame(self.B1, external_id)
        self.assertEqual(loader.load_payments(frame), (1, 0))
        self.assertEqual(loader.load_payments(frame), (0, 1))
        with get_db_connection() as conn:
            row = conn.execute(text("""
                SELECT provider, provider_account_identifier, currency,
                       payment_type, amount, gateway_fee
                FROM core.payment
                WHERE business_id = :business_id
                  AND external_payment_id = :external_id;
            """), {
                "business_id": self.B1,
                "external_id": external_id,
            }).fetchone()
        if row is None:
            raise AssertionError("Persisted payment row was not found.")
        self.assertEqual(tuple(row), (
            "manual_test_provider", "merchant-test-account", "PKR", "payment",
            Decimal("100.00"), Decimal("2.00"),
        ))

    def test_legacy_unique_key_is_retained_and_does_not_alias_accounts(self):
        loader = PaymentLoader(self.B1)
        external_id = f"{self.TEST_PREFIX}account-collision"
        self.assertEqual(loader.load_payments(
            self._payment_frame(self.B1, external_id)
        ), (1, 0))
        conflicting = self._payment_frame(
            self.B1, external_id, account="different-account"
        )
        with self.assertRaisesRegex(ValueError, "different provider identity"):
            loader.load_payments(conflicting)
        changed_amount = self._payment_frame(
            self.B1, external_id, amount="101.00"
        )
        with self.assertRaisesRegex(ValueError, "conflicts with existing transaction facts"):
            loader.load_payments(changed_amount)
        with get_db_connection() as conn:
            constraint = conn.execute(text("""
                SELECT count(*)
                FROM pg_constraint
                WHERE conrelid = 'core.payment'::regclass
                  AND conname = 'uq_payment_external';
            """)).scalar_one()
        self.assertEqual(constraint, 1)

    def test_loader_rejects_existing_payment_constraint_violations(self):
        invalid = self._payment_frame(
            self.B1, f"{self.TEST_PREFIX}bad-status", status="unknown"
        )
        with self.assertRaisesRegex(ValueError, "Unsupported payment_status"):
            PaymentLoader(self.B1).load_payments(invalid)

    def test_cross_tenant_refund_payment_link_is_rejected(self):
        original_id = self._create_payment(self.B2, f"{self.TEST_PREFIX}b2-original")
        refund = self._payment_frame(
            self.B1,
            f"{self.TEST_PREFIX}cross-tenant-refund",
            payment_type="refund",
            original_payment_id=original_id,
        )
        with self.assertRaisesRegex(ValueError, "same tenant"):
            PaymentLoader(self.B1).load_payments(refund)

    def test_settlement_entries_allow_partial_unmatched_and_repeat_links(self):
        payment_id = self._create_payment(self.B1, f"{self.TEST_PREFIX}settlement-payment")
        loader = PaymentLoader(self.B1)
        settlement = PaymentSettlement(
            provider="manual_courier_statement",
            provider_account_identifier="manual-cod-import",
            external_settlement_id=f"{self.TEST_PREFIX}settlement-1",
            currency="PKR",
            gross_amount=Decimal("120.00"),
            fee_amount=Decimal("5.00"),
            adjustment_amount=Decimal("-2.00"),
            net_amount=Decimal("113.00"),
            status="settled",
            settled_at="2026-10-01T10:00:00+05:00",
        )
        entries = [
            PaymentSettlementEntry(
                entry_type="payment",
                signed_amount=Decimal("60.00"),
                currency="PKR",
                external_entry_id=f"{self.TEST_PREFIX}entry-1",
                external_transaction_id="provider-txn-1",
                payment_id=payment_id,
            ),
            PaymentSettlementEntry(
                entry_type="payment",
                signed_amount=Decimal("40.00"),
                currency="PKR",
                external_entry_id=f"{self.TEST_PREFIX}entry-2",
                external_transaction_id="provider-txn-1",
                payment_id=payment_id,
            ),
            PaymentSettlementEntry(
                entry_type="adjustment",
                signed_amount=Decimal("-2.00"),
                currency="PKR",
                external_entry_id=f"{self.TEST_PREFIX}entry-unmatched",
            ),
        ]
        settlement_id, inserted_entries = loader.load_settlement(settlement, entries)
        self.assertEqual(inserted_entries, 3)
        replay_id, replayed_entries = loader.load_settlement(settlement, entries)
        self.assertEqual(replay_id, settlement_id)
        self.assertEqual(replayed_entries, 0)

        with get_db_connection() as conn:
            header = conn.execute(text("""
                SELECT gross_amount, fee_amount, adjustment_amount, net_amount, status
                FROM core.payment_settlement
                WHERE settlement_id = :settlement_id;
            """), {"settlement_id": settlement_id}).fetchone()
            entry_counts = conn.execute(text("""
                SELECT count(*), count(*) FILTER (WHERE payment_id IS NULL),
                       count(*) FILTER (WHERE payment_id = :payment_id)
                FROM core.payment_settlement_entry
                WHERE settlement_id = :settlement_id;
            """), {
                "settlement_id": settlement_id,
                "payment_id": payment_id,
            }).fetchone()
        if header is None or entry_counts is None:
            raise AssertionError("Persisted settlement header or entries were not found.")
        self.assertEqual(tuple(header), (
            Decimal("120.000000"), Decimal("5.000000"),
            Decimal("-2.000000"), Decimal("113.000000"), "settled",
        ))
        self.assertEqual(tuple(entry_counts), (3, 1, 2))

    def test_settlement_net_is_never_derived_and_payment_status_is_independent(self):
        loader = PaymentLoader(self.B1)
        settlement = {
            "provider": "bank_statement",
            "provider_account_identifier": "bank-account",
            "external_settlement_id": f"{self.TEST_PREFIX}net-unknown",
            "currency": "PKR",
            "gross_amount": Decimal("100.00"),
            "fee_amount": Decimal("10.00"),
            "adjustment_amount": None,
            "net_amount": None,
            "status": "pending",
        }
        settlement_id, _ = loader.load_settlement(settlement, [])
        with get_db_connection() as conn:
            net_amount = conn.execute(text("""
                SELECT net_amount
                FROM core.payment_settlement
                WHERE settlement_id = :settlement_id;
            """), {"settlement_id": settlement_id}).scalar_one()
        self.assertIsNone(net_amount)

        pending_payment = self._payment_frame(
            self.B1, f"{self.TEST_PREFIX}pending-payment", status="pending"
        )
        PaymentLoader(self.B1).load_payments(pending_payment)
        settled_header = {
            **settlement,
            "external_settlement_id": f"{self.TEST_PREFIX}independent-status",
            "status": "settled",
            "gross_amount": None,
            "fee_amount": None,
        }
        loader.load_settlement(settled_header, [])
        with get_db_connection() as conn:
            status_row = conn.execute(text("""
                SELECT p.payment_status, s.status
                FROM core.payment AS p
                JOIN core.payment_settlement AS s
                  ON s.external_settlement_id = :settlement_id
                 AND s.business_id = p.business_id
                WHERE p.external_payment_id = :payment_id;
            """), {
                "settlement_id": f"{self.TEST_PREFIX}independent-status",
                "payment_id": f"{self.TEST_PREFIX}pending-payment",
            }).fetchone()
        if status_row is None:
            raise AssertionError("Expected payment and settlement status rows were not found.")
        payment_status, settlement_status = status_row
        self.assertEqual(payment_status, "pending")
        self.assertEqual(settlement_status, "settled")

        missing_facts_id, _ = loader.load_settlement({
            **settlement,
            "external_settlement_id": f"{self.TEST_PREFIX}all-amounts-unknown",
            "gross_amount": None,
            "fee_amount": None,
            "adjustment_amount": None,
            "net_amount": None,
            "status": "unknown",
        }, [])
        with get_db_connection() as conn:
            missing_facts = conn.execute(text("""
                SELECT gross_amount, fee_amount, adjustment_amount, net_amount
                FROM core.payment_settlement
                WHERE settlement_id = :settlement_id
                  AND business_id = :business_id;
            """), {"settlement_id": missing_facts_id, "business_id": self.B1}).fetchone()
        if missing_facts is None:
            raise AssertionError("Settlement with unknown amounts was not persisted.")
        self.assertEqual(tuple(missing_facts), (None, None, None, None))

    def test_cross_tenant_settlement_payment_link_is_rejected(self):
        payment_id = self._create_payment(self.B2, f"{self.TEST_PREFIX}foreign-payment")
        settlement = PaymentSettlement(
            provider="manual_courier_statement",
            provider_account_identifier="manual-cod-import",
            external_settlement_id=f"{self.TEST_PREFIX}cross-tenant-settlement",
            currency="PKR",
            status="settled",
        )
        entry = PaymentSettlementEntry(
            entry_type="payment",
            signed_amount=Decimal("1.00"),
            currency="PKR",
            external_entry_id=f"{self.TEST_PREFIX}foreign-entry",
            payment_id=payment_id,
        )
        with self.assertRaises(IntegrityError):
            PaymentLoader(self.B1).load_settlement(settlement, [entry])

        foreign_settlement = PaymentSettlement(
            provider="manual_courier_statement",
            provider_account_identifier="manual-cod-import-b2",
            external_settlement_id=f"{self.TEST_PREFIX}foreign-settlement",
            currency="PKR",
            status="settled",
        )
        foreign_settlement_id, _ = PaymentLoader(self.B2).load_settlement(
            foreign_settlement, []
        )
        with self.assertRaises(IntegrityError):
            with get_db_connection() as conn:
                with conn.begin():
                    conn.execute(text("""
                        INSERT INTO core.payment_settlement_entry (
                            business_id, settlement_id, entry_type,
                            signed_amount, currency
                        )
                        VALUES (1, :settlement_id, 'payment', 1.00, 'PKR');
                    """), {"settlement_id": foreign_settlement_id})

    def test_cod_delivery_does_not_create_payment_or_settlement(self):
        provider = CODReconciliationProvider()
        delivered = provider.reconcile_delivered_shipment({
            "external_order_id": self.ORDER_B1_SHOPIFY,
            "tracking_number": "TRACK-DELIVERED",
            "cod_amount": 100,
            "delivered_at": "2026-10-01T17:00:00+05:00",
        })
        self.assertIsNone(delivered)

    def test_manual_cod_statement_preserves_supplied_settlement_facts(self):
        payment_id = self._create_payment(self.B1, f"{self.TEST_PREFIX}cod-payment")
        settlement = PaymentSettlement(
            provider="manual_courier_statement",
            provider_account_identifier="manual-cod-import",
            external_settlement_id=f"{self.TEST_PREFIX}verified-cod",
            currency="PKR",
            gross_amount=Decimal("4700.00"),
            fee_amount=Decimal("150.00"),
            net_amount=Decimal("4550.00"),
            status="settled",
            settled_at="2026-10-01T17:00:00+05:00",
        )
        entries = [
            PaymentSettlementEntry(
                entry_type="payment",
                signed_amount=Decimal("4700.00"),
                currency="PKR",
                external_entry_id=f"{self.TEST_PREFIX}manual-cod-payment-entry",
                payment_id=payment_id,
            ),
            PaymentSettlementEntry(
                entry_type="fee",
                signed_amount=Decimal("-150.00"),
                currency="PKR",
                external_entry_id=f"{self.TEST_PREFIX}manual-cod-fee-entry",
            ),
        ]
        sync_type = f"{self.TEST_PREFIX}manual-cod-import"
        result = run_payment_sync(
            "manual",
            [],
            sync_type=sync_type,
            business_id=self.B1,
            settlement_record={
                "provider": settlement.provider,
                "provider_account_identifier": settlement.provider_account_identifier,
                "external_settlement_id": settlement.external_settlement_id,
                "currency": settlement.currency,
                "gross_amount": settlement.gross_amount,
                "fee_amount": settlement.fee_amount,
                "net_amount": settlement.net_amount,
                "status": settlement.status,
                "settled_at": settlement.settled_at,
            },
            settlement_entries=[{
                "entry_type": entry.entry_type,
                "signed_amount": entry.signed_amount,
                "currency": entry.currency,
                "external_entry_id": entry.external_entry_id,
                "payment_id": entry.payment_id,
            } for entry in entries],
        )
        settlement_id = result["settlement_id"]
        self.assertIsNotNone(settlement_id)
        with get_db_connection() as conn:
            header = conn.execute(text("""
                SELECT gross_amount, fee_amount, net_amount
                FROM core.payment_settlement
                WHERE settlement_id = :settlement_id
                  AND business_id = :business_id;
            """), {"settlement_id": settlement_id, "business_id": self.B1}).fetchone()
            if header is None:
                raise AssertionError("Manual COD settlement header was not persisted.")
            unmatched = conn.execute(text("""
                SELECT count(*)
                FROM core.payment_settlement_entry
                WHERE settlement_id = :settlement_id
                  AND business_id = :business_id
                  AND payment_id IS NULL;
            """), {"settlement_id": settlement_id, "business_id": self.B1}).scalar_one()
        self.assertEqual(tuple(header), (
            Decimal("4700.000000"),
            Decimal("150.000000"),
            Decimal("4550.000000"),
        ))
        self.assertEqual(unmatched, 1)
        with get_db_connection() as conn:
            raw = conn.execute(text("""
                SELECT payload::text
                FROM raw.raw_data
                WHERE raw_id = :raw_id
                  AND business_id = :business_id;
            """), {"raw_id": result["raw_id"], "business_id": self.B1}).scalar_one()
            conn.execute(text("""
                DELETE FROM raw.raw_data
                WHERE raw_id = :raw_id AND business_id = :business_id;
            """), {"raw_id": result["raw_id"], "business_id": self.B1})
            conn.execute(text("""
                DELETE FROM raw.sync_log
                WHERE business_id = :business_id
                  AND source = 'payment_manual'
                  AND sync_type = :sync_type;
            """), {"business_id": self.B1, "sync_type": sync_type})
            conn.commit()
        self.assertIn(self.TEST_PREFIX + "verified-cod", raw)
        self.assertIn("4550.00", raw)

    def test_run_sync_preserves_original_redacted_payload_and_canonical_payment(self):
        payload = [{
            "external_payment_id": f"{self.TEST_PREFIX}payload",
            "external_order_id": self.ORDER_B1_SHOPIFY,
            "amount": "100.00",
            "gateway_fee": "2.00",
            "currency": "pkr",
            "payment_method": "card",
            "payment_type": "payment",
            "payment_status": "paid",
            "provider": "manual_import",
            "provider_account_identifier": "manual-test-account",
            "access_token": "do-not-store-this",
        }]
        sync_type = f"{self.TEST_PREFIX}raw-payload-sync"
        result = run_payment_sync(
            "manual", payload, sync_type=sync_type, business_id=self.B1
        )
        self.assertEqual(result["status"], "success")
        self.assertEqual(result["records_inserted"], 1)
        self.assertEqual(result["records_retained_raw_only"], 0)
        with get_db_connection() as conn:
            stored = conn.execute(text("""
                SELECT payload::text
                FROM raw.raw_data
                WHERE raw_id = :raw_id;
            """), {"raw_id": result["raw_id"]}).scalar_one()
        self.assertIn(self.TEST_PREFIX + "payload", stored)
        self.assertIn("[REDACTED]", stored)
        self.assertNotIn("do-not-store-this", stored)
        with get_db_connection() as conn:
            conn.execute(text("""
                DELETE FROM raw.raw_data
                WHERE raw_id = :raw_id AND business_id = :business_id;
            """), {"raw_id": result["raw_id"], "business_id": self.B1})
            conn.execute(text("""
                DELETE FROM raw.sync_log
                WHERE business_id = :business_id
                  AND source = 'payment_manual'
                  AND sync_type = :sync_type;
            """), {"business_id": self.B1, "sync_type": sync_type})
            conn.commit()


if __name__ == "__main__":
    unittest.main()
