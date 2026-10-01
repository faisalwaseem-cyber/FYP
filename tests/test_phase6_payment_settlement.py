"""
BRIDGE FLOW — Phase 6: Payment & Settlement Data Foundation Tests.

Validates:
    1. Transaction parsing — Stripe API, PayFast CSV, JazzCash CSV, EasyPaisa CSV, COD remittance, Manual bank transfer
    2. Order matching — Shopify numeric ID, Daraz-prefixed ID, unmatched orders skipped gracefully
    3. Fee calculation — explicit fee, estimated rate per provider, Net Received = Amount - Fee
    4. Settlement matching — SettlementBatch aggregation (gross, fees, net payout), COD shipment reconciliation
    5. Duplicate prevention — Idempotent upserts on (business_id, external_payment_id)
    6. Tenant isolation — Business 1 payments and order maps never leak to Business 2

Tests only the specified scopes. No true profit ML or dashboard tests.
"""
import sys
import unittest
from datetime import datetime, timezone
from pathlib import Path

from sqlalchemy import text

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from config.database import get_db_connection
from connectors.payment import (
    CODReconciliationProvider,
    EasyPaisaPaymentProvider,
    JazzCashPaymentProvider,
    ManualPaymentProvider,
    PayFastPaymentProvider,
    PaymentRecord,
    SettlementBatch,
    StripePaymentProvider,
    get_payment_provider,
)
from etl.payment_transformer import PaymentTransformer
from loaders.payment_loader import PaymentLoader
from pipeline.run_payment_sync import run_payment_sync


class TestPhase6PaymentSettlement(unittest.TestCase):
    """Test suite for Payment & Settlement Foundation (Phase 6)."""

    B1 = 1
    B2 = 2

    ORDER_B1_SHOPIFY = "5589123456701_p6"
    ORDER_B1_DARAZ = "daraz_9900112201_p6"
    ORDER_B2_SHOPIFY = "5589123456702_p6"

    PAY_STRIPE = "ch_test_stripe_p6_001"
    PAY_PAYFAST = "PF-TXN-P6-002"
    PAY_JAZZCASH = "JC-TXN-P6-003"
    PAY_EASYPAISA = "EP-TXN-P6-004"
    PAY_COD = "COD-POSTEX-P6-005"
    PAY_MANUAL = "MAN-IBFT-P6-006"

    @classmethod
    def setUpClass(cls):
        """Ensure test businesses exist and clean up any leftover Phase 6 test records."""
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
            # Delete payments
            conn.execute(text("""
                DELETE FROM core.payment
                WHERE business_id IN (1, 2)
                  AND (
                      external_payment_id LIKE '%_p6%'
                      OR external_payment_id LIKE '%-P6-%'
                      OR order_id IN (
                          SELECT order_id FROM core.orders
                          WHERE business_id IN (1, 2)
                            AND external_order_id IN (:o1, :o2, :o3)
                      )
                  );
            """), {"o1": cls.ORDER_B1_SHOPIFY, "o2": cls.ORDER_B1_DARAZ, "o3": cls.ORDER_B2_SHOPIFY})

            # Delete orders
            conn.execute(text("""
                DELETE FROM core.orders
                WHERE business_id IN (1, 2)
                  AND external_order_id IN (:o1, :o2, :o3);
            """), {"o1": cls.ORDER_B1_SHOPIFY, "o2": cls.ORDER_B1_DARAZ, "o3": cls.ORDER_B2_SHOPIFY})

            conn.commit()

    def setUp(self):
        """Seed test customer and orders in core.orders for B1 and B2."""
        with get_db_connection() as conn:
            # Ensure customers exist
            conn.execute(text("""
                INSERT INTO core.customer (business_id, external_customer_id, full_name, phone, city)
                VALUES (1, 'cust_p6_b1', 'Customer P6 B1', '+923001234571', 'Lahore')
                ON CONFLICT (business_id, phone) DO NOTHING;
            """))
            conn.execute(text("""
                INSERT INTO core.customer (business_id, external_customer_id, full_name, phone, city)
                VALUES (2, 'cust_p6_b2', 'Customer P6 B2', '+923001234572', 'Karachi')
                ON CONFLICT (business_id, phone) DO NOTHING;
            """))

            # Seed B1 Shopify order
            conn.execute(text("""
                INSERT INTO core.orders (
                    business_id, customer_id, external_order_id, order_date, order_status,
                    payment_method, subtotal_amount, discount_amount, shipping_charged,
                    total_amount, currency, shipping_city, source_platform
                )
                SELECT 1, customer_id, :ext_id, now(), 'confirmed',
                       'card', 4500.00, 0.0, 200.0, 4700.00, 'PKR', 'Lahore', 'shopify'
                FROM core.customer WHERE business_id = 1 AND phone = '+923001234571'
                ON CONFLICT (business_id, external_order_id) DO NOTHING;
            """), {"ext_id": self.ORDER_B1_SHOPIFY})

            # Seed B1 Daraz order
            conn.execute(text("""
                INSERT INTO core.orders (
                    business_id, customer_id, external_order_id, order_date, order_status,
                    payment_method, subtotal_amount, discount_amount, shipping_charged,
                    total_amount, currency, shipping_city, source_platform
                )
                SELECT 1, customer_id, :ext_id, now(), 'confirmed',
                       'wallet', 3000.00, 0.0, 150.0, 3150.00, 'PKR', 'Karachi', 'daraz'
                FROM core.customer WHERE business_id = 1 AND phone = '+923001234571'
                ON CONFLICT (business_id, external_order_id) DO NOTHING;
            """), {"ext_id": self.ORDER_B1_DARAZ})

            # Seed B2 Shopify order
            conn.execute(text("""
                INSERT INTO core.orders (
                    business_id, customer_id, external_order_id, order_date, order_status,
                    payment_method, subtotal_amount, discount_amount, shipping_charged,
                    total_amount, currency, shipping_city, source_platform
                )
                SELECT 2, customer_id, :ext_id, now(), 'confirmed',
                       'card', 8000.00, 0.0, 300.0, 8300.00, 'PKR', 'Islamabad', 'shopify'
                FROM core.customer WHERE business_id = 2 AND phone = '+923001234572'
                ON CONFLICT (business_id, external_order_id) DO NOTHING;
            """), {"ext_id": self.ORDER_B2_SHOPIFY})

            conn.commit()

    # =========================================================================
    # 1. TRANSACTION PARSING
    # =========================================================================

    def test_parse_stripe_api_charge(self):
        """Stripe charge API response parses to canonical PaymentRecord."""
        provider = StripePaymentProvider()
        raw_charge = [{
            "id": self.PAY_STRIPE,
            "amount": 4700.0,
            "application_fee_amount": 136.30,
            "status": "succeeded",
            "created": 1705320000,
            "metadata": {"order_id": self.ORDER_B1_SHOPIFY},
            "payment_method_details": {"type": "card"},
        }]

        records = provider.parse_transactions(raw_charge)
        self.assertEqual(len(records), 1)
        r = records[0]
        self.assertEqual(r.external_payment_id, self.PAY_STRIPE)
        self.assertEqual(r.external_order_id, self.ORDER_B1_SHOPIFY)
        self.assertEqual(r.amount, 4700.0)
        self.assertEqual(r.gateway_fee, 136.30)
        self.assertEqual(r.net_amount, 4563.70)
        self.assertEqual(r.payment_method, "card")
        self.assertEqual(r.payment_status, "paid")

    def test_parse_payfast_settlement_csv(self):
        """PayFast Pakistan settlement CSV parses to PaymentRecord."""
        provider = PayFastPaymentProvider()
        csv_data = f"""transaction_id,order_id,datetime,type,amount,status,fee
{self.PAY_PAYFAST},{self.ORDER_B1_DARAZ},2026-10-01 14:00:00,payment,3150.00,paid,78.75
"""
        records = provider.parse_transactions(csv_data)
        self.assertEqual(len(records), 1)
        r = records[0]
        self.assertEqual(r.external_payment_id, self.PAY_PAYFAST)
        self.assertEqual(r.external_order_id, self.ORDER_B1_DARAZ)
        self.assertEqual(r.amount, 3150.0)
        self.assertEqual(r.gateway_fee, 78.75)
        self.assertEqual(r.net_amount, 3071.25)
        self.assertEqual(r.payment_method, "wallet")

    def test_parse_jazzcash_statement_csv(self):
        """JazzCash portal statement CSV parses with custom column headers."""
        provider = JazzCashPaymentProvider()
        csv_data = f"""Txn Ref No,Merchant Order ID,Txn Date Time,Txn Amount,Txn Status,Commission
{self.PAY_JAZZCASH},{self.ORDER_B1_DARAZ},2026-10-01 15:30:00,3150.00,paid,47.25
"""
        records = provider.parse_transactions(csv_data)
        self.assertEqual(len(records), 1)
        r = records[0]
        self.assertEqual(r.external_payment_id, self.PAY_JAZZCASH)
        self.assertEqual(r.external_order_id, self.ORDER_B1_DARAZ)
        self.assertEqual(r.amount, 3150.0)
        self.assertEqual(r.gateway_fee, 47.25)
        self.assertEqual(r.net_amount, 3102.75)

    def test_parse_easypaisa_statement_csv(self):
        """EasyPaisa merchant statement CSV parses cleanly."""
        provider = EasyPaisaPaymentProvider()
        csv_data = f"""Transaction ID,Order ID,Date & Time,Gross Amount,Status,Commission
{self.PAY_EASYPAISA},{self.ORDER_B1_DARAZ},2026-10-01 16:00:00,3150.00,paid,47.25
"""
        records = provider.parse_transactions(csv_data)
        self.assertEqual(len(records), 1)
        r = records[0]
        self.assertEqual(r.external_payment_id, self.PAY_EASYPAISA)
        self.assertEqual(r.external_order_id, self.ORDER_B1_DARAZ)
        self.assertEqual(r.amount, 3150.0)
        self.assertEqual(r.gateway_fee, 47.25)

    def test_parse_cod_courier_remittance(self):
        """COD courier settlement sheet parses COD amount and remittance fee."""
        provider = CODReconciliationProvider()
        csv_data = f"""tracking_number,order_id,cod_amount,courier_fee,settlement_date
POSTEX-P6-999,{self.ORDER_B1_SHOPIFY},4700.00,0.00,2026-10-01
"""
        records = provider.parse_transactions(csv_data)
        self.assertEqual(len(records), 1)
        r = records[0]
        self.assertEqual(r.external_payment_id, "COD-POSTEX-P6-999")
        self.assertEqual(r.external_order_id, self.ORDER_B1_SHOPIFY)
        self.assertEqual(r.amount, 4700.0)
        self.assertEqual(r.payment_method, "cod")
        self.assertEqual(r.net_amount, 4700.0)

    def test_parse_manual_bank_transfer_json(self):
        """Manual IBFT/Raast bank transfer JSON list parses correctly."""
        provider = ManualPaymentProvider()
        json_data = f"""[
            {{
                "external_payment_id": "{self.PAY_MANUAL}",
                "external_order_id": "{self.ORDER_B1_SHOPIFY}",
                "amount": "4700.00",
                "payment_method": "bank_transfer",
                "payment_status": "paid",
                "paid_at": "2026-10-01 12:00:00"
            }}
        ]"""
        records = provider.parse_transactions(json_data)
        self.assertEqual(len(records), 1)
        r = records[0]
        self.assertEqual(r.external_payment_id, self.PAY_MANUAL)
        self.assertEqual(r.external_order_id, self.ORDER_B1_SHOPIFY)
        self.assertEqual(r.payment_method, "bank_transfer")
        self.assertEqual(r.net_amount, 4700.0)

    # =========================================================================
    # 2. ORDER MATCHING
    # =========================================================================

    def test_order_matching_shopify_and_daraz(self):
        """Transformer correctly matches both Shopify and Daraz order IDs to internal order_id."""
        loader = PaymentLoader(self.B1)
        order_map = loader.get_order_id_map()

        records = [
            PaymentRecord(
                external_payment_id="PAY-MATCH-01",
                external_order_id=self.ORDER_B1_SHOPIFY,
                amount=4700.0,
                gateway_fee=136.30,
                payment_method="card",
            ),
            PaymentRecord(
                external_payment_id="PAY-MATCH-02",
                external_order_id=self.ORDER_B1_DARAZ,
                amount=3150.0,
                gateway_fee=47.25,
                payment_method="wallet",
            ),
        ]

        transformer = PaymentTransformer(business_id=self.B1)
        df = transformer.transform_payment_records(records, order_map)

        self.assertEqual(len(df), 2)
        matched_ext_ids = set(df["external_order_id"])
        self.assertIn(self.ORDER_B1_SHOPIFY, matched_ext_ids)
        self.assertIn(self.ORDER_B1_DARAZ, matched_ext_ids)
        self.assertTrue((df["order_id"] > 0).all())

    def test_order_matching_unmatched_order_gracefully_skipped(self):
        """Transactions for non-existent orders are skipped without raising errors."""
        loader = PaymentLoader(self.B1)
        order_map = loader.get_order_id_map()

        records = [
            PaymentRecord(
                external_payment_id="PAY-GHOST-01",
                external_order_id="GHOST_ORDER_99999",
                amount=5000.0,
            )
        ]

        transformer = PaymentTransformer(business_id=self.B1)
        df = transformer.transform_payment_records(records, order_map)
        self.assertTrue(df.empty)

    # =========================================================================
    # 3. FEE CALCULATION & NET RECEIVED
    # =========================================================================

    def test_fee_calculation_explicit_vs_estimated(self):
        """Explicit fee is preserved; missing fee is calculated via provider rate."""
        # 1. Stripe with explicit fee
        stripe_provider = StripePaymentProvider(default_fee_rate=0.029)
        self.assertEqual(stripe_provider.calculate_fee(1000.0), 29.0)

        # 2. PayFast ~2.5% MDR
        payfast_provider = PayFastPaymentProvider(default_fee_rate=0.025)
        self.assertEqual(payfast_provider.calculate_fee(2000.0), 50.0)

        # 3. JazzCash ~1.5% MDR
        jazzcash_provider = JazzCashPaymentProvider(default_fee_rate=0.015)
        self.assertEqual(jazzcash_provider.calculate_fee(2000.0), 30.0)

        # 4. Net calculation: Net = Amount - Fee
        record = PaymentRecord(
            external_order_id=self.ORDER_B1_SHOPIFY,
            amount=5000.0,
            gateway_fee=125.0,
        )
        self.assertEqual(record.net_amount, 4875.0)

        # 5. Refund calculation: Net = -Amount
        refund_record = PaymentRecord(
            external_order_id=self.ORDER_B1_SHOPIFY,
            amount=1500.0,
            gateway_fee=0.0,
            payment_type="refund",
        )
        self.assertEqual(refund_record.net_amount, -1500.0)

    # =========================================================================
    # 4. SETTLEMENT MATCHING & BATCHING
    # =========================================================================

    def test_settlement_batch_aggregation(self):
        """SettlementBatch aggregates gross amounts, total gateway fees, and net payout."""
        provider = PayFastPaymentProvider()
        csv_data = f"""transaction_id,order_id,amount,fee,status
PF-01,{self.ORDER_B1_SHOPIFY},4000.00,100.00,paid
PF-02,{self.ORDER_B1_DARAZ},2000.00,50.00,paid
"""
        batch: SettlementBatch = provider.parse_settlement_batch(
            raw_data=csv_data,
            settlement_id="SETTLE-PF-20261001",
            settlement_date="2026-10-01",
        )

        self.assertEqual(batch.settlement_id, "SETTLE-PF-20261001")
        self.assertEqual(batch.transaction_count, 2)
        self.assertEqual(batch.gross_amount, 6000.0)
        self.assertEqual(batch.total_fees, 150.0)
        self.assertEqual(batch.net_payout, 5850.0)

    def test_reconcile_delivered_cod_shipment(self):
        """COD reconciliation produces valid PaymentRecord from delivered shipment."""
        reconciler = CODReconciliationProvider()
        shipment_data = {
            "business_id": self.B1,
            "external_order_id": self.ORDER_B1_SHOPIFY,
            "tracking_number": "POSTEX-DELIVERED-001",
            "cod_amount": 4700.0,
            "delivered_at": "2026-10-01T17:00:00+05:00",
            "courier_name": "postex",
        }

        rec = reconciler.reconcile_delivered_shipment(shipment_data, courier_cod_fee=0.0)
        self.assertIsNotNone(rec)
        self.assertEqual(rec.external_payment_id, "COD-POSTEX-DELIVERED-001")
        self.assertEqual(rec.amount, 4700.0)
        self.assertEqual(rec.net_amount, 4700.0)
        self.assertEqual(rec.payment_method, "cod")

    # =========================================================================
    # 5. DUPLICATE PREVENTION & IDEMPOTENCY
    # =========================================================================

    def test_duplicate_prevention_on_replay(self):
        """Ingesting the same payment twice updates in-place without duplicate records."""
        payload = f"""transaction_id,order_id,amount,fee,status
{self.PAY_PAYFAST},{self.ORDER_B1_DARAZ},3150.00,78.75,paid
"""
        # Run 1
        res1 = run_payment_sync("payfast", payload, business_id=self.B1)
        self.assertEqual(res1["status"], "success")

        # Run 2 (Replay)
        res2 = run_payment_sync("payfast", payload, business_id=self.B1)
        self.assertEqual(res2["status"], "success")

        with get_db_connection() as conn:
            count = conn.execute(text("""
                SELECT count(*) FROM core.payment
                WHERE business_id = :b_id AND external_payment_id = :pay_id;
            """), {"b_id": self.B1, "pay_id": self.PAY_PAYFAST}).scalar()
            self.assertEqual(count, 1)

    # =========================================================================
    # 6. TENANT ISOLATION
    # =========================================================================

    def test_tenant_isolation_order_resolution_and_storage(self):
        """Payments for Business 1 are never resolved or stored against Business 2 orders."""
        loader_b1 = PaymentLoader(self.B1)
        loader_b2 = PaymentLoader(self.B2)

        order_map_b1 = loader_b1.get_order_id_map()
        order_map_b2 = loader_b2.get_order_id_map()

        # B1 order must only exist in B1 map
        self.assertIn(self.ORDER_B1_SHOPIFY, order_map_b1)
        self.assertNotIn(self.ORDER_B1_SHOPIFY, order_map_b2)

        # B2 order must only exist in B2 map
        self.assertIn(self.ORDER_B2_SHOPIFY, order_map_b2)
        self.assertNotIn(self.ORDER_B2_SHOPIFY, order_map_b1)

        # Ingesting B1 order into B2 runner must result in 0 inserted payments
        res_cross = run_payment_sync(
            provider_name="stripe",
            raw_payload=[{
                "id": "ch_cross_tenant_test",
                "amount": 4700.0,
                "metadata": {"order_id": self.ORDER_B1_SHOPIFY},
            }],
            business_id=self.B2,
        )
        self.assertEqual(res_cross["records_inserted"], 0)


if __name__ == "__main__":
    unittest.main(verbosity=2)

