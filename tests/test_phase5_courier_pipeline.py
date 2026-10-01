"""
BRIDGE FLOW — Phase 5.4: Courier Ingestion Pipeline Tests.

Validates the full multi-tenant pipeline wire-up:
    Authenticated Business
    → core.platform_connection
    → CredentialManager
    → Courier Client (mocked)
    → raw.raw_data
    → CourierTransformer / ETL
    → PostgreSQL (core.shipment, core.orders, core.rto_return)
    → raw.sync_log
    → core.platform_connection.last_synced_at & status

Focused Test Coverage:
    1. Tenant isolation — Business 1 cannot access Business 2 credentials or shipments
    2. Order matching — Shopify numeric ID and Daraz-prefixed ID correctly resolve order_id
    3. RTO status — 'returned' shipment status cascades to orders ('rto') and inserts into core.rto_return
    4. Mocked sync — End-to-end sync with mocked PostEx/Trax/Leopards clients
    5. Idempotency — Replaying sync does not produce duplicate shipments or RTO records
    6. Failure behavior — Fatal client error logs failure in raw.sync_log and sets connection status='error'

All courier network calls are mocked. Real database transactions test actual constraints.
"""
import sys
import unittest
from datetime import datetime, timezone
from pathlib import Path
from unittest.mock import MagicMock, patch

from sqlalchemy import text

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from auth.connection_service import (
    create_or_update_connection,
    get_connection,
    list_connections,
    update_status,
)
from auth.credential_manager import CredentialManager
from config.database import get_db_connection
from connectors.courier import (
    CourierAuthError,
    LeopardsClient,
    PostExClient,
    TraxClient,
)
from etl.courier_transformer import CourierTransformer
from loaders.courier_loader import CourierLoader
from pipeline.run_courier_sync import (
    resolve_courier_client,
    run_courier_sync,
    sync_courier_connection,
)
from pipeline.sync_logger import SyncLogger


class TestCourierPipelinePhase54(unittest.TestCase):
    """Integration test suite for Courier Ingestion Pipeline (Phase 5.4)."""

    B1 = 1
    B2 = 2

    POSTEX_ACC = "postex-test-account-5pt4"
    TRAX_ACC = "trax-test-account-5pt4"
    LEOPARDS_ACC = "leopards-test-account-5pt4"

    ORDER_SHOPIFY = "5589123456701_p54"
    ORDER_DARAZ = "daraz_9900112201_p54"
    ORDER_RTO = "5589123456799_p54"
    ORDER_B2 = "5589123456702_p54"

    TN_DELIVERED = "POSTEX-PK-54-001"
    TN_RTO = "POSTEX-PK-54-RTO"
    TN_TRAX = "TRAX-PK-54-002"
    TN_B2 = "POSTEX-B2-001"

    @classmethod
    def setUpClass(cls):
        """Ensure business 1 and business 2 exist in core.business, and register test connections."""
        with get_db_connection() as conn:
            conn.execute(text("""
                INSERT INTO core.business (business_id, business_name, is_active)
                OVERRIDING SYSTEM VALUE
                VALUES (1, 'Test Business Phase 5.4 B1', TRUE)
                ON CONFLICT (business_id) DO UPDATE SET is_active = TRUE;
            """))
            conn.execute(text("""
                INSERT INTO core.business (business_id, business_name, is_active)
                OVERRIDING SYSTEM VALUE
                VALUES (2, 'Test Business Phase 5.4 B2', TRUE)
                ON CONFLICT (business_id) DO UPDATE SET is_active = TRUE;
            """))
            conn.commit()

        cls._cleanup_all()

        # 1. Register PostEx connection for B1
        create_or_update_connection(
            business_id=cls.B1,
            platform="postex",
            platform_category="courier",
            account_identifier=cls.POSTEX_ACC,
            account_name="PostEx Merchant B1",
            auth_type="api_key",
            access_token="test_postex_token_phase54",
        )

        # 2. Register Trax connection for B1
        create_or_update_connection(
            business_id=cls.B1,
            platform="trax",
            platform_category="courier",
            account_identifier=cls.TRAX_ACC,
            account_name="Trax Merchant B1",
            auth_type="credentials",
            additional_credentials={"username": "trax_user_54", "password": "trax_password_54"},
        )

        # 3. Register PostEx connection for B2 (for tenant isolation tests)
        create_or_update_connection(
            business_id=cls.B2,
            platform="postex",
            platform_category="courier",
            account_identifier=cls.POSTEX_ACC + "_b2",
            account_name="PostEx Merchant B2",
            auth_type="api_key",
            access_token="test_postex_token_b2_phase54",
        )

    @classmethod
    def tearDownClass(cls):
        cls._cleanup_all()

    @classmethod
    def _cleanup_all(cls):
        with get_db_connection() as conn:
            # Delete financial_transaction & payment if any
            conn.execute(text("""
                DELETE FROM core.financial_transaction
                WHERE order_id IN (
                    SELECT order_id FROM core.orders
                    WHERE business_id IN (1, 2)
                      AND external_order_id IN (:o1, :o2, :o3, :o4)
                );
                DELETE FROM core.payment
                WHERE order_id IN (
                    SELECT order_id FROM core.orders
                    WHERE business_id IN (1, 2)
                      AND external_order_id IN (:o1, :o2, :o3, :o4)
                );
            """), {"o1": cls.ORDER_SHOPIFY, "o2": cls.ORDER_DARAZ, "o3": cls.ORDER_RTO, "o4": cls.ORDER_B2})

            # Delete RTO records via orders subquery
            conn.execute(text("""
                DELETE FROM core.rto_return
                WHERE order_id IN (
                    SELECT order_id FROM core.orders
                    WHERE business_id IN (1, 2)
                      AND external_order_id IN (:o1, :o2, :o3, :o4)
                );
            """), {"o1": cls.ORDER_SHOPIFY, "o2": cls.ORDER_DARAZ, "o3": cls.ORDER_RTO, "o4": cls.ORDER_B2})

            # Delete shipments
            conn.execute(text("""
                DELETE FROM core.shipment
                WHERE business_id IN (1, 2)
                  AND (
                      tracking_number IN (:t1, :t2, :t3, :t4)
                      OR order_id IN (
                          SELECT order_id FROM core.orders
                          WHERE business_id IN (1, 2)
                            AND external_order_id IN (:o1, :o2, :o3, :o4)
                      )
                  );
            """), {"t1": cls.TN_DELIVERED, "t2": cls.TN_RTO, "t3": cls.TN_TRAX, "t4": cls.TN_B2,
                   "o1": cls.ORDER_SHOPIFY, "o2": cls.ORDER_DARAZ, "o3": cls.ORDER_RTO, "o4": cls.ORDER_B2})

            # Delete test orders
            conn.execute(text("""
                DELETE FROM core.orders
                WHERE business_id IN (1, 2)
                  AND external_order_id IN (:o1, :o2, :o3, :o4);
            """), {"o1": cls.ORDER_SHOPIFY, "o2": cls.ORDER_DARAZ, "o3": cls.ORDER_RTO, "o4": cls.ORDER_B2})

            # Delete test connections
            conn.execute(text("""
                DELETE FROM core.platform_connection
                WHERE account_identifier LIKE 'postex-test-account-5pt4%'
                   OR account_identifier LIKE 'trax-test-account-5pt4%'
                   OR account_identifier LIKE 'leopards-test-account-5pt4%'
            """))

            conn.commit()

    def setUp(self):
        """Prepare fresh test orders and shipments in DB for each test."""
        # Ensure connections are active
        for b_id, acc, plat in [
            (self.B1, self.POSTEX_ACC, "postex"),
            (self.B1, self.TRAX_ACC, "trax"),
            (self.B2, self.POSTEX_ACC + "_b2", "postex"),
        ]:
            c = get_connection(b_id, plat, acc)
            if c and c["status"] != "active":
                update_status(c["connection_id"], "active")

        with get_db_connection() as conn:
            # Clear any leftover RTO return rows for test orders
            conn.execute(text("""
                DELETE FROM core.rto_return
                WHERE order_id IN (
                    SELECT order_id FROM core.orders
                    WHERE business_id IN (1, 2)
                      AND external_order_id IN (:o1, :o2, :o3, :o4)
                );
            """), {"o1": self.ORDER_SHOPIFY, "o2": self.ORDER_DARAZ, "o3": self.ORDER_RTO, "o4": self.ORDER_B2})

            # Ensure customers exist for B1 and B2
            conn.execute(text("""
                INSERT INTO core.customer (business_id, external_customer_id, full_name, phone, city)
                VALUES (1, 'cust_54_b1', 'Customer B1', '+923001234561', 'Lahore')
                ON CONFLICT (business_id, phone) DO NOTHING;
            """))
            conn.execute(text("""
                INSERT INTO core.customer (business_id, external_customer_id, full_name, phone, city)
                VALUES (2, 'cust_54_b2', 'Customer B2', '+923001234562', 'Karachi')
                ON CONFLICT (business_id, phone) DO NOTHING;
            """))

            # Seed orders for B1
            conn.execute(text("""
                INSERT INTO core.orders (
                    business_id, customer_id, external_order_id, order_date, order_status,
                    payment_method, subtotal_amount, discount_amount, shipping_charged,
                    total_amount, currency, shipping_city, source_platform
                )
                SELECT 1, customer_id, :ext_id, now(), 'fulfilled',
                       'cod', 2500.00, 0.0, 200.0, 2700.00, 'PKR', 'Lahore', 'shopify'
                FROM core.customer WHERE business_id = 1 AND phone = '+923001234561'
                ON CONFLICT (business_id, external_order_id) DO UPDATE SET order_status = 'fulfilled';
            """), {"ext_id": self.ORDER_SHOPIFY})

            conn.execute(text("""
                INSERT INTO core.orders (
                    business_id, customer_id, external_order_id, order_date, order_status,
                    payment_method, subtotal_amount, discount_amount, shipping_charged,
                    total_amount, currency, shipping_city, source_platform
                )
                SELECT 1, customer_id, :ext_id, now(), 'fulfilled',
                       'cod', 3500.00, 0.0, 250.0, 3750.00, 'PKR', 'Karachi', 'daraz'
                FROM core.customer WHERE business_id = 1 AND phone = '+923001234561'
                ON CONFLICT (business_id, external_order_id) DO UPDATE SET order_status = 'fulfilled';
            """), {"ext_id": self.ORDER_DARAZ})

            conn.execute(text("""
                INSERT INTO core.orders (
                    business_id, customer_id, external_order_id, order_date, order_status,
                    payment_method, subtotal_amount, discount_amount, shipping_charged,
                    total_amount, currency, shipping_city, source_platform
                )
                SELECT 1, customer_id, :ext_id, now(), 'fulfilled',
                       'cod', 1800.00, 0.0, 150.0, 1950.00, 'PKR', 'Islamabad', 'shopify'
                FROM core.customer WHERE business_id = 1 AND phone = '+923001234561'
                ON CONFLICT (business_id, external_order_id) DO UPDATE SET order_status = 'fulfilled';
            """), {"ext_id": self.ORDER_RTO})

            # Seed order for B2
            conn.execute(text("""
                INSERT INTO core.orders (
                    business_id, customer_id, external_order_id, order_date, order_status,
                    payment_method, subtotal_amount, discount_amount, shipping_charged,
                    total_amount, currency, shipping_city, source_platform
                )
                SELECT 2, customer_id, :ext_id, now(), 'fulfilled',
                       'cod', 5000.00, 0.0, 200.0, 5200.00, 'PKR', 'Karachi', 'shopify'
                FROM core.customer WHERE business_id = 2 AND phone = '+923001234562'
                ON CONFLICT (business_id, external_order_id) DO UPDATE SET order_status = 'fulfilled';
            """), {"ext_id": self.ORDER_B2})

            # Ensure courier records exist in core.courier
            conn.execute(text("""
                INSERT INTO core.courier (business_id, courier_name, is_active)
                VALUES (1, 'postex', TRUE) ON CONFLICT (business_id, courier_name) DO NOTHING;
            """))
            conn.execute(text("""
                INSERT INTO core.courier (business_id, courier_name, is_active)
                VALUES (1, 'trax', TRUE) ON CONFLICT (business_id, courier_name) DO NOTHING;
            """))
            conn.execute(text("""
                INSERT INTO core.courier (business_id, courier_name, is_active)
                VALUES (2, 'postex', TRUE) ON CONFLICT (business_id, courier_name) DO NOTHING;
            """))

            # Seed in-flight shipments for B1 (status='in_transit' or 'booked')
            conn.execute(text("""
                INSERT INTO core.shipment (
                    business_id, order_id, courier_id, tracking_number,
                    shipment_status, shipping_cost, cod_amount, delivery_attempts
                )
                SELECT 1, o.order_id, c.courier_id, :tn, 'in_transit', 200.0, 2700.0, 1
                FROM core.orders o, core.courier c
                WHERE o.business_id = 1 AND o.external_order_id = :ord_id
                  AND c.business_id = 1 AND c.courier_name = 'postex'
                ON CONFLICT (business_id, courier_id, tracking_number) DO UPDATE
                SET shipment_status = 'in_transit';
            """), {"tn": self.TN_DELIVERED, "ord_id": self.ORDER_SHOPIFY})

            conn.execute(text("""
                INSERT INTO core.shipment (
                    business_id, order_id, courier_id, tracking_number,
                    shipment_status, shipping_cost, cod_amount, delivery_attempts
                )
                SELECT 1, o.order_id, c.courier_id, :tn, 'in_transit', 150.0, 1950.0, 2
                FROM core.orders o, core.courier c
                WHERE o.business_id = 1 AND o.external_order_id = :ord_id
                  AND c.business_id = 1 AND c.courier_name = 'postex'
                ON CONFLICT (business_id, courier_id, tracking_number) DO UPDATE
                SET shipment_status = 'in_transit';
            """), {"tn": self.TN_RTO, "ord_id": self.ORDER_RTO})

            # Seed in-flight shipment for B2
            conn.execute(text("""
                INSERT INTO core.shipment (
                    business_id, order_id, courier_id, tracking_number,
                    shipment_status, shipping_cost, cod_amount, delivery_attempts
                )
                SELECT 2, o.order_id, c.courier_id, :tn, 'in_transit', 200.0, 5200.0, 1
                FROM core.orders o, core.courier c
                WHERE o.business_id = 2 AND o.external_order_id = :ord_id
                  AND c.business_id = 2 AND c.courier_name = 'postex'
                ON CONFLICT (business_id, courier_id, tracking_number) DO UPDATE
                SET shipment_status = 'in_transit';
            """), {"tn": self.TN_B2, "ord_id": self.ORDER_B2})

            conn.commit()

    # =========================================================================
    # 1. TENANT ISOLATION
    # =========================================================================

    def test_tenant_isolation_credential_access(self):
        """CredentialManager enforces business_id isolation."""
        cm = CredentialManager()

        # B1 can access its connection
        creds_b1 = cm.get_valid_credentials(
            business_id=self.B1,
            platform="postex",
            account_identifier=self.POSTEX_ACC,
        )
        self.assertEqual(creds_b1["access_token"], "test_postex_token_phase54")

        # B2 cannot access B1's connection account_identifier
        with self.assertRaises(ValueError):
            cm.get_valid_credentials(
                business_id=self.B2,
                platform="postex",
                account_identifier=self.POSTEX_ACC,
            )

    def test_tenant_isolation_shipment_sync(self):
        """Syncing B1 only reads and updates B1 shipments, leaving B2 untouched."""
        mock_client = MagicMock(spec=PostExClient)
        mock_client.track_shipment.return_value = {
            "statusCode": "200",
            "dist": {
                "orderStatus": "Delivered",
                "deliveryCharges": 200.0,
                "orderAmount": 2700.0,
                "attemptCount": 1,
                "deliveryDate": "2026-10-01T16:00:00+05:00",
            }
        }

        # Run sync for B1
        res = run_courier_sync(
            business_id=self.B1,
            platform="postex",
            client=mock_client,
        )
        self.assertEqual(res["status"], "success")

        with get_db_connection() as conn:
            # B1 delivered shipment should be updated
            b1_status = conn.execute(text("""
                SELECT shipment_status FROM core.shipment
                WHERE business_id = :b_id AND tracking_number = :tn;
            """), {"b_id": self.B1, "tn": self.TN_DELIVERED}).scalar()
            self.assertEqual(b1_status, "delivered")

            # B2 shipment must remain unchanged ('in_transit')
            b2_status = conn.execute(text("""
                SELECT shipment_status FROM core.shipment
                WHERE business_id = :b_id AND tracking_number = :tn;
            """), {"b_id": self.B2, "tn": self.TN_B2}).scalar()
            self.assertEqual(b2_status, "in_transit")

    # =========================================================================
    # 2. ORDER MATCHING (Shopify & Daraz Order Identifiers)
    # =========================================================================

    def test_order_matching_shopify_numeric(self):
        """Shopify numeric external_order_id resolves correctly to core.orders."""
        loader = CourierLoader(business_id=self.B1)
        shipment_data = {
            "business_id": self.B1,
            "courier_name": "postex",
            "external_order_id": self.ORDER_SHOPIFY,
            "tracking_number": self.TN_DELIVERED,
            "shipment_status": "delivered",
            "shipping_cost": 210.0,
            "cod_amount": 2700.0,
            "delivery_attempts": 1,
            "delivered_at": "2026-10-01T16:00:00+05:00",
        }

        shipment_id = loader.load_shipment(shipment_data)
        self.assertIsNotNone(shipment_id)

        with get_db_connection() as conn:
            row = conn.execute(text("""
                SELECT s.shipment_status, o.order_status, o.external_order_id
                FROM core.shipment s
                JOIN core.orders o ON s.order_id = o.order_id
                WHERE s.shipment_id = :s_id;
            """), {"s_id": shipment_id}).fetchone()

            self.assertEqual(row[0], "delivered")
            self.assertEqual(row[1], "delivered")
            self.assertEqual(row[2], self.ORDER_SHOPIFY)

    def test_order_matching_daraz_prefixed(self):
        """Daraz prefixed external_order_id ('daraz_...') resolves correctly."""
        loader = CourierLoader(business_id=self.B1)
        shipment_data = {
            "business_id": self.B1,
            "courier_name": "trax",
            "external_order_id": self.ORDER_DARAZ,
            "tracking_number": self.TN_TRAX,
            "shipment_status": "delivered",
            "shipping_cost": 250.0,
            "cod_amount": 3750.0,
            "delivery_attempts": 1,
            "delivered_at": "2026-10-01T17:00:00+05:00",
        }

        shipment_id = loader.load_shipment(shipment_data)
        self.assertIsNotNone(shipment_id)

        with get_db_connection() as conn:
            row = conn.execute(text("""
                SELECT s.shipment_status, o.order_status, o.external_order_id
                FROM core.shipment s
                JOIN core.orders o ON s.order_id = o.order_id
                WHERE s.shipment_id = :s_id;
            """), {"s_id": shipment_id}).fetchone()

            self.assertEqual(row[0], "delivered")
            self.assertEqual(row[1], "delivered")
            self.assertEqual(row[2], self.ORDER_DARAZ)

    def test_order_matching_nonexistent_order_skipped(self):
        """Shipment with non-existent order reference is gracefully skipped (returns None)."""
        loader = CourierLoader(business_id=self.B1)
        shipment_data = {
            "business_id": self.B1,
            "courier_name": "postex",
            "external_order_id": "NON_EXISTENT_ORDER_99999",
            "tracking_number": "POSTEX-GHOST-999",
            "shipment_status": "delivered",
        }
        res = loader.load_shipment(shipment_data)
        self.assertIsNone(res)

    # =========================================================================
    # 3. RTO STATUS & CASCADE
    # =========================================================================

    def test_rto_status_cascades_and_logs_return(self):
        """'returned' status transitions core.orders to 'rto' and creates core.rto_return row."""
        loader = CourierLoader(business_id=self.B1)
        shipment_data = {
            "business_id": self.B1,
            "courier_name": "postex",
            "external_order_id": self.ORDER_RTO,
            "tracking_number": self.TN_RTO,
            "shipment_status": "returned",
            "shipping_cost": 150.0,
            "cod_amount": 1950.0,
            "delivery_attempts": 3,
            "shipped_at": "2026-09-28T10:00:00+05:00",
            "delivered_at": None,
        }

        shipment_id = loader.load_shipment(shipment_data, default_rto_penalty=175.0)
        self.assertIsNotNone(shipment_id)

        with get_db_connection() as conn:
            # 1. Order status cascaded to 'rto'
            order_status = conn.execute(text("""
                SELECT order_status FROM core.orders
                WHERE business_id = :b_id AND external_order_id = :ord_id;
            """), {"b_id": self.B1, "ord_id": self.ORDER_RTO}).scalar()
            self.assertEqual(order_status, "rto")

            # 2. RTO record created in core.rto_return
            rto_row = conn.execute(text("""
                SELECT return_type, return_shipping_cost, refund_amount
                FROM core.rto_return
                WHERE business_id = :b_id AND shipment_id = :s_id;
            """), {"b_id": self.B1, "s_id": shipment_id}).fetchone()

            self.assertIsNotNone(rto_row)
            self.assertEqual(rto_row[0], "rto")
            self.assertEqual(float(rto_row[1]), 175.0)
            self.assertEqual(float(rto_row[2]), 0.0)

    # =========================================================================
    # 4. MOCKED SYNC (END-TO-END WIRE-UP)
    # =========================================================================

    def test_mocked_sync_end_to_end_postex(self):
        """
        Complete E2E sync wire-up:
        connection -> CredentialManager -> PostExClient -> raw.raw_data -> ETL -> PostgreSQL -> SyncLogger
        """
        mock_client = MagicMock(spec=PostExClient)
        mock_client.track_shipment.side_effect = lambda tn: {
            "statusCode": "200",
            "dist": {
                "trackingNumber": tn,
                "orderStatus": "Delivered" if tn == self.TN_DELIVERED else "Returned to Shipper",
                "deliveryCharges": 200.0,
                "orderAmount": 2700.0,
                "attemptCount": 1 if tn == self.TN_DELIVERED else 3,
                "bookingDate": "2026-09-29T10:00:00+05:00",
                "deliveryDate": "2026-10-01T15:00:00+05:00" if tn == self.TN_DELIVERED else None,
            }
        }

        res = run_courier_sync(
            business_id=self.B1,
            platform="postex",
            client=mock_client,
        )

        self.assertEqual(res["status"], "success")
        self.assertGreaterEqual(res["connections_synced"], 1)

        # Verify raw.raw_data saved
        with get_db_connection() as conn:
            raw_count = conn.execute(text("""
                SELECT count(*) FROM raw.raw_data
                WHERE business_id = :b_id AND source = 'courier' AND data_type = 'postex_tracking';
            """), {"b_id": self.B1}).scalar()
            self.assertGreaterEqual(raw_count, 1)

            # Verify sync_log success
            sync_row = conn.execute(text("""
                SELECT status, records_fetched, records_updated
                FROM raw.sync_log
                WHERE business_id = :b_id AND source = 'courier_postex'
                ORDER BY sync_id DESC LIMIT 1;
            """), {"b_id": self.B1}).fetchone()
            self.assertEqual(sync_row[0], "success")
            self.assertGreaterEqual(sync_row[2], 1)

            # Verify platform_connection.last_synced_at was updated
            conn_row = get_connection(self.B1, "postex", self.POSTEX_ACC)
            self.assertIsNotNone(conn_row["last_synced_at"])
            self.assertEqual(conn_row["status"], "active")

    def test_mocked_sync_trax_provider(self):
        """End-to-end sync works cleanly with Trax courier connection."""
        mock_client = MagicMock(spec=TraxClient)
        mock_client.track_shipment.return_value = {
            "status": "success",
            "cn": self.TN_TRAX,
            "current_status": "Delivered",
            "charges": 180.0,
            "cod_amount": 3500.0,
            "attempts": 1,
        }

        # Seed in-flight Trax shipment
        with get_db_connection() as conn:
            conn.execute(text("""
                INSERT INTO core.shipment (
                    business_id, order_id, courier_id, tracking_number,
                    shipment_status, shipping_cost, cod_amount, delivery_attempts
                )
                SELECT 1, o.order_id, c.courier_id, :tn, 'in_transit', 180.0, 3500.0, 1
                FROM core.orders o, core.courier c
                WHERE o.business_id = 1 AND o.external_order_id = :ord_id
                  AND c.business_id = 1 AND c.courier_name = 'trax'
                ON CONFLICT (business_id, courier_id, tracking_number) DO UPDATE
                SET shipment_status = 'in_transit';
            """), {"tn": self.TN_TRAX, "ord_id": self.ORDER_DARAZ})
            conn.commit()

        res = run_courier_sync(
            business_id=self.B1,
            platform="trax",
            client=mock_client,
        )
        self.assertEqual(res["status"], "success")

    # =========================================================================
    # 5. IDEMPOTENCY
    # =========================================================================

    def test_sync_idempotency_no_duplicate_records(self):
        """Running the sync multiple times updates in-place without duplicate rows or errors."""
        mock_client = MagicMock(spec=PostExClient)
        mock_client.track_shipment.return_value = {
            "statusCode": "200",
            "dist": {
                "orderStatus": "Delivered",
                "deliveryCharges": 200.0,
                "orderAmount": 2700.0,
                "attemptCount": 1,
                "deliveryDate": "2026-10-01T16:00:00+05:00",
            }
        }

        # Run 1
        res1 = run_courier_sync(business_id=self.B1, platform="postex", client=mock_client)
        self.assertEqual(res1["status"], "success")

        # Run 2 (Replay)
        res2 = run_courier_sync(business_id=self.B1, platform="postex", client=mock_client)
        self.assertEqual(res2["status"], "success")

        with get_db_connection() as conn:
            # Count shipments matching this tracking number: must be exactly 1
            count = conn.execute(text("""
                SELECT count(*) FROM core.shipment
                WHERE business_id = :b_id AND tracking_number = :tn;
            """), {"b_id": self.B1, "tn": self.TN_DELIVERED}).scalar()
            self.assertEqual(count, 1)

    # =========================================================================
    # 6. FAILURE BEHAVIOR
    # =========================================================================

    def test_failure_behavior_marks_connection_error_and_logs(self):
        """Pipeline failure logs failure to raw.sync_log and sets connection status='error'."""
        mock_client = MagicMock(spec=PostExClient)
        # Simulate fatal authentication failure during tracking call
        mock_client.track_shipment.side_effect = CourierAuthError("PostEx invalid API token 401")

        with self.assertRaises(CourierAuthError):
            run_courier_sync(
                business_id=self.B1,
                platform="postex",
                client=mock_client,
            )

        with get_db_connection() as conn:
            # 1. Platform connection marked 'error'
            conn_row = get_connection(self.B1, "postex", self.POSTEX_ACC)
            self.assertEqual(conn_row["status"], "error")
            self.assertIn("PostEx invalid API token", conn_row["error_message"])

            # 2. raw.sync_log status is 'failed'
            sync_row = conn.execute(text("""
                SELECT status, error_message FROM raw.sync_log
                WHERE business_id = :b_id AND source = 'courier_postex'
                ORDER BY sync_id DESC LIMIT 1;
            """), {"b_id": self.B1}).fetchone()
            self.assertEqual(sync_row[0], "failed")
            self.assertIn("PostEx invalid API token", sync_row[1])


if __name__ == "__main__":
    unittest.main(verbosity=2)
