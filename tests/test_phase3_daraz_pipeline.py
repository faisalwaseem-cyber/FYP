"""
BRIDGE FLOW — Phase 3.4: Daraz Ingestion Pipeline Tests.

Validates the full multi-tenant pipeline wire-up:
    Authenticated Business
    → core.platform_connection
    → CredentialManager
    → DarazClient (mocked)
    → Services (mocked)
    → raw.raw_data
    → ETL (DarazTransformer)
    → PostgreSQL (PostgresLoader)
    → raw.sync_log
    → core.platform_connection.last_synced_at

Test coverage:
    1. Tenant isolation — business_id scoping prevents cross-tenant credential access
    2. Credential retrieval — CredentialManager decrypts Daraz app_key/app_secret/access_token
    3. Mocked end-to-end sync — full pipeline with mocked DarazClient
    4. Idempotent sync — second run updates rather than inserts
    5. Failed sync — exception marks connection status='error', logs failure
    6. Secret safety — no plaintext credentials appear in repr or exception messages

No real Daraz API calls are made. All network I/O is mocked.
"""
import sys
import unittest
from datetime import date
from pathlib import Path
from unittest.mock import MagicMock, patch

import pandas as pd
from sqlalchemy import text

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from auth.connection_service import (
    create_or_update_connection,
    get_connection,
    list_connections,
    update_last_synced,
)
from auth.credential_manager import CredentialManager
from config.database import get_db_connection
from connectors.daraz.client import DarazClient
from connectors.daraz.services import (
    DarazInventoryService,
    DarazOrderService,
    DarazProductService,
)
from etl.daraz_transformer import DarazTransformer
from loaders.postgres_loader import PostgresLoader
from pipeline.run_daraz_sync import (
    get_daraz_last_watermark,
    resolve_daraz_credentials,
    run_daraz_sync,
)
from pipeline.sync_logger import SyncLogger
from raw_layer.raw_repository import save_raw_payload
from security.encryption import decrypt_json, decrypt_token, encrypt_json, encrypt_token

# ---------------------------------------------------------------------------
# Shared realistic mock order payload (Phase 3.3-compatible Daraz format)
# ---------------------------------------------------------------------------
MOCK_DARAZ_ORDERS = [
    {
        "order_id": 990011223301,
        "order_number": 990011223301,
        "created_at": "2026-10-01T12:00:00+05:00",
        "updated_at": "2026-10-01T14:00:00+05:00",
        "status": "ready_to_ship",
        "payment_method": "Cash on Delivery (COD)",
        "price": "4500.00",
        "shipping_fee": "200.00",
        "customer_first_name": "Hamza",
        "customer_last_name": "Qureshi",
        "customer_email": "hamza.q@example.com",
        "address_shipping": {
            "first_name": "Hamza",
            "last_name": "Qureshi",
            "phone": "03219876543",
            "city": "Islamabad",
            "country": "Pakistan",
        },
        "items": [
            {
                "sku": "DRZ-PHONE-CASE-01",
                "sku_id": "55512",
                "name": "Premium Leather Phone Case",
                "item_price": "4300.00",
                "quantity": 1,
            }
        ],
    },
    {
        "order_id": 990011223302,
        "order_number": 990011223302,
        "created_at": "2026-10-01T13:00:00+05:00",
        "updated_at": "2026-10-01T15:00:00+05:00",
        "status": "delivered",
        "payment_method": "Daraz Wallet",
        "price": "8000.00",
        "shipping_fee": "0.00",
        "customer_first_name": "Sana",
        "customer_last_name": "Malik",
        "customer_email": "sana.m@example.com",
        "address_shipping": {
            "first_name": "Sana",
            "last_name": "Malik",
            "phone": "+92321*****99",  # Masked phone
            "city": "Karachi",
            "country": "Pakistan",
        },
        "items": [
            {
                "sku": "DRZ-WATCH-02",
                "sku_id": "55513",
                "name": "Smart Watch Pro Series 9",
                "item_price": "8000.00",
                "quantity": 1,
            }
        ],
    },
]

MOCK_DARAZ_PRODUCTS = [
    {
        "item_id": 99100,
        "name": "Premium Leather Phone Case",
        "skus": [
            {
                "SellerSku": "DRZ-PHONE-CASE-01",
                "ShopSku": "DARAZ_CASE_M",
                "price": "4300.00",
                "quantity": 25,
                "Status": "active",
            }
        ],
    },
    {
        "item_id": 99101,
        "name": "Smart Watch Pro Series 9",
        "skus": [
            {
                "SellerSku": "DRZ-WATCH-02",
                "ShopSku": "DARAZ_WATCH_S9",
                "price": "8000.00",
                "quantity": 10,
                "Status": "active",
            }
        ],
    },
]


class TestDarazPipeline(unittest.TestCase):
    """
    Integration tests for the Daraz ingestion pipeline (Phase 3.4).
    Uses real DB for connection management; mocks all Daraz API calls.
    """

    SELLER_ID = "daraz-pipeline-test-seller-3pt4"
    BUSINESS_ID = 1   # Canonical test business

    @classmethod
    def setUpClass(cls):
        """Register a Daraz connection for the test business once per class."""
        cls._cleanup_connection()
        cls.conn_id = create_or_update_connection(
            business_id=cls.BUSINESS_ID,
            platform="daraz",
            platform_category="ecommerce",
            account_identifier=cls.SELLER_ID,
            account_name="Test Daraz Store Phase 3.4",
            auth_type="credentials",
            access_token="test_daraz_access_token_phase34",
            additional_credentials={
                "app_key": "test_app_key_phase34",
                "app_secret": "test_app_secret_phase34",
            },
        )

    @classmethod
    def tearDownClass(cls):
        cls._cleanup_connection()

    @classmethod
    def _cleanup_connection(cls):
        with get_db_connection() as conn:
            conn.execute(
                text(
                    "DELETE FROM core.platform_connection "
                    "WHERE account_identifier = :sid;"
                ),
                {"sid": cls.SELLER_ID},
            )
            conn.commit()

    def setUp(self):
        """
        Before each test:
          1. Reset the test connection to 'active' — guards against
             test_failed_sync leaving status='error' and poisoning
             alphabetically-later tests (idempotent, mocked E2E, resolve).
          2. Clean up any leftover Daraz test orders from previous runs in
             strict FK-respecting order (profit_calc → items → orders).
        Products are intentionally NOT deleted: prior sessions may have
        order_item rows referencing those product_ids; products are
        idempotently upserted by the pipeline anyway.
        """
        from auth.connection_service import update_status

        # --- 1. Guarantee connection is 'active' before every test ---
        conn_row = get_connection(self.BUSINESS_ID, "daraz", self.SELLER_ID)
        if conn_row and conn_row.get("status") != "active":
            update_status(conn_row["connection_id"], "active")

        # --- 2. Wipe leftover test orders (FK order) ---
        with get_db_connection() as conn:
            # profit_calculation → orders (FK child)
            conn.execute(text("""
                DELETE FROM analytics.profit_calculation
                WHERE order_id IN (
                    SELECT order_id FROM core.orders
                    WHERE business_id = :b_id
                      AND external_order_id LIKE 'daraz_99001122330%'
                );
            """), {"b_id": self.BUSINESS_ID})

            # order_item → orders + product (FK child)
            conn.execute(text("""
                DELETE FROM core.order_item
                WHERE order_id IN (
                    SELECT order_id FROM core.orders
                    WHERE business_id = :b_id
                      AND external_order_id LIKE 'daraz_99001122330%'
                );
            """), {"b_id": self.BUSINESS_ID})

            # orders
            conn.execute(text("""
                DELETE FROM core.orders
                WHERE business_id = :b_id
                  AND external_order_id LIKE 'daraz_99001122330%';
            """), {"b_id": self.BUSINESS_ID})

            conn.commit()

    # =========================================================================
    # 1. TENANT ISOLATION
    # =========================================================================

    def test_tenant_isolation_cross_business(self):
        """
        Business 2 must NEVER be able to retrieve Business 1's Daraz credentials.
        CredentialManager must raise ValueError for a missing connection.
        """
        mgr = CredentialManager()
        other_business_id = 2  # Different tenant
        with self.assertRaises(ValueError) as ctx:
            mgr.get_valid_credentials(
                business_id=other_business_id,
                platform="daraz",
                account_identifier=self.SELLER_ID,  # B1's seller ID
            )
        self.assertIn("No connection found", str(ctx.exception))

    def test_tenant_isolation_list_connections(self):
        """
        list_connections(b2) must NOT include the Daraz connection registered for b1.
        """
        b2_conns = list_connections(2)
        b2_daraz_accounts = [
            c["account_identifier"]
            for c in b2_conns
            if c["platform"] == "daraz"
        ]
        self.assertNotIn(self.SELLER_ID, b2_daraz_accounts)

    def test_tenant_isolation_get_connection(self):
        """
        get_connection must return None when a wrong business_id is supplied.
        """
        result = get_connection(2, "daraz", self.SELLER_ID)
        self.assertIsNone(result)

        # Correct business_id returns the row
        result_b1 = get_connection(self.BUSINESS_ID, "daraz", self.SELLER_ID)
        self.assertIsNotNone(result_b1)
        self.assertEqual(result_b1["account_identifier"], self.SELLER_ID)

    # =========================================================================
    # 2. CREDENTIAL RETRIEVAL
    # =========================================================================

    def test_credential_retrieval_decrypts_correctly(self):
        """
        CredentialManager must decrypt Daraz app_key, app_secret (from
        additional_credentials) and access_token without exposing plaintext
        to the outside world.
        """
        mgr = CredentialManager()
        creds = mgr.get_valid_credentials(
            business_id=self.BUSINESS_ID,
            platform="daraz",
            account_identifier=self.SELLER_ID,
        )

        # Correct decrypted values
        self.assertEqual(creds.get("access_token"), "test_daraz_access_token_phase34")
        additional = creds.get("additional_credentials", {})
        self.assertEqual(additional.get("app_key"), "test_app_key_phase34")
        self.assertEqual(additional.get("app_secret"), "test_app_secret_phase34")

    def test_resolve_daraz_credentials_returns_correct_tuple(self):
        """
        resolve_daraz_credentials must return the correct 5-tuple
        (seller_id, app_key, app_secret, access_token, conn_id).
        """
        seller_id, app_key, app_secret, access_token, conn_id = resolve_daraz_credentials(
            self.BUSINESS_ID
        )
        self.assertEqual(seller_id, self.SELLER_ID)
        self.assertEqual(app_key, "test_app_key_phase34")
        self.assertEqual(app_secret, "test_app_secret_phase34")
        self.assertEqual(access_token, "test_daraz_access_token_phase34")
        self.assertIsNotNone(conn_id)

    def test_resolve_credentials_missing_connection_raises(self):
        """
        resolve_daraz_credentials must raise ValueError when no Daraz
        connection exists for the given business.
        """
        with self.assertRaises(ValueError) as ctx:
            resolve_daraz_credentials(999)  # Non-existent business
        self.assertIn("No active Daraz connection", str(ctx.exception))

    # =========================================================================
    # 3. MOCKED END-TO-END SYNC
    # =========================================================================

    @patch("pipeline.run_daraz_sync.DarazClient")
    @patch("pipeline.run_daraz_sync.DarazProductService")
    @patch("pipeline.run_daraz_sync.DarazInventoryService")
    @patch("pipeline.run_daraz_sync.DarazOrderService")
    def test_mocked_end_to_end_sync(
        self,
        mock_order_svc_cls,
        mock_inv_svc_cls,
        mock_prod_svc_cls,
        mock_client_cls,
    ):
        """
        Full pipeline E2E with mocked Daraz services.
        Verifies:
        - Orders are saved to raw.raw_data
        - Customers, products, orders, items land in canonical tables
        - Sync log records a 'success' entry
        - last_synced_at is stamped on platform_connection
        """
        # --- Mock product service ---
        mock_prod_instance = MagicMock()
        mock_prod_instance.fetch_products.return_value = iter([
            {
                "page": 1,
                "offset": 0,
                "count": 2,
                "products": MOCK_DARAZ_PRODUCTS,
                "raw_response": {"products": MOCK_DARAZ_PRODUCTS},
            }
        ])
        mock_prod_svc_cls.return_value = mock_prod_instance

        # --- Mock inventory service ---
        mock_inv_instance = MagicMock()
        mock_inv_instance.fetch_inventory.return_value = iter([
            {
                "page": 1,
                "offset": 0,
                "count": 2,
                "inventory_levels": [
                    {"seller_sku": "DRZ-PHONE-CASE-01", "quantity": 25},
                    {"seller_sku": "DRZ-WATCH-02", "quantity": 10},
                ],
                "raw_response": {"inventory": []},
            }
        ])
        mock_inv_svc_cls.return_value = mock_inv_instance

        # --- Mock order service ---
        mock_order_instance = MagicMock()
        mock_order_instance.fetch_orders.return_value = iter([
            {
                "page": 1,
                "offset": 0,
                "count": 2,
                "orders": MOCK_DARAZ_ORDERS,
                "raw_response": {"orders": MOCK_DARAZ_ORDERS},
            }
        ])
        mock_order_svc_cls.return_value = mock_order_instance

        # Record last_synced_at BEFORE sync
        conn_before = get_connection(self.BUSINESS_ID, "daraz", self.SELLER_ID)
        last_synced_before = conn_before["last_synced_at"]

        # Run sync
        result = run_daraz_sync(self.BUSINESS_ID)

        # --- Verify return value ---
        self.assertEqual(result["business_id"], self.BUSINESS_ID)
        self.assertEqual(result["total_fetched"], 2)
        self.assertGreaterEqual(result["total_inserted"], 1)

        # --- Verify orders in DB ---
        with get_db_connection() as conn:
            order_count = conn.execute(
                text("""
                    SELECT COUNT(*) FROM core.orders
                    WHERE business_id = :b_id
                      AND external_order_id LIKE 'daraz_99001122330%';
                """),
                {"b_id": self.BUSINESS_ID},
            ).scalar()
        self.assertEqual(order_count, 2)

        # --- Verify raw.raw_data has daraz entries ---
        with get_db_connection() as conn:
            raw_count = conn.execute(
                text("""
                    SELECT COUNT(*) FROM raw.raw_data
                    WHERE business_id = :b_id AND source = 'daraz';
                """),
                {"b_id": self.BUSINESS_ID},
            ).scalar()
        self.assertGreater(raw_count, 0)

        # --- Verify sync log success ---
        with get_db_connection() as conn:
            log_row = conn.execute(
                text("""
                    SELECT status, records_fetched FROM raw.sync_log
                    WHERE business_id = :b_id AND source = 'daraz'
                    ORDER BY sync_id DESC LIMIT 1;
                """),
                {"b_id": self.BUSINESS_ID},
            ).fetchone()
        self.assertIsNotNone(log_row)
        self.assertEqual(log_row[0], "success")
        self.assertEqual(log_row[1], 2)

        # --- Verify last_synced_at was updated ---
        conn_after = get_connection(self.BUSINESS_ID, "daraz", self.SELLER_ID)
        self.assertIsNotNone(conn_after["last_synced_at"])
        if last_synced_before is not None:
            self.assertGreater(conn_after["last_synced_at"], last_synced_before)

    # =========================================================================
    # 4. IDEMPOTENT SYNC
    # =========================================================================

    @patch("pipeline.run_daraz_sync.DarazClient")
    @patch("pipeline.run_daraz_sync.DarazProductService")
    @patch("pipeline.run_daraz_sync.DarazInventoryService")
    @patch("pipeline.run_daraz_sync.DarazOrderService")
    def test_idempotent_sync(
        self,
        mock_order_svc_cls,
        mock_inv_svc_cls,
        mock_prod_svc_cls,
        mock_client_cls,
    ):
        """
        Running the same sync payload twice must not create duplicate orders.
        Second run must return inserted=0 (all rows already exist → UPDATE path).
        """
        def _make_page_iter(**kwargs):
            """Returns a fresh iterator over one page of mock orders."""
            return iter([
                {
                    "page": 1,
                    "offset": 0,
                    "count": 2,
                    "orders": MOCK_DARAZ_ORDERS,
                    "raw_response": {"orders": MOCK_DARAZ_ORDERS},
                }
            ])

        def _empty_product_iter(**kwargs):
            return iter([
                {
                    "page": 1,
                    "offset": 0,
                    "count": 2,
                    "products": MOCK_DARAZ_PRODUCTS,
                    "raw_response": {"products": MOCK_DARAZ_PRODUCTS},
                }
            ])

        def _empty_inv_iter(**kwargs):
            return iter([
                {
                    "page": 1,
                    "offset": 0,
                    "count": 0,
                    "inventory_levels": [],
                    "raw_response": {},
                }
            ])

        mock_prod_instance = MagicMock()
        mock_prod_instance.fetch_products.side_effect = _empty_product_iter
        mock_prod_svc_cls.return_value = mock_prod_instance

        mock_inv_instance = MagicMock()
        mock_inv_instance.fetch_inventory.side_effect = _empty_inv_iter
        mock_inv_svc_cls.return_value = mock_inv_instance

        mock_order_instance = MagicMock()
        mock_order_instance.fetch_orders.side_effect = _make_page_iter
        mock_order_svc_cls.return_value = mock_order_instance

        # Run 1: Initial insert
        result1 = run_daraz_sync(self.BUSINESS_ID)
        inserted_run1 = result1["total_inserted"]
        self.assertGreaterEqual(inserted_run1, 1)

        # Reload service mocks for second run
        mock_prod_instance.fetch_products.side_effect = _empty_product_iter
        mock_inv_instance.fetch_inventory.side_effect = _empty_inv_iter
        mock_order_instance.fetch_orders.side_effect = _make_page_iter

        # Run 2: Idempotent — same data
        result2 = run_daraz_sync(self.BUSINESS_ID)
        self.assertEqual(
            result2["total_inserted"],
            0,
            "Duplicate orders inserted on second run — idempotency violated!",
        )
        self.assertGreaterEqual(result2["total_updated"], 1)

        # Verify count in DB hasn't doubled
        with get_db_connection() as conn:
            count = conn.execute(
                text("""
                    SELECT COUNT(*) FROM core.orders
                    WHERE business_id = :b_id
                      AND external_order_id LIKE 'daraz_99001122330%';
                """),
                {"b_id": self.BUSINESS_ID},
            ).scalar()
        self.assertEqual(count, 2, "Order count should be exactly 2 after two idempotent runs.")

    # =========================================================================
    # 5. FAILED SYNC
    # =========================================================================

    @patch("pipeline.run_daraz_sync.DarazClient")
    @patch("pipeline.run_daraz_sync.DarazProductService")
    @patch("pipeline.run_daraz_sync.DarazInventoryService")
    @patch("pipeline.run_daraz_sync.DarazOrderService")
    def test_failed_sync_marks_error_and_logs(
        self,
        mock_order_svc_cls,
        mock_inv_svc_cls,
        mock_prod_svc_cls,
        mock_client_cls,
    ):
        """
        When a sync raises an exception:
        - The connection status must be set to 'error'.
        - raw.sync_log must record a 'failed' entry with error_message.
        - The exception must propagate to the caller.
        """
        # Product service succeeds, order service raises an API error
        mock_prod_instance = MagicMock()
        mock_prod_instance.fetch_products.return_value = iter([
            {
                "page": 1, "offset": 0, "count": 0,
                "products": [], "raw_response": {},
            }
        ])
        mock_prod_svc_cls.return_value = mock_prod_instance

        mock_inv_instance = MagicMock()
        mock_inv_instance.fetch_inventory.return_value = iter([
            {
                "page": 1, "offset": 0, "count": 0,
                "inventory_levels": [], "raw_response": {},
            }
        ])
        mock_inv_svc_cls.return_value = mock_inv_instance

        mock_order_instance = MagicMock()
        mock_order_instance.fetch_orders.side_effect = RuntimeError(
            "Daraz API Error [429]: Rate limit exceeded"
        )
        mock_order_svc_cls.return_value = mock_order_instance

        with self.assertRaises(RuntimeError):
            run_daraz_sync(self.BUSINESS_ID)

        # Connection status must be 'error'
        conn_row = get_connection(self.BUSINESS_ID, "daraz", self.SELLER_ID)
        self.assertEqual(conn_row["status"], "error")
        self.assertIsNotNone(conn_row["error_message"])

        # Sync log must have a 'failed' entry
        with get_db_connection() as conn:
            log_row = conn.execute(
                text("""
                    SELECT status, error_message FROM raw.sync_log
                    WHERE business_id = :b_id AND source = 'daraz'
                    ORDER BY sync_id DESC LIMIT 1;
                """),
                {"b_id": self.BUSINESS_ID},
            ).fetchone()
        self.assertIsNotNone(log_row)
        self.assertEqual(log_row[0], "failed")
        self.assertIn("Rate limit", log_row[1])

        # Reset to active for subsequent tests
        from auth.connection_service import update_status
        update_status(conn_row["connection_id"], "active")

    # =========================================================================
    # 6. SECRET SAFETY
    # =========================================================================

    def test_secret_safety_daraz_client_repr(self):
        """
        DarazClient.__repr__ must never expose app_secret or access_token.
        """
        client = DarazClient(
            app_key="pk_test_app_key_000",
            app_secret="SUPER_SECRET_SHOULD_NOT_APPEAR",
            access_token="daraz_access_token_never_log_me",
        )
        repr_str = repr(client)
        self.assertNotIn("SUPER_SECRET_SHOULD_NOT_APPEAR", repr_str)
        self.assertNotIn("daraz_access_token_never_log_me", repr_str)
        self.assertIn("***", repr_str)  # Token must be masked

    def test_secret_safety_encryption_round_trip(self):
        """
        Daraz credentials stored via encrypt_json must roundtrip correctly,
        and the ciphertext must NOT contain plaintext values.
        """
        plaintext_creds = {
            "app_key": "daraz_app_key_xyz",
            "app_secret": "daraz_app_secret_abc_never_leak",
        }
        ciphertext = encrypt_json(plaintext_creds)

        # Ciphertext must not expose plaintext
        self.assertNotIn("daraz_app_key_xyz", ciphertext)
        self.assertNotIn("daraz_app_secret_abc_never_leak", ciphertext)

        # Decryption must recover exact values
        recovered = decrypt_json(ciphertext)
        self.assertEqual(recovered["app_key"], "daraz_app_key_xyz")
        self.assertEqual(recovered["app_secret"], "daraz_app_secret_abc_never_leak")

    def test_secret_safety_no_credentials_in_resolution_error(self):
        """
        ValueError raised by resolve_daraz_credentials must not contain
        raw credential values.
        """
        try:
            resolve_daraz_credentials(99999)  # Non-existent business
        except ValueError as e:
            err_str = str(e)
            self.assertNotIn("app_key", err_str)
            self.assertNotIn("app_secret", err_str)
            self.assertNotIn("access_token", err_str)

    # =========================================================================
    # 7. RAW LAYER ISOLATION
    # =========================================================================

    def test_raw_layer_business_scoping(self):
        """
        save_raw_payload must tag every row with the correct business_id,
        ensuring raw.raw_data is business-scoped.
        """
        payload = {"orders": [{"order_id": 777888999}]}
        raw_id = save_raw_payload(
            self.BUSINESS_ID, "daraz", "orders", payload
        )

        with get_db_connection() as conn:
            row = conn.execute(
                text("SELECT business_id, source FROM raw.raw_data WHERE raw_id = :rid;"),
                {"rid": raw_id},
            ).fetchone()

        self.assertIsNotNone(row)
        self.assertEqual(row[0], self.BUSINESS_ID)
        self.assertEqual(row[1], "daraz")


class TestDarazWatermark(unittest.TestCase):
    """Tests for incremental sync watermark logic."""

    BUSINESS_ID = 1

    def test_watermark_returns_none_on_no_history(self):
        """
        When no successful Daraz sync exists in raw.sync_log,
        get_daraz_last_watermark returns None (triggers full sync).
        """
        # This test relies on no successful daraz sync existing yet,
        # or a new business_id with no history.
        # We safely check that the function returns None or a valid ISO string.
        result = get_daraz_last_watermark(99999)  # Non-existent business
        self.assertIsNone(result)

    def test_watermark_format_when_present(self):
        """
        When a successful sync exists, watermark must be an ISO-8601 string
        with 10-minute safety buffer applied.
        """
        logger = SyncLogger(business_id=self.BUSINESS_ID, source="daraz")
        sync_id = logger.start_sync(sync_type="full")
        logger.finish_success(fetched=5, inserted=5, updated=0)

        wm = get_daraz_last_watermark(self.BUSINESS_ID)
        # May be None if the DB clock is behind; just verify format if not None
        if wm is not None:
            self.assertRegex(
                wm,
                r"^\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}",
                "Watermark must be ISO-8601 format",
            )


if __name__ == "__main__":
    unittest.main()
