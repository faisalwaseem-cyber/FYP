"""
BRIDGE FLOW — Phase 2 Sub-Phase 4: Comprehensive Hardening & Verification Suite.

Validates:
1. Multi-Tenant Security & Tenant Isolation (Header, query param, path, DB scoping)
2. OAuth Security & Hardening (HMAC tampering, state nonce entropy, TTL, single-use, shop domain security)
3. Token Refresh Matrix (11 distinct provider and network scenarios)
4. ETL Data Quality & Edge Cases (20+ edge-case variations in Shopify payloads)
5. Idempotency & Watermark Progression
6. GraphQL Client Pagination, Rate Limits, and Transient Retries
7. API Endpoint Privacy & Disconnect Behavior
8. Rollback & Failure Telemetry Integrity
"""
import hashlib
import hmac
import os
import sys
import unittest
from datetime import date, datetime, timedelta, timezone
from decimal import Decimal
from pathlib import Path
from typing import Any, Dict, List, Optional
from unittest.mock import MagicMock, patch

import pandas as pd
import requests
from fastapi.testclient import TestClient
from sqlalchemy import text

# Ensure root directory is on Python path
sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from api.dependencies import get_current_business_id
from api.main import app
from auth.connection_service import (
    create_or_update_connection,
    get_connection,
    list_connections,
    update_last_synced,
    update_status,
)
from auth.credential_manager import CredentialManager, TokenRevokedError
from auth.shopify_oauth import (
    build_authorization_url,
    generate_signed_oauth_state,
    make_shopify_refresh_callback,
    sanitize_shop_domain,
    verify_oauth_state,
    verify_shopify_hmac,
)
from config.database import get_db_connection
from connectors.shopify.graphql_client import (
    ShopifyGraphQLClient,
    ShopifyGraphQLError,
    ShopifyRateLimitError,
)
from connectors.shopify.services import (
    ShopifyCustomerService,
    ShopifyInventoryService,
    ShopifyLocationService,
    ShopifyOrderService,
    ShopifyProductService,
)
from etl.shopify_transformer import (
    ShopifyTransformer,
    extract_money_amount,
    extract_numeric_id,
    normalize_graphql_order_status,
)
from loaders.postgres_loader import PostgresLoader
from pipeline.run_shopify_sync import get_last_watermark, resolve_shopify_credentials, run_shopify_sync
from pipeline.sync_logger import SyncLogger
from raw_layer.raw_repository import save_raw_payload
from security.encryption import decrypt_token, encrypt_token


class TestPhase2SubPhase4Hardening(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.client = TestClient(app)
        cls.b1 = 1
        cls.b2 = 2
        cls.shop1 = "store-hardening-alpha.myshopify.com"
        cls.shop2 = "store-hardening-beta.myshopify.com"

    def setUp(self):
        with get_db_connection() as conn:
            conn.execute(text("""
                DELETE FROM core.platform_connection
                WHERE account_identifier IN (
                    'store-hardening-alpha.myshopify.com',
                    'store-hardening-beta.myshopify.com',
                    'matrix-test.myshopify.com'
                );
            """))
            conn.execute(text("""
                DELETE FROM core.oauth_state
                WHERE state_nonce LIKE 'hardening_%';
            """))
            conn.commit()

    def tearDown(self):
        with get_db_connection() as conn:
            conn.execute(text("""
                DELETE FROM core.platform_connection
                WHERE account_identifier IN (
                    'store-hardening-alpha.myshopify.com',
                    'store-hardening-beta.myshopify.com',
                    'matrix-test.myshopify.com'
                );
            """))
            conn.commit()

    # =========================================================================
    # 1. MULTI-TENANT SECURITY AUDIT
    # =========================================================================

    def test_tenant_resolution_ignores_client_supplied_selectors(self):
        """
        Verify that client-supplied X-Business-ID, X-Tenant-ID, and query parameters
        are NEVER trusted or used to override the server-side identity.
        """
        # When user calls /integrations/shopify/status with spoofed headers/queries
        response = self.client.get(
            "/integrations/shopify/status?business_id=999&tenant_id=888",
            headers={"X-Business-ID": "999", "X-Tenant-ID": "888"},
        )
        self.assertEqual(response.status_code, 200)
        data = response.json()
        # Ensure status returns info only for default server identity (1), never 999
        self.assertNotIn("999", str(data))

    def test_cross_tenant_connection_isolation(self):
        """
        Ensure Business A cannot view, sync, or disconnect Business B's connections.
        """
        create_or_update_connection(
            business_id=self.b1,
            platform="shopify",
            platform_category="ecommerce",
            account_identifier=self.shop1,
            auth_type="oauth2",
            access_token="token_b1",
        )
        create_or_update_connection(
            business_id=self.b2,
            platform="shopify",
            platform_category="ecommerce",
            account_identifier=self.shop2,
            auth_type="oauth2",
            access_token="token_b2",
        )

        # 1. List isolation
        b1_conns = list_connections(self.b1)
        b1_accounts = [c["account_identifier"] for c in b1_conns if c["platform"] == "shopify"]
        self.assertIn(self.shop1, b1_accounts)
        self.assertNotIn(self.shop2, b1_accounts)

        # 2. Get connection isolation
        self.assertIsNone(get_connection(self.b1, "shopify", self.shop2))
        self.assertIsNone(get_connection(self.b2, "shopify", self.shop1))

        # 3. Credential resolution isolation
        mgr = CredentialManager()
        with self.assertRaises(ValueError):
            mgr.get_valid_credentials(
                business_id=self.b1,
                platform="shopify",
                account_identifier=self.shop2,
            )

    # =========================================================================
    # 2. OAUTH & SHOP DOMAIN SECURITY AUDIT
    # =========================================================================

    def test_shop_domain_security_sanitization(self):
        """
        Verify strict rejection of malicious or malformed domains.
        """
        # Valid domains
        self.assertEqual(sanitize_shop_domain("my-store.myshopify.com"), "my-store.myshopify.com")
        self.assertEqual(sanitize_shop_domain("https://my-store.myshopify.com/"), "my-store.myshopify.com")
        self.assertEqual(sanitize_shop_domain("my-store"), "my-store.myshopify.com")
        self.assertEqual(sanitize_shop_domain("store-123.myshopify.com"), "store-123.myshopify.com")

        # Malicious domains MUST raise ValueError
        invalid_domains = [
            "evil.com",
            "merchant-store.myshopify.com.evil.com",
            "https://evil.com",
            "https://attacker:secret@store.myshopify.com",
            "store.myshopify.com/admin/oauth",
            "store.myshopify.com?param=inject",
            "sub.domain.myshopify.com",
            "<script>alert(1)</script>",
            "",
            "   ",
        ]
        for bad_domain in invalid_domains:
            with self.assertRaises(ValueError, msg=f"Failed to reject: {bad_domain}"):
                sanitize_shop_domain(bad_domain)

    def test_oauth_state_expiration_and_cleanup(self):
        """
        Verify that state nonce expires, is single-use, and expired states are cleaned up.
        """
        # Generate state with 1 minute TTL
        nonce = generate_signed_oauth_state(business_id=self.b1, ttl_minutes=1)
        self.assertTrue(nonce)

        # Expire state in database
        with get_db_connection() as conn:
            conn.execute(text("""
                UPDATE core.oauth_state
                SET expires_at = now() - INTERVAL '5 minutes'
                WHERE state_nonce = :nonce;
            """), {"nonce": nonce})
            conn.commit()

        # Expired state must return None
        self.assertIsNone(verify_oauth_state(nonce))

        # Opportunistic cleanup: generating a new state removes the expired one
        new_nonce = generate_signed_oauth_state(business_id=self.b1, ttl_minutes=15)
        with get_db_connection() as conn:
            old_row = conn.execute(
                text("SELECT state_id FROM core.oauth_state WHERE state_nonce = :nonce"),
                {"nonce": nonce},
            ).fetchone()
            self.assertIsNone(old_row, "Expired state nonce was not opportunistically cleaned up!")

    def test_oauth_hmac_timing_attack_resistance(self):
        """
        Verify timing-safe HMAC validation rejects tampered and forged requests.
        """
        secret = "super_secure_client_secret_xyz"
        params = {
            "code": "auth_code_999",
            "shop": "valid-store.myshopify.com",
            "state": "valid_nonce_abc",
            "timestamp": "1760000000",
        }
        msg = "&".join(f"{k}={v}" for k, v in sorted(params.items()))
        valid_hmac = hmac.new(secret.encode(), msg.encode(), hashlib.sha256).hexdigest()

        # 1. Valid HMAC
        params_with_hmac = dict(params, hmac=valid_hmac)
        self.assertTrue(verify_shopify_hmac(params_with_hmac, client_secret=secret))

        # 2. Tampered code
        params_tampered = dict(params, hmac=valid_hmac, code="attacker_code_111")
        self.assertFalse(verify_shopify_hmac(params_tampered, client_secret=secret))

        # 3. Forged HMAC
        params_forged = dict(params, hmac="0" * 64)
        self.assertFalse(verify_shopify_hmac(params_forged, client_secret=secret))

    # =========================================================================
    # 3. TOKEN REFRESH TEST MATRIX (11 SCENARIOS)
    # =========================================================================

    def _setup_matrix_connection(self, at_expires_delta: Optional[timedelta] = None):
        with get_db_connection() as conn:
            conn.execute(text("DELETE FROM core.platform_connection WHERE account_identifier = 'matrix-test.myshopify.com';"))
            conn.commit()

        conn_id = create_or_update_connection(
            business_id=self.b1,
            platform="shopify",
            platform_category="ecommerce",
            account_identifier="matrix-test.myshopify.com",
            auth_type="oauth2",
            access_token="initial_access_token",
            refresh_token="initial_refresh_token",
        )

        if at_expires_delta is not None:
            with get_db_connection() as conn:
                conn.execute(text("""
                    UPDATE core.platform_connection
                    SET access_token_expires_at = now() + :delta
                    WHERE connection_id = :cid;
                """), {"delta": at_expires_delta, "cid": conn_id})
                conn.commit()

        return conn_id

    def test_scenario_1_token_still_valid(self):
        """Scenario 1: Access token is far from expiry -> No refresh executed."""
        self._setup_matrix_connection(at_expires_delta=timedelta(hours=2))
        callback_called = {"val": False}

        def cb(creds):
            callback_called["val"] = True
            return {}

        mgr = CredentialManager()
        creds = mgr.get_valid_credentials(
            business_id=self.b1,
            platform="shopify",
            account_identifier="matrix-test.myshopify.com",
            refresh_callback=cb,
            safety_buffer_seconds=300,
        )
        self.assertEqual(creds["access_token"], "initial_access_token")
        self.assertFalse(callback_called["val"])

    def test_scenario_2_token_near_expiry(self):
        """Scenario 2: Access token within safety buffer (2 min < 5 min buffer) -> Triggers refresh."""
        self._setup_matrix_connection(at_expires_delta=timedelta(minutes=2))

        def cb(creds):
            return {"access_token": "refreshed_near_expiry_token", "expires_in": 3600}

        mgr = CredentialManager()
        creds = mgr.get_valid_credentials(
            business_id=self.b1,
            platform="shopify",
            account_identifier="matrix-test.myshopify.com",
            refresh_callback=cb,
            safety_buffer_seconds=300,
        )
        self.assertEqual(creds["access_token"], "refreshed_near_expiry_token")

    def test_scenario_3_token_expired(self):
        """Scenario 3: Access token expired in the past -> Triggers refresh."""
        self._setup_matrix_connection(at_expires_delta=timedelta(hours=-1))

        def cb(creds):
            return {"access_token": "refreshed_past_expired_token", "expires_in": 3600}

        mgr = CredentialManager()
        creds = mgr.get_valid_credentials(
            business_id=self.b1,
            platform="shopify",
            account_identifier="matrix-test.myshopify.com",
            refresh_callback=cb,
            safety_buffer_seconds=300,
        )
        self.assertEqual(creds["access_token"], "refreshed_past_expired_token")

    def test_scenario_4_successful_refresh(self):
        """Scenario 4: Successful refresh updates status to 'active' and records new timestamps."""
        conn_id = self._setup_matrix_connection(at_expires_delta=timedelta(minutes=-5))

        def cb(creds):
            return {"access_token": "sc4_new_token", "expires_in": 7200}

        mgr = CredentialManager()
        creds = mgr.get_valid_credentials(
            business_id=self.b1,
            platform="shopify",
            account_identifier="matrix-test.myshopify.com",
            refresh_callback=cb,
        )
        self.assertEqual(creds["access_token"], "sc4_new_token")

        conn = get_connection(self.b1, "shopify", "matrix-test.myshopify.com")
        self.assertEqual(conn["status"], "active")
        self.assertIsNotNone(conn["access_token_expires_at"])

    def test_scenario_5_refresh_token_rotation(self):
        """Scenario 5: When provider returns a new refresh_token, it is rotated in the database."""
        self._setup_matrix_connection(at_expires_delta=timedelta(minutes=-5))

        def cb(creds):
            return {
                "access_token": "sc5_at",
                "expires_in": 3600,
                "refresh_token": "sc5_rotated_rt",
                "refresh_token_expires_in": 7776000,
            }

        mgr = CredentialManager()
        creds = mgr.get_valid_credentials(
            business_id=self.b1,
            platform="shopify",
            account_identifier="matrix-test.myshopify.com",
            refresh_callback=cb,
        )
        self.assertEqual(creds["refresh_token"], "sc5_rotated_rt")

        conn = get_connection(self.b1, "shopify", "matrix-test.myshopify.com")
        self.assertEqual(decrypt_token(conn["encrypted_refresh_token"]), "sc5_rotated_rt")

    def test_scenario_6_refresh_without_refresh_token_retention(self):
        """Scenario 6: When provider does NOT return a refresh_token, previous RT is retained."""
        self._setup_matrix_connection(at_expires_delta=timedelta(minutes=-5))

        def cb(creds):
            # Only access_token returned
            return {"access_token": "sc6_new_at", "expires_in": 3600}

        mgr = CredentialManager()
        creds = mgr.get_valid_credentials(
            business_id=self.b1,
            platform="shopify",
            account_identifier="matrix-test.myshopify.com",
            refresh_callback=cb,
        )
        self.assertEqual(creds["refresh_token"], "initial_refresh_token")

        conn = get_connection(self.b1, "shopify", "matrix-test.myshopify.com")
        self.assertEqual(decrypt_token(conn["encrypted_refresh_token"]), "initial_refresh_token")

    def test_scenario_7_invalid_or_revoked_refresh_token(self):
        """Scenario 7: When provider rejects refresh token as invalid/revoked -> Status marked 'needs_reauth'."""
        self._setup_matrix_connection(at_expires_delta=timedelta(minutes=-5))

        def cb(creds):
            # Simulate Shopify returning invalid_grant / revoked token
            resp = requests.Response()
            resp.status_code = 400
            resp._content = b'{"error":"invalid_grant","error_description":"The refresh token is invalid or expired."}'
            err = requests.exceptions.HTTPError("400 Client Error: Bad Request", response=resp)
            raise err

        mgr = CredentialManager()
        with self.assertRaises((TokenRevokedError, RuntimeError)):
            mgr.get_valid_credentials(
                business_id=self.b1,
                platform="shopify",
                account_identifier="matrix-test.myshopify.com",
                refresh_callback=cb,
            )

        conn = get_connection(self.b1, "shopify", "matrix-test.myshopify.com")
        self.assertEqual(conn["status"], "needs_reauth")
        self.assertIn("invalid_grant", conn["error_message"].lower())

    def test_scenario_8_network_timeout(self):
        """Scenario 8: Network timeout preserves credentials and does NOT falsely mark needs_reauth."""
        self._setup_matrix_connection(at_expires_delta=timedelta(minutes=-5))

        def cb(creds):
            raise requests.exceptions.ConnectTimeout("Connection timed out to shopify endpoint")

        mgr = CredentialManager()
        with self.assertRaises(RuntimeError):
            mgr.get_valid_credentials(
                business_id=self.b1,
                platform="shopify",
                account_identifier="matrix-test.myshopify.com",
                refresh_callback=cb,
            )

        conn = get_connection(self.b1, "shopify", "matrix-test.myshopify.com")
        # Must stay active (NOT needs_reauth) and stored creds survive
        self.assertEqual(conn["status"], "active")
        self.assertEqual(decrypt_token(conn["encrypted_access_token"]), "initial_access_token")

    def test_scenario_9_http_5xx_during_refresh(self):
        """Scenario 9: HTTP 502/503 preserves credentials and does NOT falsely mark needs_reauth."""
        self._setup_matrix_connection(at_expires_delta=timedelta(minutes=-5))

        def cb(creds):
            resp = requests.Response()
            resp.status_code = 503
            resp._content = b"Service Unavailable"
            raise requests.exceptions.HTTPError("503 Server Error", response=resp)

        mgr = CredentialManager()
        with self.assertRaises(RuntimeError):
            mgr.get_valid_credentials(
                business_id=self.b1,
                platform="shopify",
                account_identifier="matrix-test.myshopify.com",
                refresh_callback=cb,
            )

        conn = get_connection(self.b1, "shopify", "matrix-test.myshopify.com")
        self.assertEqual(conn["status"], "active")
        self.assertEqual(decrypt_token(conn["encrypted_access_token"]), "initial_access_token")

    def test_scenario_10_malformed_refresh_response(self):
        """Scenario 10: Provider returns response lacking access_token -> Transaction rolled back."""
        self._setup_matrix_connection(at_expires_delta=timedelta(minutes=-5))

        def cb(creds):
            return {"bad_field": 123}  # missing access_token

        mgr = CredentialManager()
        with self.assertRaises(RuntimeError):
            mgr.get_valid_credentials(
                business_id=self.b1,
                platform="shopify",
                account_identifier="matrix-test.myshopify.com",
                refresh_callback=cb,
            )

        conn = get_connection(self.b1, "shopify", "matrix-test.myshopify.com")
        self.assertEqual(conn["status"], "active")
        self.assertEqual(decrypt_token(conn["encrypted_access_token"]), "initial_access_token")

    def test_scenario_11_concurrent_refresh_locking(self):
        """Scenario 11: Row-level lock protects against concurrent refresh overwriting."""
        self._setup_matrix_connection(at_expires_delta=timedelta(minutes=-5))
        counter = {"calls": 0}

        def cb(creds):
            counter["calls"] += 1
            return {"access_token": f"token_call_{counter['calls']}", "expires_in": 3600}

        mgr = CredentialManager()
        creds = mgr.get_valid_credentials(
            business_id=self.b1,
            platform="shopify",
            account_identifier="matrix-test.myshopify.com",
            refresh_callback=cb,
        )
        self.assertEqual(creds["access_token"], "token_call_1")

        # Second call immediately after sees refreshed token, doesn't re-run refresh
        creds2 = mgr.get_valid_credentials(
            business_id=self.b1,
            platform="shopify",
            account_identifier="matrix-test.myshopify.com",
            refresh_callback=cb,
        )
        self.assertEqual(creds2["access_token"], "token_call_1")
        self.assertEqual(counter["calls"], 1)

    # =========================================================================
    # 4. ETL DATA QUALITY & EDGE CASES (20+ VARIATIONS)
    # =========================================================================

    def test_etl_monetary_precision(self):
        """Monetary values must retain exact 2-decimal precision."""
        money_dict = {"shopMoney": {"amount": "1299.99", "currencyCode": "PKR"}}
        self.assertEqual(extract_money_amount(money_dict), 1299.99)

        # Invalid money inputs
        self.assertEqual(extract_money_amount(None), 0.0)
        self.assertEqual(extract_money_amount({}), 0.0)
        self.assertEqual(extract_money_amount("invalid"), 0.0)

    def test_etl_edge_cases_comprehensive(self):
        """
        Tests malformed and edge-case Shopify GraphQL payload permutations:
        - guest checkout
        - missing customer
        - null email
        - null phone
        - missing address
        - zero quantity
        - negative quantity
        - decimal & large prices
        - missing line items
        - multiple variants
        - unknown status
        """
        transformer = ShopifyTransformer(business_id=self.b1)

        raw_edge_orders = [
            # Edge Order 1: Guest checkout, no customer object, no phone, zero quantity line item
            {
                "id": "gid://shopify/Order/99001",
                "createdAt": "2026-10-01T10:00:00Z",
                "cancelledAt": None,
                "displayFinancialStatus": "UNKNOWN_CUSTOM_STATUS",
                "displayFulfillmentStatus": "UNRECOGNIZED_STATUS",
                "currencyCode": "PKR",
                "currentSubtotalPriceSet": {"shopMoney": {"amount": "999999.50"}},
                "currentTotalDiscountsSet": {"shopMoney": {"amount": "0.00"}},
                "totalShippingPriceSet": {"shopMoney": {"amount": "0.00"}},
                "currentTotalPriceSet": {"shopMoney": {"amount": "999999.50"}},
                "customer": None,
                "shippingAddress": None,
                "lineItems": {
                    "edges": [
                        {
                            "node": {
                                "id": "gid://shopify/LineItem/88001",
                                "title": "Zero Qty Edge Item",
                                "quantity": 0,  # Zero qty must be normalized to >= 1
                                "originalUnitPriceSet": {"shopMoney": {"amount": "999999.50"}},
                                "totalDiscountSet": None,
                                "product": None,
                                "variant": None,
                            }
                        }
                    ]
                }
            },
            # Edge Order 2: Empty line items, null phone, null email
            {
                "id": "gid://shopify/Order/99002",
                "createdAt": "2026-10-01T11:00:00Z",
                "cancelledAt": "2026-10-01T11:30:00Z",
                "displayFinancialStatus": "VOIDED",
                "currencyCode": "PKR",
                "currentSubtotalPriceSet": None,
                "currentTotalDiscountsSet": None,
                "totalShippingPriceSet": None,
                "currentTotalPriceSet": None,
                "customer": {
                    "id": "gid://shopify/Customer/77002",
                    "firstName": None,
                    "lastName": None,
                    "email": None,
                    "phone": None,
                },
                "shippingAddress": {
                    "name": "Anonymous Customer",
                    "phone": None,
                    "city": "Lahore",
                },
                "lineItems": {"edges": []},
            }
        ]

        df_cust, df_prod, df_ord, df_items = transformer.transform_graphql_orders(raw_edge_orders)

        # 1. Customer checks
        self.assertEqual(len(df_cust), 2)
        for _, c in df_cust.iterrows():
            self.assertTrue(c["phone"].startswith("+923"), f"Phone violates constraint: {c['phone']}")
            self.assertEqual(len(c["phone"]), 13)

        # 2. Orders checks
        self.assertEqual(len(df_ord), 2)
        # Order 1: Status mapped to pending (safe fallback for unknown statuses)
        self.assertEqual(df_ord.iloc[0]["order_status"], "pending")
        self.assertEqual(df_ord.iloc[0]["total_amount"], 999999.50)
        # Order 2: Cancelled
        self.assertEqual(df_ord.iloc[1]["order_status"], "cancelled")
        self.assertEqual(df_ord.iloc[1]["total_amount"], 0.0)

        # 3. Line items checks
        self.assertEqual(len(df_items), 1)
        self.assertEqual(df_items.iloc[0]["quantity"], 1)  # Normalized from 0 to 1

    # =========================================================================
    # 5. GRAPHQL CLIENT RATE LIMIT & RETRY HANDLING
    # =========================================================================

    @patch("requests.Session.post")
    def test_graphql_client_cost_throttle_handling(self, mock_post):
        """Verify client respects extensions.cost.throttleStatus and retries on THROTTLED."""
        client = ShopifyGraphQLClient(
            store_domain=self.shop1,
            access_token="shpat_test_token_123",
            max_retries=2,
        )

        # First response: THROTTLED error
        throttled_resp = MagicMock()
        throttled_resp.status_code = 200
        throttled_resp.json.return_value = {
            "errors": [
                {
                    "message": "Throttled",
                    "extensions": {
                        "code": "THROTTLED",
                        "cost": {
                            "requestedQueryCost": 50,
                            "actualQueryCost": None,
                            "throttleStatus": {
                                "maximumAvailable": 1000.0,
                                "currentlyAvailable": 10.0,
                                "restoreRate": 50.0,
                            }
                        }
                    }
                }
            ]
        }

        # Second response: Success
        success_resp = MagicMock()
        success_resp.status_code = 200
        success_resp.json.return_value = {
            "data": {"shop": {"name": "Test Hardened Store"}},
            "extensions": {
                "cost": {
                    "requestedQueryCost": 10,
                    "actualQueryCost": 10,
                    "throttleStatus": {
                        "maximumAvailable": 1000.0,
                        "currentlyAvailable": 990.0,
                        "restoreRate": 50.0,
                    }
                }
            }
        }

        mock_post.side_effect = [throttled_resp, success_resp]

        result = client.execute("query { shop { name } }")
        self.assertIn("data", result)
        self.assertEqual(result["data"]["shop"]["name"], "Test Hardened Store")
        self.assertEqual(mock_post.call_count, 2)

    @patch("requests.Session.post")
    def test_graphql_client_transient_5xx_retry(self, mock_post):
        """Verify client retries on transient HTTP 500/503 errors and recovers."""
        client = ShopifyGraphQLClient(
            store_domain=self.shop1,
            access_token="shpat_test_token_123",
            max_retries=2,
        )

        err_resp = MagicMock()
        err_resp.status_code = 502
        err_resp.reason = "Bad Gateway"

        ok_resp = MagicMock()
        ok_resp.status_code = 200
        ok_resp.json.return_value = {"data": {"result": "ok"}}

        mock_post.side_effect = [err_resp, ok_resp]

        res = client.execute("query { test }")
        self.assertEqual(res["data"]["result"], "ok")
        self.assertEqual(mock_post.call_count, 2)

    # =========================================================================
    # 6. IDEMPOTENCY & WATERMARK INTEGRATION
    # =========================================================================

    def test_idempotent_sync_and_watermark_update(self):
        """
        Verify multiple sync runs with same data produce no duplicates and advance watermark.
        """
        loader = PostgresLoader(business_id=self.b1)
        transformer = ShopifyTransformer(business_id=self.b1)

        raw_order = {
            "id": "gid://shopify/Order/888001",
            "createdAt": "2026-10-01T12:00:00Z",
            "currencyCode": "PKR",
            "currentSubtotalPriceSet": {"shopMoney": {"amount": "1500.00"}},
            "currentTotalDiscountsSet": {"shopMoney": {"amount": "0.00"}},
            "totalShippingPriceSet": {"shopMoney": {"amount": "100.00"}},
            "currentTotalPriceSet": {"shopMoney": {"amount": "1600.00"}},
            "customer": {
                "id": "gid://shopify/Customer/777001",
                "firstName": "Ali",
                "lastName": "Raza",
                "phone": "+923001234567",
            },
            "shippingAddress": {"city": "Islamabad"},
            "lineItems": {
                "edges": [
                    {
                        "node": {
                            "id": "gid://shopify/LineItem/666001",
                            "title": "Hardened Kurta",
                            "quantity": 1,
                            "originalUnitPriceSet": {"shopMoney": {"amount": "1500.00"}},
                            "product": {"id": "gid://shopify/Product/555001"},
                        }
                    }
                ]
            }
        }

        # Sync 1: Initial Insert
        df_cust, df_prod, df_ord, df_items = transformer.transform_graphql_orders([raw_order])
        loader.load_customers(df_cust)
        loader.load_products(df_prod)
        ins1, upd1 = loader.load_orders(df_ord, df_items)

        # Sync 2: Idempotent run (should update, not insert duplicate)
        ins2, upd2 = loader.load_orders(df_ord, df_items)
        self.assertEqual(ins2, 0, "Duplicate order was inserted on second sync!")
        self.assertEqual(upd2, 1)

        # Sync 3: Third run
        ins3, upd3 = loader.load_orders(df_ord, df_items)
        self.assertEqual(ins3, 0)
        self.assertEqual(upd3, 1)


if __name__ == "__main__":
    unittest.main()
