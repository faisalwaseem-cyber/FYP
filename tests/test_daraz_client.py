"""
Unit Test Suite for Daraz Open Platform Client (Phase 3.2).

Validates:
1. Successful request execution (GET & POST)
2. HMAC-SHA256 request signing matching Daraz specification
3. API error response inspection (DarazAPIError)
4. Rate-limit and throttling handling (DarazRateLimitError)
5. Transient HTTP 5xx retry and backoff recovery
6. Offset-based pagination across multiple pages, single page, and empty page
7. Credential safety (token masking in __repr__ and exceptions)
"""
import hashlib
import hmac
import sys
import unittest
from pathlib import Path
from unittest.mock import MagicMock, patch

import requests

# Ensure root directory is on Python path
sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from connectors.daraz.client import DarazAPIError, DarazClient, DarazRateLimitError


class TestDarazClient(unittest.TestCase):
    def setUp(self):
        self.app_key = "123456"
        self.app_secret = "test_daraz_app_secret_xyz"
        self.access_token = "daraz_access_token_abc"
        self.client = DarazClient(
            app_key=self.app_key,
            app_secret=self.app_secret,
            access_token=self.access_token,
            base_url="https://api.daraz.pk/rest",
            timeout=5.0,
            max_retries=2,
        )

    # ------------------------------------------------------------------
    # 1. Authentication & Signature Generation
    # ------------------------------------------------------------------

    def test_signature_generation(self):
        """
        Daraz signature algorithm:
        HMAC-SHA256(app_secret, api_path + sorted({key}{value})).upper()
        """
        api_path = "/orders/get"
        params = {
            "app_key": "123456",
            "timestamp": "1700000000000",
            "sign_method": "sha256",
            "limit": 100,
        }

        # Expected manual calculation
        sorted_pairs = sorted(params.items(), key=lambda x: str(x[0]))
        expected_query = api_path + "".join([f"{k}{v}" for k, v in sorted_pairs])
        expected_sign = hmac.new(
            self.app_secret.encode("utf-8"),
            expected_query.encode("utf-8"),
            hashlib.sha256,
        ).hexdigest().upper()

        computed_sign = self.client.sign_request(api_path, params)
        self.assertEqual(computed_sign, expected_sign)
        self.assertTrue(computed_sign.isupper())

    # ------------------------------------------------------------------
    # 2. Successful Request Execution
    # ------------------------------------------------------------------

    @patch("requests.Session.get")
    def test_successful_request_get(self, mock_get):
        mock_resp = MagicMock()
        mock_resp.status_code = 200
        mock_resp.json.return_value = {
            "code": "0",
            "data": {
                "orders": [
                    {"order_id": 991, "status": "delivered", "price": "1500.00"}
                ]
            }
        }
        mock_get.return_value = mock_resp

        result = self.client.fetch_orders(status="delivered")
        self.assertIn("orders", result)
        self.assertEqual(len(result["orders"]), 1)
        self.assertEqual(result["orders"][0]["order_id"], 991)
        mock_get.assert_called_once()

    @patch("requests.Session.post")
    def test_successful_request_post(self, mock_post):
        mock_resp = MagicMock()
        mock_resp.status_code = 200
        mock_resp.json.return_value = {
            "code": "0",
            "data": {
                "access_token": "new_refreshed_at_123",
                "expires_in": 2592000,
                "refresh_token": "new_rotated_rt_456",
            }
        }
        mock_post.return_value = mock_resp

        result = self.client.refresh_access_token("existing_rt")
        self.assertEqual(result["access_token"], "new_refreshed_at_123")
        self.assertEqual(result["refresh_token"], "new_rotated_rt_456")
        mock_post.assert_called_once()

    # ------------------------------------------------------------------
    # 3. API Error Handling
    # ------------------------------------------------------------------

    @patch("requests.Session.get")
    def test_api_error_response(self, mock_get):
        mock_resp = MagicMock()
        mock_resp.status_code = 200
        mock_resp.json.return_value = {
            "code": "IllegalAccessToken",
            "message": "The access token has expired.",
            "request_id": "req_001_abc",
        }
        mock_get.return_value = mock_resp

        with self.assertRaises(DarazAPIError) as ctx:
            self.client.fetch_orders()

        self.assertEqual(ctx.exception.code, "IllegalAccessToken")
        self.assertIn("expired", ctx.exception.error_message)

    # ------------------------------------------------------------------
    # 4. Rate-Limit & Throttling Handling
    # ------------------------------------------------------------------

    @patch("requests.Session.get")
    def test_rate_limit_http_429(self, mock_get):
        mock_resp = MagicMock()
        mock_resp.status_code = 429
        mock_get.return_value = mock_resp

        with self.assertRaises(DarazRateLimitError):
            self.client.fetch_orders()

        self.assertEqual(mock_get.call_count, 3)  # Initial + 2 retries

    @patch("requests.Session.get")
    def test_rate_limit_business_code(self, mock_get):
        mock_resp = MagicMock()
        mock_resp.status_code = 200
        mock_resp.json.return_value = {
            "code": "ApiCallLimit",
            "message": "API call limit reached for this period",
        }
        mock_get.return_value = mock_resp

        with self.assertRaises(DarazRateLimitError):
            self.client.fetch_orders()

        self.assertEqual(mock_get.call_count, 3)

    # ------------------------------------------------------------------
    # 5. Transient Server Error Retries
    # ------------------------------------------------------------------

    @patch("requests.Session.get")
    def test_transient_error_retry_and_recovery(self, mock_get):
        err_resp = MagicMock()
        err_resp.status_code = 502

        ok_resp = MagicMock()
        ok_resp.status_code = 200
        ok_resp.json.return_value = {"code": "0", "data": {"orders": []}}

        mock_get.side_effect = [err_resp, ok_resp]

        res = self.client.fetch_orders()
        self.assertEqual(res, {"orders": []})
        self.assertEqual(mock_get.call_count, 2)

    # ------------------------------------------------------------------
    # 6. Offset Pagination
    # ------------------------------------------------------------------

    @patch("requests.Session.get")
    def test_pagination_multiple_pages(self, mock_get):
        # Page 1: 100 items (full page, triggers next page)
        resp1 = MagicMock()
        resp1.status_code = 200
        resp1.json.return_value = {
            "code": "0",
            "data": {"orders": [{"id": f"ord_{i}"} for i in range(100)]}
        }

        # Page 2: 45 items (< 100, terminates)
        resp2 = MagicMock()
        resp2.status_code = 200
        resp2.json.return_value = {
            "code": "0",
            "data": {"orders": [{"id": f"ord_{i}"} for i in range(100, 145)]}
        }

        mock_get.side_effect = [resp1, resp2]

        pages = list(self.client.paginate("/orders/get", data_key="orders", page_size=100))
        self.assertEqual(len(pages), 2)
        self.assertEqual(pages[0]["page"], 1)
        self.assertEqual(pages[0]["count"], 100)
        self.assertEqual(pages[1]["page"], 2)
        self.assertEqual(pages[1]["count"], 45)
        self.assertEqual(mock_get.call_count, 2)

    @patch("requests.Session.get")
    def test_pagination_single_page(self, mock_get):
        resp = MagicMock()
        resp.status_code = 200
        resp.json.return_value = {
            "code": "0",
            "data": {"orders": [{"id": "ord_1"}, {"id": "ord_2"}]}
        }
        mock_get.return_value = resp

        pages = list(self.client.paginate("/orders/get", data_key="orders", page_size=100))
        self.assertEqual(len(pages), 1)
        self.assertEqual(pages[0]["count"], 2)
        self.assertEqual(mock_get.call_count, 1)

    @patch("requests.Session.get")
    def test_pagination_empty_page(self, mock_get):
        resp = MagicMock()
        resp.status_code = 200
        resp.json.return_value = {"code": "0", "data": {"orders": []}}
        mock_get.return_value = resp

        pages = list(self.client.paginate("/orders/get", data_key="orders", page_size=100))
        self.assertEqual(len(pages), 1)
        self.assertEqual(pages[0]["count"], 0)

    # ------------------------------------------------------------------
    # 7. Credential Safety
    # ------------------------------------------------------------------

    def test_credential_safety(self):
        repr_str = repr(self.client)
        self.assertIn("123456", repr_str)
        # Token and secret must NEVER be visible
        self.assertNotIn("test_daraz_app_secret_xyz", repr_str)
        self.assertNotIn("daraz_access_token_abc", repr_str)
        self.assertIn("token='***'", repr_str)


if __name__ == "__main__":
    unittest.main()
