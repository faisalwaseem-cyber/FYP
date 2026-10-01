"""
Unit Test Suite — Courier Clients (BRIDGE FLOW Phase 5.2)

Coverage (all tests use mocked HTTP — no network, no database):

PostEx
  1.  Authentication — token injected correctly in request header
  2.  Tracking — successful track_shipment response parsed correctly
  3.  Order status — get_order_status by merchant reference
  4.  COD settlement — get_cod_settlement range call and list extraction
  5.  HTTP 401/403 → CourierAuthError (not retried)
  6.  HTTP 429 → CourierRateLimitError after retries
  7.  HTTP 5xx transient → retry and recover
  8.  Business-level error (statusCode != 200) → CourierAPIError
  9.  Tracking not found → CourierTrackingNotFoundError
  10. Credential safety — api_token not leaked in repr

Trax
  11. Authentication — Basic Auth header sent correctly
  12. Tracking — successful track_shipment response
  13. Order status — get_order_status by merchant reference
  14. Tracking history — get_tracking_history returns list
  15. COD settlement — get_cod_settlement returns list
  16. HTTP 401 → CourierAuthError
  17. HTTP 429 → CourierRateLimitError after retries
  18. HTTP 5xx → retry and recover
  19. Business-level 'status: error' body → CourierAPIError
  20. HTTP 404 → CourierTrackingNotFoundError
  21. Credential safety — password not leaked in repr

Leopards
  22. Authentication — api_key and api_password in query params
  23. Tracking — successful track_shipment response
  24. Order status — get_order_status by order_ids param
  25. COD settlement — get_cod_settlement returns list
  26. HTTP 401 → CourierAuthError
  27. HTTP 429 → CourierRateLimitError after retries
  28. HTTP 5xx → retry and recover
  29. Business error status=0 with generic message → CourierAPIError
  30. Business error status=0 with 'invalid credentials' → CourierAuthError
  31. Business error status=0 with 'not found' → CourierTrackingNotFoundError
  32. Credential safety — api_password not leaked in repr

Base / common
  33. Missing required credentials raise ValueError at construction
  34. Empty tracking number raises ValueError before any HTTP call
  35. Unsupported optional method raises NotImplementedError
"""
import sys
import unittest
from pathlib import Path
from unittest.mock import MagicMock, call, patch

import requests

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from connectors.courier import (
    CourierAPIError,
    CourierAuthError,
    CourierRateLimitError,
    CourierTrackingNotFoundError,
    LeopardsClient,
    PostExClient,
    TraxClient,
)
from connectors.courier.base import BaseCourierClient


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _mock_response(status_code: int, json_data=None, headers=None) -> MagicMock:
    r = MagicMock(spec=requests.Response)
    r.status_code = status_code
    r.headers = headers or {}
    r.json.return_value = json_data or {}
    r.raise_for_status = MagicMock()
    if status_code >= 400:
        r.raise_for_status.side_effect = requests.exceptions.HTTPError(
            f"HTTP {status_code}", response=r
        )
    return r


# ===========================================================================
#  PostEx Tests
# ===========================================================================

class TestPostExClient(unittest.TestCase):

    def setUp(self):
        self.token = "pk_live_test_token_1234567890"
        self.client = PostExClient(
            api_token=self.token,
            max_retries=2,
            timeout=5,
        )

    # ------------------------------------------------------------------
    # 1. Authentication — token injected in session header
    # ------------------------------------------------------------------

    def test_auth_token_in_session_header(self):
        """Token must be present in 'token' session header."""
        self.assertEqual(self.client.session.headers.get("token"), self.token)
        self.assertIn("application/json", self.client.session.headers.get("Content-Type", ""))

    # ------------------------------------------------------------------
    # 2. Tracking — successful response
    # ------------------------------------------------------------------

    @patch("requests.Session.get")
    def test_track_shipment_success(self, mock_get):
        mock_get.return_value = _mock_response(200, {
            "statusCode": "200",
            "statusMessage": "Success",
            "dist": {
                "trackingNumber": "POSTEX-PK-001",
                "orderStatus": "Delivered",
                "orderAmount": 3500.0,
                "deliveryCharges": 200.0,
                "attemptCount": 1,
            }
        })

        result = self.client.track_shipment("POSTEX-PK-001")

        self.assertIn("dist", result)
        self.assertEqual(result["dist"]["orderStatus"], "Delivered")
        mock_get.assert_called_once()

        called_url = mock_get.call_args[0][0]
        self.assertIn("POSTEX-PK-001", called_url)
        self.assertIn("track-order", called_url)

    # ------------------------------------------------------------------
    # 3. Order status by merchant reference
    # ------------------------------------------------------------------

    @patch("requests.Session.get")
    def test_get_order_status_success(self, mock_get):
        mock_get.return_value = _mock_response(200, {
            "statusCode": "200",
            "dist": {
                "orderRefNumber": "5589000001",
                "orderStatus": "In Transit",
            }
        })

        result = self.client.get_order_status("5589000001")

        self.assertIn("dist", result)
        self.assertEqual(result["dist"]["orderRefNumber"], "5589000001")
        called_url = mock_get.call_args[0][0]
        self.assertIn("get-order", called_url)
        self.assertIn("5589000001", called_url)

    # ------------------------------------------------------------------
    # 4. COD settlement
    # ------------------------------------------------------------------

    @patch("requests.Session.get")
    def test_get_cod_settlement_returns_list(self, mock_get):
        mock_get.return_value = _mock_response(200, {
            "statusCode": "200",
            "dist": [
                {"trackingNumber": "POSTEX-001", "orderAmount": 1000},
                {"trackingNumber": "POSTEX-002", "orderAmount": 2500},
            ]
        })

        records = self.client.get_cod_settlement("2026-09-01", "2026-09-30")

        self.assertIsInstance(records, list)
        self.assertEqual(len(records), 2)
        called_url = mock_get.call_args[0][0]
        self.assertIn("cod-settlement", called_url)

    # ------------------------------------------------------------------
    # 5. HTTP 401 / 403 → CourierAuthError (not retried)
    # ------------------------------------------------------------------

    @patch("requests.Session.get")
    def test_http_401_raises_auth_error(self, mock_get):
        mock_get.return_value = _mock_response(401, {})
        with self.assertRaises(CourierAuthError):
            self.client.track_shipment("ANY-CN")
        # Must not be retried — only called once
        self.assertEqual(mock_get.call_count, 1)

    @patch("requests.Session.get")
    def test_http_403_raises_auth_error(self, mock_get):
        mock_get.return_value = _mock_response(403, {})
        with self.assertRaises(CourierAuthError):
            self.client.track_shipment("ANY-CN")
        self.assertEqual(mock_get.call_count, 1)

    # ------------------------------------------------------------------
    # 6. HTTP 429 → CourierRateLimitError after retries
    # ------------------------------------------------------------------

    @patch("time.sleep")
    @patch("requests.Session.get")
    def test_http_429_raises_rate_limit_after_retries(self, mock_get, mock_sleep):
        mock_get.return_value = _mock_response(429, {})
        with self.assertRaises(CourierRateLimitError):
            self.client.track_shipment("ANY-CN")
        # Initial attempt + 2 retries = 3 calls
        self.assertEqual(mock_get.call_count, 3)

    # ------------------------------------------------------------------
    # 7. HTTP 5xx → retry and recover on success
    # ------------------------------------------------------------------

    @patch("time.sleep")
    @patch("requests.Session.get")
    def test_http_5xx_retries_then_succeeds(self, mock_get, mock_sleep):
        err = _mock_response(503, {})
        ok = _mock_response(200, {"statusCode": "200", "dist": {"orderStatus": "Booked"}})
        mock_get.side_effect = [err, ok]

        result = self.client.track_shipment("ANY-CN")
        self.assertIn("dist", result)
        self.assertEqual(mock_get.call_count, 2)

    # ------------------------------------------------------------------
    # 8. Business-level error (statusCode != "200") → CourierAPIError
    # ------------------------------------------------------------------

    @patch("requests.Session.get")
    def test_business_error_raises_courier_api_error(self, mock_get):
        mock_get.return_value = _mock_response(200, {
            "statusCode": "400",
            "statusMessage": "Invalid tracking number format",
        })
        with self.assertRaises(CourierAPIError) as ctx:
            self.client.track_shipment("INVALID-FORMAT")
        self.assertEqual(ctx.exception.code, "400")
        self.assertIn("Invalid tracking", str(ctx.exception))

    # ------------------------------------------------------------------
    # 9. Tracking not found → CourierTrackingNotFoundError
    # ------------------------------------------------------------------

    @patch("requests.Session.get")
    def test_tracking_not_found_raises_correct_error(self, mock_get):
        mock_get.return_value = _mock_response(200, {
            "statusCode": "404",
            "statusMessage": "Order not found",
        })
        with self.assertRaises(CourierTrackingNotFoundError):
            self.client.track_shipment("GHOST-CN-999")

    # ------------------------------------------------------------------
    # 10. Credential safety
    # ------------------------------------------------------------------

    def test_repr_does_not_expose_full_token(self):
        repr_str = repr(self.client)
        # Must not contain the full token
        self.assertNotIn(self.token, repr_str)
        # Must contain a masked hint
        self.assertIn("***", repr_str)
        # Provider name must be present
        self.assertIn("postex", repr_str)

    def test_empty_token_raises_value_error(self):
        with self.assertRaises(ValueError):
            PostExClient(api_token="")

    def test_empty_tracking_number_raises_value_error(self):
        with self.assertRaises(ValueError):
            self.client.track_shipment("")


# ===========================================================================
#  Trax Tests
# ===========================================================================

class TestTraxClient(unittest.TestCase):

    def setUp(self):
        self.username = "trax_merchant_user"
        self.password = "s3cur3_trax_pass_xyz"
        self.client = TraxClient(
            username=self.username,
            password=self.password,
            max_retries=2,
            timeout=5,
        )

    # ------------------------------------------------------------------
    # 11. Authentication — Basic Auth set correctly
    # ------------------------------------------------------------------

    def test_basic_auth_set_on_session(self):
        self.assertEqual(self.client.session.auth, (self.username, self.password))

    # ------------------------------------------------------------------
    # 12. Tracking — successful response
    # ------------------------------------------------------------------

    @patch("requests.Session.request")
    def test_track_shipment_success(self, mock_request):
        mock_request.return_value = _mock_response(200, {
            "status": "success",
            "cn": "TRAX-001-PK",
            "current_status": "In Transit",
            "last_location": "Karachi Hub",
        })

        result = self.client.track_shipment("TRAX-001-PK")

        self.assertEqual(result["cn"], "TRAX-001-PK")
        self.assertEqual(result["current_status"], "In Transit")

        call_kwargs = mock_request.call_args
        called_url = call_kwargs[0][1]
        self.assertIn("TRAX-001-PK", called_url)

    # ------------------------------------------------------------------
    # 13. Order status by merchant reference
    # ------------------------------------------------------------------

    @patch("requests.Session.request")
    def test_get_order_status_success(self, mock_request):
        mock_request.return_value = _mock_response(200, {
            "status": "success",
            "reference": "ORD-78910",
            "shipment_status": "Delivered",
        })

        result = self.client.get_order_status("ORD-78910")

        self.assertEqual(result["reference"], "ORD-78910")
        called_url = mock_request.call_args[0][1]
        self.assertIn("ORD-78910", called_url)

    # ------------------------------------------------------------------
    # 14. Tracking history
    # ------------------------------------------------------------------

    @patch("requests.Session.request")
    def test_get_tracking_history_returns_list(self, mock_request):
        mock_request.return_value = _mock_response(200, {
            "status": "success",
            "events": [
                {"timestamp": "2026-09-29T10:00:00", "status": "Booked", "location": "Lahore"},
                {"timestamp": "2026-09-30T08:00:00", "status": "In Transit", "location": "Islamabad"},
            ]
        })

        history = self.client.get_tracking_history("TRAX-001-PK")

        self.assertIsInstance(history, list)
        self.assertEqual(len(history), 2)
        self.assertEqual(history[0]["status"], "Booked")

    # ------------------------------------------------------------------
    # 15. COD settlement
    # ------------------------------------------------------------------

    @patch("requests.Session.request")
    def test_get_cod_settlement_returns_list(self, mock_request):
        mock_request.return_value = _mock_response(200, {
            "status": "success",
            "data": [
                {"cn": "TRAX-001", "amount": 1500, "settlement_date": "2026-09-30"},
                {"cn": "TRAX-002", "amount": 3000, "settlement_date": "2026-09-30"},
            ]
        })

        records = self.client.get_cod_settlement("2026-09-01", "2026-09-30")

        self.assertIsInstance(records, list)
        self.assertEqual(len(records), 2)

    # ------------------------------------------------------------------
    # 16. HTTP 401 → CourierAuthError
    # ------------------------------------------------------------------

    @patch("requests.Session.request")
    def test_http_401_raises_auth_error(self, mock_request):
        mock_request.return_value = _mock_response(401, {})
        with self.assertRaises(CourierAuthError):
            self.client.track_shipment("TRAX-CN")
        self.assertEqual(mock_request.call_count, 1)

    # ------------------------------------------------------------------
    # 17. HTTP 429 → CourierRateLimitError after retries
    # ------------------------------------------------------------------

    @patch("time.sleep")
    @patch("requests.Session.request")
    def test_http_429_raises_rate_limit_after_retries(self, mock_request, mock_sleep):
        mock_request.return_value = _mock_response(429, {})
        with self.assertRaises(CourierRateLimitError):
            self.client.track_shipment("TRAX-CN")
        self.assertEqual(mock_request.call_count, 3)

    # ------------------------------------------------------------------
    # 18. HTTP 5xx → retry and recover
    # ------------------------------------------------------------------

    @patch("time.sleep")
    @patch("requests.Session.request")
    def test_http_5xx_retries_then_succeeds(self, mock_request, mock_sleep):
        err = _mock_response(502, {})
        ok = _mock_response(200, {"status": "success", "cn": "TRAX-CN"})
        mock_request.side_effect = [err, ok]

        result = self.client.track_shipment("TRAX-CN")
        self.assertEqual(result["cn"], "TRAX-CN")
        self.assertEqual(mock_request.call_count, 2)

    # ------------------------------------------------------------------
    # 19. Business-level status 'error' → CourierAPIError
    # ------------------------------------------------------------------

    @patch("requests.Session.request")
    def test_business_error_body_raises_api_error(self, mock_request):
        mock_request.return_value = _mock_response(200, {
            "status": "error",
            "code": "TRAX_INVALID_CN",
            "message": "The provided CN is malformed.",
        })
        with self.assertRaises(CourierAPIError) as ctx:
            self.client.track_shipment("BAD-CN")
        self.assertIn("TRAX_INVALID_CN", ctx.exception.code)

    # ------------------------------------------------------------------
    # 20. HTTP 404 → CourierTrackingNotFoundError
    # ------------------------------------------------------------------

    @patch("requests.Session.request")
    def test_http_404_raises_not_found(self, mock_request):
        mock_request.return_value = _mock_response(404, {})
        # Override raise_for_status since 404 is handled before that
        mock_request.return_value.raise_for_status = MagicMock()
        with self.assertRaises(CourierTrackingNotFoundError):
            self.client.track_shipment("GHOST-CN")

    # ------------------------------------------------------------------
    # 21. Credential safety
    # ------------------------------------------------------------------

    def test_repr_does_not_expose_password(self):
        repr_str = repr(self.client)
        self.assertNotIn(self.password, repr_str)
        self.assertIn(self.username, repr_str)
        self.assertIn("trax", repr_str)

    def test_missing_username_raises_value_error(self):
        with self.assertRaises(ValueError):
            TraxClient(username="", password="pass")

    def test_missing_password_raises_value_error(self):
        with self.assertRaises(ValueError):
            TraxClient(username="user", password="")

    def test_empty_tracking_number_raises_value_error(self):
        with self.assertRaises(ValueError):
            self.client.track_shipment("   ")


# ===========================================================================
#  Leopards Tests
# ===========================================================================

class TestLeopardsClient(unittest.TestCase):

    def setUp(self):
        self.api_key = "leopards_key_abcdef1234"
        self.api_password = "leopards_secret_pass_xyz"
        self.client = LeopardsClient(
            api_key=self.api_key,
            api_password=self.api_password,
            max_retries=2,
            timeout=5,
        )

    # ------------------------------------------------------------------
    # 22. Authentication — api_key and api_password in query params
    # ------------------------------------------------------------------

    @patch("requests.Session.get")
    def test_auth_params_in_request(self, mock_get):
        mock_get.return_value = _mock_response(200, {
            "status": 1,
            "packet_list": [{"booked_packet_cn": "LEP-001", "booked_packet_status": "Delivered"}]
        })

        self.client.track_shipment("LEP-001")

        called_params = mock_get.call_args[1]["params"]
        self.assertEqual(called_params["api_key"], self.api_key)
        self.assertEqual(called_params["api_password"], self.api_password)

    # ------------------------------------------------------------------
    # 23. Tracking — successful response
    # ------------------------------------------------------------------

    @patch("requests.Session.get")
    def test_track_shipment_success(self, mock_get):
        mock_get.return_value = _mock_response(200, {
            "status": 1,
            "packet_list": [
                {
                    "booked_packet_cn": "LEP-9001",
                    "booked_packet_status": "Delivered",
                    "booked_packet_collect_amount": "2500",
                }
            ]
        })

        result = self.client.track_shipment("LEP-9001")

        self.assertEqual(result["status"], 1)
        self.assertEqual(result["packet_list"][0]["booked_packet_cn"], "LEP-9001")
        called_url = mock_get.call_args[0][0]
        self.assertIn("track_n_trace", called_url)

    # ------------------------------------------------------------------
    # 24. Order status by order_ids param
    # ------------------------------------------------------------------

    @patch("requests.Session.get")
    def test_get_order_status_success(self, mock_get):
        mock_get.return_value = _mock_response(200, {
            "status": 1,
            "packet_list": [{"booked_packet_cn": "LEP-9001", "booked_packet_status": "In Transit"}]
        })

        result = self.client.get_order_status("ORD-12345")

        self.assertEqual(result["status"], 1)
        called_params = mock_get.call_args[1]["params"]
        self.assertEqual(called_params["order_ids"], "ORD-12345")
        called_url = mock_get.call_args[0][0]
        self.assertIn("get_shipment_status", called_url)

    # ------------------------------------------------------------------
    # 25. COD settlement returns list
    # ------------------------------------------------------------------

    @patch("requests.Session.get")
    def test_get_cod_settlement_returns_list(self, mock_get):
        mock_get.return_value = _mock_response(200, {
            "status": 1,
            "packet_list": [
                {"booked_packet_cn": "LEP-001", "booked_packet_collect_amount": "1800"},
                {"booked_packet_cn": "LEP-002", "booked_packet_collect_amount": "3200"},
            ]
        })

        records = self.client.get_cod_settlement("2026-09-01", "2026-09-30")

        self.assertIsInstance(records, list)
        self.assertEqual(len(records), 2)
        called_url = mock_get.call_args[0][0]
        self.assertIn("get_packets_cod_details", called_url)

    # ------------------------------------------------------------------
    # 26. HTTP 401 → CourierAuthError
    # ------------------------------------------------------------------

    @patch("requests.Session.get")
    def test_http_401_raises_auth_error(self, mock_get):
        mock_get.return_value = _mock_response(401, {})
        with self.assertRaises(CourierAuthError):
            self.client.track_shipment("LEP-CN")
        self.assertEqual(mock_get.call_count, 1)

    # ------------------------------------------------------------------
    # 27. HTTP 429 → CourierRateLimitError after retries
    # ------------------------------------------------------------------

    @patch("time.sleep")
    @patch("requests.Session.get")
    def test_http_429_raises_rate_limit_after_retries(self, mock_get, mock_sleep):
        mock_get.return_value = _mock_response(429, {})
        with self.assertRaises(CourierRateLimitError):
            self.client.track_shipment("LEP-CN")
        self.assertEqual(mock_get.call_count, 3)

    # ------------------------------------------------------------------
    # 28. HTTP 5xx → retry and recover
    # ------------------------------------------------------------------

    @patch("time.sleep")
    @patch("requests.Session.get")
    def test_http_5xx_retries_then_succeeds(self, mock_get, mock_sleep):
        err = _mock_response(503, {})
        ok = _mock_response(200, {
            "status": 1,
            "packet_list": [{"booked_packet_cn": "LEP-CN"}]
        })
        mock_get.side_effect = [err, ok]

        result = self.client.track_shipment("LEP-CN")
        self.assertEqual(result["status"], 1)
        self.assertEqual(mock_get.call_count, 2)

    # ------------------------------------------------------------------
    # 29. Business error status=0 generic → CourierAPIError
    # ------------------------------------------------------------------

    @patch("requests.Session.get")
    def test_business_error_status_0_raises_api_error(self, mock_get):
        mock_get.return_value = _mock_response(200, {
            "status": 0,
            "error": "The requested resource is unavailable",
        })
        with self.assertRaises(CourierAPIError):
            self.client.track_shipment("LEP-CN")

    # ------------------------------------------------------------------
    # 30. Business error status=0 with 'invalid credentials' → CourierAuthError
    # ------------------------------------------------------------------

    @patch("requests.Session.get")
    def test_business_error_invalid_credentials_raises_auth_error(self, mock_get):
        mock_get.return_value = _mock_response(200, {
            "status": 0,
            "error": "Invalid credentials, please check your API key",
        })
        with self.assertRaises(CourierAuthError):
            self.client.track_shipment("LEP-CN")

    # ------------------------------------------------------------------
    # 31. Business error status=0 with 'not found' → CourierTrackingNotFoundError
    # ------------------------------------------------------------------

    @patch("requests.Session.get")
    def test_business_error_not_found_raises_tracking_not_found(self, mock_get):
        mock_get.return_value = _mock_response(200, {
            "status": 0,
            "error": "No record found for the provided tracking number",
        })
        with self.assertRaises(CourierTrackingNotFoundError):
            self.client.track_shipment("GHOST-LEP")

    # ------------------------------------------------------------------
    # 32. Credential safety
    # ------------------------------------------------------------------

    def test_repr_does_not_expose_api_password(self):
        repr_str = repr(self.client)
        self.assertNotIn(self.api_password, repr_str)
        self.assertIn("***", repr_str)
        self.assertIn("leopards", repr_str)

    def test_missing_api_key_raises_value_error(self):
        with self.assertRaises(ValueError):
            LeopardsClient(api_key="", api_password="pass")

    def test_missing_api_password_raises_value_error(self):
        with self.assertRaises(ValueError):
            LeopardsClient(api_key="key", api_password="")

    def test_empty_reference_raises_value_error(self):
        with self.assertRaises(ValueError):
            self.client.get_order_status("")


# ===========================================================================
#  Base / common contract tests
# ===========================================================================

class TestBaseCourierContract(unittest.TestCase):

    def test_postex_is_base_courier_client(self):
        """PostExClient must satisfy the BaseCourierClient interface."""
        client = PostExClient(api_token="dummy_token")
        self.assertIsInstance(client, BaseCourierClient)

    def test_trax_is_base_courier_client(self):
        client = TraxClient(username="u", password="p")
        self.assertIsInstance(client, BaseCourierClient)

    def test_leopards_is_base_courier_client(self):
        client = LeopardsClient(api_key="k", api_password="p")
        self.assertIsInstance(client, BaseCourierClient)

    def test_postex_unsupported_history_raises_not_implemented(self):
        """PostEx does not expose a verified history endpoint — must raise."""
        client = PostExClient(api_token="dummy_token")
        with self.assertRaises(NotImplementedError):
            client.get_tracking_history("ANY-CN")

    def test_leopards_unsupported_history_raises_not_implemented(self):
        """Leopards does not expose a verified history endpoint — must raise."""
        client = LeopardsClient(api_key="k", api_password="p")
        with self.assertRaises(NotImplementedError):
            client.get_tracking_history("ANY-CN")

    def test_default_repr_is_credential_safe(self):
        """BaseCourierClient default __repr__ should not expose secrets."""
        client = PostExClient(api_token="super_secret_token_xyz")
        self.assertNotIn("super_secret_token_xyz", repr(client))


if __name__ == "__main__":
    unittest.main(verbosity=2)
