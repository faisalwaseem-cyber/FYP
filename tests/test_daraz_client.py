"""Focused unit tests for the Daraz Open Platform client."""
import hashlib
import hmac
import sys
import unittest
from pathlib import Path
from unittest.mock import Mock, patch

import requests

# Ensure root directory is on Python path
sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from connectors.daraz.client import (
    DARAZ_API_BASE_URL,
    DarazAPIError,
    DarazClient,
    DarazHTTPError,
    DarazTimeoutError,
    build_signing_string,
    generate_signature,
)


class TestDarazSigning(unittest.TestCase):
    def test_official_signature_example(self):
        params = {"foo": "1", "bar": "2", "foo_bar": "3", "foobar": "4"}
        canonical = "/test/apibar2foo1foo_bar3foobar4"
        expected = "FE01B44E2D0F8A799FDC350C3EDDA02F3BFCE762E60DA58768964FF1CD949437"

        self.assertEqual(build_signing_string("/test/api", params), canonical)
        self.assertEqual(generate_signature("/test/api", params, "test-secret"), expected)

    def test_ascii_parameter_sorting(self):
        params = {"foobar": "4", "foo_bar": "3", "foo": "1", "bar": "2"}
        self.assertEqual(build_signing_string("/test/api", params), "/test/apibar2foo1foo_bar3foobar4")

    def test_excludes_sign_and_empty_parameters(self):
        params = {"z": "last", "sign": "ignored", "empty": "", "none": None, "a": "first"}
        self.assertEqual(build_signing_string("/test", params), "/testafirstzlast")

    def test_excludes_byte_array_parameters(self):
        self.assertEqual(build_signing_string("/test", {"blob": b"private", "q": "v"}), "/testqv")

    def test_includes_access_token_when_present(self):
        self.assertEqual(
            build_signing_string("/orders/get", {"access_token": "seller-token"}),
            "/orders/getaccess_tokenseller-token",
        )

    def test_appends_body_string(self):
        self.assertEqual(
            build_signing_string("/product/create", {"app_key": "123"}, "<Request/>"),
            "/product/createapp_key123<Request/>",
        )

    def test_signature_is_uppercase_hex_hmac_sha256(self):
        signature = generate_signature("/test", {"a": "1"}, "secret")
        expected = hmac.new(b"secret", b"/testa1", hashlib.sha256).hexdigest().upper()
        self.assertEqual(signature, expected)
        self.assertRegex(signature, r"^[0-9A-F]{64}$")


class TestDarazClient(unittest.TestCase):
    def setUp(self):
        self.session = Mock()
        self.client = DarazClient(
            app_key="123456",
            app_secret="test-secret",
            access_token="seller-token",
            session=self.session,
        )

    @staticmethod
    def response(payload=None, status_code=200):
        response = Mock()
        response.status_code = status_code
        response.json.return_value = payload if payload is not None else {"code": "0", "data": {}}
        return response

    def test_request_construction_uses_verified_common_parameters(self):
        self.session.request.return_value = self.response({"code": "0", "data": {"ok": True}})
        with patch("connectors.daraz.client.time.time", return_value=1700000000):
            result = self.client.execute("/orders/get", {"status": "shipped"})

        self.assertEqual(result, {"ok": True})
        call = self.session.request.call_args.kwargs
        self.assertEqual(call["method"], "GET")
        self.assertEqual(call["url"], f"{DARAZ_API_BASE_URL}/orders/get")
        self.assertEqual(call["timeout"], 30.0)
        params = call["params"]
        self.assertEqual(params["app_key"], "123456")
        self.assertEqual(params["timestamp"], "1700000000000")
        self.assertEqual(params["access_token"], "seller-token")
        self.assertEqual(params["sign_method"], "hmac")
        self.assertEqual(params["status"], "shipped")
        self.assertEqual(
            params["sign"],
            generate_signature("/orders/get", {key: value for key, value in params.items() if key != "sign"}, "test-secret"),
        )

    def test_rejects_client_supplied_business_id(self):
        with self.assertRaisesRegex(ValueError, "business_id"):
            self.client.execute("/orders/get", {"business_id": 42})
        self.session.request.assert_not_called()

    def test_missing_credentials_are_rejected_without_echoing_values(self):
        with self.assertRaisesRegex(ValueError, "App Key"):
            DarazClient("", "secret-value")
        with self.assertRaisesRegex(ValueError, "App Secret"):
            DarazClient("app-key", "")

    def test_builds_client_from_managed_credentials(self):
        client = DarazClient.from_managed_credentials({
            "auth_type": "oauth2",
            "access_token": "managed-token",
            "additional_credentials": {"app_key": "managed-key", "app_secret": "managed-secret"},
        })
        self.assertEqual(client._app_key, "managed-key")
        self.assertEqual(client._access_token, "managed-token")
        self.assertNotIn("managed-secret", repr(client))

    def test_http_errors_do_not_expose_response_body(self):
        self.session.request.return_value = self.response(status_code=500)
        with self.assertRaises(DarazHTTPError) as caught:
            self.client.execute("/orders/get")
        self.assertEqual(caught.exception.status_code, 500)
        self.assertNotIn("seller-token", str(caught.exception))
        self.assertNotIn("test-secret", str(caught.exception))

    def test_api_errors_do_not_expose_provider_message(self):
        self.session.request.return_value = self.response({
            "code": "InvalidToken",
            "message": "seller-token test-secret",
        })
        with self.assertRaises(DarazAPIError) as caught:
            self.client.execute("/orders/get")
        self.assertNotIn("seller-token", str(caught.exception))
        self.assertNotIn("test-secret", str(caught.exception))

    def test_timeout_does_not_expose_request_details(self):
        self.session.request.side_effect = requests.Timeout("seller-token test-secret in request URL")
        with self.assertRaises(DarazTimeoutError) as caught:
            self.client.execute("/orders/get")
        self.assertNotIn("seller-token", str(caught.exception))
        self.assertNotIn("test-secret", str(caught.exception))

    def test_token_endpoints_omit_existing_access_token(self):
        self.session.request.return_value = self.response({"code": "0", "data": {"access_token": "new-token"}})
        self.client.create_access_token("authorization-code")
        params = self.session.request.call_args.kwargs["params"]
        self.assertEqual(params["code"], "authorization-code")
        self.assertNotIn("access_token", params)

    def test_order_pagination_uses_offset_and_limit(self):
        first = self.response({"code": "0", "data": {"orders": [{"id": 1}, {"id": 2}]}})
        second = self.response({"code": "0", "data": {"orders": [{"id": 3}]}})
        self.session.request.side_effect = [first, second]

        pages = list(self.client.paginate("/orders/get", {"created_after": "2025-01-01"}, page_size=2))

        self.assertEqual([page["offset"] for page in pages], [0, 2])
        self.assertEqual([page["count"] for page in pages], [2, 1])
        self.assertEqual(self.session.request.call_args_list[0].kwargs["params"]["limit"], 2)
        self.assertEqual(self.session.request.call_args_list[1].kwargs["params"]["offset"], 2)

    def test_product_pagination_uses_documented_limit_and_date_filter(self):
        self.session.request.return_value = self.response({"code": "0", "data": {"products": [{"item_id": "1"}]}})
        pages = list(self.client.paginate(
            "/products/get",
            {"update_after": "2025-01-01T00:00:00+00:00"},
        ))
        params = self.session.request.call_args.kwargs["params"]
        self.assertEqual(params["limit"], 50)
        self.assertEqual(params["filter"], "all")
        self.assertEqual(params["update_after"], "2025-01-01T00:00:00+00:00")
        self.assertEqual(pages[0]["records"], [{"item_id": "1"}])

    def test_page_size_limits_are_enforced(self):
        with self.assertRaises(ValueError):
            list(self.client.paginate("/orders/get", page_size=101))
        with self.assertRaises(ValueError):
            list(self.client.paginate("/products/get", page_size=51))

    def test_credential_values_are_not_repr_or_exception_text(self):
        rendered = repr(self.client)
        self.assertNotIn("123456", rendered)
        self.assertNotIn("test-secret", rendered)
        self.assertNotIn("seller-token", rendered)


if __name__ == "__main__":
    unittest.main()
