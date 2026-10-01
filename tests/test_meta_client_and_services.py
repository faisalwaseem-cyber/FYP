"""
Unit Test Suite for Meta Marketing API Client & Services (Phase 4.2).

Validates:
1. Successful request execution (GET & POST)
2. Authentication (Bearer header, appsecret_proof HMAC-SHA256, 401/190 errors, CredentialManager tenant resolution)
3. Cursor-based pagination (multi-page, max_pages cutoff, empty data termination)
4. Rate limiting (HTTP 429, error codes 17/80004/613, X-Business-Use-Case-Usage parsing)
5. API error handling (MetaAPIError attributes, transient 5xx & network timeout retries)
6. Insights & entity services retrieval (Insights, Campaigns, Ad Sets, Ads)
7. Secret safety (token masking in __repr__ and exception strings)
"""
import hashlib
import hmac
import json
import sys
import unittest
from pathlib import Path
from unittest.mock import MagicMock, patch

import requests

# Ensure project root is on Python path
sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from connectors.meta.client import (
    META_API_VERSION,
    MetaAPIError,
    MetaAuthenticationError,
    MetaClient,
    MetaRateLimitError,
)
from connectors.meta.services import (
    MetaAdService,
    MetaAdSetService,
    MetaCampaignService,
    MetaInsightsService,
)


class TestMetaClientAndServices(unittest.TestCase):
    def setUp(self):
        self.access_token = "EAABtest_secret_meta_token_1234567890"
        self.ad_account_id = "120205849302840"
        self.app_secret = "test_meta_app_secret_abc123"
        self.client = MetaClient(
            access_token=self.access_token,
            ad_account_id=self.ad_account_id,
            app_secret=self.app_secret,
            api_version="v26.0",
            timeout=5.0,
            max_retries=2,
        )

    # ------------------------------------------------------------------
    # 1. Successful Request
    # ------------------------------------------------------------------

    @patch("requests.Session.request")
    def test_successful_get_request(self, mock_request):
        mock_resp = MagicMock()
        mock_resp.ok = True
        mock_resp.status_code = 200
        mock_resp.headers = requests.structures.CaseInsensitiveDict()
        mock_resp.json.return_value = {
            "data": [
                {"id": "camp_001", "name": "Summer Lawn Sale", "status": "ACTIVE"}
            ]
        }
        mock_request.return_value = mock_resp

        result = self.client.get(f"{self.client.ad_account_id}/campaigns")
        self.assertIn("data", result)
        self.assertEqual(len(result["data"]), 1)
        self.assertEqual(result["data"][0]["id"], "camp_001")
        mock_request.assert_called_once()

    @patch("requests.Session.request")
    def test_successful_post_request(self, mock_request):
        mock_resp = MagicMock()
        mock_resp.ok = True
        mock_resp.status_code = 200
        mock_resp.headers = requests.structures.CaseInsensitiveDict()
        mock_resp.json.return_value = {"success": True, "id": "12345"}
        mock_request.return_value = mock_resp

        result = self.client.post("act_120205849302840/budget", json_body={"daily_budget": 5000})
        self.assertTrue(result.get("success"))
        mock_request.assert_called_once()

    # ------------------------------------------------------------------
    # 2. Authentication
    # ------------------------------------------------------------------

    def test_bearer_authorization_header(self):
        auth_header = self.client.session.headers.get("Authorization")
        self.assertEqual(auth_header, f"Bearer {self.access_token}")

    def test_appsecret_proof_generation(self):
        expected_proof = hmac.new(
            self.app_secret.encode("utf-8"),
            self.access_token.encode("utf-8"),
            hashlib.sha256,
        ).hexdigest()
        computed_proof = self.client.get_appsecret_proof()
        self.assertEqual(computed_proof, expected_proof)

    @patch("requests.Session.request")
    def test_appsecret_proof_injected_in_request(self, mock_request):
        mock_resp = MagicMock()
        mock_resp.ok = True
        mock_resp.status_code = 200
        mock_resp.headers = requests.structures.CaseInsensitiveDict()
        mock_resp.json.return_value = {"data": []}
        mock_request.return_value = mock_resp

        self.client.get(f"{self.client.ad_account_id}/campaigns", params={"limit": 10})

        # Check call arguments
        _, kwargs = mock_request.call_args
        params = kwargs.get("params", {})
        self.assertIn("appsecret_proof", params)
        self.assertEqual(params["appsecret_proof"], self.client.get_appsecret_proof())

    @patch("requests.Session.request")
    def test_authentication_error_code_190(self, mock_request):
        mock_resp = MagicMock()
        mock_resp.ok = False
        mock_resp.status_code = 400
        mock_resp.headers = requests.structures.CaseInsensitiveDict()
        mock_resp.json.return_value = {
            "error": {
                "message": "Error validating access token: Session has expired.",
                "type": "OAuthException",
                "code": 190,
                "error_subcode": 463,
                "fbtrace_id": "trace_auth_123",
            }
        }
        mock_request.return_value = mock_resp

        with self.assertRaises(MetaAuthenticationError) as ctx:
            self.client.get(f"{self.client.ad_account_id}/campaigns")

        self.assertEqual(ctx.exception.code, 190)
        self.assertEqual(ctx.exception.error_subcode, 463)
        self.assertEqual(ctx.exception.fbtrace_id, "trace_auth_123")

    @patch("requests.Session.request")
    def test_authentication_error_http_401(self, mock_request):
        mock_resp = MagicMock()
        mock_resp.ok = False
        mock_resp.status_code = 401
        mock_resp.headers = requests.structures.CaseInsensitiveDict()
        mock_resp.json.return_value = {
            "error": {
                "message": "Invalid OAuth access token.",
                "type": "OAuthException",
                "code": 190,
            }
        }
        mock_request.return_value = mock_resp

        with self.assertRaises(MetaAuthenticationError):
            self.client.get(f"{self.client.ad_account_id}/campaigns")

    def test_tenant_resolution_via_credential_manager(self):
        mock_cm = MagicMock()
        mock_cm.get_valid_credentials.return_value = {
            "access_token": "resolved_tenant_token_abc",
            "account_identifier": "act_999888777",
            "additional_credentials": {
                "app_secret": "resolved_secret_xyz",
            },
        }

        tenant_client = MetaClient.for_tenant(
            business_id=42,
            account_identifier="act_999888777",
            credential_manager=mock_cm,
        )

        mock_cm.get_valid_credentials.assert_called_once_with(
            business_id=42,
            platform="meta",
            account_identifier="act_999888777",
        )
        self.assertEqual(tenant_client.ad_account_id, "act_999888777")
        self.assertEqual(tenant_client.api_version, "v26.0")
        self.assertEqual(tenant_client._access_token, "resolved_tenant_token_abc")
        self.assertEqual(tenant_client._app_secret, "resolved_secret_xyz")

    # ------------------------------------------------------------------
    # 3. Pagination
    # ------------------------------------------------------------------

    @patch.object(MetaClient, "get")
    def test_cursor_pagination_multiple_pages(self, mock_get):
        page1 = {
            "data": [{"id": "camp_1", "name": "Campaign 1"}],
            "paging": {
                "cursors": {"after": "cursor_page_2"},
                "next": "https://graph.facebook.com/v26.0/act_123/campaigns?after=cursor_page_2",
            },
        }
        page2 = {
            "data": [{"id": "camp_2", "name": "Campaign 2"}],
            "paging": {"cursors": {"after": "cursor_page_3"}},
        }
        page3 = {
            "data": [],
            "paging": {},
        }
        mock_get.side_effect = [page1, page2, page3]

        pages = list(self.client.paginate("act_120205849302840/campaigns", limit=1))
        self.assertEqual(len(pages), 3)
        self.assertEqual(pages[0]["count"], 1)
        self.assertEqual(pages[1]["count"], 1)
        self.assertEqual(pages[2]["count"], 0)
        self.assertEqual(pages[0]["data"][0]["id"], "camp_1")
        self.assertEqual(pages[1]["data"][0]["id"], "camp_2")

    @patch.object(MetaClient, "get")
    def test_cursor_pagination_max_pages_cutoff(self, mock_get):
        mock_get.return_value = {
            "data": [{"id": "camp_1"}],
            "paging": {"cursors": {"after": "next_cursor"}},
        }
        pages = list(self.client.paginate("act_120205849302840/campaigns", limit=10, max_pages=1))
        self.assertEqual(len(pages), 1)
        self.assertEqual(mock_get.call_count, 1)

    @patch.object(MetaClient, "get")
    def test_cursor_pagination_empty_data(self, mock_get):
        mock_get.return_value = {"data": [], "paging": {}}
        pages = list(self.client.paginate("act_120205849302840/campaigns"))
        self.assertEqual(len(pages), 1)
        self.assertEqual(pages[0]["count"], 0)
        self.assertEqual(pages[0]["data"], [])

    # ------------------------------------------------------------------
    # 4. Rate Limiting
    # ------------------------------------------------------------------

    @patch("requests.Session.request")
    def test_rate_limit_http_429(self, mock_request):
        mock_resp = MagicMock()
        mock_resp.ok = False
        mock_resp.status_code = 429
        mock_resp.headers = requests.structures.CaseInsensitiveDict({
            "x-business-use-case-usage": json.dumps({
                "120205849302840": [
                    {
                        "type": "ads_management",
                        "call_count": 100,
                        "total_cputime": 100,
                        "total_time": 95,
                        "estimated_time_to_regain_access": 5,
                    }
                ]
            })
        })
        mock_resp.json.return_value = {
            "error": {
                "message": "User request limit reached",
                "code": 17,
                "error_subcode": 2446079,
            }
        }
        mock_request.return_value = mock_resp

        with self.assertRaises(MetaRateLimitError) as ctx:
            self.client.get(f"{self.client.ad_account_id}/campaigns")

        self.assertEqual(ctx.exception.code, 17)
        self.assertEqual(ctx.exception.estimated_time_to_regain_access, 5)
        self.assertIn("business_use_case", ctx.exception.usage_info)

    @patch("requests.Session.request")
    def test_rate_limit_error_codes(self, mock_request):
        for code in (4, 17, 613, 80004):
            mock_resp = MagicMock()
            mock_resp.ok = False
            mock_resp.status_code = 400
            mock_resp.headers = requests.structures.CaseInsensitiveDict()
            mock_resp.json.return_value = {
                "error": {
                    "message": f"Calls to this api have exceeded rate limit (code {code})",
                    "code": code,
                }
            }
            mock_request.return_value = mock_resp

            with self.assertRaises(MetaRateLimitError):
                self.client.get(f"{self.client.ad_account_id}/campaigns")

    def test_rate_limit_header_parsing(self):
        headers = requests.structures.CaseInsensitiveDict({
            "x-business-use-case-usage": '{"act_123": [{"call_count": 80, "total_time": 70, "estimated_time_to_regain_access": 0}]}',
            "x-app-usage": '{"call_count": 45, "total_cputime": 20, "total_time": 30}',
            "x-ad-account-usage": '{"acc_id_util_pct": 55}',
        })
        usage = MetaClient.parse_rate_limit_headers(headers)
        self.assertIn("business_use_case", usage)
        self.assertIn("app_usage", usage)
        self.assertIn("ad_account_usage", usage)
        self.assertEqual(usage["app_usage"]["call_count"], 45)

    # ------------------------------------------------------------------
    # 5. API Error & Retry Handling
    # ------------------------------------------------------------------

    @patch("requests.Session.request")
    def test_api_error_attributes(self, mock_request):
        mock_resp = MagicMock()
        mock_resp.ok = False
        mock_resp.status_code = 400
        mock_resp.headers = requests.structures.CaseInsensitiveDict()
        mock_resp.json.return_value = {
            "error": {
                "message": "Invalid parameter: time_range",
                "type": "OAuthException",
                "code": 100,
                "error_subcode": 1487390,
                "fbtrace_id": "trace_xyz_999",
            }
        }
        mock_request.return_value = mock_resp

        with self.assertRaises(MetaAPIError) as ctx:
            self.client.get(f"{self.client.ad_account_id}/insights")

        self.assertEqual(ctx.exception.code, 100)
        self.assertEqual(ctx.exception.error_subcode, 1487390)
        self.assertEqual(ctx.exception.fbtrace_id, "trace_xyz_999")
        self.assertIn("Invalid parameter: time_range", str(ctx.exception))

    @patch("time.sleep", return_value=None)
    @patch("requests.Session.request")
    def test_transient_5xx_retry_and_recovery(self, mock_request, mock_sleep):
        mock_503 = MagicMock()
        mock_503.ok = False
        mock_503.status_code = 503
        mock_503.headers = requests.structures.CaseInsensitiveDict()
        mock_503.json.return_value = {"error": {"message": "Service Temporarily Unavailable", "code": 2}}

        mock_200 = MagicMock()
        mock_200.ok = True
        mock_200.status_code = 200
        mock_200.headers = requests.structures.CaseInsensitiveDict()
        mock_200.json.return_value = {"data": [{"id": "camp_ok"}]}

        mock_request.side_effect = [mock_503, mock_200]

        result = self.client.get(f"{self.client.ad_account_id}/campaigns")
        self.assertEqual(result["data"][0]["id"], "camp_ok")
        self.assertEqual(mock_request.call_count, 2)
        mock_sleep.assert_called_once()

    @patch("time.sleep", return_value=None)
    @patch("requests.Session.request")
    def test_network_timeout_retry_and_recovery(self, mock_request, mock_sleep):
        mock_200 = MagicMock()
        mock_200.ok = True
        mock_200.status_code = 200
        mock_200.headers = requests.structures.CaseInsensitiveDict()
        mock_200.json.return_value = {"data": [{"id": "camp_timeout_recovered"}]}

        mock_request.side_effect = [requests.exceptions.Timeout("Connection timed out"), mock_200]

        result = self.client.get(f"{self.client.ad_account_id}/campaigns")
        self.assertEqual(result["data"][0]["id"], "camp_timeout_recovered")
        self.assertEqual(mock_request.call_count, 2)
        mock_sleep.assert_called_once()

    # ------------------------------------------------------------------
    # 6. Insights & Services Retrieval
    # ------------------------------------------------------------------

    @patch.object(MetaClient, "paginate")
    def test_insights_service_retrieval(self, mock_paginate):
        mock_paginate.return_value = iter([
            {
                "page": 1,
                "count": 2,
                "data": [
                    {
                        "campaign_id": "120205849302840",
                        "campaign_name": "Eid Lawn Launch",
                        "spend": "4500.00",
                        "impressions": "18200",
                        "clicks": "620",
                        "actions": [{"action_type": "purchase", "value": "14"}],
                        "action_values": [{"action_type": "purchase", "value": "49000.00"}],
                    }
                ],
                "paging": {},
                "raw_response": {},
            }
        ])

        insights_service = MetaInsightsService(self.client)
        pages = list(insights_service.fetch_insights(
            level="campaign",
            since="2026-09-01",
            until="2026-09-30",
            time_increment=1,
            action_attribution_windows=["1d_click", "7d_click", "1d_view"],
        ))

        self.assertEqual(len(pages), 1)
        self.assertEqual(pages[0]["count"], 2)
        record = pages[0]["insights"][0]
        self.assertEqual(record["spend"], "4500.00")
        self.assertEqual(record["impressions"], "18200")
        self.assertEqual(record["clicks"], "620")
        self.assertEqual(record["actions"][0]["value"], "14")
        self.assertEqual(record["action_values"][0]["value"], "49000.00")

        # Verify paginate params
        mock_paginate.assert_called_once()
        endpoint, kwargs = mock_paginate.call_args[0][0], mock_paginate.call_args[1]
        self.assertEqual(endpoint, f"{self.client.ad_account_id}/insights")
        self.assertIn("time_range", kwargs["params"])
        self.assertEqual(kwargs["params"]["time_increment"], "1")

    @patch.object(MetaClient, "paginate")
    def test_campaign_service_fetch(self, mock_paginate):
        mock_paginate.return_value = iter([
            {
                "page": 1,
                "count": 1,
                "data": [{"id": "camp_123", "name": "Brand Awareness", "status": "ACTIVE"}],
                "paging": {},
                "raw_response": {},
            }
        ])

        campaign_service = MetaCampaignService(self.client)
        pages = list(campaign_service.fetch_campaigns(status="ACTIVE"))
        self.assertEqual(len(pages), 1)
        self.assertEqual(pages[0]["campaigns"][0]["id"], "camp_123")

    @patch.object(MetaClient, "paginate")
    def test_adset_and_ad_services_fetch(self, mock_paginate):
        mock_paginate.side_effect = lambda *args, **kwargs: iter([
            {
                "page": 1,
                "count": 1,
                "data": [{"id": "node_1"}],
                "paging": {},
                "raw_response": {},
            }
        ])

        adset_service = MetaAdSetService(self.client)
        adset_pages = list(adset_service.fetch_adsets(campaign_id="camp_123"))
        self.assertEqual(len(adset_pages), 1)
        self.assertEqual(adset_pages[0]["adsets"][0]["id"], "node_1")

        ad_service = MetaAdService(self.client)
        ad_pages = list(ad_service.fetch_ads(adset_id="adset_123"))
        self.assertEqual(len(ad_pages), 1)
        self.assertEqual(ad_pages[0]["ads"][0]["id"], "node_1")

    # ------------------------------------------------------------------
    # 7. Secret Safety
    # ------------------------------------------------------------------

    def test_repr_masks_access_token_and_secret(self):
        client_repr = repr(self.client)
        client_str = str(self.client)

        self.assertNotIn(self.access_token, client_repr)
        self.assertNotIn(self.app_secret, client_repr)
        self.assertNotIn(self.access_token, client_str)
        self.assertNotIn(self.app_secret, client_str)

        self.assertIn("token='***'", client_repr)
        self.assertIn("act_120205849302840", client_repr)
        self.assertIn("v26.0", client_repr)

    def test_exception_does_not_leak_access_token(self):
        err = MetaAPIError(
            message="An error occurred",
            code=190,
            error_subcode=463,
            fbtrace_id="trace_test",
        )
        err_msg = str(err)
        self.assertNotIn(self.access_token, err_msg)
        self.assertNotIn(self.app_secret, err_msg)
        self.assertIn("code=190", err_msg)


if __name__ == "__main__":
    unittest.main()
