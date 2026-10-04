"""Focused mocked tests for the Phase 4.2 Meta Ads client and services."""

import json
import unittest
from unittest.mock import MagicMock

from connectors.meta_ads import (
    MetaAPIError,
    MetaAdAccountService,
    MetaAdService,
    MetaAdSetService,
    MetaAdsClient,
    MetaAuthenticationError,
    MetaCampaignService,
    MetaInsightsService,
)


ACCESS_TOKEN = "meta-test-token-never-log"


class FakeResponse:
    def __init__(self, payload, status_code=200):
        self._payload = payload
        self.status_code = status_code

    def json(self):
        return self._payload


class TestPhase42MetaAdsClient(unittest.TestCase):
    def setUp(self):
        self.credentials = MagicMock()
        self.credentials.get_valid_credentials.return_value = {
            "access_token": ACCESS_TOKEN
        }
        self.session = MagicMock()
        self.connection_lookup = MagicMock(
            return_value={"connection_id": 42}
        )
        self.status_updater = MagicMock()
        self.client = MetaAdsClient(
            business_id=71,
            account_identifier="act_123456",
            credential_manager=self.credentials,
            session=self.session,
            connection_lookup=self.connection_lookup,
            status_updater=self.status_updater,
        )

    def respond(self, payload, status_code=200):
        self.session.get.return_value = FakeResponse(payload, status_code)

    def test_successful_request_uses_authenticated_header(self):
        self.respond({"data": [{"id": "campaign-1"}], "paging": {}})
        result = self.client.fetch_campaigns()
        self.assertEqual(result, [{"id": "campaign-1"}])
        self.credentials.get_valid_credentials.assert_called_once_with(
            business_id=71,
            platform="meta_ads",
            account_identifier="act_123456",
            refresh_callback=None,
        )
        self.assertEqual(
            self.session.get.call_args.kwargs["headers"]["Authorization"],
            f"Bearer {ACCESS_TOKEN}",
        )
        self.assertIn("/v25.0/act_123456/campaigns", self.session.get.call_args.args[0])

    def test_api_error_is_safe(self):
        self.respond(
            {"error": {"message": ACCESS_TOKEN, "code": 100}},
            status_code=400,
        )
        with self.assertRaises(MetaAPIError) as raised:
            self.client.fetch_campaigns()
        self.assertEqual(raised.exception.api_code, 100)
        self.assertNotIn(ACCESS_TOKEN, str(raised.exception))

    def test_invalid_token_marks_connection_needs_reauth(self):
        self.respond(
            {"error": {"message": "Invalid token", "code": 190, "error_subcode": 463}},
            status_code=400,
        )
        with self.assertRaises(MetaAuthenticationError):
            self.client.fetch_campaigns()
        self.connection_lookup.assert_called_once_with(71, "meta_ads", "act_123456")
        self.status_updater.assert_called_once_with(
            42,
            "needs_reauth",
            "Meta rejected the access token; reauthentication is required.",
        )

    def test_expired_credentials_do_not_trigger_refresh(self):
        self.credentials.get_valid_credentials.side_effect = ValueError(
            "Access token is expired and no refresh_callback was provided."
        )
        with self.assertRaises(MetaAuthenticationError):
            self.client.fetch_campaigns()
        self.credentials.get_valid_credentials.assert_called_once()
        self.assertIsNone(
            self.credentials.get_valid_credentials.call_args.kwargs["refresh_callback"]
        )
        self.status_updater.assert_called_once()
        self.assertEqual(self.status_updater.call_args.args[1], "needs_reauth")
        self.session.get.assert_not_called()

    def test_pagination_uses_cursor_without_following_provider_url(self):
        self.session.get.side_effect = [
            FakeResponse(
                {
                    "data": [{"id": "campaign-1"}],
                    "paging": {
                        "next": "https://evil.example/?after=cursor-2&access_token=leak",
                        "cursors": {"after": "cursor-2"},
                    },
                }
            ),
            FakeResponse({"data": [{"id": "campaign-2"}], "paging": {}}),
        ]
        self.assertEqual(
            self.client.fetch_campaigns(),
            [{"id": "campaign-1"}, {"id": "campaign-2"}],
        )
        self.assertEqual(self.session.get.call_count, 2)
        self.assertEqual(self.session.get.call_args.kwargs["params"]["after"], "cursor-2")
        self.assertTrue(
            self.session.get.call_args.args[0].startswith("https://graph.facebook.com/")
        )

    def test_accessible_ad_account_retrieval(self):
        expected = [{"id": "act_123456", "account_id": "123456", "name": "Store"}]
        self.respond({"data": expected, "paging": {}})
        self.assertEqual(MetaAdAccountService(self.client).fetch_accounts(), expected)
        self.assertIn("/me/adaccounts", self.session.get.call_args.args[0])

    def test_campaign_retrieval(self):
        self.respond({"data": [{"id": "campaign-1"}], "paging": {}})
        self.assertEqual(MetaCampaignService(self.client).fetch_campaigns(), [{"id": "campaign-1"}])

    def test_ad_set_retrieval(self):
        self.respond({"data": [{"id": "adset-1"}], "paging": {}})
        result = MetaAdSetService(self.client).fetch_ad_sets(campaign_id="campaign-1")
        self.assertEqual(result, [{"id": "adset-1"}])
        self.assertIn("/campaign-1/adsets", self.session.get.call_args.args[0])

    def test_ad_retrieval(self):
        self.respond({"data": [{"id": "ad-1"}], "paging": {}})
        result = MetaAdService(self.client).fetch_ads(ad_set_id="adset-1")
        self.assertEqual(result, [{"id": "ad-1"}])
        self.assertIn("/adset-1/ads", self.session.get.call_args.args[0])

    def test_insights_retrieval_preserves_meta_attributed_fields(self):
        row = {
            "spend": "10.00",
            "action_values": [{"action_type": "purchase", "value": "20"}],
            "purchase_roas": [{"action_type": "purchase", "value": "2"}],
        }
        self.respond({"data": [row], "paging": {}})
        self.assertEqual(
            MetaInsightsService(self.client).fetch_insights(
                level="campaign", since="2026-09-01", until="2026-09-30"
            ),
            [row],
        )
        self.assertEqual(self.session.get.call_args.kwargs["params"]["level"], "campaign")
        self.assertIn("action_values", self.session.get.call_args.kwargs["params"]["fields"])
        self.assertIn("purchase_roas", self.session.get.call_args.kwargs["params"]["fields"])

    def test_insights_date_filtering(self):
        self.respond({"data": [], "paging": {}})
        self.client.fetch_insights(
            level="account", since="2026-09-01", until="2026-09-30", time_increment=1
        )
        params = self.session.get.call_args.kwargs["params"]
        self.assertEqual(
            json.loads(params["time_range"]),
            {"since": "2026-09-01", "until": "2026-09-30"},
        )
        self.assertEqual(params["time_increment"], 1)

    def test_insights_level_handling(self):
        self.respond({"data": [], "paging": {}})
        for level in ("account", "campaign", "adset", "ad"):
            self.client.fetch_insights(
                level=level, since="2026-09-01", until="2026-09-02"
            )
            self.assertEqual(self.session.get.call_args.kwargs["params"]["level"], level)
        with self.assertRaises(ValueError):
            self.client.fetch_insights(
                level="business", since="2026-09-01", until="2026-09-02"
            )

    def test_business_context_is_forwarded_from_server_side_client(self):
        self.respond({"data": [], "paging": {}})
        self.client.fetch_campaigns()
        call_kwargs = self.credentials.get_valid_credentials.call_args.kwargs
        self.assertEqual(call_kwargs["business_id"], 71)
        self.assertNotIn("business_id", self.session.get.call_args.kwargs["params"])
        with self.assertRaises(ValueError):
            MetaAdsClient(
                business_id="71",
                account_identifier="act_123456",
                credential_manager=self.credentials,
                session=self.session,
            )

    def test_credential_safety_in_repr_and_errors(self):
        self.respond({"error": {"message": ACCESS_TOKEN, "code": 100}}, 400)
        self.assertNotIn(ACCESS_TOKEN, repr(self.client))
        with self.assertRaises(MetaAPIError) as raised:
            self.client.fetch_campaigns()
        self.assertNotIn(ACCESS_TOKEN, str(raised.exception))
        self.assertNotIn(ACCESS_TOKEN, repr(raised.exception))

    def test_empty_and_malformed_collection_responses(self):
        self.respond({"data": [], "paging": {}})
        self.assertEqual(self.client.fetch_campaigns(), [])
        self.respond({"unexpected": []})
        with self.assertRaises(MetaAPIError):
            self.client.fetch_campaigns()


if __name__ == "__main__":
    unittest.main()