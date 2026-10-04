"""Focused Meta OAuth and ingestion wiring tests; no external API calls."""

import json
import unittest
from contextlib import ExitStack
from uuid import uuid4
from unittest.mock import MagicMock, patch

import pandas as pd
from fastapi.testclient import TestClient
from sqlalchemy import text

from api.dependencies import get_current_business_id
from api.main import app
from auth.shopify_oauth import generate_signed_oauth_state, verify_oauth_state
from config.database import get_db_connection
from pipeline.run_meta_ads_sync import run_meta_ads_sync


class FakeOAuthResponse:
    def __init__(self, payload, status_code=200):
        self.payload = payload
        self.status_code = status_code

    def json(self):
        return self.payload


class TestMetaOAuthState(unittest.TestCase):
    def test_existing_shopify_state_default_remains_compatible(self):
        state = generate_signed_oauth_state(1)
        self.assertEqual(verify_oauth_state(state), 1)
        self.assertIsNone(verify_oauth_state(state))

    def test_state_is_platform_bound_single_use_and_replay_safe(self):
        state = generate_signed_oauth_state(1, platform="meta_ads")
        self.assertGreaterEqual(len(state), 32)
        self.assertIsNone(verify_oauth_state(state, platform="shopify"))
        self.assertEqual(verify_oauth_state(state, platform="meta_ads"), 1)
        self.assertIsNone(verify_oauth_state(state, platform="meta_ads"))

    def test_expired_state_is_rejected_and_consumed_only_when_valid(self):
        state = f"expired-{uuid4().hex}"
        with get_db_connection() as connection:
            connection.execute(
                text("""
                    INSERT INTO core.oauth_state (business_id, state_nonce, platform, expires_at)
                    VALUES (1, :state, 'meta_ads', now() - INTERVAL '1 minute');
                """),
                {"state": state},
            )
            connection.commit()
        try:
            self.assertIsNone(verify_oauth_state(state, platform="meta_ads"))
            with get_db_connection() as connection:
                remaining = connection.execute(
                    text("SELECT count(*) FROM core.oauth_state WHERE state_nonce = :state"),
                    {"state": state},
                ).scalar()
            self.assertEqual(remaining, 1)
        finally:
            with get_db_connection() as connection:
                connection.execute(
                    text("DELETE FROM core.oauth_state WHERE state_nonce = :state"),
                    {"state": state},
                )
                connection.commit()


class TestMetaOAuthHelpers(unittest.TestCase):
    def test_authorization_url_requests_only_ads_read_and_never_app_secret(self):
        with patch("auth.meta_oauth.META_APP_ID", "app-id"), patch(
            "auth.meta_oauth.META_APP_SECRET", "app-secret-never-client-side"
        ), patch(
            "auth.meta_oauth.META_REDIRECT_URI",
            "https://bridge.example/integrations/meta/callback",
        ):
            from auth.meta_oauth import build_meta_authorization_url

            url = build_meta_authorization_url("opaque-state")
        self.assertIn("/v25.0/dialog/oauth?", url)
        self.assertIn("scope=ads_read", url)
        self.assertIn("response_type=code", url)
        self.assertIn("state=opaque-state", url)
        self.assertNotIn("app-secret-never-client-side", url)

    @patch("auth.meta_oauth.requests.get")
    def test_code_exchange_is_server_side_and_returns_only_token_lifetime_data(self, mock_get):
        mock_get.return_value = FakeOAuthResponse({
            "access_token": "fixture-access-token",
            "expires_in": 3600,
            "token_type": "bearer",
        })
        with patch("auth.meta_oauth.META_APP_ID", "fixture-app-id"), patch(
            "auth.meta_oauth.META_APP_SECRET", "fixture-app-secret"
        ), patch(
            "auth.meta_oauth.META_REDIRECT_URI", "https://bridge.example/callback"
        ):
            from auth.meta_oauth import exchange_meta_authorization_code

            result = exchange_meta_authorization_code("fixture-one-time-code")
        self.assertEqual(result, {
            "access_token": "fixture-access-token",
            "expires_in": 3600,
        })
        self.assertEqual(
            mock_get.call_args.kwargs["params"]["client_secret"],
            "fixture-app-secret",
        )
        self.assertEqual(result["access_token"], "fixture-access-token")

    @patch("auth.meta_oauth.requests.get")
    def test_code_exchange_errors_do_not_leak_provider_payload(self, mock_get):
        mock_get.return_value = FakeOAuthResponse({
            "error": {"message": "fixture-app-secret", "code": 190}
        }, status_code=400)
        with patch("auth.meta_oauth.META_APP_ID", "fixture-app-id"), patch(
            "auth.meta_oauth.META_APP_SECRET", "fixture-app-secret"
        ), patch(
            "auth.meta_oauth.META_REDIRECT_URI", "https://bridge.example/callback"
        ):
            from auth.meta_oauth import MetaOAuthError, exchange_meta_authorization_code

            with self.assertRaises(MetaOAuthError) as raised:
                exchange_meta_authorization_code("fixture-one-time-code")
        self.assertNotIn("fixture-app-secret", str(raised.exception))
        self.assertNotIn("fixture-one-time-code", str(raised.exception))


class TestMetaIntegrationRoutes(unittest.TestCase):
    BUSINESS_ID = 71

    def setUp(self):
        app.dependency_overrides[get_current_business_id] = lambda: self.BUSINESS_ID
        self.client = TestClient(app)

    def tearDown(self):
        app.dependency_overrides.pop(get_current_business_id, None)

    @patch("api.main.build_meta_authorization_url", return_value="https://facebook.example/dialog")
    @patch("api.main.generate_signed_oauth_state", return_value="opaque-meta-state")
    def test_connect_uses_authenticated_business_and_ignores_client_tenant(self, mock_state, mock_url):
        response = self.client.post("/integrations/meta/connect?business_id=999")
        self.assertEqual(response.status_code, 200)
        self.assertEqual(response.json(), {"authorization_url": "https://facebook.example/dialog"})
        mock_state.assert_called_once_with(self.BUSINESS_ID, platform="meta_ads")
        mock_url.assert_called_once_with("opaque-meta-state")
        self.assertNotIn("business_id", response.json()["authorization_url"])

    @patch("api.main.create_or_update_connection")
    @patch("api.main.fetch_meta_accessible_ad_accounts")
    @patch("api.main.exchange_meta_authorization_code")
    @patch("api.main.verify_oauth_state", return_value=BUSINESS_ID)
    def test_callback_registers_all_accessible_accounts_without_redirecting_tokens(
        self, mock_state, mock_exchange, mock_accounts, mock_store
    ):
        mock_exchange.return_value = {
            "access_token": "fixture-access-token",
            "expires_in": 3600,
        }
        mock_accounts.return_value = [
            {"id": "act_12345", "name": "Store One"},
            {"id": "act_67890", "name": "Store Two"},
            {"id": "not-an-ad-account", "name": "Invalid"},
            {"id": "act_12345", "name": "Duplicate"},
        ]
        response = self.client.get(
            "/integrations/meta/callback?code=fixture-code&state=fixture-state",
            follow_redirects=False,
        )
        self.assertEqual(response.status_code, 303)
        redirect = response.headers["location"]
        self.assertIn("meta=connected", redirect)
        self.assertIn("accounts=2", redirect)
        for secret in ("fixture-access-token", "fixture-code", "fixture-state", "app_secret"):
            self.assertNotIn(secret, redirect)
        mock_state.assert_called_once_with("fixture-state", platform="meta_ads")
        mock_accounts.assert_called_once_with(self.BUSINESS_ID, "fixture-access-token")
        self.assertEqual(mock_store.call_count, 2)
        stored_connections = [call.kwargs for call in mock_store.call_args_list]
        self.assertEqual(
            {connection["account_identifier"] for connection in stored_connections},
            {"act_12345", "act_67890"},
        )
        first_connection = stored_connections[0]
        self.assertEqual(first_connection["business_id"], self.BUSINESS_ID)
        self.assertEqual(first_connection["platform"], "meta_ads")
        self.assertEqual(first_connection["platform_category"], "marketing")
        self.assertEqual(first_connection["auth_type"], "oauth2")
        self.assertEqual(first_connection["access_token"], "fixture-access-token")
        self.assertEqual(first_connection["expires_in"], 3600)

    @patch("api.main.exchange_meta_authorization_code")
    @patch("api.main.verify_oauth_state", return_value=None)
    def test_invalid_or_expired_state_stops_before_token_exchange(self, mock_state, mock_exchange):
        response = self.client.get(
            "/integrations/meta/callback?code=fixture-code&state=expired",
            follow_redirects=False,
        )
        self.assertEqual(response.status_code, 303)
        self.assertIn("reason=invalid_state", response.headers["location"])
        mock_exchange.assert_not_called()

    @patch("api.main.exchange_meta_authorization_code")
    @patch("api.main.verify_oauth_state", return_value=BUSINESS_ID)
    def test_denied_authorization_consumes_state_without_exchange(self, mock_state, mock_exchange):
        response = self.client.get(
            "/integrations/meta/callback?error=access_denied&state=denied-state",
            follow_redirects=False,
        )
        self.assertEqual(response.status_code, 303)
        self.assertIn("reason=authorization_denied", response.headers["location"])
        mock_exchange.assert_not_called()

    @patch("api.main.list_connections")
    def test_status_exposes_safe_multi_account_metadata_only(self, mock_list):
        mock_list.return_value = [{
            "platform": "meta_ads",
            "account_identifier": "act_12345",
            "account_name": "Store",
            "status": "active",
            "scopes": "ads_read",
            "installed_at": None,
            "last_synced_at": None,
            "encrypted_access_token": "ciphertext-secret",
        }]
        response = self.client.get("/integrations/meta/status")
        self.assertEqual(response.status_code, 200)
        payload = response.json()
        self.assertTrue(payload["connected"])
        self.assertEqual(len(payload["accounts"]), 1)
        self.assertNotIn("ciphertext-secret", response.text)
        self.assertNotIn("encrypted_access_token", response.text)
        mock_list.assert_called_once_with(self.BUSINESS_ID)

    @patch("api.main.update_status")
    @patch("api.main.list_connections")
    def test_disconnect_is_business_scoped_and_can_select_one_account(self, mock_list, mock_update):
        mock_list.return_value = [
            {"connection_id": 10, "platform": "meta_ads", "account_identifier": "act_1"},
            {"connection_id": 11, "platform": "meta_ads", "account_identifier": "act_2"},
            {"connection_id": 12, "platform": "shopify", "account_identifier": "store"},
        ]
        response = self.client.post(
            "/integrations/meta/disconnect?account_identifier=act_2&business_id=999"
        )
        self.assertEqual(response.status_code, 200)
        self.assertEqual(response.json()["accounts_disconnected"], 1)
        mock_list.assert_called_once_with(self.BUSINESS_ID)
        mock_update.assert_called_once_with(11, "disconnected")

    @patch("api.main.run_meta_ads_sync", return_value={"status": "success"})
    def test_sync_passes_server_business_and_selected_account(self, mock_sync):
        response = self.client.post(
            "/integrations/meta/sync?business_id=999&account_identifier=act_12345&days_back=4"
        )
        self.assertEqual(response.status_code, 200)
        mock_sync.assert_called_once_with(
            business_id=self.BUSINESS_ID,
            days_back=4,
            account_identifier="act_12345",
        )


class TestMetaPipelineWiring(unittest.TestCase):
    def test_services_are_preserved_before_transform_and_loader(self):
        from pipeline import run_meta_ads_sync as pipeline

        events = []
        connection = {
            "connection_id": 55,
            "platform": "meta_ads",
            "account_identifier": "act_12345",
            "status": "active",
        }

        def service_result(entity, records):
            def fetch(*args, **kwargs):
                events.append(f"fetch:{entity}")
                return records
            return fetch

        with ExitStack() as stack:
            stack.enter_context(patch.object(pipeline, "list_connections", return_value=[connection]))
            stack.enter_context(patch.object(pipeline, "MetaAdsClient"))
            account_service = MagicMock()
            account_service.fetch_accounts.side_effect = service_result(
                "accounts", [{"id": "act_12345", "name": "Store"}]
            )
            campaign_service = MagicMock()
            campaign_service.fetch_campaigns.side_effect = service_result(
                "campaigns", [{"id": "campaign-1", "name": "Campaign", "status": "ACTIVE"}]
            )
            ad_set_service = MagicMock()
            ad_set_service.fetch_ad_sets.side_effect = service_result("ad_sets", [{"id": "adset-1"}])
            ad_service = MagicMock()
            ad_service.fetch_ads.side_effect = service_result("ads", [{"id": "ad-1"}])
            ad_service.fetch_creatives.side_effect = service_result("creatives", [{"id": "creative-1"}])
            insights_service = MagicMock()
            insights_service.fetch_insights.side_effect = service_result(
                "insights", [{"campaign_id": "campaign-1", "date_start": "2026-10-01"}]
            )
            stack.enter_context(patch.object(pipeline, "MetaAdAccountService", return_value=account_service))
            stack.enter_context(patch.object(pipeline, "MetaCampaignService", return_value=campaign_service))
            stack.enter_context(patch.object(pipeline, "MetaAdSetService", return_value=ad_set_service))
            stack.enter_context(patch.object(pipeline, "MetaAdService", return_value=ad_service))
            stack.enter_context(patch.object(pipeline, "MetaInsightsService", return_value=insights_service))

            class Transformer:
                def __init__(self, business_id):
                    self.business_id = business_id

                def transform_campaigns(self, records):
                    events.append("transform:campaigns")
                    return pd.DataFrame([{"external_campaign_id": "campaign-1"}])

                def transform_insights(self, records, *, level):
                    events.append("transform:insights")
                    return pd.DataFrame(), pd.DataFrame([{"spend_amount": 1}])

            loader = MagicMock()
            loader.load_campaigns.return_value = 1
            loader.load_ad_spend.return_value = (2, 3)
            stack.enter_context(patch.object(pipeline, "MetaAdsTransformer", Transformer))
            stack.enter_context(patch.object(pipeline, "MetaAdsLoader", return_value=loader))
            stack.enter_context(patch.object(
                pipeline,
                "save_raw_payload",
                side_effect=lambda business_id, source, data_type, payload: events.append(f"raw:{data_type}"),
            ))
            stack.enter_context(patch.object(pipeline, "SyncLogger"))
            stack.enter_context(patch.object(pipeline, "update_last_synced"))

            result = run_meta_ads_sync(business_id=71, days_back=3)

        self.assertEqual(result["accounts_synced"], 1)
        self.assertEqual(result["campaigns_synced"], 1)
        self.assertEqual(result["spend_inserted"], 2)
        self.assertEqual(result["spend_updated"], 3)
        self.assertLess(events.index("raw:insights"), events.index("transform:campaigns"))
        self.assertLess(events.index("transform:campaigns"), events.index("transform:insights"))
        self.assertLess(events.index("raw:insights"), events.index("transform:insights"))
        loader.load_campaigns.assert_called_once()
        loader.load_ad_spend.assert_called_once()