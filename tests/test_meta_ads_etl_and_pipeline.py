"""
High-Value Unit and Integration Test Suite for Meta Ads ETL & Data Loading (Phase 4.3).

Validates:
1. MetaAdsTransformer: Campaign status normalization and business_id scoping.
2. MetaAdsTransformer: Insights extraction of spend, impressions, clicks, attribution conversions (purchase/omni/pixel).
3. MetaAdsTransformer: Edge cases (empty, missing dates, malformed numbers).
4. MetaAdsLoader: Idempotent upsert into core.marketing_campaign and core.ad_spend.
5. MetaAdsLoader: Cross-tenant isolation (Business A vs Business B).
6. End-to-End Pipeline: Services → Raw → Transformer → Loader execution with telemetry.
"""
import json
import sys
import unittest
from datetime import date
from decimal import Decimal
from pathlib import Path
from unittest.mock import MagicMock, patch

import pandas as pd
from sqlalchemy import text

# Ensure project root is on Python path
sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from config.database import get_db_connection
from etl.meta_ads_transformer import (
    MetaAdsTransformer,
    normalize_meta_status,
    parse_meta_money,
    parse_meta_int,
)
from loaders.meta_ads_loader import MetaAdsLoader
from pipeline.run_meta_ads_sync import run_meta_ads_sync
from raw_layer.raw_repository import save_raw_payload


class TestMetaAdsETL(unittest.TestCase):
    def setUp(self):
        self.business_id = 1
        self.transformer = MetaAdsTransformer(business_id=self.business_id)

    # ------------------------------------------------------------------
    # 1. Normalization & Helper Tests
    # ------------------------------------------------------------------

    def test_status_normalization(self):
        self.assertEqual(normalize_meta_status("ACTIVE"), "active")
        self.assertEqual(normalize_meta_status("active"), "active")
        self.assertEqual(normalize_meta_status("ARCHIVED"), "archived")
        self.assertEqual(normalize_meta_status("DELETED"), "archived")
        self.assertEqual(normalize_meta_status("PAUSED"), "paused")
        self.assertIsNone(normalize_meta_status("UNKNOWN"))
        self.assertIsNone(normalize_meta_status(None))

    def test_money_and_int_parsers(self):
        self.assertEqual(parse_meta_money("123.456"), Decimal("123.46"))
        self.assertEqual(parse_meta_money("0"), Decimal("0.00"))
        self.assertIsNone(parse_meta_money("-50.0"))
        self.assertIsNone(parse_meta_money(None))
        self.assertIsNone(parse_meta_money("invalid"))

        self.assertEqual(parse_meta_int("150"), 150)
        self.assertEqual(parse_meta_int("9007199254740993"), 9007199254740993)
        self.assertIsNone(parse_meta_int(12.8))
        self.assertIsNone(parse_meta_int("-5"))
        self.assertIsNone(parse_meta_int(None))
        self.assertIsNone(parse_meta_int("invalid"))

    # ------------------------------------------------------------------
    # 2. Campaign Transformation & Scoping
    # ------------------------------------------------------------------

    def test_transform_campaigns(self):
        raw_campaigns = [
            {"id": "meta_c1", "name": "Summer Sale", "status": "ACTIVE"},
            {"id": "meta_c2", "name": "Winter Promo", "effective_status": "ARCHIVED"},
            {"id": "meta_c1", "name": "Summer Sale Duplicate", "status": "ACTIVE"},  # Dup
        ]
        df = self.transformer.transform_campaigns(raw_campaigns)

        self.assertEqual(len(df), 2)
        self.assertListEqual(list(df.columns), ["business_id", "external_campaign_id", "campaign_name", "campaign_status"])
        self.assertTrue((df["business_id"] == self.business_id).all())
        row1 = df[df["external_campaign_id"] == "meta_c1"].iloc[0]
        self.assertEqual(row1["campaign_status"], "active")
        row2 = df[df["external_campaign_id"] == "meta_c2"].iloc[0]
        self.assertEqual(row2["campaign_status"], "archived")

    def test_campaigns_with_missing_required_fields_are_rejected(self):
        frame = self.transformer.transform_campaigns([
            {"id": "missing-name", "status": "ACTIVE"},
            {"id": "missing-status", "name": "No status"},
            {"name": "No identifier", "status": "ACTIVE"},
        ])
        self.assertTrue(frame.empty)
        self.assertEqual(len(self.transformer.rejected_records), 3)

    def test_page_envelopes_are_flattened_without_changing_record_ids(self):
        pages = iter([
            {"data": [{"id": "page-campaign", "name": "Paged", "status": "ACTIVE"}]}
        ])
        frame = self.transformer.transform_campaigns(pages)
        self.assertEqual(frame.iloc[0]["external_campaign_id"], "page-campaign")

    # ------------------------------------------------------------------
    # 3. Insights Transformation & Attribution Conversions
    # ------------------------------------------------------------------

    def test_transform_campaign_level_insights_keeps_attribution_out_of_canonical(self):
        raw_insights = [
            {
                "campaign_id": "120205849302840",
                "campaign_name": "Conversion Campaign",
                "date_start": "2026-09-28",
                "spend": "3500.50",
                "impressions": "14500",
                "clicks": "480",
                "actions": [
                    {"action_type": "link_click", "value": "480"},
                    {"action_type": "purchase", "value": "12"},
                    {"action_type": "omni_purchase", "value": "12"},
                ],
                "action_values": [
                    {"action_type": "purchase", "value": "42000.00"},
                    {"action_type": "omni_purchase", "value": "42000.00"},
                ],
            }
        ]

        df_c, df_s = self.transformer.transform_daily_insights(raw_insights)

        self.assertTrue(df_c.empty)
        self.assertEqual(len(df_s), 1)

        spend_row = df_s.iloc[0]
        self.assertEqual(spend_row["business_id"], self.business_id)
        self.assertEqual(spend_row["external_campaign_id"], "120205849302840")
        self.assertEqual(spend_row["spend_date"], date(2026, 9, 28))
        self.assertEqual(spend_row["spend_amount"], Decimal("3500.50"))
        self.assertEqual(spend_row["impressions"], 14500)
        self.assertEqual(spend_row["clicks"], 480)
        self.assertNotIn("purchases", df_s.columns)
        self.assertNotIn("purchase_value", df_s.columns)
        self.assertEqual(raw_insights[0]["action_values"][0]["value"], "42000.00")
        self.assertEqual(raw_insights[0]["actions"][1]["value"], "12")
        self.assertTrue(df_c.empty)

    def test_insights_preserve_all_direct_schema_supported_metrics(self):
        raw = [{
            "campaign_id": "meta-campaign-1",
            "date_start": "2026-09-28",
            "spend": "12.34",
            "impressions": "500",
            "clicks": "20",
            "reach": "400",
            "ctr": "4.0",
            "cpc": "0.617",
            "cpm": "24.68",
            "actions": [{"action_type": "purchase", "value": "3"}],
            "conversions": [{"action_type": "purchase", "value": "3"}],
            "results": [{"indicator": "purchase", "value": "3"}],
            "action_values": [{"action_type": "purchase", "value": "99.00"}],
            "purchase_roas": [{"action_type": "purchase", "value": "8.0"}],
        }]
        _, spend = self.transformer.transform_insights(raw, level="campaign")
        self.assertEqual(
            spend.iloc[0][["spend_amount", "impressions", "clicks"]].tolist(),
            [Decimal("12.34"), 500, 20],
        )
        self.assertNotIn("reach", spend.columns)
        self.assertNotIn("ctr", spend.columns)
        self.assertNotIn("cpc", spend.columns)
        self.assertNotIn("cpm", spend.columns)
        self.assertNotIn("purchases", spend.columns)
        self.assertNotIn("purchase_value", spend.columns)
        self.assertIn("action_values", raw[0])
        self.assertIn("purchase_roas", raw[0])

    def test_business_id_is_server_supplied_not_provider_supplied(self):
        campaign = {
            "business_id": 999,
            "id": "meta-campaign-tenant",
            "name": "Tenant campaign",
            "status": "ACTIVE",
        }
        frame = self.transformer.transform_campaigns([campaign])
        self.assertEqual(frame.iloc[0]["business_id"], self.business_id)

    def test_transform_insights_rejects_missing_required_and_malformed_values(self):
        raw_insights = [
            {"campaign_name": "No ID"},
            {"campaign_id": "c_bad_date", "date_start": "bad-date-format", "spend": "1", "impressions": "1", "clicks": "1"},
            {"campaign_id": "c_missing_metrics", "date_start": "2026-09-29"},
            {"campaign_id": "c_bad_number", "date_start": "2026-09-29", "spend": "NaN", "impressions": "1.5", "clicks": "x"},
            {
                "campaign_id": "c_valid",
                "date_start": "2026-09-29",
                "spend": "0.00",
                "impressions": "0",
                "clicks": "0",
            },
        ]
        df_c, df_s = self.transformer.transform_daily_insights(raw_insights)
        self.assertTrue(df_c.empty)
        self.assertEqual(len(df_s), 1)
        self.assertEqual(df_s.iloc[0]["spend_amount"], Decimal("0.00"))
        self.assertEqual(df_s.iloc[0]["impressions"], 0)
        self.assertEqual(len(self.transformer.rejected_records), 4)

    def test_empty_insights_and_unsupported_levels_are_safe(self):
        campaigns, spend = self.transformer.transform_insights([], level="campaign")
        self.assertTrue(campaigns.empty)
        self.assertTrue(spend.empty)
        for level in ("account", "adset", "ad"):
            _, spend = self.transformer.transform_insights(
                [{"campaign_id": "c1", "date_start": "2026-09-29", "spend": "1", "impressions": "1", "clicks": "1"}],
                level=level,
            )
            self.assertTrue(spend.empty)
            self.assertEqual(
                self.transformer.rejected_records[0]["reason"],
                "only campaign-level Insights map to core.ad_spend",
            )


class ScalarResult:
    def __init__(self, value):
        self.value = value

    def scalar(self):
        return self.value


class TestMetaAdsLoaderUnit(unittest.TestCase):
    def setUp(self):
        self.database_patcher = patch("loaders.meta_ads_loader.get_db_connection")
        self.database_context = self.database_patcher.start()
        self.addCleanup(self.database_patcher.stop)
        self.connection = MagicMock()
        self.database_context.return_value.__enter__.return_value = self.connection
        self.connection.execute.return_value.scalar.return_value = 7
        self.loader = MetaAdsLoader(business_id=71)
        self.connection.reset_mock()

    @staticmethod
    def spend_frame(business_id=71):
        return pd.DataFrame([{
            "business_id": business_id,
            "external_campaign_id": "meta-campaign-1",
            "spend_date": date(2026, 9, 28),
            "spend_amount": Decimal("12.34"),
            "impressions": 500,
            "clicks": 20,
            "action_values": [{"action_type": "purchase", "value": "99.00"}],
            "purchase_roas": [{"action_type": "purchase", "value": "8.0"}],
        }])

    def test_campaign_upsert_is_tenant_scoped_and_idempotent(self):
        frame = pd.DataFrame([{
            "business_id": 71,
            "external_campaign_id": "meta-campaign-1",
            "campaign_name": "Campaign",
            "campaign_status": "active",
        }])
        self.assertEqual(self.loader.load_campaigns(frame), 1)
        self.assertEqual(self.loader.load_campaigns(frame), 1)
        self.assertEqual(self.connection.execute.call_count, 2)
        query_text = str(self.connection.execute.call_args_list[0].args[0])
        parameters = self.connection.execute.call_args_list[0].args[1]
        self.assertIn("ON CONFLICT (business_id, platform_id, external_campaign_id)", query_text)
        self.assertEqual(parameters["business_id"], 71)
        self.assertEqual(parameters["platform_id"], 7)
        self.assertEqual(self.connection.commit.call_count, 2)

    def test_loader_rejects_other_tenant_before_database_write(self):
        with self.assertRaises(ValueError):
            self.loader.load_ad_spend(self.spend_frame(business_id=72))
        self.connection.execute.assert_not_called()

    def test_spend_fk_is_resolved_and_attribution_is_not_loaded(self):
        self.connection.execute.side_effect = [ScalarResult(905), ScalarResult(True)]
        self.assertEqual(self.loader.load_ad_spend(self.spend_frame()), (1, 0))
        lookup_text = str(self.connection.execute.call_args_list[0].args[0])
        lookup_parameters = self.connection.execute.call_args_list[0].args[1]
        insert_text = str(self.connection.execute.call_args_list[1].args[0])
        insert_parameters = self.connection.execute.call_args_list[1].args[1]
        self.assertIn("business_id = :business_id", lookup_text)
        self.assertEqual(lookup_parameters["business_id"], 71)
        self.assertEqual(insert_parameters["campaign_id"], 905)
        self.assertIn("ON CONFLICT (campaign_id, spend_date)", insert_text)
        self.assertNotIn("purchase_value", insert_text.split("ON CONFLICT", 1)[0])
        self.assertNotIn("action_values", insert_parameters)
        self.assertNotIn("purchase_roas", insert_parameters)

    def test_repeated_spend_load_uses_unique_key_upsert(self):
        self.connection.execute.side_effect = [
            ScalarResult(905), ScalarResult(True),
            ScalarResult(905), ScalarResult(False),
        ]
        self.assertEqual(self.loader.load_ad_spend(pd.concat(
            [self.spend_frame(), self.spend_frame()], ignore_index=True
        )), (1, 1))

    def test_missing_campaign_fk_rolls_back_without_placeholder_campaign(self):
        self.connection.execute.side_effect = [ScalarResult(None)]
        with self.assertRaisesRegex(ValueError, "campaign must be loaded"):
            self.loader.load_ad_spend(self.spend_frame())
        self.connection.rollback.assert_called_once()
        self.connection.commit.assert_not_called()
        query_text = str(self.connection.execute.call_args.args[0])
        self.assertTrue(query_text.lstrip().startswith("SELECT campaign_id"))

    def test_failure_after_prior_row_rolls_back_whole_spend_batch(self):
        second_row = self.spend_frame().iloc[0].to_dict()
        second_row["spend_date"] = date(2026, 9, 29)
        batch = pd.concat([self.spend_frame(), pd.DataFrame([second_row])], ignore_index=True)
        self.connection.execute.side_effect = [
            ScalarResult(905), ScalarResult(True), ScalarResult(None),
        ]
        with self.assertRaises(ValueError):
            self.loader.load_ad_spend(batch)
        self.connection.rollback.assert_called_once()
        self.connection.commit.assert_not_called()

    def test_malformed_values_are_rejected_before_database_write(self):
        frame = self.spend_frame().astype({"impressions": object})
        frame.at[0, "impressions"] = "1.5"
        with self.assertRaisesRegex(ValueError, "impressions"):
            self.loader.load_ad_spend(frame)
        self.connection.execute.assert_not_called()

    def test_malformed_date_is_rejected_before_database_write(self):
        frame = self.spend_frame()
        frame["spend_date"] = ["not-a-date"]
        with self.assertRaisesRegex(ValueError, "spend_date"):
            self.loader.load_ad_spend(frame)
        self.connection.execute.assert_not_called()

    @patch("raw_layer.raw_repository.get_db_connection")
    def test_raw_meta_payload_is_preserved_with_server_tenant(self, database_context):
        raw_connection = MagicMock()
        database_context.return_value.__enter__.return_value = raw_connection
        raw_connection.execute.return_value.scalar.return_value = 301
        payload = {
            "data": [{
                "id": "meta-adset-1",
                "campaign_id": "meta-campaign-1",
                "actions": [{"action_type": "purchase", "value": "3"}],
                "action_values": [{"action_type": "purchase", "value": "99.00"}],
            }]
        }
        self.assertEqual(
            save_raw_payload(71, "meta_ads", "adsets", payload),
            301,
        )
        parameters = raw_connection.execute.call_args.args[1]
        self.assertEqual(parameters["business_id"], 71)
        self.assertEqual(json.loads(parameters["payload"]), payload)
        self.assertEqual(parameters["source"], "meta_ads")


class TestMetaAdsLoaderAndPipeline(unittest.TestCase):
    def setUp(self):
        self.business_id = 1
        self.loader = MetaAdsLoader(business_id=self.business_id)

    # ------------------------------------------------------------------
    # 4. Idempotent Database Upserts
    # ------------------------------------------------------------------

    def test_idempotent_campaign_and_spend_load(self):
        test_camp_id = "test_meta_camp_idempotent_999"
        test_date = "2026-09-30"

        df_campaigns = pd.DataFrame([
            {
                "business_id": self.business_id,
                "external_campaign_id": test_camp_id,
                "campaign_name": "Idempotency Test Campaign",
                "campaign_status": "active",
            }
        ])

        # Step 1: Load campaign
        self.loader.load_campaigns(df_campaigns)

        df_spend_1 = pd.DataFrame([
            {
                "business_id": self.business_id,
                "external_campaign_id": test_camp_id,
                "spend_date": test_date,
                "spend_amount": 1000.00,
                "impressions": 5000,
                "clicks": 150,
                "purchases": 5,
                "purchase_value": 15000.00,
            }
        ])

        # Step 2: First load -> Insert
        ins1, upd1 = self.loader.load_ad_spend(df_spend_1)
        self.assertEqual(ins1, 1)
        self.assertEqual(upd1, 0)

        # Step 3: Second load with updated spend -> Update (idempotent, no duplicates)
        df_spend_2 = pd.DataFrame([
            {
                "business_id": self.business_id,
                "external_campaign_id": test_camp_id,
                "spend_date": test_date,
                "spend_amount": 1250.00,  # updated spend
                "impressions": 5500,
                "clicks": 180,
                "purchases": 6,
                "purchase_value": 18000.00,
            }
        ])
        ins2, upd2 = self.loader.load_ad_spend(df_spend_2)
        self.assertEqual(ins2, 0)
        self.assertEqual(upd2, 1)

        # Verify in database
        with get_db_connection() as conn:
            row = conn.execute(text("""
                SELECT s.spend_amount, s.impressions, s.clicks, s.purchases, s.purchase_value
                FROM core.ad_spend s
                JOIN core.marketing_campaign c ON s.campaign_id = c.campaign_id
                WHERE s.business_id = :b_id 
                  AND c.external_campaign_id = :ext_id 
                  AND s.spend_date = CAST(:dt AS DATE)
            """), {"b_id": self.business_id, "ext_id": test_camp_id, "dt": test_date}).fetchone()

            if row is None:
                self.fail("Expected the idempotent Meta ad-spend row to be loaded.")
            self.assertEqual(float(row[0]), 1250.00)
            self.assertEqual(row[1], 5500)
            self.assertEqual(row[2], 180)
            self.assertEqual(row[3], 0)
            self.assertEqual(float(row[4]), 0.00)

            # Cleanup test record
            conn.execute(text("""
                DELETE FROM core.ad_spend WHERE business_id = :b_id AND campaign_id IN (
                    SELECT campaign_id FROM core.marketing_campaign WHERE external_campaign_id = :ext_id
                )
            """), {"b_id": self.business_id, "ext_id": test_camp_id})
            conn.execute(text("""
                DELETE FROM core.marketing_campaign WHERE business_id = :b_id AND external_campaign_id = :ext_id
            """), {"b_id": self.business_id, "ext_id": test_camp_id})
            conn.commit()

    # ------------------------------------------------------------------
    # 5. Cross-Tenant Isolation
    # ------------------------------------------------------------------

    def test_cross_tenant_isolation(self):
        b1_loader = MetaAdsLoader(business_id=1)
        camp_id = "cross_tenant_camp_123"

        df_c = pd.DataFrame([
            {
                "business_id": 1,
                "external_campaign_id": camp_id,
                "campaign_name": "Tenant 1 Campaign",
                "campaign_status": "active",
            }
        ])
        b1_loader.load_campaigns(df_c)

        # Query from Business 2 perspective must return nothing
        with get_db_connection() as conn:
            b2_count = conn.execute(text("""
                SELECT count(*) FROM core.marketing_campaign
                WHERE business_id = 2 AND external_campaign_id = :ext_id
            """), {"ext_id": camp_id}).scalar()
            self.assertEqual(b2_count, 0)

            # Cleanup
            conn.execute(text("DELETE FROM core.marketing_campaign WHERE business_id = 1 AND external_campaign_id = :ext_id"), {"ext_id": camp_id})
            conn.commit()

    # ------------------------------------------------------------------
    # 6. End-to-End Pipeline Execution (Mocked Live Flow)
    # ------------------------------------------------------------------

    @patch("pipeline.run_meta_ads_sync.update_last_synced")
    @patch("pipeline.run_meta_ads_sync.list_connections")
    @patch("connectors.meta_ads.client.MetaAdsClient._make_request")
    def test_mocked_end_to_end_pipeline(self, mock_request, mock_connections, mock_last_synced):
        mock_connections.return_value = [{
            "connection_id": 991,
            "platform": "meta_ads",
            "account_identifier": "act_120205849302840",
            "status": "active",
        }]

        def mock_request_side_effect(endpoint, params=None):
            if endpoint == "me/adaccounts":
                return {"data": [{"id": "act_120205849302840", "name": "Test Account"}], "paging": {}}
            if endpoint.endswith("/campaigns"):
                return {
                    "data": [
                        {"id": "camp_e2e_1", "name": "E2E Test Campaign", "status": "ACTIVE"}
                    ],
                    "paging": {},
                }
            if endpoint.endswith("/insights"):
                return {
                    "data": [
                        {
                            "campaign_id": "camp_e2e_1",
                            "campaign_name": "E2E Test Campaign",
                            "date_start": "2026-09-28",
                            "spend": "2500.00",
                            "impressions": "10000",
                            "clicks": "300",
                            "actions": [{"action_type": "purchase", "value": "8"}],
                            "action_values": [{"action_type": "purchase", "value": "28000.00"}],
                        }
                    ],
                    "paging": {},
                }
            return {"data": []}

        mock_request.side_effect = mock_request_side_effect

        result = run_meta_ads_sync(business_id=self.business_id, days_back=3)

        self.assertEqual(result["status"], "success")
        self.assertEqual(result["campaigns_synced"], 1)
        self.assertEqual(result["insights_fetched"], 1)

        # Verify raw payload saved
        with get_db_connection() as conn:
            raw_count = conn.execute(text("""
                SELECT count(*) FROM raw.raw_data
                WHERE business_id = :b_id AND source = 'meta_ads'
            """), {"b_id": self.business_id}).scalar()
            if raw_count is None:
                self.fail("Expected a raw Meta payload count.")
            self.assertGreater(raw_count, 0)

            # Cleanup test records
            conn.execute(text("""
                DELETE FROM core.ad_spend WHERE business_id = :b_id AND campaign_id IN (
                    SELECT campaign_id FROM core.marketing_campaign WHERE external_campaign_id = 'camp_e2e_1'
                )
            """), {"b_id": self.business_id})
            conn.execute(text("""
                DELETE FROM core.marketing_campaign WHERE business_id = :b_id AND external_campaign_id = 'camp_e2e_1'
            """), {"b_id": self.business_id})
            conn.commit()


if __name__ == "__main__":
    unittest.main()

