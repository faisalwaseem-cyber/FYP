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
        self.assertEqual(normalize_meta_status("UNKNOWN"), "paused")
        self.assertEqual(normalize_meta_status(None), "paused")

    def test_money_and_int_parsers(self):
        self.assertEqual(parse_meta_money("123.456"), 123.46)
        self.assertEqual(parse_meta_money("0"), 0.0)
        self.assertEqual(parse_meta_money("-50.0"), 0.0)
        self.assertEqual(parse_meta_money(None), 0.0)
        self.assertEqual(parse_meta_money("invalid"), 0.0)

        self.assertEqual(parse_meta_int("150"), 150)
        self.assertEqual(parse_meta_int(12.8), 12)
        self.assertEqual(parse_meta_int("-5"), 0)
        self.assertEqual(parse_meta_int(None), 0)
        self.assertEqual(parse_meta_int("invalid"), 0)

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

    # ------------------------------------------------------------------
    # 3. Insights Transformation & Attribution Conversions
    # ------------------------------------------------------------------

    def test_transform_daily_insights_attribution(self):
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

        self.assertEqual(len(df_c), 1)
        self.assertEqual(len(df_s), 1)

        spend_row = df_s.iloc[0]
        self.assertEqual(spend_row["business_id"], self.business_id)
        self.assertEqual(spend_row["external_campaign_id"], "120205849302840")
        self.assertEqual(spend_row["spend_date"], "2026-09-28")
        self.assertEqual(spend_row["spend_amount"], 3500.50)
        self.assertEqual(spend_row["impressions"], 14500)
        self.assertEqual(spend_row["clicks"], 480)
        self.assertEqual(spend_row["purchases"], 12)
        self.assertEqual(spend_row["purchase_value"], 42000.00)

    def test_transform_daily_insights_edge_cases(self):
        # Missing date, invalid date, and missing campaign_id
        raw_insights = [
            {"campaign_name": "No ID"},
            {"campaign_id": "c_bad_date", "date_start": "bad-date-format"},
            {"campaign_id": "c_no_date"},
            {
                "campaign_id": "c_valid",
                "date_start": "2026-09-29",
                "spend": None,
                "impressions": None,
                "clicks": None,
            },
        ]
        df_c, df_s = self.transformer.transform_daily_insights(raw_insights)
        self.assertEqual(len(df_c), 1)
        self.assertEqual(len(df_s), 1)
        self.assertEqual(df_s.iloc[0]["spend_amount"], 0.0)
        self.assertEqual(df_s.iloc[0]["impressions"], 0)


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

            self.assertIsNotNone(row)
            self.assertEqual(float(row[0]), 1250.00)
            self.assertEqual(row[1], 5500)
            self.assertEqual(row[2], 180)
            self.assertEqual(row[3], 6)
            self.assertEqual(float(row[4]), 18000.00)

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

    @patch("pipeline.run_meta_ads_sync.resolve_meta_credentials")
    @patch("connectors.meta.client.MetaClient.request")
    def test_mocked_end_to_end_pipeline(self, mock_request, mock_resolve):
        mock_resolve.return_value = ("test_mock_token", "act_120205849302840", "mock_secret", None)

        def mock_request_side_effect(method, path, **kwargs):
            if "campaigns" in path:
                return {
                    "data": [
                        {"id": "camp_e2e_1", "name": "E2E Test Campaign", "status": "ACTIVE"}
                    ],
                    "paging": {},
                }
            elif "insights" in path:
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

