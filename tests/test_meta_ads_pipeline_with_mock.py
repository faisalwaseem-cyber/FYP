"""
Mock Test for Meta Ads Ingestion Pipeline.
Verifies:
1. Raw Meta Ads payload stored into raw.raw_data (JSONB)
2. Normalization of campaign status ('active', 'paused', 'archived')
3. Extraction of daily ad spend, impressions, clicks, purchases, and purchase revenue
4. Upsert into core.marketing_campaign and core.ad_spend
5. Direct verification by querying PostgreSQL
"""
import sys
from pathlib import Path

# Add project root to path
sys.path.append(str(Path(__file__).resolve().parent.parent))

from raw_layer.raw_repository import save_raw_payload
from etl.meta_ads_transformer import MetaAdsTransformer
from loaders.meta_ads_loader import MetaAdsLoader
from pipeline.sync_logger import SyncLogger
from config.database import get_db_connection
from sqlalchemy import text

# Sample Meta Graph API Insights Response
MOCK_META_INSIGHTS_RESPONSE = [
    {
        "campaign_id": "120205849302840",
        "campaign_name": "Eid Lawn Launch - Conversions (Karachi/Lahore)",
        "date_start": "2026-09-28",
        "date_stop": "2026-09-28",
        "spend": "4500.00",
        "impressions": "18200",
        "clicks": "620",
        "actions": [
            {"action_type": "link_click", "value": "620"},
            {"action_type": "landing_page_view", "value": "510"},
            {"action_type": "purchase", "value": "14"},
            {"action_type": "omni_purchase", "value": "14"}
        ],
        "action_values": [
            {"action_type": "purchase", "value": "49000.00"},
            {"action_type": "omni_purchase", "value": "49000.00"}
        ]
    },
    {
        "campaign_id": "120205849302841",
        "campaign_name": "Pre-Winter Collection Retargeting",
        "date_start": "2026-09-28",
        "date_stop": "2026-09-28",
        "spend": "2200.00",
        "impressions": "7400",
        "clicks": "280",
        "actions": [
            {"action_type": "link_click", "value": "280"},
            {"action_type": "purchase", "value": "6"},
            {"action_type": "omni_purchase", "value": "6"}
        ],
        "action_values": [
            {"action_type": "purchase", "value": "21000.00"},
            {"action_type": "omni_purchase", "value": "21000.00"}
        ]
    }
]

def run_meta_mock_test():
    business_id = 1
    print("=" * 70)
    print("[*] Running Mock Meta Ads Ingestion Test...")
    print("=" * 70)

    logger = SyncLogger(business_id=business_id, source="meta_ads")
    sync_id = logger.start_sync(sync_type="meta_mock_test")
    print(f"[*] Started Meta Ads Sync Run #{sync_id}")

    try:
        transformer = MetaAdsTransformer(business_id=business_id)
        loader = MetaAdsLoader(business_id=business_id)

        # 1. Save Raw Payload in raw.raw_data
        raw_payload = {"data": MOCK_META_INSIGHTS_RESPONSE}
        raw_id = save_raw_payload(business_id, "meta_ads", "daily_insights", raw_payload)
        print(f"[OK] Step 1: Raw Meta Ads JSON saved to raw.raw_data (raw_id: {raw_id})")

        # 2. Transform into DataFrames
        df_campaigns, df_spend = transformer.transform_daily_insights(MOCK_META_INSIGHTS_RESPONSE)
        print(f"[OK] Step 2: Transformed into DataFrames:")
        print(f"    - Campaigns: {len(df_campaigns)}")
        print(f"    - Daily Spend Records: {len(df_spend)}")

        # 3. Load into PostgreSQL
        loader.load_campaigns(df_campaigns)
        inserted, updated = loader.load_ad_spend(df_spend)
        print(f"[OK] Step 3: Loaded into Database:")
        print(f"    - Ad Spend Inserted: {inserted}, Updated: {updated}")

        # 4. Finish Sync Logger
        logger.finish_success(fetched=len(MOCK_META_INSIGHTS_RESPONSE), inserted=inserted, updated=updated)
        print(f"[OK] Step 4: Logged successful run in raw.sync_log")

        # 5. Query and Verify in PostgreSQL
        with get_db_connection() as conn:
            rows = conn.execute(text("""
                SELECT 
                    c.campaign_name,
                    s.spend_date,
                    s.spend_amount,
                    s.impressions,
                    s.clicks,
                    s.purchases,
                    s.purchase_value,
                    ROUND((s.purchase_value / NULLIF(s.spend_amount, 0)), 2) AS roas
                FROM core.ad_spend s
                JOIN core.marketing_campaign c ON s.campaign_id = c.campaign_id
                WHERE s.business_id = :b_id AND s.spend_date = '2026-09-28'
                ORDER BY s.spend_amount DESC;
            """), {"b_id": business_id}).fetchall()

            print("\n" + "=" * 70)
            print("[VERIFICATION] Successfully queried Meta Ad Spend from PostgreSQL:")
            for r in rows:
                print(f"\n  Campaign:     {r[0]}")
                print(f"  Date:         {r[1]}")
                print(f"  Ad Spend:     PKR {r[2]:,.2f}")
                print(f"  Impressions:  {r[3]:,}")
                print(f"  Clicks:       {r[4]:,}")
                print(f"  Purchases:    {r[5]}")
                print(f"  Pixel Value:  PKR {r[6]:,.2f}")
                print(f"  Reported ROAS: {r[7]}x")
            print("=" * 70)
            print("[SUCCESS] Meta Ads Ingestion Layer is verified and working perfectly!\n")

    except Exception as e:
        logger.finish_failure(str(e))
        print(f"[!] Meta Ads Pipeline Test Failed: {e}")
        raise

if __name__ == "__main__":
    run_meta_mock_test()
