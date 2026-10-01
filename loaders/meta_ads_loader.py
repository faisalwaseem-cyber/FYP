"""
PostgreSQL Meta Ads Loader for BRIDGE FLOW (Phase 4.3).

Idempotently upserts:
1. core.marketing_campaign (external campaign configuration and status)
2. core.ad_spend (daily spend, impressions, clicks, purchases, purchase revenue)

Design invariants:
- Strictly scoped by business_id (no cross-tenant leakage).
- Atomic transactions with rollback on failure.
- Auto-provisions missing campaign records to preserve foreign key integrity.
- ON CONFLICT clauses ensure 100% idempotent replayability.
"""
from typing import Optional, Tuple
import pandas as pd
from sqlalchemy import text
from config.database import get_db_connection


class MetaAdsLoader:
    def __init__(self, business_id: int, platform_name: str = "meta_ads"):
        if not business_id or business_id <= 0:
            raise ValueError(f"Invalid business_id: {business_id}. Must be positive integer.")
        self.business_id = int(business_id)
        self.platform_name = platform_name
        self.platform_id = self._get_or_create_meta_platform()

    def _get_or_create_meta_platform(self) -> int:
        """
        Retrieves or registers marketing platform in core.marketing_platform.
        """
        query = text("""
            INSERT INTO core.marketing_platform (platform_name, created_at)
            VALUES (:platform_name, now())
            ON CONFLICT (platform_name) DO UPDATE
            SET platform_name = EXCLUDED.platform_name
            RETURNING platform_id;
        """)
        with get_db_connection() as conn:
            p_id = conn.execute(query, {"platform_name": self.platform_name}).scalar()
            conn.commit()
            return int(p_id)

    def load_campaigns(self, df_campaigns: pd.DataFrame) -> int:
        """
        Upserts campaign configurations into core.marketing_campaign.
        Returns the number of campaigns processed.
        """
        if df_campaigns is None or df_campaigns.empty:
            return 0

        query = text("""
            INSERT INTO core.marketing_campaign (
                business_id, platform_id, external_campaign_id, campaign_name, campaign_status, created_at
            )
            VALUES (
                :business_id, :platform_id, :external_campaign_id, :campaign_name, :campaign_status, now()
            )
            ON CONFLICT (business_id, platform_id, external_campaign_id) DO UPDATE
            SET campaign_name = EXCLUDED.campaign_name,
                campaign_status = EXCLUDED.campaign_status;
        """)

        count = 0
        with get_db_connection() as conn:
            for _, r in df_campaigns.iterrows():
                conn.execute(query, {
                    "business_id": self.business_id,
                    "platform_id": self.platform_id,
                    "external_campaign_id": str(r["external_campaign_id"]),
                    "campaign_name": str(r["campaign_name"]),
                    "campaign_status": str(r["campaign_status"]),
                })
                count += 1
            conn.commit()
        return count

    def load_ad_spend(self, df_spend: pd.DataFrame) -> Tuple[int, int]:
        """
        Upserts daily ad spend records into core.ad_spend.
        Ensures foreign key campaign_id exists for each external_campaign_id.

        Returns:
            (inserted_count, updated_count)
        """
        if df_spend is None or df_spend.empty:
            return 0, 0

        ensure_camp_query = text("""
            INSERT INTO core.marketing_campaign (
                business_id, platform_id, external_campaign_id, campaign_name, campaign_status, created_at
            )
            VALUES (
                :business_id, :platform_id, :ext_camp_id, 'Auto-provisioned Meta Campaign', 'active', now()
            )
            ON CONFLICT (business_id, platform_id, external_campaign_id) DO NOTHING;
        """)

        spend_query = text("""
            INSERT INTO core.ad_spend (
                business_id, campaign_id, spend_date, spend_amount,
                impressions, clicks, purchases, purchase_value, created_at
            )
            VALUES (
                :business_id,
                (SELECT campaign_id FROM core.marketing_campaign 
                 WHERE business_id = :business_id AND platform_id = :platform_id AND external_campaign_id = :ext_camp_id LIMIT 1),
                CAST(:spend_date AS DATE),
                :spend_amount, :impressions, :clicks, :purchases, :purchase_value, now()
            )
            ON CONFLICT (campaign_id, spend_date) DO UPDATE
            SET spend_amount = EXCLUDED.spend_amount,
                impressions = EXCLUDED.impressions,
                clicks = EXCLUDED.clicks,
                purchases = EXCLUDED.purchases,
                purchase_value = EXCLUDED.purchase_value
            RETURNING (xmax = 0) AS is_inserted;
        """)

        inserted = 0
        updated = 0

        with get_db_connection() as conn:
            for _, r in df_spend.iterrows():
                ext_camp_id = str(r["external_campaign_id"])

                # 1. Ensure parent campaign exists for FK constraint
                conn.execute(ensure_camp_query, {
                    "business_id": self.business_id,
                    "platform_id": self.platform_id,
                    "ext_camp_id": ext_camp_id,
                })

                # 2. Upsert ad spend
                res = conn.execute(spend_query, {
                    "business_id": self.business_id,
                    "platform_id": self.platform_id,
                    "ext_camp_id": ext_camp_id,
                    "spend_date": r["spend_date"],
                    "spend_amount": float(r["spend_amount"]),
                    "impressions": int(r["impressions"]),
                    "clicks": int(r["clicks"]),
                    "purchases": int(r["purchases"]),
                    "purchase_value": float(r["purchase_value"]),
                })
                is_ins = res.scalar()
                if is_ins:
                    inserted += 1
                else:
                    updated += 1
            conn.commit()

        return inserted, updated

