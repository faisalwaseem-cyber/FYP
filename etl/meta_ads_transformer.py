"""
Production-Ready Meta Ads Transformer for BRIDGE FLOW (Phase 4.3).

Transforms Meta Marketing API Graph API v26.0 payloads into canonical relational DataFrames:
1. Campaigns: core.marketing_campaign (business_id, external_campaign_id, campaign_name, campaign_status)
2. Daily Ad Spend: core.ad_spend (business_id, external_campaign_id, spend_date, spend_amount,
   impressions, clicks, purchases, purchase_value)

Design invariants:
- Strictly scoped by business_id (no cross-tenant leakage).
- Idempotent: duplicates dropped on unique keys prior to database ingestion.
- Canonical status normalization ('active', 'paused', 'archived').
- Handles multi-attribution conversion action types (pixel, omni, onsite).
- Monetary precision: non-negative floats/decimals rounded to 2 decimal places.
"""
from datetime import datetime
from decimal import Decimal, ROUND_HALF_UP
from typing import Any, Dict, Generator, List, Optional, Tuple, Union

import pandas as pd


def normalize_meta_status(raw_status: Optional[str]) -> str:
    """
    Standardizes Meta campaign status into canonical enum:
    ('active', 'paused', 'archived').
    """
    if not raw_status:
        return "paused"
    s = str(raw_status).strip().upper()
    if s == "ACTIVE":
        return "active"
    elif s in ("ARCHIVED", "DELETED"):
        return "archived"
    return "paused"


def parse_meta_money(val: Any) -> float:
    """Safely converts spend/currency values to float rounded to 2 decimal places."""
    if val is None or val == "":
        return 0.0
    try:
        d = Decimal(str(val)).quantize(Decimal("0.01"), rounding=ROUND_HALF_UP)
        return max(0.0, float(d))
    except Exception:
        return 0.0


def parse_meta_int(val: Any) -> int:
    """Safely converts impression/click counts to non-negative integer."""
    if val is None or val == "":
        return 0
    try:
        return max(0, int(float(str(val))))
    except Exception:
        return 0


# Recognized conversion action types in Meta Ads Graph API
PURCHASE_ACTION_TYPES = {
    "purchase",
    "omni_purchase",
    "offsite_conversion.fb_pixel_purchase",
    "onsite_web_purchase",
    "web_in_store_purchase",
}


class MetaAdsTransformer:
    """
    Transforms Meta Marketing API Graph payloads into canonical DataFrames.
    Guarantees tenant isolation via mandatory business_id.
    """

    def __init__(self, business_id: int):
        if not business_id or business_id <= 0:
            raise ValueError(f"Invalid business_id: {business_id}. Must be positive integer.")
        self.business_id = int(business_id)

    def transform_campaigns(
        self,
        raw_campaigns: Union[List[Dict[str, Any]], Generator[Dict[str, Any], None, None]],
    ) -> pd.DataFrame:
        """
        Transforms campaign records into canonical core.marketing_campaign schema.
        Accepts either a list of campaign dicts or service page yields.
        """
        campaigns_list: List[Dict[str, Any]] = []

        # Flatten if generator of pages
        items: List[Dict[str, Any]] = []
        if isinstance(raw_campaigns, list):
            items = raw_campaigns
        else:
            for page in raw_campaigns:
                items.extend(page.get("campaigns", page.get("data", [])))

        for c in items:
            ext_id = str(c.get("id") or "").strip()
            if not ext_id:
                continue

            name = str(c.get("name") or f"Campaign {ext_id}").strip()
            raw_status = c.get("effective_status") or c.get("status")
            status = normalize_meta_status(raw_status)

            campaigns_list.append({
                "business_id": self.business_id,
                "external_campaign_id": ext_id,
                "campaign_name": name,
                "campaign_status": status,
            })

        df = pd.DataFrame(campaigns_list)
        if not df.empty:
            df = df.drop_duplicates(subset=["business_id", "external_campaign_id"]).reset_index(drop=True)
        return df

    def transform_daily_insights(
        self,
        raw_insights: Union[List[Dict[str, Any]], Generator[Dict[str, Any], None, None]],
    ) -> Tuple[pd.DataFrame, pd.DataFrame]:
        """
        Parses Meta daily insights into (campaigns_df, ad_spend_df).
        Extracts spend, impressions, clicks, pixel purchases, and attributed purchase values.
        """
        campaigns_list: List[Dict[str, Any]] = []
        spend_list: List[Dict[str, Any]] = []

        # Flatten if generator of pages
        items: List[Dict[str, Any]] = []
        if isinstance(raw_insights, list):
            items = raw_insights
        else:
            for page in raw_insights:
                items.extend(page.get("insights", page.get("data", [])))

        for row in items:
            ext_camp_id = str(row.get("campaign_id") or "").strip()
            if not ext_camp_id:
                continue

            camp_name = str(row.get("campaign_name") or f"Campaign {ext_camp_id}").strip()
            spend_date = str(row.get("date_start") or row.get("date_stop") or "").strip()

            # Ensure spend_date is valid date format
            if not spend_date:
                continue
            try:
                # Validate format YYYY-MM-DD
                datetime.strptime(spend_date, "%Y-%m-%d")
            except ValueError:
                continue

            spend_amount = parse_meta_money(row.get("spend"))
            impressions = parse_meta_int(row.get("impressions"))
            clicks = parse_meta_int(row.get("clicks"))

            # Extract purchases count across Meta conversion action types
            purchases = 0
            for action in row.get("actions", []) or []:
                if action.get("action_type") in PURCHASE_ACTION_TYPES:
                    purchases = max(purchases, parse_meta_int(action.get("value")))

            # Extract attributed revenue across Meta conversion value types
            purchase_val = 0.0
            for val in row.get("action_values", []) or []:
                if val.get("action_type") in PURCHASE_ACTION_TYPES:
                    purchase_val = max(purchase_val, parse_meta_money(val.get("value")))

            campaigns_list.append({
                "business_id": self.business_id,
                "external_campaign_id": ext_camp_id,
                "campaign_name": camp_name,
                "campaign_status": "active",
            })

            spend_list.append({
                "business_id": self.business_id,
                "external_campaign_id": ext_camp_id,
                "spend_date": spend_date,
                "spend_amount": spend_amount,
                "impressions": impressions,
                "clicks": clicks,
                "purchases": purchases,
                "purchase_value": purchase_val,
            })

        df_c = (
            pd.DataFrame(campaigns_list).drop_duplicates(subset=["business_id", "external_campaign_id"]).reset_index(drop=True)
            if campaigns_list
            else pd.DataFrame()
        )
        df_s = (
            pd.DataFrame(spend_list).drop_duplicates(subset=["business_id", "external_campaign_id", "spend_date"]).reset_index(drop=True)
            if spend_list
            else pd.DataFrame()
        )

        return df_c, df_s

