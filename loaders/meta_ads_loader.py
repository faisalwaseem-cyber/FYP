"""Tenant-scoped, transactional loader for schema-supported Meta Ads data."""

from datetime import date
from decimal import Decimal, InvalidOperation
from typing import Any, Dict, List, Tuple

import pandas as pd
from sqlalchemy import text

from config.database import get_db_connection
from etl.normalizers import normalize_date


_CENT = Decimal("0.01")
_MAX_NUMERIC_12_2 = Decimal("9999999999.99")
_MAX_BIGINT = 9223372036854775807
_CAMPAIGN_COLUMNS = {
    "business_id", "external_campaign_id", "campaign_name", "campaign_status"
}
_SPEND_COLUMNS = {
    "business_id", "external_campaign_id", "spend_date", "spend_amount",
    "impressions", "clicks",
}


def _required_text(value: Any, field_name: str) -> str:
    if value is None or isinstance(value, bool) or pd.isna(value):
        raise ValueError(f"{field_name} is required.")
    clean_value = str(value).strip()
    if not clean_value:
        raise ValueError(f"{field_name} is required.")
    return clean_value


def _required_business_id(value: Any, expected: int) -> None:
    if pd.isna(value) or isinstance(value, bool):
        raise ValueError("Every Meta row must carry the server-resolved business_id.")
    try:
        row_business_id = int(value)
    except (TypeError, ValueError, OverflowError):
        raise ValueError("Meta row business_id is invalid.") from None
    if row_business_id != expected or str(value).strip() not in {str(expected), f"{expected}.0"}:
        raise ValueError("Meta row business_id does not match the loader tenant.")


def _required_money(value: Any) -> Decimal:
    if value is None or isinstance(value, bool) or pd.isna(value):
        raise ValueError("spend_amount is required.")
    try:
        amount = Decimal(str(value))
        if not amount.is_finite() or amount < 0:
            raise ValueError("spend_amount must be finite and non-negative.")
        if amount.quantize(_CENT) != amount or amount > _MAX_NUMERIC_12_2:
            raise ValueError("spend_amount exceeds NUMERIC(12,2) precision.")
    except (InvalidOperation, TypeError, ValueError) as exc:
        if isinstance(exc, ValueError) and str(exc).startswith("spend_amount"):
            raise
        raise ValueError("spend_amount is not a valid NUMERIC(12,2) value.") from None
    return amount


def _required_count(value: Any, field_name: str) -> int:
    if value is None or isinstance(value, bool) or pd.isna(value):
        raise ValueError(f"{field_name} is required.")
    try:
        number = Decimal(str(value))
        if (
            not number.is_finite()
            or number < 0
            or number != number.to_integral_value()
            or number > _MAX_BIGINT
        ):
            raise ValueError(f"{field_name} must be a non-negative integer within BIGINT range.")
    except (InvalidOperation, TypeError, ValueError) as exc:
        if isinstance(exc, ValueError) and str(exc).startswith(field_name):
            raise
        raise ValueError(f"{field_name} is not a valid integer.") from None
    return int(number)


class MetaAdsLoader:
    """Upsert campaigns and campaign-level direct metrics only."""

    def __init__(self, business_id: int, platform_name: str = "meta_ads"):
        if isinstance(business_id, bool) or not isinstance(business_id, int) or business_id <= 0:
            raise ValueError("business_id must be a positive server-resolved integer.")
        self.business_id = business_id
        self.platform_name = platform_name
        self.platform_id = self._get_or_create_meta_platform()

    def _get_or_create_meta_platform(self) -> int:
        query = text("""
            INSERT INTO core.marketing_platform (platform_name, created_at)
            VALUES (:platform_name, now())
            ON CONFLICT (platform_name) DO UPDATE
            SET platform_name = EXCLUDED.platform_name
            RETURNING platform_id;
        """)
        with get_db_connection() as conn:
            try:
                platform_id = conn.execute(
                    query, {"platform_name": self.platform_name}
                ).scalar()
                if platform_id is None:
                    raise RuntimeError("Meta marketing platform could not be resolved.")
                conn.commit()
                return int(platform_id)
            except Exception:
                conn.rollback()
                raise

    def _validate_rows(
        self,
        frame: pd.DataFrame,
        required_columns: set,
        row_type: str,
    ) -> List[Dict[str, Any]]:
        if not required_columns.issubset(frame.columns):
            missing = sorted(required_columns.difference(frame.columns))
            raise ValueError(f"Meta {row_type} frame is missing required columns: {', '.join(missing)}.")

        rows: List[Dict[str, Any]] = []
        for _, row in frame.iterrows():
            _required_business_id(row["business_id"], self.business_id)
            external_id = _required_text(row["external_campaign_id"], "external_campaign_id")
            values: Dict[str, Any] = {
                "business_id": self.business_id,
                "external_campaign_id": external_id,
            }
            if row_type == "campaign":
                campaign_name = _required_text(row["campaign_name"], "campaign_name")
                campaign_status = _required_text(row["campaign_status"], "campaign_status")
                if campaign_status not in {"active", "paused", "archived"}:
                    raise ValueError("campaign_status violates the canonical status constraint.")
                values.update({
                    "campaign_name": campaign_name,
                    "campaign_status": campaign_status,
                })
            else:
                try:
                    spend_date = normalize_date(row["spend_date"])
                except (TypeError, ValueError):
                    raise ValueError("spend_date must be a valid date.") from None
                if not isinstance(spend_date, date):
                    raise ValueError("spend_date must be a valid date.")
                values.update({
                    "spend_date": spend_date,
                    "spend_amount": _required_money(row["spend_amount"]),
                    "impressions": _required_count(row["impressions"], "impressions"),
                    "clicks": _required_count(row["clicks"], "clicks"),
                })
            rows.append(values)
        return rows

    def load_campaigns(self, df_campaigns: pd.DataFrame) -> int:
        if df_campaigns is None or df_campaigns.empty:
            return 0
        rows = self._validate_rows(df_campaigns, _CAMPAIGN_COLUMNS, "campaign")
        query = text("""
            INSERT INTO core.marketing_campaign (
                business_id, platform_id, external_campaign_id, campaign_name, campaign_status, created_at
            ) VALUES (
                :business_id, :platform_id, :external_campaign_id, :campaign_name, :campaign_status, now()
            )
            ON CONFLICT (business_id, platform_id, external_campaign_id) DO UPDATE
            SET campaign_name = EXCLUDED.campaign_name,
                campaign_status = EXCLUDED.campaign_status;
        """)

        with get_db_connection() as conn:
            try:
                for row in rows:
                    conn.execute(query, {**row, "platform_id": self.platform_id})
                conn.commit()
            except Exception:
                conn.rollback()
                raise
        return len(rows)

    def load_ad_spend(self, df_spend: pd.DataFrame) -> Tuple[int, int]:
        """Upsert schema-supported direct metrics after tenant-scoped campaign lookup."""
        if df_spend is None or df_spend.empty:
            return 0, 0
        rows = self._validate_rows(df_spend, _SPEND_COLUMNS, "ad spend")
        campaign_query = text("""
            SELECT campaign_id
            FROM core.marketing_campaign
            WHERE business_id = :business_id
              AND platform_id = :platform_id
              AND external_campaign_id = :external_campaign_id;
        """)
        spend_query = text("""
            INSERT INTO core.ad_spend (
                business_id, campaign_id, spend_date, spend_amount,
                impressions, clicks, created_at
            ) VALUES (
                :business_id, :campaign_id, :spend_date, :spend_amount,
                :impressions, :clicks, now()
            )
            ON CONFLICT (campaign_id, spend_date) DO UPDATE
            SET spend_amount = EXCLUDED.spend_amount,
                impressions = EXCLUDED.impressions,
                clicks = EXCLUDED.clicks,
                purchases = 0,
                purchase_value = 0
            RETURNING (xmax = 0) AS is_inserted;
        """)

        inserted = 0
        updated = 0
        with get_db_connection() as conn:
            try:
                for row in rows:
                    campaign_id = conn.execute(campaign_query, {
                        "business_id": self.business_id,
                        "platform_id": self.platform_id,
                        "external_campaign_id": row["external_campaign_id"],
                    }).scalar()
                    if campaign_id is None:
                        raise ValueError(
                            "Meta campaign must be loaded before its ad spend; "
                            "no campaign will be fabricated."
                        )
                    is_inserted = conn.execute(spend_query, {
                        **row,
                        "campaign_id": campaign_id,
                    }).scalar()
                    if is_inserted:
                        inserted += 1
                    else:
                        updated += 1
                conn.commit()
            except Exception:
                conn.rollback()
                raise
        return inserted, updated

