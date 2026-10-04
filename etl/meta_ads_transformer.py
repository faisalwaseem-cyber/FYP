"""Transform supported Meta Ads records to the existing canonical schema."""

from collections.abc import Iterable, Mapping
from datetime import date
from decimal import Decimal, InvalidOperation, ROUND_HALF_UP
from typing import Any, Dict, List, Optional, Tuple

import pandas as pd

from etl.normalizers import normalize_date


_CAMPAIGN_COLUMNS = [
    "business_id", "external_campaign_id", "campaign_name", "campaign_status"
]
_SPEND_COLUMNS = [
    "business_id", "external_campaign_id", "spend_date", "spend_amount",
    "impressions", "clicks",
]
_CENT = Decimal("0.01")
_MAX_NUMERIC_12_2 = Decimal("9999999999.99")
_MAX_BIGINT = 9223372036854775807
_STATUS_MAP = {
    "ACTIVE": "active",
    "PAUSED": "paused",
    "ARCHIVED": "archived",
    "DELETED": "archived",
}


def normalize_meta_status(raw_status: Optional[str]) -> Optional[str]:
    """Map only Meta statuses compatible with the campaign CHECK constraint."""
    if not isinstance(raw_status, str):
        return None
    return _STATUS_MAP.get(raw_status.strip().upper())


def parse_meta_money(value: Any) -> Optional[Decimal]:
    """Parse a valid non-negative NUMERIC(12,2) value without inventing zero."""
    if value is None or isinstance(value, bool) or not str(value).strip():
        return None
    try:
        amount = Decimal(str(value).strip().replace(",", ""))
        if not amount.is_finite() or amount < 0:
            return None
        amount = amount.quantize(_CENT, rounding=ROUND_HALF_UP)
    except (InvalidOperation, ValueError, TypeError):
        return None
    return amount if amount <= _MAX_NUMERIC_12_2 else None


def parse_meta_int(value: Any) -> Optional[int]:
    """Parse a non-negative integral Meta metric without truncation/defaulting."""
    if value is None or isinstance(value, bool) or not str(value).strip():
        return None
    try:
        number = Decimal(str(value).strip().replace(",", ""))
        if not number.is_finite() or number < 0 or number != number.to_integral_value():
            return None
        result = int(number)
    except (InvalidOperation, ValueError, TypeError, OverflowError):
        return None
    return result if result <= _MAX_BIGINT else None


def _identifier(value: Any) -> Optional[str]:
    if isinstance(value, bool) or value is None:
        return None
    normalized = str(value).strip()
    return normalized or None


class MetaAdsTransformer:
    """Create schema-compatible campaign and campaign-day frames for one tenant."""

    def __init__(self, business_id: int):
        if isinstance(business_id, bool) or not isinstance(business_id, int) or business_id <= 0:
            raise ValueError("business_id must be a positive server-resolved integer.")
        self.business_id = business_id
        self.rejected_records: List[Dict[str, Optional[str]]] = []

    def _reject(self, record_type: str, external_id: Any, reason: str) -> None:
        self.rejected_records.append({
            "record_type": record_type,
            "external_id": _identifier(external_id),
            "reason": reason,
        })

    def _records(self, values: Any, record_type: str) -> List[Mapping]:
        if isinstance(values, Mapping):
            values = values.get("data")
        if not isinstance(values, Iterable) or isinstance(values, (str, bytes)):
            self._reject(record_type, None, "provider response is not a record collection")
            return []

        records: List[Mapping] = []
        for item in values:
            if isinstance(item, Mapping):
                if "data" in item and not ("id" in item or "campaign_id" in item):
                    records.extend(self._records(item.get("data", []), record_type))
                else:
                    records.append(item)
            else:
                self._reject(record_type, None, "provider record is not an object")
        return records

    def transform_campaigns(self, raw_campaigns: Any) -> pd.DataFrame:
        """Transform campaign objects; unsupported statuses/required fields are rejected."""
        self.rejected_records = []
        rows: List[Dict[str, Any]] = []
        for campaign in self._records(raw_campaigns, "campaign"):
            external_id = _identifier(campaign.get("id"))
            name = campaign.get("name")
            status_value = campaign.get("effective_status") or campaign.get("status")
            status = normalize_meta_status(status_value)
            if not external_id or not isinstance(name, str) or not name.strip() or status is None:
                self._reject("campaign", external_id, "missing or unsupported required id, name, or status")
                continue
            rows.append({
                "business_id": self.business_id,
                "external_campaign_id": external_id,
                "campaign_name": name.strip(),
                "campaign_status": status,
            })
        return pd.DataFrame(rows, columns=_CAMPAIGN_COLUMNS).drop_duplicates(
            subset=["business_id", "external_campaign_id"]
        ).reset_index(drop=True)

    def transform_insights(
        self,
        raw_insights: Any,
        *,
        level: Optional[str] = None,
    ) -> Tuple[pd.DataFrame, pd.DataFrame]:
        """Map campaign-level direct metrics; preserve attributed metrics in raw storage."""
        self.rejected_records = []
        if level is not None and level not in {"account", "campaign", "adset", "ad"}:
            raise ValueError("level must be account, campaign, adset, or ad.")

        spend_rows: List[Dict[str, Any]] = []
        for insight in self._records(raw_insights, "insight"):
            campaign_id = _identifier(insight.get("campaign_id"))
            row_level = level or (
                "ad" if insight.get("ad_id") else
                "adset" if insight.get("adset_id") else
                "campaign" if campaign_id else "account"
            )
            if row_level != "campaign":
                self._reject("insight", campaign_id, "only campaign-level Insights map to core.ad_spend")
                continue

            raw_date = insight.get("date_start")
            try:
                spend_date = normalize_date(raw_date)
            except (TypeError, ValueError):
                spend_date = None
            spend_amount = parse_meta_money(insight.get("spend"))
            impressions = parse_meta_int(insight.get("impressions"))
            clicks = parse_meta_int(insight.get("clicks"))
            if (
                not campaign_id
                or not isinstance(spend_date, date)
                or spend_amount is None
                or impressions is None
                or clicks is None
            ):
                self._reject(
                    "insight",
                    campaign_id,
                    "missing or malformed campaign_id, date_start, spend, impressions, or clicks",
                )
                continue

            spend_rows.append({
                "business_id": self.business_id,
                "external_campaign_id": campaign_id,
                "spend_date": spend_date,
                "spend_amount": spend_amount,
                "impressions": impressions,
                "clicks": clicks,
            })

        empty_campaigns = pd.DataFrame(columns=_CAMPAIGN_COLUMNS)
        spend_frame = pd.DataFrame(spend_rows, columns=_SPEND_COLUMNS).drop_duplicates(
            subset=["business_id", "external_campaign_id", "spend_date"]
        ).reset_index(drop=True)
        return empty_campaigns, spend_frame

    def transform_daily_insights(self, raw_insights: Any) -> Tuple[pd.DataFrame, pd.DataFrame]:
        """Backward-compatible campaign-level Insights transformation entry point."""
        return self.transform_insights(raw_insights, level="campaign")

