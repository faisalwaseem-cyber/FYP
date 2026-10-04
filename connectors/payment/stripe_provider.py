"""Offline parsing helpers for supplied Stripe transaction exports."""
from __future__ import annotations

import csv
import io
import json
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional, Union

from connectors.payment.base import (
    BasePaymentProvider,
    PaymentRecord,
    normalize_method,
    normalize_status,
    normalize_type,
    safe_parse_money,
)


def _currency(value: Any) -> Optional[str]:
    if isinstance(value, str) and len(value.strip()) == 3:
        return value.strip().upper()
    return None


def _account(row: Dict[str, Any]) -> Optional[str]:
    value = row.get("provider_account_identifier")
    return str(value).strip() if value is not None and str(value).strip() else None


class StripePaymentProvider(BasePaymentProvider):
    PROVIDER_NAME = "stripe"
    SUPPORTED_METHODS = {"api", "csv", "settlement"}

    def parse_transactions(self, raw_data: Union[List[Dict[str, Any]], Dict[str, Any], str]) -> List[PaymentRecord]:
        if isinstance(raw_data, str):
            stripped = raw_data.strip()
            if stripped.startswith(("[", "{")):
                parsed = json.loads(stripped)
                if isinstance(parsed, dict):
                    return self.parse_transactions(parsed.get("data", []))
                if isinstance(parsed, list):
                    return self._parse_api_transactions(parsed)
            return self._parse_csv(raw_data)
        if isinstance(raw_data, list):
            return self._parse_api_transactions(raw_data)
        if isinstance(raw_data, dict) and isinstance(raw_data.get("data"), list):
            return self._parse_api_transactions(raw_data["data"])
        return []

    def _parse_api_transactions(self, items: List[Dict[str, Any]]) -> List[PaymentRecord]:
        records = []
        for item in items:
            metadata = item.get("metadata") or {}
            object_type = item.get("object")
            if object_type == "refund":
                payment_type = "refund"
                original_id = item.get("charge")
            elif object_type == "charge":
                payment_type = "payment"
                original_id = None
            else:
                payment_type = normalize_type(item.get("type"))
                original_id = item.get("charge") if payment_type == "refund" else None

            created = item.get("created")
            paid_at = None
            if isinstance(created, (int, float)):
                paid_at = datetime.fromtimestamp(int(created), tz=timezone.utc).isoformat()
            elif created:
                paid_at = str(created)

            method_details = item.get("payment_method_details") or {}
            method = normalize_method(
                method_details.get("type") or item.get("payment_method")
            )
            fee_value = item.get("gateway_fee")
            records.append(PaymentRecord(
                external_order_id=str(metadata.get("order_id") or metadata.get("shopify_order_id") or "").strip() or None,
                amount=safe_parse_money(item.get("canonical_amount")),
                gateway_fee=safe_parse_money(fee_value),
                external_payment_id=str(item["id"]).strip() if item.get("id") else None,
                payment_method=method,
                payment_type=payment_type,
                payment_status=normalize_status(item.get("status")),
                paid_at=paid_at,
                currency=_currency(item.get("currency")),
                provider=self.PROVIDER_NAME,
                provider_account_identifier=_account(item),
                original_external_payment_id=str(original_id).strip() if original_id else None,
                raw_metadata=item,
            ))
        return records

    def _parse_csv(self, csv_content: str) -> List[PaymentRecord]:
        records = []
        for row in csv.DictReader(io.StringIO(csv_content.strip())):
            payment_id = row.get("external_payment_id")
            order_id = row.get("external_order_id")
            created = row.get("paid_at")
            records.append(PaymentRecord(
                external_order_id=str(order_id).strip() if order_id and str(order_id).strip() else None,
                amount=safe_parse_money(row.get("amount")),
                gateway_fee=safe_parse_money(row.get("gateway_fee")),
                external_payment_id=str(payment_id).strip() if payment_id and str(payment_id).strip() else None,
                payment_method=normalize_method(row.get("payment_method")),
                payment_type=normalize_type(row.get("payment_type")),
                payment_status=normalize_status(row.get("payment_status")),
                paid_at=str(created).strip() if created else None,
                currency=_currency(row.get("currency")),
                provider=self.PROVIDER_NAME,
                provider_account_identifier=_account(row),
                original_external_payment_id=row.get("original_payment_id") or None,
                raw_metadata=dict(row),
            ))
        return records
