"""Offline parsing of merchant-supplied manual payment records."""
from __future__ import annotations

import csv
import io
import json
from typing import Any, Dict, List, Union

from connectors.payment.base import (
    BasePaymentProvider,
    PaymentRecord,
    normalize_method,
    normalize_status,
    normalize_type,
    safe_parse_money,
)


class ManualPaymentProvider(BasePaymentProvider):
    PROVIDER_NAME = "manual"
    SUPPORTED_METHODS = {"csv", "json"}

    def parse_transactions(self, raw_data: Union[str, List[Dict[str, Any]]]) -> List[PaymentRecord]:
        if isinstance(raw_data, str):
            stripped = raw_data.strip()
            rows = json.loads(stripped) if stripped.startswith("[") else list(csv.DictReader(io.StringIO(stripped)))
        elif isinstance(raw_data, list):
            rows = raw_data
        else:
            return []
        if not isinstance(rows, list):
            return []

        records = []
        for row in rows:
            payment_id = row.get("external_payment_id")
            order_id = row.get("external_order_id")
            currency = row.get("currency")
            account = row.get("provider_account_identifier")
            original_id = row.get("original_external_payment_id")
            records.append(PaymentRecord(
                external_order_id=str(order_id).strip() if order_id and str(order_id).strip() else None,
                amount=safe_parse_money(row.get("amount")),
                gateway_fee=safe_parse_money(row.get("gateway_fee")),
                external_payment_id=str(payment_id).strip() if payment_id and str(payment_id).strip() else None,
                payment_method=normalize_method(row.get("payment_method")),
                payment_type=normalize_type(row.get("payment_type")),
                payment_status=normalize_status(row.get("payment_status")),
                paid_at=str(row.get("paid_at")).strip() if row.get("paid_at") else None,
                currency=str(currency).strip().upper() if currency and len(str(currency).strip()) == 3 else None,
                provider=str(row.get("provider")).strip() if row.get("provider") else None,
                provider_account_identifier=str(account).strip() if account and str(account).strip() else None,
                original_external_payment_id=str(original_id).strip() if original_id else None,
                raw_metadata=dict(row),
            ))
        return records
