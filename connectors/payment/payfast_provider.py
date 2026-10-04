"""Offline parsing of supplied PayFast transaction rows."""
from __future__ import annotations

import csv
import io
from typing import Any, Dict, List, Union

from connectors.payment.base import (
    BasePaymentProvider,
    PaymentRecord,
    normalize_method,
    normalize_status,
    normalize_type,
    safe_parse_money,
)


class PayFastPaymentProvider(BasePaymentProvider):
    PROVIDER_NAME = "payfast"
    SUPPORTED_METHODS = {"csv", "settlement"}

    def parse_transactions(self, raw_data: Union[str, List[Dict[str, Any]]]) -> List[PaymentRecord]:
        rows = list(csv.DictReader(io.StringIO(raw_data.strip()))) if isinstance(raw_data, str) else raw_data
        if not isinstance(rows, list):
            return []
        records = []
        for row in rows:
            payment_id = row.get("external_payment_id")
            order_id = row.get("external_order_id")
            records.append(PaymentRecord(
                external_order_id=str(order_id).strip() if order_id and str(order_id).strip() else None,
                amount=safe_parse_money(row.get("canonical_amount")),
                gateway_fee=safe_parse_money(row.get("gateway_fee")),
                external_payment_id=str(payment_id).strip() if payment_id and str(payment_id).strip() else None,
                payment_method=normalize_method(row.get("payment_method")),
                payment_type=normalize_type(row.get("payment_type")),
                payment_status=normalize_status(row.get("payment_status")),
                paid_at=str(row.get("paid_at")).strip() if row.get("paid_at") else None,
                currency=str(row.get("currency")).strip().upper() if row.get("currency") else None,
                provider=self.PROVIDER_NAME,
                provider_account_identifier=(
                    str(row.get("provider_account_identifier")).strip()
                    if row.get("provider_account_identifier") else None
                ),
                original_external_payment_id=row.get("original_external_payment_id") or None,
                raw_metadata=dict(row),
            ))
        return records
