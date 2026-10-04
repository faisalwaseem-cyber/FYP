"""Offline parsing of supplied JazzCash transaction rows."""
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


class JazzCashPaymentProvider(BasePaymentProvider):
    PROVIDER_NAME = "jazzcash"
    SUPPORTED_METHODS = {"csv", "settlement"}

    def parse_transactions(self, raw_data: Union[str, List[Dict[str, Any]]]) -> List[PaymentRecord]:
        rows = list(csv.DictReader(io.StringIO(raw_data.strip()))) if isinstance(raw_data, str) else raw_data
        if not isinstance(rows, list):
            return []
        records = []
        for row in rows:
            payment_id = row.get("external_payment_id")
            order_id = row.get("external_order_id")
            amount = row.get("canonical_amount")
            fee = row.get("gateway_fee")
            status = row.get("payment_status")
            transaction_type = row.get("payment_type")
            date = row.get("paid_at")
            currency = row.get("currency")
            account = row.get("provider_account_identifier")
            records.append(PaymentRecord(
                external_order_id=str(order_id).strip() if order_id and str(order_id).strip() else None,
                amount=safe_parse_money(amount),
                gateway_fee=safe_parse_money(fee),
                external_payment_id=str(payment_id).strip() if payment_id and str(payment_id).strip() else None,
                payment_method=normalize_method(row.get("payment_method")),
                payment_type=normalize_type(transaction_type),
                payment_status=normalize_status(status),
                paid_at=str(date).strip() if date else None,
                currency=str(currency).strip().upper() if currency and len(str(currency).strip()) == 3 else None,
                provider=self.PROVIDER_NAME,
                provider_account_identifier=str(account).strip() if account and str(account).strip() else None,
                original_external_payment_id=row.get("original_payment_id") or None,
                raw_metadata=dict(row),
            ))
        return records
