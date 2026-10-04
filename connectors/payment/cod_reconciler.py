"""Offline parsing of supplied COD remittance/statement rows."""
from __future__ import annotations

import csv
import io
from typing import Any, Dict, List, Optional, Union

from connectors.payment.base import (
    BasePaymentProvider,
    PaymentRecord,
    normalize_status,
    normalize_type,
    safe_parse_money,
)


class CODReconciliationProvider(BasePaymentProvider):
    PROVIDER_NAME = "cod"
    SUPPORTED_METHODS = {"csv", "settlement"}

    def parse_transactions(self, raw_data: Union[str, List[Dict[str, Any]], Dict[str, Any]]) -> List[PaymentRecord]:
        if isinstance(raw_data, str):
            rows = list(csv.DictReader(io.StringIO(raw_data.strip())))
        elif isinstance(raw_data, list):
            rows = raw_data
        elif isinstance(raw_data, dict):
            inner = raw_data.get("data") or raw_data.get("records") or []
            rows = inner if isinstance(inner, list) else []
        else:
            return []

        records = []
        for row in rows:
            payment_id = row.get("external_payment_id")
            order_id = row.get("external_order_id")
            amount = row.get("amount")
            fee = row.get("gateway_fee")
            currency = row.get("currency") or row.get("Currency")
            account = row.get("provider_account_identifier")
            date = row.get("paid_at")
            records.append(PaymentRecord(
                external_order_id=str(order_id).strip() if order_id and str(order_id).strip() else None,
                amount=safe_parse_money(amount),
                gateway_fee=safe_parse_money(fee),
                external_payment_id=str(payment_id).strip() if payment_id and str(payment_id).strip() else None,
                payment_method="cod",
                payment_type=normalize_type(row.get("payment_type")),
                payment_status=normalize_status(row.get("payment_status")),
                paid_at=str(date).strip() if date else None,
                currency=str(currency).strip().upper() if currency and len(str(currency).strip()) == 3 else None,
                provider=str(row.get("provider")).strip() if row.get("provider") else None,
                provider_account_identifier=str(account).strip() if account and str(account).strip() else None,
                raw_metadata=dict(row),
            ))
        return records

    def reconcile_delivered_shipment(
        self,
        shipment_data: Dict[str, Any],
        courier_cod_fee: Optional[float] = None,
    ) -> None:
        """Delivery is not evidence that COD money was remitted or settled."""
        return None
