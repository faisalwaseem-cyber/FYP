"""
PayFast Pakistan Settlement Provider — BRIDGE FLOW Phase 6
==========================================================

Integration methods supported:
- CSV / Excel merchant settlement statement export
- Merchant transaction reconciliation

In Pakistan, PayFast (APPS) delivers merchant settlement reports through
their merchant portal. This provider ingests those reports, normalizes column
aliases, parses exact MDR gateway fees (or defaults to ~2.5%), and matches orders.
"""
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
    PROVIDER_NAME: str = "payfast"
    SUPPORTED_METHODS = {"csv", "settlement"}

    COLUMN_ALIASES = {
        "txn_id":   ["transaction_id", "txn_id", "txnid", "Transaction ID", "Merchant Txn ID"],
        "order_id": ["order_id", "order_ref", "orderid", "Order ID", "Reference", "Basket ID"],
        "amount":   ["amount", "Amount", "total", "Gross Amount", "Transaction Amount"],
        "fee":      ["fee", "Fee", "gateway_fee", "charges", "MDR", "Commission"],
        "status":   ["status", "Status", "payment_status", "Txn Status"],
        "type":     ["type", "Type", "txn_type", "transaction_type"],
        "date":     ["datetime", "date", "Date", "created_at", "Timestamp", "Settlement Date"],
    }

    def __init__(self, default_fee_rate: float = 0.025):
        # 2.5% standard PayFast Pakistan MDR
        super().__init__(default_fee_rate=default_fee_rate)

    def _resolve(self, row: Dict[str, Any], key: str) -> str:
        for alias in self.COLUMN_ALIASES.get(key, []):
            if alias in row and row[alias] is not None:
                return str(row[alias]).strip()
        return ""

    def parse_transactions(self, raw_data: Union[str, List[Dict[str, Any]]]) -> List[PaymentRecord]:
        """
        Parses either a CSV string or a list of row dicts from PayFast settlement statements.
        """
        if isinstance(raw_data, str):
            reader = csv.DictReader(io.StringIO(raw_data.strip()))
            rows = list(reader)
        elif isinstance(raw_data, list):
            rows = raw_data
        else:
            return []

        records: List[PaymentRecord] = []
        for r in rows:
            ext_order_id = self._resolve(r, "order_id")
            ext_payment_id = self._resolve(r, "txn_id") or None

            amount = safe_parse_money(self._resolve(r, "amount") or r.get("amount", 0))

            raw_fee = self._resolve(r, "fee")
            if raw_fee:
                fee = safe_parse_money(raw_fee)
            else:
                fee = self.calculate_fee(amount, method="wallet")

            status = normalize_status(self._resolve(r, "status") or "paid")
            pay_type = normalize_type(self._resolve(r, "type") or "payment")
            paid_at = self._resolve(r, "date") or None

            records.append(PaymentRecord(
                external_payment_id=ext_payment_id,
                external_order_id=ext_order_id,
                amount=amount,
                gateway_fee=fee,
                payment_method="wallet",
                payment_type=pay_type,
                payment_status=status,
                settlement_status="settled" if status == "paid" else "pending",
                settlement_date=paid_at,
                paid_at=paid_at,
                currency="PKR",
                gateway_name=self.PROVIDER_NAME,
                raw_metadata=dict(r),
            ))

        return records

