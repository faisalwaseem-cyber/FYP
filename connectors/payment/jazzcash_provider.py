"""
JazzCash Pakistan Settlement Provider — BRIDGE FLOW Phase 6
===========================================================

Integration methods supported:
- CSV / Excel merchant settlement statement export (Mobilink Microfinance Bank)

JazzCash provides daily/weekly settlement statements through their merchant portal.
Typical fee: 1.5% for JazzCash mobile account transactions.
"""
from __future__ import annotations

import csv
import io
from typing import Any, Dict, List, Union

from connectors.payment.base import (
    BasePaymentProvider,
    PaymentRecord,
    normalize_status,
    normalize_type,
    safe_parse_money,
)


class JazzCashPaymentProvider(BasePaymentProvider):
    PROVIDER_NAME: str = "jazzcash"
    SUPPORTED_METHODS = {"csv", "settlement"}

    COLUMN_ALIASES = {
        "txn_id":   ["Txn Ref No", "transaction_id", "Txn ID", "Transaction Ref", "pp_TxnRefNo"],
        "order_id": ["Merchant Order ID", "order_id", "Order ID", "pp_BillReference", "Reference"],
        "amount":   ["Txn Amount", "amount", "Amount", "pp_Amount", "Gross Amount"],
        "fee":      ["Commission", "fee", "Fee", "Bank Charges", "FED", "Charges"],
        "status":   ["Txn Status", "status", "Status", "Response Code", "pp_ResponseCode"],
        "type":     ["Txn Type", "type", "Type"],
        "date":     ["Txn Date Time", "date", "Date", "Timestamp", "Settlement Date"],
    }

    def __init__(self, default_fee_rate: float = 0.015):
        # 1.5% standard JazzCash merchant rate
        super().__init__(default_fee_rate=default_fee_rate)

    def _resolve(self, row: Dict[str, Any], key: str) -> str:
        for alias in self.COLUMN_ALIASES.get(key, []):
            if alias in row and row[alias] is not None:
                return str(row[alias]).strip()
        return ""

    def parse_transactions(self, raw_data: Union[str, List[Dict[str, Any]]]) -> List[PaymentRecord]:
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

            raw_status = self._resolve(r, "status")
            status = "paid" if raw_status in ("000", "0", "paid", "success", "completed") else normalize_status(raw_status)
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

