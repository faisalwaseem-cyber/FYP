"""
Manual & Bank Transfer Payment Provider — BRIDGE FLOW Phase 6
==============================================================

Supports manual payment entries, bank statement imports (IBFT, Raast, Kuickpay),
and generic payment reconciliation files.
"""
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
    PROVIDER_NAME: str = "manual"
    SUPPORTED_METHODS = {"csv", "json"}

    def __init__(self, default_fee_rate: float = 0.0):
        # 0.0% fee for manual / direct bank transfers
        super().__init__(default_fee_rate=default_fee_rate)

    def parse_transactions(self, raw_data: Union[str, List[Dict[str, Any]]]) -> List[PaymentRecord]:
        if isinstance(raw_data, str):
            stripped = raw_data.strip()
            if stripped.startswith("["):
                try:
                    rows = json.loads(stripped)
                except Exception:
                    rows = list(csv.DictReader(io.StringIO(stripped)))
            else:
                rows = list(csv.DictReader(io.StringIO(stripped)))
        elif isinstance(raw_data, list):
            rows = raw_data
        else:
            return []

        records: List[PaymentRecord] = []
        for r in rows:
            ext_order_id = str(
                r.get("external_order_id")
                or r.get("order_id")
                or r.get("order_ref")
                or r.get("Reference")
                or ""
            ).strip()

            ext_payment_id = (
                r.get("external_payment_id")
                or r.get("transaction_id")
                or r.get("receipt_no")
            )
            if ext_payment_id:
                ext_payment_id = str(ext_payment_id).strip()

            amount = safe_parse_money(r.get("amount", 0))
            raw_fee = r.get("gateway_fee") or r.get("fee")
            fee = safe_parse_money(raw_fee) if raw_fee is not None else 0.0

            method = normalize_method(r.get("payment_method") or r.get("method") or "bank_transfer")
            status = normalize_status(r.get("payment_status") or r.get("status") or "paid")
            pay_type = normalize_type(r.get("payment_type") or r.get("type") or "payment")
            paid_at = str(r.get("paid_at") or r.get("date") or "").strip() or None

            records.append(PaymentRecord(
                external_payment_id=ext_payment_id,
                external_order_id=ext_order_id,
                amount=amount,
                gateway_fee=fee,
                payment_method=method,
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

