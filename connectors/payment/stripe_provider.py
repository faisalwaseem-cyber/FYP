"""
Stripe Payment & Settlement Provider — BRIDGE FLOW Phase 6
==========================================================

Integration methods supported:
- REST API (GET /v1/charges, GET /v1/refunds, GET /v1/payouts)
- CSV Export parsing (Stripe payments and balance statements)

Fee Calculation:
- Standard card MDR: 2.9% (or application_fee_amount from API / fee column from CSV)
- Exact Net Received = Amount - Gateway Fee
"""
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


class StripePaymentProvider(BasePaymentProvider):
    PROVIDER_NAME: str = "stripe"
    SUPPORTED_METHODS = {"api", "csv", "settlement"}

    def __init__(self, default_fee_rate: float = 0.029):
        # 2.9% default card fee
        super().__init__(default_fee_rate=default_fee_rate)

    def parse_transactions(self, raw_data: Union[List[Dict[str, Any]], str]) -> List[PaymentRecord]:
        """
        Parses either:
        - List of dicts (from Stripe API charges/refunds endpoint)
        - CSV string (from Stripe dashboard export)
        """
        if isinstance(raw_data, str):
            # Check if JSON string or CSV
            stripped = raw_data.strip()
            if stripped.startswith("[") or stripped.startswith("{"):
                parsed = json.loads(stripped)
                if isinstance(parsed, dict) and "data" in parsed:
                    return self._parse_api_charges(parsed["data"])
                elif isinstance(parsed, list):
                    return self._parse_api_charges(parsed)
            return self._parse_csv(raw_data)
        elif isinstance(raw_data, list):
            return self._parse_api_charges(raw_data)
        elif isinstance(raw_data, dict) and "data" in raw_data:
            return self._parse_api_charges(raw_data["data"])
        return []

    def _parse_api_charges(self, charges: List[Dict[str, Any]]) -> List[PaymentRecord]:
        records: List[PaymentRecord] = []
        for ch in charges:
            # Order matching: check metadata.order_id, shopify_order_id, or transfer_group
            metadata = ch.get("metadata") or {}
            ext_order_id = str(
                metadata.get("order_id")
                or metadata.get("shopify_order_id")
                or ch.get("transfer_group")
                or ch.get("description")
                or ""
            ).strip()

            if not ext_order_id:
                # If no order id in metadata, check if id can be used or skip
                ext_order_id = ch.get("id", "")

            amount = safe_parse_money(ch.get("amount", 0))

            # Gateway fee from application_fee_amount or estimate
            fee_raw = ch.get("application_fee_amount")
            if fee_raw is not None:
                fee = safe_parse_money(fee_raw)
            else:
                fee = self.calculate_fee(amount, method="card")

            # Paid at timestamp
            paid_at = None
            created = ch.get("created")
            if created:
                if isinstance(created, (int, float)):
                    paid_at = datetime.fromtimestamp(int(created), tz=timezone.utc).isoformat()
                else:
                    paid_at = str(created)

            # Payment type & status
            amount_refunded = safe_parse_money(ch.get("amount_refunded", 0))
            is_refund = (amount_refunded >= amount and amount > 0)
            pay_type = "refund" if is_refund else "payment"

            status = normalize_status(ch.get("status", "paid"))

            records.append(PaymentRecord(
                external_payment_id=ch.get("id"),
                external_order_id=ext_order_id,
                amount=amount,
                gateway_fee=fee,
                payment_method="card",
                payment_type=pay_type,
                payment_status=status,
                settlement_status="settled" if status == "paid" else "pending",
                paid_at=paid_at,
                currency=str(ch.get("currency", "PKR")).upper(),
                gateway_name=self.PROVIDER_NAME,
                raw_metadata=ch,
            ))
        return records

    def _parse_csv(self, csv_content: str) -> List[PaymentRecord]:
        records: List[PaymentRecord] = []
        reader = csv.DictReader(io.StringIO(csv_content.strip()))
        for row in reader:
            # Flexible column lookup
            ext_payment_id = row.get("id") or row.get("Transaction ID") or row.get("payment_id")
            ext_order_id = (
                row.get("order_id")
                or row.get("Description")
                or row.get("Order ID")
                or row.get("metadata_order_id")
                or ""
            ).strip()

            amount = safe_parse_money(row.get("Amount") or row.get("amount", 0))
            fee_raw = row.get("Fee") or row.get("fee") or row.get("gateway_fee")
            if fee_raw is not None and str(fee_raw).strip():
                fee = safe_parse_money(fee_raw)
            else:
                fee = self.calculate_fee(amount, method="card")

            status = normalize_status(row.get("Status") or row.get("status", "paid"))
            pay_type = normalize_type(row.get("Type") or row.get("type", "payment"))
            paid_at = row.get("Created (UTC)") or row.get("created") or row.get("paid_at")

            records.append(PaymentRecord(
                external_payment_id=ext_payment_id,
                external_order_id=ext_order_id,
                amount=amount,
                gateway_fee=fee,
                payment_method="card",
                payment_type=pay_type,
                payment_status=status,
                settlement_status="settled" if status == "paid" else "pending",
                paid_at=str(paid_at).strip() if paid_at else None,
                currency="PKR",
                gateway_name=self.PROVIDER_NAME,
                raw_metadata=dict(row),
            ))
        return records

