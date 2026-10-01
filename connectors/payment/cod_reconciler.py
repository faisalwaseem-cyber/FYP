"""
COD Reconciliation Provider — BRIDGE FLOW Phase 6
==================================================

Reconciles Cash On Delivery (COD) remittances from couriers (PostEx, Trax,
Leopards, TCS, etc.) into the canonical payment & settlement model.

Canonical Objective:
Order → Payment (COD) → Gateway/Courier Fee → Settlement → Net Received

Integration Methods Supported:
1. Courier COD Settlement API (PostEx / Leopards / Trax API payloads)
2. Courier Remittance CSV / Excel Statements
3. In-flight Shipment Reconciler (derives paid COD record from delivered shipment)
"""
from __future__ import annotations

import csv
import io
from typing import Any, Dict, List, Optional, Union

from connectors.payment.base import (
    BasePaymentProvider,
    PaymentRecord,
    safe_parse_money,
)


class CODReconciliationProvider(BasePaymentProvider):
    PROVIDER_NAME: str = "cod"
    SUPPORTED_METHODS = {"api", "csv", "settlement"}

    COLUMN_ALIASES = {
        "tracking_num": ["tracking_number", "trackingNumber", "cn", "booked_packet_cn", "CN", "Tracking No"],
        "order_id":     ["external_order_id", "orderRefNumber", "order_id", "booked_packet_order_id", "Reference", "Order ID"],
        "amount":       ["cod_amount", "orderAmount", "amount", "booked_packet_collect_amount", "Collected Amount", "COD Amount"],
        "fee":          ["courier_fee", "cod_fee", "cash_handling_fee", "fee", "Charges", "Deduction"],
        "date":         ["settlement_date", "settlementDate", "settled_date", "Date", "Remittance Date"],
    }

    def __init__(self, default_fee_rate: float = 0.0):
        # COD gateway fee is 0% by default unless the courier charges explicit COD handling fees
        super().__init__(default_fee_rate=default_fee_rate)

    def _resolve(self, row: Dict[str, Any], key: str) -> str:
        for alias in self.COLUMN_ALIASES.get(key, []):
            if alias in row and row[alias] is not None:
                return str(row[alias]).strip()
        return ""

    def parse_transactions(self, raw_data: Union[str, List[Dict[str, Any]]]) -> List[PaymentRecord]:
        """
        Parses courier COD settlement API records or CSV remittance reports.
        """
        if isinstance(raw_data, str):
            reader = csv.DictReader(io.StringIO(raw_data.strip()))
            rows = list(reader)
        elif isinstance(raw_data, list):
            rows = raw_data
        elif isinstance(raw_data, dict):
            # Might be wrapped under 'dist' or 'data' or 'packet_list'
            inner = raw_data.get("dist") or raw_data.get("data") or raw_data.get("packet_list") or []
            rows = inner if isinstance(inner, list) else [raw_data]
        else:
            return []

        records: List[PaymentRecord] = []
        for r in rows:
            ext_order_id = self._resolve(r, "order_id")
            tracking_num = self._resolve(r, "tracking_num")

            # Fallback identifier for COD payment
            ext_payment_id = f"COD-{tracking_num}" if tracking_num else (f"COD-{ext_order_id}" if ext_order_id else None)

            amount = safe_parse_money(self._resolve(r, "amount") or r.get("amount", 0))

            raw_fee = self._resolve(r, "fee")
            fee = safe_parse_money(raw_fee) if raw_fee else self.calculate_fee(amount, method="cod")

            settle_date = self._resolve(r, "date") or None

            records.append(PaymentRecord(
                external_payment_id=ext_payment_id,
                external_order_id=ext_order_id,
                amount=amount,
                gateway_fee=fee,
                payment_method="cod",
                payment_type="payment",
                payment_status="paid",
                settlement_status="settled",
                settlement_date=settle_date,
                paid_at=settle_date,
                currency="PKR",
                gateway_name=self.PROVIDER_NAME,
                raw_metadata=dict(r),
            ))

        return records

    def reconcile_delivered_shipment(
        self,
        shipment_data: Dict[str, Any],
        courier_cod_fee: float = 0.0,
    ) -> Optional[PaymentRecord]:
        """
        Generates a PaymentRecord from a delivered core.shipment record when
        cash was collected upon delivery.
        """
        cod_amount = safe_parse_money(shipment_data.get("cod_amount", 0))
        if cod_amount <= 0:
            return None

        ext_order_id = str(shipment_data.get("external_order_id", "")).strip()
        tracking_num = str(shipment_data.get("tracking_number", "")).strip()
        delivered_at = shipment_data.get("delivered_at")

        return PaymentRecord(
            external_payment_id=f"COD-{tracking_num}" if tracking_num else f"COD-{ext_order_id}",
            external_order_id=ext_order_id,
            amount=cod_amount,
            gateway_fee=safe_parse_money(courier_cod_fee),
            payment_method="cod",
            payment_type="payment",
            payment_status="paid",
            settlement_status="settled",
            settlement_date=str(delivered_at) if delivered_at else None,
            paid_at=str(delivered_at) if delivered_at else None,
            currency="PKR",
            gateway_name=f"cod_{shipment_data.get('courier_name', 'courier')}",
            raw_metadata=shipment_data,
        )

