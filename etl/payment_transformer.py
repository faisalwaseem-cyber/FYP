"""
Payment ETL Transformer — BRIDGE FLOW Phase 6
=============================================

Transforms raw payment and settlement records into canonical DataFrames
matching core.payment.

Canonical Flow:
Order → Payment → Gateway Fee → Settlement → Net Received

Columns produced:
  business_id, order_id, external_payment_id, payment_method,
  payment_type, payment_status, amount, gateway_fee, net_amount, paid_at
"""
from __future__ import annotations

import logging
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional, Union

import pandas as pd

from connectors.payment.base import (
    VALID_PAYMENT_METHODS,
    VALID_PAYMENT_STATUSES,
    VALID_PAYMENT_TYPES,
    PaymentRecord,
    normalize_method,
    normalize_status,
    normalize_type,
    safe_parse_money,
)
from connectors.payment.provider_factory import get_payment_provider

logger = logging.getLogger(__name__)


class PaymentTransformer:
    """
    Transforms PaymentRecord objects or raw provider records into canonical DataFrames.
    """

    def __init__(self, business_id: int):
        self.business_id = business_id

    def transform_payment_records(
        self,
        records: List[PaymentRecord],
        order_map: Dict[str, int],
    ) -> pd.DataFrame:
        """
        Transforms a list of PaymentRecord dataclasses into a canonical DataFrame.
        Unmatched orders are skipped.
        """
        rows: List[Dict[str, Any]] = []

        for r in records:
            ext_order_id = str(r.external_order_id or "").strip()
            order_id = order_map.get(ext_order_id)
            if not order_id:
                logger.warning(
                    "PaymentTransformer: order '%s' not found for business %s. Skipping.",
                    ext_order_id,
                    self.business_id,
                )
                continue

            amount = safe_parse_money(r.amount)
            fee = safe_parse_money(r.gateway_fee)
            net = max(0.0, round(amount - fee, 2)) if r.payment_type != "refund" else -amount

            method = r.payment_method if r.payment_method in VALID_PAYMENT_METHODS else normalize_method(r.payment_method)
            p_type = r.payment_type if r.payment_type in VALID_PAYMENT_TYPES else normalize_type(r.payment_type)
            status = r.payment_status if r.payment_status in VALID_PAYMENT_STATUSES else normalize_status(r.payment_status)

            ext_payment_id = r.external_payment_id or f"PAY-ORD-{order_id}-{p_type}"

            rows.append({
                "business_id":         self.business_id,
                "order_id":            int(order_id),
                "external_payment_id": str(ext_payment_id),
                "external_order_id":   ext_order_id,
                "payment_method":      method,
                "payment_type":        p_type,
                "payment_status":      status,
                "amount":              amount,
                "gateway_fee":         fee,
                "net_amount":          net,
                "paid_at":             r.paid_at,
                "gateway_name":        r.gateway_name,
            })

        if not rows:
            return pd.DataFrame(columns=[
                "business_id", "order_id", "external_payment_id", "external_order_id",
                "payment_method", "payment_type", "payment_status", "amount",
                "gateway_fee", "net_amount", "paid_at", "gateway_name"
            ])

        return pd.DataFrame(rows).drop_duplicates(subset=["business_id", "external_payment_id"])

    def transform_provider_data(
        self,
        provider_name: str,
        raw_data: Any,
        order_map: Dict[str, int],
    ) -> pd.DataFrame:
        """
        Parses raw data using the designated provider and transforms it into the canonical DataFrame.
        """
        provider = get_payment_provider(provider_name)
        records = provider.parse_transactions(raw_data)
        return self.transform_payment_records(records, order_map)


# ---------------------------------------------------------------------------
# Backward Compatibility Wrappers (Used by existing pipelines/tests)
# ---------------------------------------------------------------------------

def transform_stripe_charges(
    charges: List[Dict[str, Any]],
    external_order_id_map: Dict[str, int],
    business_id: int,
) -> pd.DataFrame:
    """Legacy compatibility function for Stripe charge transformation."""
    transformer = PaymentTransformer(business_id=business_id)
    return transformer.transform_provider_data(
        provider_name="stripe",
        raw_data=charges,
        order_map=external_order_id_map,
    )


def transform_csv_payments(
    raw_records: List[Dict[str, Any]],
    external_order_id_map: Dict[str, int],
    business_id: int,
    gateway_name: str = "manual",
) -> pd.DataFrame:
    """Legacy compatibility function for CSV payments transformation."""
    transformer = PaymentTransformer(business_id=business_id)
    provider_name = gateway_name if gateway_name in ("payfast", "easypaisa", "jazzcash", "cod", "stripe") else "manual"
    return transformer.transform_provider_data(
        provider_name=provider_name,
        raw_data=raw_records,
        order_map=external_order_id_map,
    )

