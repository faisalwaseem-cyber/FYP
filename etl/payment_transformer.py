"""Transforms payment candidates only when canonical facts are verified."""
from __future__ import annotations

import logging
import re
from decimal import Decimal
from typing import Any, Dict, List, Optional

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
from etl.normalizers import normalize_timestamp

logger = logging.getLogger(__name__)


def _fits_numeric(value: Decimal, precision: int, scale: int) -> bool:
    try:
        rounded = value.quantize(Decimal(1).scaleb(-scale))
    except ArithmeticError:
        return False
    return rounded == value and abs(value) < Decimal(10) ** (precision - scale)


class PaymentTransformer:
    """Transforms verified records into rows accepted by the existing schema."""

    def __init__(self, business_id: int):
        if not isinstance(business_id, int) or business_id <= 0:
            raise ValueError("business_id must be a positive server-resolved integer")
        self.business_id = business_id
        self.rejected_records: List[Dict[str, str]] = []

    def transform_payment_records(
        self,
        records: List[PaymentRecord],
        order_map: Dict[str, int],
        original_payment_map: Optional[Dict[str, Dict[str, Any]]] = None,
    ) -> pd.DataFrame:
        rows: List[Dict[str, Any]] = []
        self.rejected_records = []
        original_payment_map = original_payment_map or {}

        def reject(reason: str) -> None:
            self.rejected_records.append({"reason": reason})
            logger.warning("PaymentTransformer retained a payment raw-only: %s", reason)

        for record in records:
            external_order_id = str(record.external_order_id or "").strip()
            order_id = order_map.get(external_order_id)
            external_payment_id = str(record.external_payment_id or "").strip()
            provider = str(record.provider or "").strip().lower()
            account = str(record.provider_account_identifier or "").strip()
            currency = str(record.currency or "").strip().upper()
            method = record.payment_method if record.payment_method in VALID_PAYMENT_METHODS else normalize_method(record.payment_method)
            payment_type = record.payment_type if record.payment_type in VALID_PAYMENT_TYPES else normalize_type(record.payment_type)
            status = record.payment_status if record.payment_status in VALID_PAYMENT_STATUSES else normalize_status(record.payment_status)
            amount = safe_parse_money(record.amount)
            fee = safe_parse_money(record.gateway_fee)

            if not external_payment_id:
                reject("provider transaction identifier is missing")
                continue
            if not provider or provider == "unknown":
                reject("provider identity is missing")
                continue
            if not account:
                reject("provider account identifier is missing")
                continue
            if provider == "manual" and not account:
                account = "manual"
            if not re.fullmatch(r"[A-Z]{3}", currency):
                reject("explicit transaction currency is missing or invalid")
                continue
            if method is None:
                reject("payment method is not recognized")
                continue
            if payment_type is None:
                reject("transaction type is not recognized")
                continue
            if status is None:
                reject("provider status cannot be mapped to the existing payment constraint")
                continue
            if amount is None or amount < 0:
                reject("payment amount is missing or negative")
                continue
            if fee is None or fee < 0:
                reject("explicit gateway fee is missing or negative")
                continue
            if not _fits_numeric(amount, 12, 2) or not _fits_numeric(fee, 12, 2):
                reject("payment amount or fee exceeds the existing NUMERIC(12,2) precision")
                continue

            original_id = None
            if record.original_external_payment_id:
                if payment_type != "refund":
                    reject("original transaction reference supplied for a non-refund")
                    continue
                if record.original_external_payment_id == external_payment_id:
                    reject("refund cannot reference itself as the original transaction")
                    continue
                original = original_payment_map.get(record.original_external_payment_id)
                if (
                    original is not None
                    and original.get("provider") == provider
                    and original.get("provider_account_identifier") == account
                    and original.get("currency") == currency
                    and original.get("payment_type") == "payment"
                    and record.original_external_payment_id != external_payment_id
                ):
                    original_id = original.get("payment_id")
                    if order_id is None:
                        order_id = original.get("order_id")
            if not order_id:
                reject("no tenant-scoped order match")
                continue

            rows.append({
                "business_id": self.business_id,
                "order_id": int(order_id),
                "external_payment_id": external_payment_id,
                "payment_method": method,
                "payment_type": payment_type,
                "payment_status": status,
                "amount": amount,
                "gateway_fee": fee,
                "paid_at": normalize_timestamp(record.paid_at),
                "provider": provider,
                "provider_account_identifier": account,
                "currency": currency,
                "original_payment_id": original_id,
                "external_order_id": external_order_id,
            })

        columns = [
            "business_id", "order_id", "external_payment_id", "payment_method",
            "payment_type", "payment_status", "amount", "gateway_fee", "paid_at",
            "provider", "provider_account_identifier", "currency",
            "original_payment_id", "external_order_id",
        ]
        if not rows:
            return pd.DataFrame(columns=columns)
        frame = pd.DataFrame(rows, columns=columns)
        if frame["external_payment_id"].duplicated(keep=False).any():
            raise ValueError(
                "Input contains duplicate external payment IDs within the business; "
                "the retained legacy uniqueness rule cannot disambiguate them."
            )
        return frame

    def transform_provider_data(
        self,
        provider_name: str,
        raw_data: Any,
        order_map: Dict[str, int],
        original_payment_map: Optional[Dict[str, Dict[str, Any]]] = None,
    ) -> pd.DataFrame:
        provider = get_payment_provider(provider_name)
        records = provider.parse_transactions(raw_data)
        return self.transform_payment_records(records, order_map, original_payment_map)


def transform_stripe_charges(
    charges: List[Dict[str, Any]],
    external_order_id_map: Dict[str, int],
    business_id: int,
) -> pd.DataFrame:
    return PaymentTransformer(business_id).transform_provider_data(
        "stripe", charges, external_order_id_map
    )


def transform_csv_payments(
    raw_records: List[Dict[str, Any]],
    external_order_id_map: Dict[str, int],
    business_id: int,
    gateway_name: str = "manual",
) -> pd.DataFrame:
    provider_name = gateway_name if gateway_name in (
        "payfast", "easypaisa", "jazzcash", "cod", "stripe"
    ) else "manual"
    return PaymentTransformer(business_id).transform_provider_data(
        provider_name, raw_records, external_order_id_map
    )
