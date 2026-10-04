"""
Provider-neutral payment and settlement records.

Values that cannot be established from a source remain unknown and are not
filled with estimates or inferred identifiers.
"""
from __future__ import annotations

import re
from abc import ABC, abstractmethod
from dataclasses import dataclass, field
from decimal import Decimal, InvalidOperation, ROUND_HALF_UP
from typing import Any, Dict, List, Optional, Set

VALID_PAYMENT_METHODS: Set[str] = {
    "cod", "card", "bank_transfer", "wallet", "other"
}
VALID_PAYMENT_TYPES: Set[str] = {"payment", "refund"}
VALID_PAYMENT_STATUSES: Set[str] = {"pending", "paid", "failed"}
VALID_SETTLEMENT_STATUSES: Set[str] = {
    "pending", "settled", "failed", "cancelled", "unknown"
}
VALID_SETTLEMENT_ENTRY_TYPES: Set[str] = {
    "payment", "refund", "fee", "adjustment", "reserve", "other"
}


@dataclass
class PaymentRecord:
    """A provider transaction candidate; incomplete records remain raw-only."""

    external_order_id: Optional[str]
    amount: Optional[Decimal]
    gateway_fee: Optional[Decimal] = None
    external_payment_id: Optional[str] = None
    payment_method: Optional[str] = None
    payment_type: Optional[str] = None
    payment_status: Optional[str] = None
    paid_at: Optional[str] = None
    currency: Optional[str] = None
    provider: Optional[str] = None
    provider_account_identifier: Optional[str] = None
    original_external_payment_id: Optional[str] = None
    raw_metadata: Dict[str, Any] = field(default_factory=dict)

    @property
    def gateway_name(self) -> Optional[str]:
        """Backward-compatible name for the canonical provider."""
        return self.provider

    @property
    def net_amount(self) -> None:
        """Net received is a settlement fact, not a transaction calculation."""
        return None

    def to_dict(self) -> Dict[str, Any]:
        return {
            "external_payment_id": self.external_payment_id,
            "external_order_id": self.external_order_id,
            "payment_method": self.payment_method,
            "payment_type": self.payment_type,
            "payment_status": self.payment_status,
            "amount": str(self.amount) if self.amount is not None else None,
            "gateway_fee": str(self.gateway_fee) if self.gateway_fee is not None else None,
            "paid_at": self.paid_at,
            "currency": self.currency,
            "provider": self.provider,
            "provider_account_identifier": self.provider_account_identifier,
            "original_external_payment_id": self.original_external_payment_id,
            "raw_metadata": self.raw_metadata,
        }


@dataclass
class PaymentSettlement:
    """Provider-reported payout/statement facts; amounts are never derived."""

    provider: str
    provider_account_identifier: str
    currency: str
    status: str
    external_settlement_id: Optional[str] = None
    gross_amount: Optional[Decimal] = None
    fee_amount: Optional[Decimal] = None
    adjustment_amount: Optional[Decimal] = None
    net_amount: Optional[Decimal] = None
    settled_at: Optional[str] = None
    expected_arrival_at: Optional[str] = None
    provider_status: Optional[str] = None


@dataclass
class PaymentSettlementEntry:
    """A provider-reported settlement line, optionally matched to a payment."""

    entry_type: str
    signed_amount: Decimal
    currency: str
    external_entry_id: Optional[str] = None
    external_transaction_id: Optional[str] = None
    payment_id: Optional[int] = None
    provider_status: Optional[str] = None


def safe_parse_money(value: Any) -> Optional[Decimal]:
    """Parse a supplied amount without replacing missing/invalid values by zero."""
    if value is None or isinstance(value, bool):
        return None
    if isinstance(value, str):
        normalized = value.strip()
        if not normalized:
            return None
        if "," in normalized:
            if not re.fullmatch(r"[+-]?\d{1,3}(?:,\d{3})+(?:\.\d+)?", normalized):
                return None
            normalized = normalized.replace(",", "")
    else:
        normalized = str(value)
    try:
        amount = Decimal(normalized)
    except (InvalidOperation, ValueError):
        return None
    if not amount.is_finite():
        return None
    return amount.quantize(Decimal("0.01"), rounding=ROUND_HALF_UP)


def normalize_method(raw: Optional[str]) -> Optional[str]:
    if not raw:
        return None
    value = str(raw).strip().lower()
    return value if value in VALID_PAYMENT_METHODS else None


def normalize_status(raw: Optional[str]) -> Optional[str]:
    """Accept only canonical statuses; provider-specific aliases remain raw-only."""
    if not raw:
        return None
    value = str(raw).strip().lower()
    return value if value in VALID_PAYMENT_STATUSES else None


def normalize_type(raw: Optional[str]) -> Optional[str]:
    if not raw:
        return None
    value = str(raw).strip().lower()
    return value if value in VALID_PAYMENT_TYPES else None


class BasePaymentProvider(ABC):
    PROVIDER_NAME: str = "base"
    SUPPORTED_METHODS: Set[str] = {"api", "csv", "settlement"}

    @abstractmethod
    def parse_transactions(self, raw_data: Any) -> List[PaymentRecord]:
        """Parse transaction candidates while preserving unknown values."""
        raise NotImplementedError
