"""
Payment & Settlement Provider Abstraction — BRIDGE FLOW Phase 6
===============================================================

Defines the common interface, data models, and normalization rules for payment
gateways, mobile wallets, and COD reconciliation providers.

Integration Methods Supported:
------------------------------
1. REST API                 (e.g., Stripe charges & balance transactions)
2. Settlement API           (e.g., Courier COD settlement API, Stripe payouts)
3. CSV / Excel Statement    (e.g., PayFast, JazzCash, EasyPaisa merchant portal exports)
4. COD Reconciliation       (e.g., PostEx, Trax, Leopards cash remittance)
5. Manual / Bank Statement  (e.g., direct IBFT, Raast, Kuickpay)

Canonical Objective:
--------------------
Order → Payment → Gateway Fee → Settlement → Net Received

Database Constraints Verified (ecommerce_intelligence_schema.sql):
-------------------------------------------------------------------
core.payment:
  payment_method: 'cod' | 'card' | 'bank_transfer' | 'wallet' | 'other'
  payment_type:   'payment' | 'refund'
  payment_status: 'pending' | 'paid' | 'failed'
  amount:         NUMERIC(12,2) >= 0
  gateway_fee:    NUMERIC(12,2) >= 0
"""
from __future__ import annotations

from abc import ABC, abstractmethod
from dataclasses import dataclass, field
from datetime import datetime, timezone
from decimal import Decimal, InvalidOperation
from typing import Any, Dict, List, Optional, Set


# ---------------------------------------------------------------------------
# Canonical Enum Sets
# ---------------------------------------------------------------------------

VALID_PAYMENT_METHODS: Set[str] = {
    "cod", "card", "bank_transfer", "wallet", "other"
}

VALID_PAYMENT_TYPES: Set[str] = {
    "payment", "refund"
}

VALID_PAYMENT_STATUSES: Set[str] = {
    "pending", "paid", "failed"
}

VALID_SETTLEMENT_STATUSES: Set[str] = {
    "settled", "unsettled", "pending"
}


# ---------------------------------------------------------------------------
# Data Models
# ---------------------------------------------------------------------------

@dataclass
class PaymentRecord:
    """
    Standardized payment record produced by any payment provider.
    Represents an atomic transaction matching an order.
    """
    external_order_id: str
    amount: float
    gateway_fee: float = 0.0
    external_payment_id: Optional[str] = None
    payment_method: str = "other"
    payment_type: str = "payment"
    payment_status: str = "paid"
    settlement_status: str = "settled"
    settlement_date: Optional[str] = None
    paid_at: Optional[str] = None
    currency: str = "PKR"
    gateway_name: str = "unknown"
    raw_metadata: Dict[str, Any] = field(default_factory=dict)

    @property
    def net_amount(self) -> float:
        """
        Calculates Net Received: Amount - Gateway Fee.
        For refunds, returns -Amount.
        """
        if self.payment_type == "refund":
            return -round(self.amount, 2)
        return max(0.0, round(self.amount - self.gateway_fee, 2))

    def to_dict(self) -> Dict[str, Any]:
        """Converts to a dictionary suitable for DataFrame construction or logging."""
        return {
            "external_payment_id": self.external_payment_id,
            "external_order_id": self.external_order_id,
            "payment_method": self.payment_method,
            "payment_type": self.payment_type,
            "payment_status": self.payment_status,
            "amount": round(self.amount, 2),
            "gateway_fee": round(self.gateway_fee, 2),
            "net_amount": self.net_amount,
            "settlement_status": self.settlement_status,
            "settlement_date": self.settlement_date,
            "paid_at": self.paid_at,
            "currency": self.currency,
            "gateway_name": self.gateway_name,
            "raw_metadata": self.raw_metadata,
        }


@dataclass
class SettlementBatch:
    """
    Represents a collection of settled payments from a provider payout.
    """
    settlement_id: str
    gateway_name: str
    settlement_date: str
    gross_amount: float
    total_fees: float
    net_payout: float
    transactions: List[PaymentRecord] = field(default_factory=list)

    @property
    def transaction_count(self) -> int:
        return len(self.transactions)


# ---------------------------------------------------------------------------
# Utility Normalizers
# ---------------------------------------------------------------------------

def safe_parse_money(val: Any) -> float:
    """Safely parses any numeric or string representation into a float >= 0.0 rounded to 2 decimals."""
    if val is None:
        return 0.0
    try:
        s = str(val).strip().replace(",", "")
        dec = Decimal(s)
        return max(0.0, float(round(dec, 2)))
    except (InvalidOperation, ValueError):
        return 0.0


def normalize_method(raw: Optional[str]) -> str:
    """Normalizes raw gateway string to canonical core.payment.payment_method."""
    if not raw:
        return "other"
    s = str(raw).strip().lower()

    if any(k in s for k in ("cod", "cash on delivery", "cash_on_delivery", "cash", "manual")):
        return "cod"
    if any(k in s for k in ("card", "visa", "master", "credit", "debit", "stripe", "checkout", "amex")):
        return "card"
    if any(k in s for k in ("wallet", "easypaisa", "easy paisa", "jazzcash", "jazz cash", "payfast", "sadapay", "nayapay")):
        return "wallet"
    if any(k in s for k in ("bank", "transfer", "ibft", "raast", "kuickpay", "wire")):
        return "bank_transfer"

    return "other"


def normalize_status(raw: Optional[str]) -> str:
    """Normalizes raw status string to canonical core.payment.payment_status."""
    if not raw:
        return "paid"
    s = str(raw).strip().lower()

    if any(k in s for k in ("paid", "succeeded", "success", "complete", "completed", "settled", "captured")):
        return "paid"
    if any(k in s for k in ("pending", "processing", "in_review", "authorized", "initiated")):
        return "pending"
    if any(k in s for k in ("failed", "failure", "declined", "void", "voided", "cancelled", "canceled", "rejected")):
        return "failed"

    return "paid"


def normalize_type(raw: Optional[str]) -> str:
    """Normalizes transaction type to canonical core.payment.payment_type ('payment' | 'refund')."""
    if not raw:
        return "payment"
    s = str(raw).strip().lower()
    if any(k in s for k in ("refund", "reversal", "chargeback", "return")):
        return "refund"
    return "payment"


# ---------------------------------------------------------------------------
# Abstract Base Payment Provider
# ---------------------------------------------------------------------------

class BasePaymentProvider(ABC):
    """
    Abstract Base Class for all payment and settlement providers.
    """

    PROVIDER_NAME: str = "base"
    SUPPORTED_METHODS: Set[str] = {"api", "csv", "settlement"}

    def __init__(self, default_fee_rate: float = 0.0):
        self.default_fee_rate = default_fee_rate

    @abstractmethod
    def parse_transactions(self, raw_data: Any) -> List[PaymentRecord]:
        """
        Parses raw API response, CSV string, or dict list into standardized PaymentRecords.
        """
        pass

    def calculate_fee(
        self,
        amount: float,
        method: str = "other",
        payment_type: str = "payment",
    ) -> float:
        """
        Calculates gateway fee for an amount when not explicitly provided by the provider.
        """
        if payment_type == "refund":
            return 0.0
        return round(amount * self.default_fee_rate, 2)

    def parse_settlement_batch(
        self,
        raw_data: Any,
        settlement_id: Optional[str] = None,
        settlement_date: Optional[str] = None,
    ) -> SettlementBatch:
        """
        Aggregates parsed transactions into a unified SettlementBatch.
        Default implementation parses transactions and computes batch sums.
        """
        txns = self.parse_transactions(raw_data)
        gross = sum(t.amount for t in txns if t.payment_type == "payment")
        total_fees = sum(t.gateway_fee for t in txns)
        net = round(gross - total_fees, 2)

        s_id = settlement_id or f"SETTLE-{self.PROVIDER_NAME}-{datetime.now(timezone.utc).strftime('%Y%m%d%H%M%S')}"
        s_date = settlement_date or datetime.now(timezone.utc).strftime("%Y-%m-%d")

        return SettlementBatch(
            settlement_id=s_id,
            gateway_name=self.PROVIDER_NAME,
            settlement_date=s_date,
            gross_amount=round(gross, 2),
            total_fees=round(total_fees, 2),
            net_payout=net,
            transactions=txns,
        )

