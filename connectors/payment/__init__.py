"""
Payment and Settlement Foundation — BRIDGE FLOW Phase 6
=======================================================

Exports:
- BasePaymentProvider, PaymentRecord, PaymentSettlement, PaymentSettlementEntry
- Provider implementations: JazzCash, COD reconciliation, Manual
- Provider factory: get_payment_provider
"""
from connectors.payment.base import (
    VALID_PAYMENT_METHODS,
    VALID_PAYMENT_STATUSES,
    VALID_PAYMENT_TYPES,
    VALID_SETTLEMENT_ENTRY_TYPES,
    VALID_SETTLEMENT_STATUSES,
    BasePaymentProvider,
    PaymentRecord,
    PaymentSettlement,
    PaymentSettlementEntry,
    normalize_method,
    normalize_status,
    normalize_type,
    safe_parse_money,
)
from connectors.payment.cod_reconciler import CODReconciliationProvider
from connectors.payment.jazzcash_provider import JazzCashPaymentProvider
from connectors.payment.manual_provider import ManualPaymentProvider
from connectors.payment.provider_factory import get_payment_provider

__all__ = [
    # Base abstraction & data models
    "BasePaymentProvider",
    "PaymentRecord",
    "PaymentSettlement",
    "PaymentSettlementEntry",
    # Enums & normalizers
    "VALID_PAYMENT_METHODS",
    "VALID_PAYMENT_STATUSES",
    "VALID_PAYMENT_TYPES",
    "VALID_SETTLEMENT_ENTRY_TYPES",
    "VALID_SETTLEMENT_STATUSES",
    "normalize_method",
    "normalize_status",
    "normalize_type",
    "safe_parse_money",
    # Providers
    "CODReconciliationProvider",
    "JazzCashPaymentProvider",
    "ManualPaymentProvider",
    # Factory
    "get_payment_provider",
]
