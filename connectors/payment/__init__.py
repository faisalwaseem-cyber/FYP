"""
Payment and Settlement Foundation — BRIDGE FLOW Phase 6
=======================================================

Exports:
- BasePaymentProvider, PaymentRecord, SettlementBatch
- Provider implementations: Stripe, PayFast, JazzCash, EasyPaisa, COD, Manual
- Provider factory: get_payment_provider
"""
from connectors.payment.base import (
    VALID_PAYMENT_METHODS,
    VALID_PAYMENT_STATUSES,
    VALID_PAYMENT_TYPES,
    VALID_SETTLEMENT_STATUSES,
    BasePaymentProvider,
    PaymentRecord,
    SettlementBatch,
    normalize_method,
    normalize_status,
    normalize_type,
    safe_parse_money,
)
from connectors.payment.cod_reconciler import CODReconciliationProvider
from connectors.payment.easypaisa_provider import EasyPaisaPaymentProvider
from connectors.payment.jazzcash_provider import JazzCashPaymentProvider
from connectors.payment.manual_provider import ManualPaymentProvider
from connectors.payment.payfast_provider import PayFastPaymentProvider
from connectors.payment.provider_factory import get_payment_provider
from connectors.payment.stripe_provider import StripePaymentProvider

__all__ = [
    # Base abstraction & data models
    "BasePaymentProvider",
    "PaymentRecord",
    "SettlementBatch",
    # Enums & normalizers
    "VALID_PAYMENT_METHODS",
    "VALID_PAYMENT_STATUSES",
    "VALID_PAYMENT_TYPES",
    "VALID_SETTLEMENT_STATUSES",
    "normalize_method",
    "normalize_status",
    "normalize_type",
    "safe_parse_money",
    # Providers
    "CODReconciliationProvider",
    "EasyPaisaPaymentProvider",
    "JazzCashPaymentProvider",
    "ManualPaymentProvider",
    "PayFastPaymentProvider",
    "StripePaymentProvider",
    # Factory
    "get_payment_provider",
]

