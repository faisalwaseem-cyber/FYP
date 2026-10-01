"""
Payment Provider Factory — BRIDGE FLOW Phase 6
==============================================

Factory for resolving and instantiating payment and settlement providers.
"""
from __future__ import annotations

from typing import Dict, Type

from connectors.payment.base import BasePaymentProvider
from connectors.payment.cod_reconciler import CODReconciliationProvider
from connectors.payment.easypaisa_provider import EasyPaisaPaymentProvider
from connectors.payment.jazzcash_provider import JazzCashPaymentProvider
from connectors.payment.manual_provider import ManualPaymentProvider
from connectors.payment.payfast_provider import PayFastPaymentProvider
from connectors.payment.stripe_provider import StripePaymentProvider

_REGISTRY: Dict[str, Type[BasePaymentProvider]] = {
    "stripe": StripePaymentProvider,
    "payfast": PayFastPaymentProvider,
    "jazzcash": JazzCashPaymentProvider,
    "easypaisa": EasyPaisaPaymentProvider,
    "cod": CODReconciliationProvider,
    "manual": ManualPaymentProvider,
    "bank_transfer": ManualPaymentProvider,
}


def get_payment_provider(provider_name: str, **kwargs) -> BasePaymentProvider:
    """
    Returns an instance of the requested payment provider.

    Parameters
    ----------
    provider_name : str
        'stripe' | 'payfast' | 'jazzcash' | 'easypaisa' | 'cod' | 'manual'
    """
    key = provider_name.strip().lower()
    cls = _REGISTRY.get(key)
    if not cls:
        raise ValueError(
            f"Unsupported payment provider '{provider_name}'. "
            f"Available providers: {list(_REGISTRY.keys())}"
        )
    return cls(**kwargs)

