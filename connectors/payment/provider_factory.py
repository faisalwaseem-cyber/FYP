"""
Payment Provider Factory — BRIDGE FLOW Phase 6
==============================================

Factory for resolving and instantiating payment and settlement providers.
"""
from __future__ import annotations

from typing import Dict, Type

from connectors.payment.base import BasePaymentProvider
from connectors.payment.cod_reconciler import CODReconciliationProvider
from connectors.payment.jazzcash_provider import JazzCashPaymentProvider
from connectors.payment.manual_provider import ManualPaymentProvider

_REGISTRY: Dict[str, Type[BasePaymentProvider]] = {
    "jazzcash": JazzCashPaymentProvider,
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
        'jazzcash' | 'cod' | 'manual' | 'bank_transfer'
    """
    key = provider_name.strip().lower()
    cls = _REGISTRY.get(key)
    if not cls:
        raise ValueError(
            f"Unsupported payment provider '{provider_name}'. "
            f"Available providers: {list(_REGISTRY.keys())}"
        )
    return cls(**kwargs)
