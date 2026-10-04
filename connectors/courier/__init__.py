"""Shared courier client contracts and active M&P scope.

The M&P integration remains incomplete until its API contract is verified.
"""
from connectors.courier.base import (
    BaseCourierClient,
    CourierAPIError,
    CourierAuthError,
    CourierRateLimitError,
    CourierTrackingNotFoundError,
)
ACTIVE_COURIER_PLATFORMS = ("m&p",)
M_AND_P_IMPLEMENTATION_STATUS = "incomplete"

__all__ = [
    "BaseCourierClient",
    "CourierAPIError",
    "CourierAuthError",
    "CourierRateLimitError",
    "CourierTrackingNotFoundError",
    "ACTIVE_COURIER_PLATFORMS",
    "M_AND_P_IMPLEMENTATION_STATUS",
]
