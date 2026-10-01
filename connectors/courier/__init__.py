"""
connectors/courier/__init__.py — Courier client package (Phase 5.2)

Exports the common exception hierarchy and all provider-specific clients.
Import from here for clean, provider-agnostic coupling:

    from connectors.courier import PostExClient, TraxClient, LeopardsClient
    from connectors.courier import CourierAuthError, CourierRateLimitError
"""
from connectors.courier.base import (
    BaseCourierClient,
    CourierAPIError,
    CourierAuthError,
    CourierRateLimitError,
    CourierTrackingNotFoundError,
)
from connectors.courier.leopards_client import LeopardsClient
from connectors.courier.postex_client import PostExClient
from connectors.courier.trax_client import TraxClient

__all__ = [
    # Shared interface
    "BaseCourierClient",
    # Custom exceptions
    "CourierAPIError",
    "CourierAuthError",
    "CourierRateLimitError",
    "CourierTrackingNotFoundError",
    # Provider clients
    "LeopardsClient",
    "PostExClient",
    "TraxClient",
]
