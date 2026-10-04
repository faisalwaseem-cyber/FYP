"""
BRIDGE FLOW — Courier Client Base Interface (Phase 5.2)

Defines the common abstract contract all provider-specific courier clients
must implement. Each concrete client:

  - Receives decrypted credentials at construction time (injected by
    CredentialManager — no DB reads inside clients).
  - Never writes to the database.
  - Is independently testable via mock HTTP responses.
  - Exposes a safe __repr__ that never leaks credentials.

Custom exceptions are also defined here so callers can catch courier
errors uniformly regardless of provider.
"""
from __future__ import annotations

from abc import ABC, abstractmethod
from typing import Any, Dict, List, Optional


# ---------------------------------------------------------------------------
# Courier-level exceptions
# ---------------------------------------------------------------------------

class CourierAuthError(RuntimeError):
    """
    Raised when the courier API explicitly rejects the credentials
    (HTTP 401 / 403 or a provider-specific auth error code).
    """
    pass


class CourierRateLimitError(RuntimeError):
    """
    Raised when the courier API signals too-many-requests
    (HTTP 429 or a business-level throttle code) after all retries.
    """
    pass


class CourierAPIError(RuntimeError):
    """
    Raised when the courier API returns a business-level error
    response that is not a transient server error or rate limit.

    Attributes
    ----------
    code : str
        Provider-specific error code string.
    provider : str
        Name of the configured courier provider.
    """

    def __init__(self, code: str, message: str, provider: str = ""):
        self.code = str(code)
        self.provider = provider
        super().__init__(f"[{provider.upper()}] Courier API Error [{code}]: {message}")


class CourierTrackingNotFoundError(RuntimeError):
    """
    Raised when the courier API confirms the tracking number does not exist.
    This is a permanent (non-retryable) error — distinct from a transient 5xx.
    """
    pass


# ---------------------------------------------------------------------------
# Abstract base client
# ---------------------------------------------------------------------------

class BaseCourierClient(ABC):
    """
    Abstract courier client interface.

    All provider clients must inherit from this class and implement every
    abstract method. The interface intentionally covers only verified endpoints
    that all supported couriers actually expose.

    Design invariants
    -----------------
    * No database interaction of any kind inside a client.
    * Credentials are received as plain strings at construction time;
      decryption is the caller's responsibility (CredentialManager).
    * __repr__ must never expose credentials.
    * Rate-limit backoff and transient-5xx retries are the client's internal
      responsibility — callers should not need to wrap clients in retry loops.
    """

    # Subclasses must declare this class-level name for safe logging.
    PROVIDER_NAME: str = "unknown"

    # ------------------------------------------------------------------
    # Required methods
    # ------------------------------------------------------------------

    @abstractmethod
    def track_shipment(self, tracking_number: str) -> Dict[str, Any]:
        """
        Retrieves real-time tracking details for a single shipment.

        Parameters
        ----------
        tracking_number : str
            The courier-assigned tracking / CN number.

        Returns
        -------
        dict
            Raw JSON-deserialized response from the provider.

        Raises
        ------
        CourierTrackingNotFoundError
            If the provider confirms the tracking number does not exist.
        CourierAuthError
            If the API rejects the credentials.
        CourierRateLimitError
            If the API rate-limits the request after retries.
        CourierAPIError
            For any other business-level error.
        RuntimeError
            For unrecoverable network or HTTP errors after retries.
        """
        ...

    @abstractmethod
    def get_order_status(self, reference: str) -> Dict[str, Any]:
        """
        Retrieves shipment details using the merchant's own order reference /
        invoice number (as opposed to the courier's tracking number).

        Parameters
        ----------
        reference : str
            The merchant-side order reference (e.g. Shopify order number).

        Returns
        -------
        dict
            Raw JSON-deserialized response from the provider.
        """
        ...

    # ------------------------------------------------------------------
    # Optional methods (raise NotImplementedError if not supported)
    # ------------------------------------------------------------------

    def get_tracking_history(self, tracking_number: str) -> List[Dict[str, Any]]:
        """
        Returns the full event / status history for a shipment.
        Override in clients whose API supports this endpoint.
        Default: raises NotImplementedError.
        """
        raise NotImplementedError(
            f"{self.PROVIDER_NAME} does not expose a verified tracking-history endpoint."
        )

    def get_cod_settlement(self, from_date: str, to_date: str) -> List[Dict[str, Any]]:
        """
        Returns COD (Cash On Delivery) settlement/remittance records for the
        given date range.  Override in clients whose API supports this endpoint.
        Default: raises NotImplementedError.
        """
        raise NotImplementedError(
            f"{self.PROVIDER_NAME} does not expose a verified COD-settlement endpoint."
        )

    # ------------------------------------------------------------------
    # Credential-safe representation
    # ------------------------------------------------------------------

    def __repr__(self) -> str:
        return f"<{self.__class__.__name__} provider='{self.PROVIDER_NAME}'>"
