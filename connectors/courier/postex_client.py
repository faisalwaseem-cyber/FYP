"""
PostEx Courier API Client — BRIDGE FLOW Phase 5.2
Provider: PostEx Pakistan (https://api.postex.pk)

Authentication:  API Token header  (`token: <api_token>`)
Auth type:       api_key  (static token, no refresh cycle)

Verified endpoints
------------------
GET  /order/v1/track-order/{trackingNumber}   — real-time tracking by CN
GET  /order/v1/get-order/{orderRefNumber}     — lookup by merchant reference
GET  /order/v2/cod-settlement                 — COD remittance report

Design invariants
-----------------
* No database reads or writes inside this client.
* Credentials are injected at construction time by the caller (CredentialManager).
* API token is never exposed in __repr__, logs, or exception messages.
* Rate-limit (HTTP 429) and transient server errors (5xx) are retried with
  exponential back-off up to `max_retries`.
* 401 / 403 responses immediately raise CourierAuthError (not retried).
* Tracking-not-found conditions raise CourierTrackingNotFoundError.
"""
from __future__ import annotations

import time
from typing import Any, Dict, List, Optional

import requests

from connectors.courier.base import (
    BaseCourierClient,
    CourierAPIError,
    CourierAuthError,
    CourierRateLimitError,
    CourierTrackingNotFoundError,
)


class PostExClient(BaseCourierClient):
    """
    PostEx Pakistan REST API client.

    Parameters
    ----------
    api_token : str
        Decrypted PostEx merchant API token (from CredentialManager).
    base_url : str
        PostEx integration API root (default: production endpoint).
    timeout : float
        Per-request network timeout in seconds.
    max_retries : int
        Maximum number of retries for 429 / 5xx errors.
    """

    PROVIDER_NAME = "postex"

    # Verified PostEx API endpoint root (documented at https://api.postex.pk)
    _DEFAULT_BASE_URL = "https://api.postex.pk/services/integration/api"

    def __init__(
        self,
        api_token: str,
        base_url: str = _DEFAULT_BASE_URL,
        timeout: float = 20.0,
        max_retries: int = 3,
    ):
        if not api_token or not api_token.strip():
            raise ValueError("PostExClient: api_token must be a non-empty string.")

        self._api_token = api_token.strip()
        self.base_url = base_url.rstrip("/")
        self.timeout = timeout
        self.max_retries = max_retries

        self.session = requests.Session()
        self.session.headers.update(
            {
                "token": self._api_token,
                "Content-Type": "application/json",
                "Accept": "application/json",
            }
        )

    # ------------------------------------------------------------------
    # Credential-safe representation
    # ------------------------------------------------------------------

    def __repr__(self) -> str:
        token_hint = f"***{self._api_token[-4:]}" if len(self._api_token) > 4 else "***"
        return (
            f"<PostExClient provider='postex' "
            f"base_url='{self.base_url}' token='{token_hint}'>"
        )

    # ------------------------------------------------------------------
    # Internal HTTP helper
    # ------------------------------------------------------------------

    def _get(self, url: str, params: Optional[Dict[str, Any]] = None) -> Dict[str, Any]:
        """
        Executes a GET request with retry logic for 429 and 5xx responses.

        Raises
        ------
        CourierAuthError      — HTTP 401 / 403
        CourierRateLimitError — HTTP 429 exhausted after retries
        RuntimeError          — 5xx exhausted after retries, or network failure
        """
        attempt = 0
        while attempt <= self.max_retries:
            try:
                response = self.session.get(url, params=params, timeout=self.timeout)
            except requests.exceptions.RequestException as exc:
                attempt += 1
                if attempt > self.max_retries:
                    raise RuntimeError(
                        f"PostEx network error after {self.max_retries} retries: {exc}"
                    ) from exc
                time.sleep(2 ** attempt)
                continue

            if response.status_code in (401, 403):
                raise CourierAuthError(
                    f"PostEx API rejected credentials (HTTP {response.status_code}). "
                    "Verify the api_token in CredentialManager."
                )

            if response.status_code == 429:
                attempt += 1
                if attempt > self.max_retries:
                    raise CourierRateLimitError(
                        f"PostEx rate limit (HTTP 429) exceeded after {self.max_retries} retries."
                    )
                retry_after = float(response.headers.get("Retry-After", 2 ** attempt))
                time.sleep(retry_after)
                continue

            if response.status_code >= 500:
                attempt += 1
                if attempt > self.max_retries:
                    raise RuntimeError(
                        f"PostEx server error HTTP {response.status_code} "
                        f"persisted after {self.max_retries} retries."
                    )
                time.sleep(2 ** attempt)
                continue

            response.raise_for_status()

            data = response.json()

            # PostEx wraps successful responses: {"statusCode": "200", "statusMessage": "...", "dist": {...}}
            status_code_str = str(data.get("statusCode", "")).strip()
            if status_code_str and status_code_str not in ("200", "201", "0", ""):
                msg = data.get("statusMessage", "Unknown PostEx error")
                raise CourierAPIError(
                    code=status_code_str,
                    message=msg,
                    provider=self.PROVIDER_NAME,
                )

            return data

        raise RuntimeError(
            f"PostEx: max retries ({self.max_retries}) exceeded for {url}"
        )

    # ------------------------------------------------------------------
    # BaseCourierClient — required methods
    # ------------------------------------------------------------------

    def track_shipment(self, tracking_number: str) -> Dict[str, Any]:
        """
        Retrieves real-time tracking details for a shipment by CN number.

        Endpoint: GET /order/v1/track-order/{trackingNumber}

        Parameters
        ----------
        tracking_number : str
            PostEx Consignment Number (CN).

        Returns
        -------
        dict
            Full PostEx API response. Relevant fields:
            - dist.trackingNumber
            - dist.orderStatus
            - dist.orderAmount  (COD amount)
            - dist.deliveryCharges
            - dist.attemptCount
            - dist.bookingDate / deliveryDate

        Raises
        ------
        CourierTrackingNotFoundError
            If PostEx returns statusCode 404 or 'not found'.
        """
        if not tracking_number or not tracking_number.strip():
            raise ValueError("tracking_number must be a non-empty string.")

        url = f"{self.base_url}/order/v1/track-order/{tracking_number.strip()}"
        try:
            return self._get(url)
        except CourierAPIError as exc:
            if exc.code in ("404", "NOT_FOUND") or "not found" in str(exc).lower():
                raise CourierTrackingNotFoundError(
                    f"PostEx: tracking number '{tracking_number}' not found."
                ) from exc
            raise

    def get_order_status(self, reference: str) -> Dict[str, Any]:
        """
        Retrieves shipment details using the merchant's order reference number
        (e.g. the Shopify Order ID stored as orderRefNumber in PostEx).

        Endpoint: GET /order/v1/get-order/{orderRefNumber}

        Parameters
        ----------
        reference : str
            Merchant-side order reference / Shopify order number.

        Returns
        -------
        dict
            Full PostEx API response containing order details.
        """
        if not reference or not reference.strip():
            raise ValueError("reference must be a non-empty string.")

        url = f"{self.base_url}/order/v1/get-order/{reference.strip()}"
        try:
            return self._get(url)
        except CourierAPIError as exc:
            if exc.code in ("404", "NOT_FOUND") or "not found" in str(exc).lower():
                raise CourierTrackingNotFoundError(
                    f"PostEx: order reference '{reference}' not found."
                ) from exc
            raise

    # ------------------------------------------------------------------
    # BaseCourierClient — optional verified endpoint
    # ------------------------------------------------------------------

    def get_cod_settlement(self, from_date: str, to_date: str) -> List[Dict[str, Any]]:
        """
        Retrieves COD (Cash On Delivery) settlement/remittance records for the
        given date range.

        Endpoint: GET /order/v2/cod-settlement?fromDate=YYYY-MM-DD&toDate=YYYY-MM-DD

        Parameters
        ----------
        from_date : str
            Start date in 'YYYY-MM-DD' format.
        to_date : str
            End date in 'YYYY-MM-DD' format.

        Returns
        -------
        list[dict]
            List of COD settlement records from PostEx.
            Each record typically contains:
            - trackingNumber
            - orderAmount  (COD collected)
            - deliveryCharges
            - settlementDate
        """
        url = f"{self.base_url}/order/v2/cod-settlement"
        data = self._get(url, params={"fromDate": from_date, "toDate": to_date})
        # PostEx wraps the list in data.dist or data.data
        records = data.get("dist") or data.get("data") or []
        if isinstance(records, dict):
            records = [records]
        return records if isinstance(records, list) else []
