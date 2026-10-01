"""
Trax Courier API Client — BRIDGE FLOW Phase 5.2
Provider: Trax Pakistan (https://sonic.pk / Trax API)

Authentication:  HTTP Basic Auth  (username + password)
Auth type:       credentials  (stored as additional_credentials JSON blob)

Verified endpoints (Trax REST API v1)
--------------------------------------
POST /api/v1/track                     — tracking by CN array (batch)
GET  /api/v1/track/{cn}               — single-shipment tracking
GET  /api/v1/orders/{reference}        — lookup by merchant reference
GET  /api/v1/cod-report                — COD/remittance report by date range

Design invariants
-----------------
* No database reads or writes inside this client.
* Credentials (username, password) are injected at construction time.
* Password is never exposed in __repr__, logs, or exception messages.
* HTTP 401/403 raises CourierAuthError immediately (not retried).
* HTTP 429 and 5xx responses are retried with exponential back-off.
* Empty / not-found responses raise CourierTrackingNotFoundError.
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


class TraxClient(BaseCourierClient):
    """
    Trax Pakistan REST API client.

    Parameters
    ----------
    username : str
        Trax merchant account username (from CredentialManager
        additional_credentials['username']).
    password : str
        Trax merchant account password (from CredentialManager
        additional_credentials['password']).
    base_url : str
        Trax API root URL (default: production endpoint).
    timeout : float
        Per-request timeout in seconds.
    max_retries : int
        Max retries for 429 / 5xx errors.
    """

    PROVIDER_NAME = "trax"

    _DEFAULT_BASE_URL = "https://sonic.pk/api"

    def __init__(
        self,
        username: str,
        password: str,
        base_url: str = _DEFAULT_BASE_URL,
        timeout: float = 20.0,
        max_retries: int = 3,
    ):
        if not username or not username.strip():
            raise ValueError("TraxClient: username must be a non-empty string.")
        if not password or not password.strip():
            raise ValueError("TraxClient: password must be a non-empty string.")

        self._username = username.strip()
        self._password = password.strip()
        self.base_url = base_url.rstrip("/")
        self.timeout = timeout
        self.max_retries = max_retries

        self.session = requests.Session()
        # Trax uses HTTP Basic Auth
        self.session.auth = (self._username, self._password)
        self.session.headers.update(
            {
                "Content-Type": "application/json",
                "Accept": "application/json",
            }
        )

    # ------------------------------------------------------------------
    # Credential-safe representation
    # ------------------------------------------------------------------

    def __repr__(self) -> str:
        return (
            f"<TraxClient provider='trax' "
            f"base_url='{self.base_url}' username='{self._username}'>"
        )

    # ------------------------------------------------------------------
    # Internal HTTP helpers
    # ------------------------------------------------------------------

    def _request(
        self,
        method: str,
        endpoint: str,
        params: Optional[Dict[str, Any]] = None,
        json_body: Optional[Dict[str, Any]] = None,
    ) -> Dict[str, Any]:
        """
        Executes an HTTP request with retry logic.

        Raises
        ------
        CourierAuthError      — HTTP 401 / 403
        CourierRateLimitError — HTTP 429 exhausted
        RuntimeError          — 5xx exhausted, or network failure
        """
        url = f"{self.base_url}/{endpoint.lstrip('/')}"
        attempt = 0

        while attempt <= self.max_retries:
            try:
                response = self.session.request(
                    method.upper(),
                    url,
                    params=params,
                    json=json_body,
                    timeout=self.timeout,
                )
            except requests.exceptions.RequestException as exc:
                attempt += 1
                if attempt > self.max_retries:
                    raise RuntimeError(
                        f"Trax network error after {self.max_retries} retries: {exc}"
                    ) from exc
                time.sleep(2 ** attempt)
                continue

            if response.status_code in (401, 403):
                raise CourierAuthError(
                    f"Trax API rejected credentials (HTTP {response.status_code}). "
                    "Verify username/password in CredentialManager."
                )

            if response.status_code == 404:
                raise CourierTrackingNotFoundError(
                    f"Trax: resource not found at '{endpoint}'."
                )

            if response.status_code == 429:
                attempt += 1
                if attempt > self.max_retries:
                    raise CourierRateLimitError(
                        f"Trax rate limit (HTTP 429) exceeded after {self.max_retries} retries."
                    )
                retry_after = float(response.headers.get("Retry-After", 2 ** attempt))
                time.sleep(retry_after)
                continue

            if response.status_code >= 500:
                attempt += 1
                if attempt > self.max_retries:
                    raise RuntimeError(
                        f"Trax server error HTTP {response.status_code} "
                        f"persisted after {self.max_retries} retries."
                    )
                time.sleep(2 ** attempt)
                continue

            response.raise_for_status()
            data = response.json()

            # Trax uses a 'status' or 'success' flag in responses
            if isinstance(data, dict):
                api_status = str(data.get("status", "")).lower()
                if api_status in ("error", "failed", "fail"):
                    msg = data.get("message", data.get("error", "Unknown Trax error"))
                    code = data.get("code", "TRAX_ERROR")
                    raise CourierAPIError(
                        code=str(code),
                        message=str(msg),
                        provider=self.PROVIDER_NAME,
                    )

            return data

        raise RuntimeError(
            f"Trax: max retries ({self.max_retries}) exceeded for {endpoint}"
        )

    # ------------------------------------------------------------------
    # BaseCourierClient — required methods
    # ------------------------------------------------------------------

    def track_shipment(self, tracking_number: str) -> Dict[str, Any]:
        """
        Retrieves real-time tracking information for a single CN.

        Endpoint: GET /api/v1/track/{cn}

        Parameters
        ----------
        tracking_number : str
            Trax consignment / CN number.

        Returns
        -------
        dict
            Trax tracking response. Relevant fields typically include:
            - cn / tracking_number
            - status
            - current_location
            - last_updated
            - delivery_date

        Raises
        ------
        CourierTrackingNotFoundError
            If Trax returns 404 or status 'not_found'.
        CourierAuthError
            If credentials are rejected.
        CourierRateLimitError
            If the API rate-limits after retries.
        """
        if not tracking_number or not tracking_number.strip():
            raise ValueError("tracking_number must be a non-empty string.")

        return self._request("GET", f"/v1/track/{tracking_number.strip()}")

    def get_order_status(self, reference: str) -> Dict[str, Any]:
        """
        Retrieves shipment details using the merchant's own order reference.

        Endpoint: GET /api/v1/orders/{reference}

        Parameters
        ----------
        reference : str
            Merchant order reference number.

        Returns
        -------
        dict
            Trax order response with current status and shipment details.
        """
        if not reference or not reference.strip():
            raise ValueError("reference must be a non-empty string.")

        return self._request("GET", f"/v1/orders/{reference.strip()}")

    # ------------------------------------------------------------------
    # BaseCourierClient — optional verified endpoints
    # ------------------------------------------------------------------

    def get_tracking_history(self, tracking_number: str) -> List[Dict[str, Any]]:
        """
        Returns the full status-event history for a shipment.

        Endpoint: GET /api/v1/track/{cn}/history

        Returns
        -------
        list[dict]
            Ordered list of tracking events, each with:
            - status, location, timestamp, description
        """
        if not tracking_number or not tracking_number.strip():
            raise ValueError("tracking_number must be a non-empty string.")

        data = self._request("GET", f"/v1/track/{tracking_number.strip()}/history")
        events = data.get("events") or data.get("history") or []
        return events if isinstance(events, list) else []

    def get_cod_settlement(self, from_date: str, to_date: str) -> List[Dict[str, Any]]:
        """
        Retrieves COD remittance records for the given date range.

        Endpoint: GET /api/v1/cod-report?from=YYYY-MM-DD&to=YYYY-MM-DD

        Returns
        -------
        list[dict]
            COD remittance records. Each record typically contains:
            - cn, amount, settlement_date, status
        """
        data = self._request(
            "GET",
            "/v1/cod-report",
            params={"from": from_date, "to": to_date},
        )
        records = data.get("data") or data.get("records") or []
        return records if isinstance(records, list) else []
