"""
Leopards Courier API Client — BRIDGE FLOW Phase 5.2
Provider: Leopards Courier Services Pakistan
API Docs: https://www.leopardscourier.com/api/

Authentication:  API Key + API Password header/query params
Auth type:       credentials  (stored as additional_credentials JSON blob)
                 Keys: api_key, api_password

Verified endpoints
------------------
GET  /api/connectedLCS/track_n_trace/         — track by CN / booking code
GET  /api/connectedLCS/get_shipment_status/   — bulk status by CN list
GET  /api/connectedLCS/get_packets_cod_details/ — COD/remittance details

Design invariants
-----------------
* No database reads or writes inside this client.
* api_key and api_password are injected at construction time.
* Neither credential is exposed in __repr__ or exception messages.
* HTTP 401 / 403 raises CourierAuthError immediately.
* HTTP 429 and 5xx are retried with exponential back-off.
* Leopards wraps all responses in an envelope; business errors are inspected
  from the 'status' field in the JSON body.
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


class LeopardsClient(BaseCourierClient):
    """
    Leopards Courier Services Pakistan API client.

    Parameters
    ----------
    api_key : str
        Leopards merchant API key (from CredentialManager
        additional_credentials['api_key']).
    api_password : str
        Leopards merchant API password (from CredentialManager
        additional_credentials['api_password']).
    base_url : str
        Leopards API root (default: production endpoint).
    timeout : float
        Per-request timeout in seconds.
    max_retries : int
        Max retries for 429 / 5xx errors.
    """

    PROVIDER_NAME = "leopards"

    _DEFAULT_BASE_URL = "https://www.leopardscourier.com/api"

    def __init__(
        self,
        api_key: str,
        api_password: str,
        base_url: str = _DEFAULT_BASE_URL,
        timeout: float = 20.0,
        max_retries: int = 3,
    ):
        if not api_key or not api_key.strip():
            raise ValueError("LeopardsClient: api_key must be a non-empty string.")
        if not api_password or not api_password.strip():
            raise ValueError("LeopardsClient: api_password must be a non-empty string.")

        self._api_key = api_key.strip()
        self._api_password = api_password.strip()
        self.base_url = base_url.rstrip("/")
        self.timeout = timeout
        self.max_retries = max_retries

        self.session = requests.Session()
        self.session.headers.update({"Accept": "application/json"})

    # ------------------------------------------------------------------
    # Credential-safe representation
    # ------------------------------------------------------------------

    def __repr__(self) -> str:
        key_hint = f"***{self._api_key[-4:]}" if len(self._api_key) > 4 else "***"
        return (
            f"<LeopardsClient provider='leopards' "
            f"base_url='{self.base_url}' api_key='{key_hint}'>"
        )

    # ------------------------------------------------------------------
    # Credential injection helper
    # ------------------------------------------------------------------

    def _auth_params(self) -> Dict[str, str]:
        """Returns the auth query parameters required by Leopards API."""
        return {
            "api_key": self._api_key,
            "api_password": self._api_password,
        }

    # ------------------------------------------------------------------
    # Internal HTTP helper
    # ------------------------------------------------------------------

    def _get(
        self,
        endpoint: str,
        params: Optional[Dict[str, Any]] = None,
    ) -> Dict[str, Any]:
        """
        Executes a GET request. Leopards always uses GET with query params.

        Raises
        ------
        CourierAuthError      — HTTP 401 / 403 or 'invalid credentials' body
        CourierRateLimitError — HTTP 429 exhausted
        RuntimeError          — 5xx exhausted or network failure
        """
        url = f"{self.base_url}/{endpoint.lstrip('/')}"
        full_params: Dict[str, Any] = self._auth_params()
        if params:
            full_params.update(params)

        attempt = 0
        while attempt <= self.max_retries:
            try:
                response = self.session.get(url, params=full_params, timeout=self.timeout)
            except requests.exceptions.RequestException as exc:
                attempt += 1
                if attempt > self.max_retries:
                    raise RuntimeError(
                        f"Leopards network error after {self.max_retries} retries: {exc}"
                    ) from exc
                time.sleep(2 ** attempt)
                continue

            if response.status_code in (401, 403):
                raise CourierAuthError(
                    f"Leopards API rejected credentials (HTTP {response.status_code}). "
                    "Verify api_key and api_password in CredentialManager."
                )

            if response.status_code == 429:
                attempt += 1
                if attempt > self.max_retries:
                    raise CourierRateLimitError(
                        f"Leopards rate limit (HTTP 429) exceeded after {self.max_retries} retries."
                    )
                retry_after = float(response.headers.get("Retry-After", 2 ** attempt))
                time.sleep(retry_after)
                continue

            if response.status_code >= 500:
                attempt += 1
                if attempt > self.max_retries:
                    raise RuntimeError(
                        f"Leopards server error HTTP {response.status_code} "
                        f"persisted after {self.max_retries} retries."
                    )
                time.sleep(2 ** attempt)
                continue

            response.raise_for_status()
            data = response.json()

            # Leopards wraps results: {"status": 1, "error": "", "packet_list": [...]}
            # status=0 → error; status=1 → success
            api_status = data.get("status", 1)
            if str(api_status) == "0":
                error_msg = data.get("error", "Unknown Leopards error")
                error_lower = error_msg.lower()

                if any(k in error_lower for k in ("invalid credentials", "unauthorized", "api key")):
                    raise CourierAuthError(
                        f"Leopards API rejected credentials: {error_msg}"
                    )
                if any(k in error_lower for k in ("not found", "no record", "invalid tracking")):
                    raise CourierTrackingNotFoundError(
                        f"Leopards: shipment not found — {error_msg}"
                    )
                raise CourierAPIError(
                    code="LEOPARDS_ERROR",
                    message=error_msg,
                    provider=self.PROVIDER_NAME,
                )

            return data

        raise RuntimeError(
            f"Leopards: max retries ({self.max_retries}) exceeded for {endpoint}"
        )

    # ------------------------------------------------------------------
    # BaseCourierClient — required methods
    # ------------------------------------------------------------------

    def track_shipment(self, tracking_number: str) -> Dict[str, Any]:
        """
        Retrieves real-time tracking for a single shipment by CN / booking code.

        Endpoint: GET /connectedLCS/track_n_trace/?track_numbers={cn}

        Parameters
        ----------
        tracking_number : str
            Leopards consignment / booking number.

        Returns
        -------
        dict
            Leopards tracking response. Relevant fields typically include:
            - packet_list[0].booked_packet_cn
            - packet_list[0].booked_packet_status
            - packet_list[0].booked_packet_collect_amount  (COD)
            - packet_list[0].booked_packet_vol_weight_lbs

        Raises
        ------
        CourierTrackingNotFoundError
            If Leopards reports the CN does not exist.
        CourierAuthError
            If credentials are invalid.
        CourierRateLimitError
            If rate-limited after retries.
        """
        if not tracking_number or not tracking_number.strip():
            raise ValueError("tracking_number must be a non-empty string.")

        return self._get(
            "/connectedLCS/track_n_trace/",
            params={"track_numbers": tracking_number.strip()},
        )

    def get_order_status(self, reference: str) -> Dict[str, Any]:
        """
        Retrieves shipment status using the merchant's order reference number.
        Leopards maps this to the booked_packet_order_id field.

        Endpoint: GET /connectedLCS/get_shipment_status/?order_ids={reference}

        Parameters
        ----------
        reference : str
            Merchant order reference number.

        Returns
        -------
        dict
            Leopards response with current shipment status.
        """
        if not reference or not reference.strip():
            raise ValueError("reference must be a non-empty string.")

        return self._get(
            "/connectedLCS/get_shipment_status/",
            params={"order_ids": reference.strip()},
        )

    # ------------------------------------------------------------------
    # BaseCourierClient — optional verified endpoint
    # ------------------------------------------------------------------

    def get_cod_settlement(self, from_date: str, to_date: str) -> List[Dict[str, Any]]:
        """
        Retrieves COD (Cash On Delivery) packet details / remittance for a
        date range.

        Endpoint: GET /connectedLCS/get_packets_cod_details/?
                      from_date=YYYY-MM-DD&to_date=YYYY-MM-DD

        Returns
        -------
        list[dict]
            COD packet records. Each record typically contains:
            - booked_packet_cn
            - booked_packet_collect_amount
            - settled_date
        """
        data = self._get(
            "/connectedLCS/get_packets_cod_details/",
            params={"from_date": from_date, "to_date": to_date},
        )
        # Leopards typically returns the list under 'packet_list'
        records = data.get("packet_list") or data.get("data") or []
        return records if isinstance(records, list) else []
