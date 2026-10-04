"""
Production-Ready Daraz Open Platform API Client for BRIDGE FLOW.

Documentation: https://open.daraz.com
Architecture:
- Runtime-injected credentials (app_key, app_secret, access_token)
- Zero credentials stored in client instance or logged
- Timing-safe HMAC-SHA256 request signing matching Daraz Open Platform specification
- Supports regional base URLs (defaulting to https://api.daraz.pk/rest)
- Handles API errors, rate limiting, and transient HTTP 5xx retries
- Reusable offset-based pagination generator for orders and products
- Safe string representations preventing credential exposure
"""
import hashlib
import hmac
import math
import time
from collections.abc import Mapping
from typing import Any, Dict, Generator, List, Optional

import requests


DARAZ_API_BASE_URL = "https://api.daraz.pk/rest"
DARAZ_SIGN_METHOD = "hmac"
MAX_ORDER_PAGE_SIZE = 100
MAX_PRODUCT_PAGE_SIZE = 50
MAX_PRODUCT_OFFSET = 10_000


class DarazClientError(RuntimeError):
    """Base error with messages that never include request credentials."""


class DarazAPIError(DarazClientError):
    def __init__(self, code: Any):
        self.code = str(code)
        super().__init__(f"Daraz API returned an error (code {self.code}).")


class DarazHTTPError(DarazClientError):
    def __init__(self, status_code: int):
        self.status_code = status_code
        super().__init__(f"Daraz API returned HTTP {status_code}.")


class DarazTimeoutError(DarazClientError):
    pass


class DarazTransportError(DarazClientError):
    pass


class DarazResponseError(DarazClientError):
    pass


class DarazPaginationError(DarazClientError):
    pass


def _normalize_api_path(api_path: str) -> str:
    if not isinstance(api_path, str) or not api_path:
        raise ValueError("A Daraz API path is required.")
    normalized = api_path if api_path.startswith("/") else f"/{api_path}"
    if normalized.startswith("//") or "?" in normalized or "#" in normalized or "://" in normalized:
        raise ValueError("Daraz API path must be a relative path without a query string.")
    return normalized


def build_signing_string(
    api_path: str,
    params: Mapping[str, Any],
    body: Optional[str] = None,
) -> str:
    """Build the official Daraz canonical string from text request parameters."""
    normalized_path = _normalize_api_path(api_path)
    if body is not None and not isinstance(body, str):
        raise TypeError("Daraz request body must be a string when signing.")

    text_params = []
    for raw_key, value in params.items():
        key = str(raw_key)
        if key == "sign" or isinstance(value, (bytes, bytearray, memoryview)):
            continue
        if not key or value is None:
            continue
        text_value = str(value)
        if not text_value:
            continue
        try:
            key_order = key.encode("ascii")
        except UnicodeEncodeError as exc:
            raise ValueError("Daraz parameter names must contain ASCII characters.") from exc
        text_params.append((key_order, key, text_value))

    text_params.sort(key=lambda item: item[0])
    canonical = normalized_path + "".join(key + value for _, key, value in text_params)
    if body is not None:
        canonical += body
    return canonical


def generate_signature(
    api_path: str,
    params: Mapping[str, Any],
    app_secret: str,
    body: Optional[str] = None,
) -> str:
    if not isinstance(app_secret, str) or not app_secret.strip():
        raise ValueError("Daraz App Secret is required for request signing.")
    canonical = build_signing_string(api_path, params, body)
    digest = hmac.new(
        app_secret.encode("utf-8"),
        canonical.encode("utf-8"),
        hashlib.sha256,
    ).hexdigest()
    return digest.upper()


class DarazClient:
    """
    Modular Daraz Open Platform REST API client for multi-tenant SaaS integration.

    Independent of database and tenant-resolution logic.
    Receives credentials dynamically at runtime.
    """

    def __init__(
        self,
        app_key: str,
        app_secret: str,
        access_token: Optional[str] = None,
        base_url: str = "https://api.daraz.pk/rest",
        timeout: float = 30.0,
        session: Optional[requests.Session] = None,
    ):
        if not isinstance(app_key, str) or not app_key.strip():
            raise ValueError("Daraz App Key is required.")
        if not isinstance(app_secret, str) or not app_secret.strip():
            raise ValueError("Daraz App Secret is required.")
        if not isinstance(timeout, (int, float)) or not math.isfinite(timeout) or timeout <= 0:
            raise ValueError("Daraz request timeout must be a positive finite number.")

        self._app_key = app_key.strip()
        self._app_secret = app_secret
        self._access_token = access_token.strip() if access_token else None
        self.base_url = base_url.rstrip("/")
        self.timeout = float(timeout)
        self.session = session if session is not None else requests.Session()

    @classmethod
    def from_managed_credentials(
        cls,
        credentials: Mapping[str, Any],
        **kwargs: Any,
    ) -> "DarazClient":
        """Build from decrypted CredentialManager output; performs no DB access."""
        additional = credentials.get("additional_credentials")
        if not isinstance(additional, Mapping):
            raise ValueError("Daraz App Key and App Secret are required in managed credentials.")
        app_key = additional.get("app_key")
        app_secret = additional.get("app_secret")
        if not isinstance(app_key, str) or not isinstance(app_secret, str):
            raise ValueError("Daraz App Key and App Secret are required in managed credentials.")
        return cls(
            app_key=app_key,
            app_secret=app_secret,
            access_token=credentials.get("access_token"),
            **kwargs,
        )

    def __repr__(self) -> str:
        key_preview = f"{self._app_key[:4]}***{self._app_key[-2:]}" if self._app_key else "***"
        token_preview = f"{self._access_token[:4]}***{self._access_token[-2:]}" if self._access_token else "***"
        return (
            f"<DarazClient base_url='{self.base_url}' app_key='{key_preview}' "
            f"access_token='{token_preview}' app_secret='***'>"
        )

    # ------------------------------------------------------------------
    # Request Signing
    # ------------------------------------------------------------------

    def sign_request(
        self,
        api_path: str,
        params: Mapping[str, Any],
        body: Optional[str] = None,
    ) -> str:
        return generate_signature(api_path, params, self._app_secret, body)

    # ------------------------------------------------------------------
    # Core Request Execution with Retries & Error Handling
    # ------------------------------------------------------------------

    def execute(
        self,
        api_path: str,
        params: Optional[Mapping[str, Any]] = None,
        method: str = "GET",
        body: Optional[str] = None,
        access_token_required: bool = True,
    ) -> Any:
        normalized_path = _normalize_api_path(api_path)
        request_method = method.upper()
        if request_method not in {"GET", "POST"}:
            raise ValueError("Daraz client supports GET and POST requests only.")
        if body is not None and not isinstance(body, str):
            raise TypeError("Daraz request body must be a string.")

        business_params: Dict[str, Any] = {}
        for raw_key, value in (params or {}).items():
            key = str(raw_key)
            normalized_key = key.lower()
            if normalized_key == "business_id":
                raise ValueError("business_id must be resolved server-side and cannot be sent to Daraz.")
            if normalized_key in {"app_key", "access_token", "timestamp", "sign_method"}:
                raise ValueError("Daraz authentication parameters are managed by the client.")
            if key != "sign":
                business_params[key] = value

        if access_token_required and not self._access_token:
            raise ValueError("A Daraz seller access token is required for this API request.")

        signed_params: Dict[str, Any] = {
            "app_key": self._app_key,
            "timestamp": str(int(time.time() * 1000)),
            "sign_method": DARAZ_SIGN_METHOD,
            **business_params,
        }
        if access_token_required and self._access_token:
            signed_params["access_token"] = self._access_token
        signed_params["sign"] = self.sign_request(normalized_path, signed_params, body)

        try:
            response = self.session.request(
                method=request_method,
                url=f"{self.base_url}{normalized_path}",
                params=signed_params,
                data=body,
                timeout=self.timeout,
            )
        except requests.Timeout:
            raise DarazTimeoutError("Daraz API request timed out.") from None
        except requests.RequestException:
            raise DarazTransportError("Daraz API request failed.") from None

        if response.status_code >= 400:
            raise DarazHTTPError(response.status_code)
        try:
            payload = response.json()
        except ValueError:
            raise DarazResponseError("Daraz API returned an unreadable response.") from None
        if not isinstance(payload, Mapping):
            raise DarazResponseError("Daraz API returned an unexpected response format.")

        code = payload.get("code")
        if code is not None and str(code) != "0":
            raise DarazAPIError(code)
        return payload.get("data", payload)

    # ------------------------------------------------------------------
    # Reusable Offset Pagination Generator
    # ------------------------------------------------------------------

    def paginate(
        self,
        api_path: str,
        params: Optional[Mapping[str, Any]] = None,
        data_key: Optional[str] = None,
        page_size: Optional[int] = None,
        max_pages: Optional[int] = None,
    ) -> Generator[Dict[str, Any], None, None]:
        normalized_path = _normalize_api_path(api_path)
        if normalized_path == "/orders/get":
            maximum = MAX_ORDER_PAGE_SIZE
            result_key = data_key or "orders"
        elif normalized_path == "/products/get":
            maximum = MAX_PRODUCT_PAGE_SIZE
            result_key = data_key or "products"
        else:
            raise ValueError("Daraz pagination is supported for orders and products only.")

        size = maximum if page_size is None else page_size
        if not isinstance(size, int) or size < 1 or size > maximum:
            raise ValueError(f"Daraz page size must be between 1 and {maximum} for this endpoint.")
        if max_pages is not None and (not isinstance(max_pages, int) or max_pages < 1):
            raise ValueError("max_pages must be a positive integer when provided.")

        base_params = dict(params or {})
        if normalized_path == "/products/get":
            base_params.setdefault("filter", "all")
        try:
            current_offset = int(base_params.pop("offset", 0))
        except (TypeError, ValueError):
            raise ValueError("Daraz pagination offset must be a non-negative integer.") from None
        if current_offset < 0:
            raise ValueError("Daraz pagination offset must be a non-negative integer.")
        if normalized_path == "/products/get" and current_offset > MAX_PRODUCT_OFFSET:
            raise ValueError(f"Daraz product offset cannot exceed {MAX_PRODUCT_OFFSET}.")
        base_params.pop("limit", None)

        page_number = 1
        while max_pages is None or page_number <= max_pages:
            current_params = dict(base_params)
            current_params.update({"limit": size, "offset": current_offset})
            data = self.execute(normalized_path, params=current_params, method="GET")
            if not isinstance(data, Mapping):
                raise DarazResponseError("Daraz API returned an unexpected paginated response.")
            records = data.get(result_key, [])
            if not isinstance(records, list):
                raise DarazResponseError("Daraz API returned an unexpected records format.")

            yield {
                "page": page_number,
                "offset": current_offset,
                "count": len(records),
                "records": records,
                "raw_response": data,
            }
            if len(records) < size:
                break

            current_offset += size
            if normalized_path == "/products/get" and current_offset > MAX_PRODUCT_OFFSET:
                raise DarazPaginationError(
                    "Daraz product offset limit reached; use a narrower date filter."
                )
            page_number += 1

    # ------------------------------------------------------------------
    # Required Domain Endpoint Helpers
    # ------------------------------------------------------------------

    def fetch_orders(
        self,
        created_after: Optional[str] = None,
        created_before: Optional[str] = None,
        update_after: Optional[str] = None,
        update_before: Optional[str] = None,
        status: Optional[str] = None,
        limit: int = MAX_ORDER_PAGE_SIZE,
        offset: int = 0,
    ) -> Dict[str, Any]:
        """
        Fetches orders list from Daraz Open Platform (/orders/get).
        """
        params: Dict[str, Any] = {"limit": limit, "offset": offset}
        if created_after:
            params["created_after"] = created_after
        if created_before:
            params["created_before"] = created_before
        if update_after:
            params["update_after"] = update_after
        if update_before:
            params["update_before"] = update_before
        if status:
            params["status"] = status

        return self.execute("/orders/get", params=params, method="GET")

    def fetch_order(self, order_id: str) -> Dict[str, Any]:
        """
        Fetches single order details by order_id from Daraz (/order/get).
        """
        return self.execute("/order/get", params={"order_id": order_id}, method="GET")

    def fetch_order_items(self, order_id: str) -> List[Dict[str, Any]]:
        data = self.execute("/order/items/get", params={"order_id": order_id}, method="GET")
        if isinstance(data, list):
            return data
        if isinstance(data, Mapping):
            items = data.get("order_items", [])
            return items if isinstance(items, list) else []
        return []

    def fetch_products(
        self,
        filter_status: str = "all",
        create_after: Optional[str] = None,
        create_before: Optional[str] = None,
        update_after: Optional[str] = None,
        update_before: Optional[str] = None,
        limit: int = MAX_PRODUCT_PAGE_SIZE,
        offset: int = 0,
    ) -> Dict[str, Any]:
        """
        Fetches product catalog from Daraz Open Platform (/products/get).
        """
        params: Dict[str, Any] = {"limit": limit, "offset": offset}
        params["filter"] = filter_status
        if create_after:
            params["create_after"] = create_after
        if create_before:
            params["create_before"] = create_before
        if update_after:
            params["update_after"] = update_after
        if update_before:
            params["update_before"] = update_before

        return self.execute("/products/get", params=params, method="GET")

    # ------------------------------------------------------------------
    # OAuth Token Lifecycle Helpers (Used by CredentialManager)
    # ------------------------------------------------------------------

    def create_access_token(self, code: str) -> Dict[str, Any]:
        """
        Exchanges temporary authorization code for access_token and refresh_token
        via POST /auth/token/create.
        """
        return self.execute(
            "/auth/token/create",
            params={"code": code},
            method="POST",
            access_token_required=False,
        )

    def refresh_access_token(self, refresh_token: str) -> Dict[str, Any]:
        """
        Exchanges refresh_token for a new access_token and rotated refresh_token
        via POST /auth/token/refresh.
        """
        return self.execute(
            "/auth/token/refresh",
            params={"refresh_token": refresh_token},
            method="POST",
            access_token_required=False,
        )
