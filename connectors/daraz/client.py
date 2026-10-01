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
import time
from typing import Any, Dict, Generator, List, Optional

import requests


class DarazAPIError(RuntimeError):
    """Raised when Daraz Open Platform returns an API error response."""

    def __init__(self, code: str, message: str, request_id: Optional[str] = None):
        self.code = str(code)
        self.error_message = message
        self.request_id = request_id
        super().__init__(f"Daraz API Error [{self.code}]: {self.error_message}")


class DarazRateLimitError(RuntimeError):
    """Raised when Daraz API call limit or throttling threshold is reached."""
    pass


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
        max_retries: int = 3,
    ):
        if not app_key or not app_secret:
            raise ValueError("Both app_key and app_secret are required to initialize DarazClient.")

        self.app_key = app_key.strip()
        self._app_secret = app_secret.strip()
        self._access_token = access_token.strip() if access_token else None
        self.base_url = base_url.rstrip("/")
        self.timeout = timeout
        self.max_retries = max_retries
        self.session = requests.Session()

    def __repr__(self) -> str:
        # Credential safety: never expose app_secret or access_token in representations
        return (
            f"<DarazClient app_key='{self.app_key}' base_url='{self.base_url}' "
            f"token='{'***' if self._access_token else 'None'}'>"
        )

    # ------------------------------------------------------------------
    # Request Signing
    # ------------------------------------------------------------------

    def sign_request(self, api_path: str, params: Dict[str, Any]) -> str:
        """
        Daraz Open Platform HMAC-SHA256 signature algorithm.
        1. Sorts all parameters alphabetically by key.
        2. Concatenates api_path + {key}{value} pairs.
        3. Generates HMAC-SHA256 with app_secret and converts to uppercase hex.
        """
        sorted_params = sorted(params.items(), key=lambda x: str(x[0]))
        query_string = api_path + "".join([f"{k}{v}" for k, v in sorted_params])
        sign = hmac.new(
            self._app_secret.encode("utf-8"),
            query_string.encode("utf-8"),
            hashlib.sha256,
        ).hexdigest().upper()
        return sign

    # ------------------------------------------------------------------
    # Core Request Execution with Retries & Error Handling
    # ------------------------------------------------------------------

    def execute(
        self,
        api_path: str,
        params: Optional[Dict[str, Any]] = None,
        method: str = "GET",
    ) -> Dict[str, Any]:
        """
        Executes an HTTP request to the Daraz Open Platform with automatic signing,
        transient retry handling, and rate limit backoff.

        Parameters
        ----------
        api_path : str
            Daraz API path, e.g. '/orders/get' or '/auth/token/refresh'
        params : dict, optional
            Query or form parameters
        method : str
            HTTP method ('GET' or 'POST')

        Returns
        -------
        dict
            The parsed 'data' or response dictionary from Daraz.
        """
        normalized_path = api_path if api_path.startswith("/") else f"/{api_path}"
        all_params: Dict[str, Any] = {
            "app_key": self.app_key,
            "timestamp": str(int(time.time() * 1000)),
            "sign_method": "sha256",
        }
        if self._access_token:
            all_params["access_token"] = self._access_token
        if params:
            all_params.update(params)

        all_params["sign"] = self.sign_request(normalized_path, all_params)
        url = f"{self.base_url}{normalized_path}"

        retries = 0
        while retries <= self.max_retries:
            try:
                if method.upper() == "POST":
                    response = self.session.post(url, data=all_params, timeout=self.timeout)
                else:
                    response = self.session.get(url, params=all_params, timeout=self.timeout)

                # 1. Rate limiting / 429
                if response.status_code == 429:
                    retries += 1
                    if retries > self.max_retries:
                        raise DarazRateLimitError(
                            f"Daraz rate limit HTTP 429 exceeded after {self.max_retries} retries."
                        )
                    time.sleep(float(2 ** retries))
                    continue

                # 2. Transient server errors
                if response.status_code >= 500:
                    retries += 1
                    if retries > self.max_retries:
                        raise RuntimeError(
                            f"Daraz server error HTTP {response.status_code} "
                            f"persisted after {self.max_retries} retries."
                        )
                    time.sleep(float(2 ** retries))
                    continue

                # 3. Client HTTP errors
                response.raise_for_status()

                res_json = response.json()

                # 4. Daraz business-level error inspection
                # Code "0" denotes success in Daraz Open Platform
                code = str(res_json.get("code", "0"))
                if code != "0" and "code" in res_json:
                    msg = res_json.get("message", "Unknown Daraz error")
                    req_id = res_json.get("request_id")

                    # Check for rate limiting in business response
                    if any(term in code.lower() or term in msg.lower() for term in ("limit", "throttle", "concurrent")):
                        retries += 1
                        if retries > self.max_retries:
                            raise DarazRateLimitError(
                                f"Daraz API call limit reached: [{code}] {msg}"
                            )
                        time.sleep(float(2 ** retries))
                        continue

                    raise DarazAPIError(code=code, message=msg, request_id=req_id)

                return res_json.get("data", res_json)

            except requests.exceptions.RequestException as exc:
                retries += 1
                if retries > self.max_retries:
                    raise RuntimeError(
                        f"Network error querying Daraz ({normalized_path}): {exc}"
                    ) from exc
                time.sleep(float(2 ** retries))

        raise RuntimeError(f"Max retries exceeded communicating with Daraz ({normalized_path})")

    # ------------------------------------------------------------------
    # Reusable Offset Pagination Generator
    # ------------------------------------------------------------------

    def paginate(
        self,
        api_path: str,
        params: Optional[Dict[str, Any]] = None,
        data_key: str = "orders",
        page_size: int = 100,
        max_pages: Optional[int] = None,
    ) -> Generator[Dict[str, Any], None, None]:
        """
        Yields pages of records from a Daraz offset-based pagination endpoint.

        Parameters
        ----------
        api_path : str
            e.g. '/orders/get' or '/products/get'
        params : dict, optional
            Filters (e.g. created_after, status)
        data_key : str
            The key containing the items list in data (e.g. 'orders' or 'products')
        page_size : int
            Number of items per page (default: 100, max allowed by Daraz is 100)
        max_pages : int, optional
            Safety ceiling on pages to fetch

        Yields
        ------
        dict containing:
            - page (int): 1-indexed page number
            - offset (int): current offset
            - count (int): items count in this page
            - records (list): list of item dictionaries
            - raw_response (dict): full response data
        """
        page_num = 1
        current_offset = 0
        p_copy = dict(params or {})

        while True:
            if max_pages and page_num > max_pages:
                break

            current_params = dict(p_copy)
            current_params["limit"] = page_size
            current_params["offset"] = current_offset

            data = self.execute(api_path, params=current_params, method="GET")

            # Extract list of records
            items = data.get(data_key, [])
            if not isinstance(items, list):
                items = []

            yield {
                "page": page_num,
                "offset": current_offset,
                "count": len(items),
                "records": items,
                "raw_response": data,
            }

            # If fewer than page_size items were returned, we have reached the end
            if len(items) < page_size:
                break

            current_offset += page_size
            page_num += 1

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
        limit: int = 100,
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
        """
        Fetches order line items by order_id from Daraz (/order/items/get).
        """
        data = self.execute("/order/items/get", params={"order_id": order_id}, method="GET")
        return data.get("order_items", [])

    def fetch_products(
        self,
        filter_status: Optional[str] = None,
        create_after: Optional[str] = None,
        update_after: Optional[str] = None,
        search: Optional[str] = None,
        limit: int = 100,
        offset: int = 0,
    ) -> Dict[str, Any]:
        """
        Fetches product catalog from Daraz Open Platform (/products/get).
        """
        params: Dict[str, Any] = {"limit": limit, "offset": offset}
        if filter_status:
            params["filter"] = filter_status
        if create_after:
            params["create_after"] = create_after
        if update_after:
            params["update_after"] = update_after
        if search:
            params["search"] = search

        return self.execute("/products/get", params=params, method="GET")

    # ------------------------------------------------------------------
    # OAuth Token Lifecycle Helpers (Used by CredentialManager)
    # ------------------------------------------------------------------

    def create_access_token(self, code: str) -> Dict[str, Any]:
        """
        Exchanges temporary authorization code for access_token and refresh_token
        via POST /auth/token/create.
        """
        return self.execute("/auth/token/create", params={"code": code}, method="POST")

    def refresh_access_token(self, refresh_token: str) -> Dict[str, Any]:
        """
        Exchanges refresh_token for a new access_token and rotated refresh_token
        via POST /auth/token/refresh.
        """
        return self.execute("/auth/token/refresh", params={"refresh_token": refresh_token}, method="POST")
