"""
Production-Ready Shopify GraphQL Admin API Client for BRIDGE FLOW.

Features:
- Dynamic store domain & runtime decrypted token injection
- Configurable Shopify API version (defaulting to 2026-07)
- Cost-based rate-limiting using extensions.cost.throttleStatus
  (maximumAvailable, currentlyAvailable, restoreRate)
- Adaptive retry and backoff on throttling and transient HTTP 5xx errors
- Reusable cursor pagination generator for all GraphQL connection types
- Strict credential masking in logs and exception representations
"""
import re
import time
from typing import Any, Callable, Dict, Generator, List, Optional, Sequence, Union

import requests

from config.settings import SHOPIFY_API_VERSION


class ShopifyGraphQLError(RuntimeError):
    """Raised when Shopify GraphQL returns execution errors."""

    def __init__(self, errors: List[Dict[str, Any]], message: Optional[str] = None):
        self.errors = errors
        clean_msg = message or f"Shopify GraphQL returned errors: {errors}"
        super().__init__(clean_msg)


class ShopifyRateLimitError(RuntimeError):
    """Raised when maximum retries are exceeded on Shopify GraphQL throttling."""
    pass


class ShopifyGraphQLClient:
    """
    Modular Shopify GraphQL Admin API client for multi-tenant SaaS integration.

    Independent of database and tenant-resolution logic.
    Accepts dynamic store domain and decrypted token securely at runtime.
    """

    def __init__(
        self,
        store_domain: str,
        access_token: str,
        api_version: Optional[str] = None,
        timeout: float = 30.0,
        max_retries: int = 5,
    ):
        if not store_domain or not access_token:
            raise ValueError("Both store_domain and access_token are required.")

        self.store_domain = self._sanitize_domain(store_domain)
        self._access_token = access_token.strip()
        self.api_version = (api_version or SHOPIFY_API_VERSION or "2026-07").strip()
        self.timeout = timeout
        self.max_retries = max_retries
        self.endpoint_url = f"https://{self.store_domain}/admin/api/{self.api_version}/graphql.json"

        self._session = requests.Session()
        self._session.headers.update({
            "X-Shopify-Access-Token": self._access_token,
            "Content-Type": "application/json",
            "Accept": "application/json",
        })

    def __repr__(self) -> str:
        # Credential safety: never expose access_token in representations
        return (
            f"<ShopifyGraphQLClient domain='{self.store_domain}' "
            f"api_version='{self.api_version}' token='***'>"
        )

    # ------------------------------------------------------------------
    # Domain Sanitization
    # ------------------------------------------------------------------

    @staticmethod
    def _sanitize_domain(domain: str) -> str:
        clean = domain.strip().lower()
        clean = clean.replace("https://", "").replace("http://", "").strip("/")
        if not clean.endswith(".myshopify.com"):
            clean = f"{clean}.myshopify.com"
        pattern = re.compile(r"^[a-zA-Z0-9][a-zA-Z0-9\-]*\.myshopify\.com$")
        if not pattern.match(clean):
            raise ValueError(f"Invalid Shopify store domain: '{domain}'")
        return clean

    # ------------------------------------------------------------------
    # Query Execution with Cost Throttling & Retries
    # ------------------------------------------------------------------

    def execute(
        self,
        query: str,
        variables: Optional[Dict[str, Any]] = None,
    ) -> Dict[str, Any]:
        """
        Executes a GraphQL query or mutation against Shopify Admin API.

        Handles:
        - HTTP 5xx transient server error retries
        - Rate limiting / 429 / THROTTLED backoff based on throttleStatus
        - GraphQL execution error inspection

        Returns
        -------
        dict
            Parsed response JSON containing 'data' and optional 'extensions'.
        """
        payload = {"query": query}
        if variables:
            payload["variables"] = variables

        retries = 0
        while retries <= self.max_retries:
            try:
                response = self._session.post(
                    self.endpoint_url,
                    json=payload,
                    timeout=self.timeout,
                )

                # 1. Check for HTTP 429 Too Many Requests
                if response.status_code == 429:
                    retries += 1
                    if retries > self.max_retries:
                        raise ShopifyRateLimitError(
                            f"Shopify rate limit exceeded after {self.max_retries} retries."
                        )
                    wait_seconds = self._extract_wait_time(response)
                    time.sleep(wait_seconds)
                    continue

                # 2. Check for transient 5xx server errors
                if response.status_code >= 500:
                    retries += 1
                    if retries > self.max_retries:
                        raise RuntimeError(
                            f"Shopify server error HTTP {response.status_code} "
                            f"persisted after {self.max_retries} retries."
                        )
                    wait_seconds = float(2 ** retries)
                    time.sleep(wait_seconds)
                    continue

                # 3. Client HTTP error (4xx other than 429)
                if response.status_code >= 400:
                    # Strip any potential sensitive details from raw error text
                    raise RuntimeError(
                        f"Shopify HTTP error {response.status_code}: {response.reason}"
                    )

                data = response.json()

                # 4. Check for top-level GraphQL errors
                errors = data.get("errors")
                if errors:
                    if self._is_throttled_error(errors):
                        retries += 1
                        if retries > self.max_retries:
                            raise ShopifyRateLimitError(
                                f"Shopify GraphQL throttled after {self.max_retries} retries."
                            )
                        wait_seconds = self._extract_wait_time(response, data)
                        time.sleep(wait_seconds)
                        continue
                    else:
                        raise ShopifyGraphQLError(errors)

                # 5. Proactive throttle management: inspect cost extensions
                self._handle_proactive_throttle(data.get("extensions", {}).get("cost"))

                return data

            except requests.exceptions.RequestException as e:
                retries += 1
                if retries > self.max_retries:
                    raise RuntimeError(
                        f"Network error communicating with Shopify ({self.store_domain}): {e}"
                    ) from e
                time.sleep(float(2 ** retries))

        raise RuntimeError(f"Max retries exceeded communicating with {self.store_domain}")

    # ------------------------------------------------------------------
    # Throttle & Rate Limit Inspection
    # ------------------------------------------------------------------

    @staticmethod
    def _is_throttled_error(errors: List[Dict[str, Any]]) -> bool:
        """Determines if the GraphQL errors array represents a THROTTLED state."""
        for err in errors:
            msg = str(err.get("message", "")).lower()
            code = str(err.get("extensions", {}).get("code", "")).upper()
            if "throttled" in msg or code == "THROTTLED":
                return True
        return False

    @staticmethod
    def _extract_wait_time(
        response: requests.Response,
        data: Optional[Dict[str, Any]] = None,
    ) -> float:
        """
        Calculates wait duration from throttleStatus or Retry-After header.
        Uses: (maximumAvailable - currentlyAvailable) / restoreRate
        """
        # 1. Check HTTP Retry-After header
        retry_header = response.headers.get("Retry-After")
        if retry_header:
            try:
                return max(1.0, float(retry_header))
            except (ValueError, TypeError):
                pass

        # 2. Check extensions.cost.throttleStatus
        cost_ext = {}
        if data and isinstance(data, dict):
            cost_ext = data.get("extensions", {}).get("cost", {})
        else:
            try:
                cost_ext = response.json().get("extensions", {}).get("cost", {})
            except Exception:
                pass

        throttle_status = cost_ext.get("throttleStatus", {})
        if throttle_status:
            currently_available = float(throttle_status.get("currentlyAvailable", 0.0))
            restore_rate = float(throttle_status.get("restoreRate", 50.0))
            requested_cost = float(cost_ext.get("requestedQueryCost", 50.0))

            if restore_rate > 0:
                deficit = max(0.0, requested_cost - currently_available)
                wait = (deficit / restore_rate) + 0.2  # 200ms buffer
                return max(1.0, min(wait, 30.0))

        # Default fallback backoff
        return 2.0

    @staticmethod
    def _handle_proactive_throttle(cost: Optional[Dict[str, Any]]) -> None:
        """
        Proactively pauses if currentlyAvailable capacity is too depleted
        to execute another typical query without triggering a 429.
        """
        if not cost or not isinstance(cost, dict):
            return

        throttle_status = cost.get("throttleStatus")
        if not throttle_status or not isinstance(throttle_status, dict):
            return

        currently_available = float(throttle_status.get("currentlyAvailable", 1000.0))
        restore_rate = float(throttle_status.get("restoreRate", 50.0))
        actual_cost = float(cost.get("actualQueryCost", 10.0))

        # If available points drop below 2x the query cost, pause briefly to restore
        safety_floor = actual_cost * 2.0
        if currently_available < safety_floor and restore_rate > 0:
            deficit = safety_floor - currently_available
            sleep_time = (deficit / restore_rate) + 0.05
            time.sleep(min(sleep_time, 5.0))

    # ------------------------------------------------------------------
    # Reusable Cursor Pagination Generator
    # ------------------------------------------------------------------

    def paginate(
        self,
        query: str,
        variables: Optional[Dict[str, Any]] = None,
        connection_path: Union[str, Sequence[str]] = ("orders",),
        page_size: int = 50,
        max_pages: Optional[int] = None,
    ) -> Generator[Dict[str, Any], None, None]:
        """
        Yields pages of records from a Shopify GraphQL connection query.

        Parameters
        ----------
        query : str
            GraphQL query string containing $first and $after parameters.
        variables : dict, optional
            Variables dict for the GraphQL query.
        connection_path : str or sequence of str
            Path to the connection dict in response['data'].
            e.g. ("orders",) or ("location", "inventoryLevels")
        page_size : int
            Number of items to request per page ($first).
        max_pages : int, optional
            Optional safety limit on maximum pages to traverse.

        Yields
        ------
        dict containing:
            - page (int): 1-indexed page number
            - edges (list): raw edges array
            - nodes (list): extracted node objects from edges
            - page_info (dict): hasNextPage and endCursor
            - cost (dict): query cost metrics
            - raw_response (dict): full API response
        """
        vars_copy = dict(variables or {})
        vars_copy["first"] = page_size

        if isinstance(connection_path, str):
            path_keys = [connection_path]
        else:
            path_keys = list(connection_path)

        page_number = 1
        has_next_page = True
        cursor = vars_copy.get("after")

        while has_next_page:
            if max_pages and page_number > max_pages:
                break

            current_vars = dict(vars_copy)
            if cursor:
                current_vars["after"] = cursor
            elif "after" in current_vars and current_vars["after"] is None:
                del current_vars["after"]

            response_data = self.execute(query, current_vars)

            # Navigate to the target connection dictionary in response['data']
            curr: Any = response_data.get("data") or {}
            for k in path_keys:
                if isinstance(curr, dict):
                    curr = curr.get(k)
                else:
                    curr = None
                    break

            if not isinstance(curr, dict):
                # Connection path not found or empty
                break

            edges = curr.get("edges") or []
            nodes = [e["node"] for e in edges if isinstance(e, dict) and "node" in e]
            page_info = curr.get("pageInfo") or {}

            yield {
                "page": page_number,
                "edges": edges,
                "nodes": nodes,
                "page_info": page_info,
                "cost": response_data.get("extensions", {}).get("cost", {}),
                "raw_response": response_data,
            }

            has_next_page = bool(page_info.get("hasNextPage", False))
            cursor = page_info.get("endCursor")

            if not has_next_page or not cursor:
                break

            page_number += 1
