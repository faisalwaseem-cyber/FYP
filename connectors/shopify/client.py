import time
import re
import requests
from typing import Generator, Dict, Any, Optional
from config.settings import SHOPIFY_API_VERSION

class ShopifyClient:
    """
    Shopify Admin REST API Client.
    Features:
    - Cursor-based pagination using RFC 5988 Link headers (page_info)
    - Automatic 429 Too Many Requests rate-limit back-off with Retry-After
    - Server error retry mechanism (5xx)
    """
    def __init__(self, store_domain: str, access_token: str, api_version: str = SHOPIFY_API_VERSION):
        if not store_domain or not access_token:
            raise ValueError("Both store_domain and access_token are required.")
            
        clean_domain = store_domain.replace("https://", "").replace("http://", "").strip("/")
        if not clean_domain.endswith(".myshopify.com"):
            clean_domain = f"{clean_domain}.myshopify.com"

        self.store_domain = clean_domain
        self.access_token = access_token
        self.api_version = api_version
        self.base_url = f"https://{self.store_domain}/admin/api/{self.api_version}"
        
        self.session = requests.Session()
        self.session.headers.update({
            "X-Shopify-Access-Token": self.access_token,
            "Content-Type": "application/json",
            "Accept": "application/json",
        })

    def _make_request(self, url: str, params: Optional[Dict[str, Any]] = None, max_retries: int = 5) -> requests.Response:
        retries = 0
        while retries < max_retries:
            try:
                response = self.session.get(url, params=params, timeout=30)
                
                # Check for rate limit
                if response.status_code == 429:
                    retry_after = float(response.headers.get("Retry-After", 2.0))
                    print(f"[Shopify Rate Limit] 429 hit. Pausing for {retry_after:.2f}s...")
                    time.sleep(retry_after)
                    retries += 1
                    continue
                
                # Check for server transient errors (500, 502, 503, 504)
                if response.status_code >= 500:
                    wait_time = (retries + 1) * 2
                    print(f"[Shopify Server Error] HTTP {response.status_code}. Retrying in {wait_time}s...")
                    time.sleep(wait_time)
                    retries += 1
                    continue

                response.raise_for_status()
                return response

            except requests.exceptions.RequestException as e:
                retries += 1
                if retries >= max_retries:
                    raise RuntimeError(f"Network error communicating with Shopify: {e}") from e
                time.sleep(2)

        raise RuntimeError(f"Max retries ({max_retries}) exceeded for {url}")

    def fetch_orders(
        self,
        updated_at_min: Optional[str] = None,
        limit: int = 250
    ) -> Generator[Dict[str, Any], None, None]:
        """
        Yields batches of orders from Shopify using cursor pagination.
        
        Note:
        `status=any` is mandatory. Without it, Shopify returns only open orders,
        excluding fulfilled, archived, and cancelled orders.
        """
        url = f"{self.base_url}/orders.json"
        params: Optional[Dict[str, Any]] = {
            "status": "any",
            "limit": limit
        }
        if updated_at_min:
            params["updated_at_min"] = updated_at_min

        page_number = 1
        while url:
            print(f"[*] Requesting Shopify orders (Page {page_number})...")
            response = self._make_request(url, params=params)
            data = response.json()
            orders = data.get("orders", [])

            yield {
                "page": page_number,
                "orders": orders,
                "raw_response": data
            }

            # Parse cursor pagination from the Link header
            # Header format: <https://.../orders.json?limit=250&page_info=xxx>; rel="next"
            link_header = response.headers.get("Link", "")
            next_url = None
            if link_header:
                matches = re.findall(r'<([^>]+)>;\s*rel="([^"]+)"', link_header)
                for matched_url, rel in matches:
                    if rel == "next":
                        next_url = matched_url
                        break

            # In cursor pagination, URL contains page_info; params must be omitted
            url = next_url
            params = None
            page_number += 1
