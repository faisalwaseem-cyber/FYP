"""
Stripe Payments API Client.
Fetches charges and refunds from Stripe.
Documentation: https://stripe.com/docs/api/charges/list
"""
import time
import requests
from typing import Dict, Any, List, Optional

class StripeClient:
    def __init__(self, secret_key: str, api_version: str = "2024-04-10"):
        self.secret_key = secret_key
        self.base_url = "https://api.stripe.com/v1"
        self.session = requests.Session()
        self.session.auth = (self.secret_key, "")
        self.session.headers.update({
            "Stripe-Version": api_version,
            "Accept": "application/json"
        })

    def _make_request(self, endpoint: str, params: Optional[Dict[str, Any]] = None) -> Dict[str, Any]:
        url = f"{self.base_url}/{endpoint}"
        retries = 0
        while retries < 4:
            response = self.session.get(url, params=params, timeout=30)
            if response.status_code == 429:
                time.sleep(float(response.headers.get("Retry-After", 2)))
                retries += 1
                continue
            response.raise_for_status()
            return response.json()
        raise RuntimeError(f"Max retries exceeded for Stripe endpoint: {endpoint}")

    def fetch_charges(self, created_after: Optional[int] = None, limit: int = 100) -> List[Dict[str, Any]]:
        """
        Fetches charges. Uses auto-pagination via starting_after cursor.
        Endpoint: GET /v1/charges
        """
        params: Dict[str, Any] = {"limit": limit}
        if created_after:
            params["created[gte]"] = created_after

        all_charges = []
        while True:
            data = self._make_request("charges", params=params)
            charges = data.get("data", [])
            all_charges.extend(charges)
            if not data.get("has_more") or not charges:
                break
            params["starting_after"] = charges[-1]["id"]
        return all_charges

    def fetch_refunds(self, created_after: Optional[int] = None, limit: int = 100) -> List[Dict[str, Any]]:
        """
        Fetches refunds.
        Endpoint: GET /v1/refunds
        """
        params: Dict[str, Any] = {"limit": limit}
        if created_after:
            params["created[gte]"] = created_after

        all_refunds = []
        while True:
            data = self._make_request("refunds", params=params)
            refunds = data.get("data", [])
            all_refunds.extend(refunds)
            if not data.get("has_more") or not refunds:
                break
            params["starting_after"] = refunds[-1]["id"]
        return all_refunds
