"""
Meta Marketing API (Graph API) Client.
Fetches:
1. Campaigns: id, name, status, effective_status
2. Daily Insights: spend, impressions, clicks, actions (purchases), action_values
"""
import time
import requests
from typing import Dict, Any, List, Optional
from datetime import date, timedelta

class MetaAdsClient:
    def __init__(self, access_token: str, ad_account_id: str, api_version: str = "v20.0"):
        self.access_token = access_token
        # Ensure account ID has 'act_' prefix
        raw_id = str(ad_account_id).replace("act_", "").strip()
        self.ad_account_id = f"act_{raw_id}"
        self.api_version = api_version
        self.base_url = f"https://graph.facebook.com/{self.api_version}"
        self.session = requests.Session()
        self.session.headers.update({
            "Authorization": f"Bearer {self.access_token}",
            "Accept": "application/json"
        })

    def _make_request(self, endpoint: str, params: Optional[Dict[str, Any]] = None, max_retries: int = 3) -> Dict[str, Any]:
        url = f"{self.base_url}/{endpoint}"
        retries = 0
        while retries < max_retries:
            try:
                response = self.session.get(url, params=params, timeout=30)
                
                # Check for Meta Rate Limit (Error subcode 2446079 or 17)
                if response.status_code == 400 or response.status_code == 429:
                    err_data = response.json().get("error", {})
                    if err_data.get("code") in (17, 80004, 613):
                        print("[!] Meta API Rate limit reached. Pausing for 60 seconds...")
                        time.sleep(60)
                        retries += 1
                        continue

                response.raise_for_status()
                return response.json()

            except requests.exceptions.RequestException as e:
                retries += 1
                if retries >= max_retries:
                    raise RuntimeError(f"Error communicating with Meta Graph API ({endpoint}): {e}") from e
                time.sleep(3)

        raise RuntimeError(f"Max retries exceeded for Meta API: {endpoint}")

    def fetch_campaigns(self) -> List[Dict[str, Any]]:
        """
        Fetches all campaigns configured for the ad account.
        Endpoint: GET /act_{account_id}/campaigns
        """
        params = {
            "fields": "id,name,status,effective_status,created_time",
            "limit": 100
        }
        res = self._make_request(f"{self.ad_account_id}/campaigns", params=params)
        return res.get("data", [])

    def fetch_daily_insights(self, since: str, until: str) -> List[Dict[str, Any]]:
        """
        Fetches daily ad spend and performance metrics grouped by campaign.
        Endpoint: GET /act_{account_id}/insights
        `time_increment=1` enforces day-by-day granularity.
        """
        params = {
            "level": "campaign",
            "fields": "campaign_id,campaign_name,spend,impressions,clicks,actions,action_values",
            "time_increment": 1,
            "time_range": f'{{"since":"{since}","until":"{until}"}}',
            "limit": 200
        }
        res = self._make_request(f"{self.ad_account_id}/insights", params=params)
        return res.get("data", [])
