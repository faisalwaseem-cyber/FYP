"""Read-only client for Meta's versioned Marketing API."""

import json
import re
from collections.abc import Mapping
from datetime import date
from typing import Any, Dict, List, Optional
from urllib.parse import parse_qs, urlsplit

import requests

from auth.connection_service import get_connection, update_status
from auth.credential_manager import CredentialManager


class MetaAPIError(RuntimeError):
    """Safe provider error that never includes request credentials or raw payloads."""

    def __init__(
        self,
        message: str,
        *,
        status_code: Optional[int] = None,
        api_code: Optional[Any] = None,
        api_subcode: Optional[Any] = None,
    ) -> None:
        super().__init__(message)
        self.status_code = status_code
        self.api_code = api_code
        self.api_subcode = api_subcode


class MetaAuthenticationError(MetaAPIError):
    """Raised when Meta rejects a token or stored credentials are expired."""


class MetaAdsClient:
    API_VERSION = "v25.0"
    PLATFORM = "meta_ads"
    INSIGHTS_FIELDS = (
        "account_id,account_name,campaign_id,campaign_name,adset_id,adset_name,"
        "ad_id,ad_name,date_start,date_stop,spend,impressions,clicks,reach,"
        "ctr,cpc,cpm,actions,conversions,results,actions_results,action_values,"
        "purchase_roas,website_purchase_roas"
    )

    def __init__(
        self,
        business_id: int,
        account_identifier: str,
        ad_account_id: Optional[str] = None,
        *,
        credential_manager: Optional[CredentialManager] = None,
        base_url: str = "https://graph.facebook.com",
        timeout: float = 30,
        session: Optional[requests.Session] = None,
        connection_lookup: Any = get_connection,
        status_updater: Any = update_status,
    ) -> None:
        if not isinstance(business_id, int) or isinstance(business_id, bool):
            raise ValueError("A server-resolved business_id is required.")
        if not account_identifier or not str(account_identifier).strip():
            raise ValueError("A Meta connection account_identifier is required.")
        parsed_base = urlsplit(base_url)
        if parsed_base.scheme not in {"http", "https"} or not parsed_base.netloc:
            raise ValueError("base_url must be an absolute HTTP(S) URL.")

        self.business_id = business_id
        self.account_identifier = str(account_identifier).strip()
        self.ad_account_id = self._normalize_account_id(
            ad_account_id or self.account_identifier
        )
        self.api_version = self.API_VERSION
        self.base_url = base_url.rstrip("/")
        self.timeout = timeout
        self._credential_manager = credential_manager or CredentialManager()
        self._session = session or requests.Session()
        self._connection_lookup = connection_lookup
        self._status_updater = status_updater

    def __repr__(self) -> str:
        return (
            f"{type(self).__name__}(business_id={self.business_id!r}, "
            f"account_identifier={self.account_identifier!r}, "
            f"ad_account_id={self.ad_account_id!r}, "
            f"api_version={self.api_version!r}, credentials=<redacted>)"
        )

    @staticmethod
    def _normalize_account_id(account_id: str) -> str:
        raw_id = str(account_id).strip()
        if raw_id.startswith("act_"):
            raw_id = raw_id[4:]
        if not raw_id or not re.fullmatch(r"[A-Za-z0-9_-]+", raw_id):
            raise ValueError("Invalid Meta ad account identifier.")
        return f"act_{raw_id}"

    @staticmethod
    def _validate_object_id(object_id: str) -> str:
        value = str(object_id).strip()
        if not value or not re.fullmatch(r"[A-Za-z0-9_-]+", value):
            raise ValueError("Invalid Meta object identifier.")
        return value

    def _mark_needs_reauth(self) -> None:
        try:
            connection = self._connection_lookup(
                self.business_id, self.PLATFORM, self.account_identifier
            )
            if connection and connection.get("connection_id") is not None:
                self._status_updater(
                    connection["connection_id"],
                    "needs_reauth",
                    "Meta rejected the access token; reauthentication is required.",
                )
        except Exception:
            return

    def _get_access_token(self) -> str:
        try:
            credentials = self._credential_manager.get_valid_credentials(
                business_id=self.business_id,
                platform=self.PLATFORM,
                account_identifier=self.account_identifier,
                refresh_callback=None,
            )
        except ValueError as exc:
            if "expired" in str(exc).lower() or "no refresh_callback" in str(exc):
                self._mark_needs_reauth()
                raise MetaAuthenticationError(
                    "Meta access token is expired; reauthentication is required."
                ) from None
            raise MetaAuthenticationError(
                "Meta credentials are unavailable for this connection."
            ) from None
        except Exception:
            raise MetaAPIError("Meta credentials could not be loaded.") from None

        if not isinstance(credentials, Mapping):
            raise MetaAPIError("Meta credentials have an invalid format.")
        access_token = credentials.get("access_token")
        if not isinstance(access_token, str) or not access_token:
            raise MetaAuthenticationError("Meta access token is missing.")
        return access_token

    def _make_request(
        self,
        endpoint: str,
        params: Optional[Dict[str, Any]] = None,
    ) -> Dict[str, Any]:
        safe_endpoint = endpoint.strip("/")
        if not safe_endpoint or any(part in {".", ".."} for part in safe_endpoint.split("/")):
            raise ValueError("Invalid Meta API endpoint.")

        url = f"{self.base_url}/{self.api_version}/{safe_endpoint}"
        headers = {
            "Authorization": f"Bearer {self._get_access_token()}",
            "Accept": "application/json",
        }
        try:
            response = self._session.get(
                url,
                params=params,
                headers=headers,
                timeout=self.timeout,
            )
        except requests.RequestException:
            raise MetaAPIError("Meta API request failed before a response was received.") from None

        try:
            payload = response.json()
        except (ValueError, requests.RequestException):
            payload = None

        if not isinstance(payload, Mapping):
            raise MetaAPIError(
                "Meta API returned a malformed response.",
                status_code=response.status_code,
            )

        api_error = payload.get("error")
        if response.status_code == 401 or (
            isinstance(api_error, Mapping) and api_error.get("code") == 190
        ):
            self._mark_needs_reauth()
            raise MetaAuthenticationError(
                "Meta rejected the access token; reauthentication is required.",
                status_code=response.status_code,
                api_code=api_error.get("code") if isinstance(api_error, Mapping) else None,
                api_subcode=api_error.get("error_subcode") if isinstance(api_error, Mapping) else None,
            )

        if response.status_code >= 400 or api_error is not None:
            if isinstance(api_error, Mapping):
                api_code = api_error.get("code")
                api_subcode = api_error.get("error_subcode")
            else:
                api_code = None
                api_subcode = None
            raise MetaAPIError(
                "Meta API returned an error response.",
                status_code=response.status_code,
                api_code=api_code,
                api_subcode=api_subcode,
            )

        return dict(payload)

    def _fetch_all(
        self,
        endpoint: str,
        params: Optional[Dict[str, Any]] = None,
        max_pages: Optional[int] = None,
    ) -> List[Dict[str, Any]]:
        if max_pages is not None and max_pages < 1:
            raise ValueError("max_pages must be at least 1.")

        current_params = dict(params or {})
        records: List[Dict[str, Any]] = []
        seen_cursors = set()
        page_number = 0

        while True:
            payload = self._make_request(endpoint, params=current_params)
            page_data = payload.get("data")
            if not isinstance(page_data, list) or any(
                not isinstance(item, Mapping) for item in page_data
            ):
                raise MetaAPIError("Meta API returned malformed collection data.")
            records.extend(dict(item) for item in page_data)
            page_number += 1
            if max_pages is not None and page_number >= max_pages:
                break

            paging = payload.get("paging", {})
            if not isinstance(paging, Mapping):
                raise MetaAPIError("Meta API returned malformed pagination data.")
            next_page = paging.get("next")
            if not next_page:
                break

            cursors = paging.get("cursors", {})
            after = cursors.get("after") if isinstance(cursors, Mapping) else None
            if not after and isinstance(next_page, str):
                after_values = parse_qs(urlsplit(next_page).query).get("after")
                after = after_values[0] if after_values else None
            if not isinstance(after, str) or not after or after in seen_cursors:
                raise MetaAPIError("Meta API returned invalid pagination cursors.")

            seen_cursors.add(after)
            current_params["after"] = after

        return records

    def fetch_accessible_ad_accounts(
        self, max_pages: Optional[int] = None
    ) -> List[Dict[str, Any]]:
        return self._fetch_all(
            "me/adaccounts",
            {"fields": "id,account_id,name,currency,timezone_name", "limit": 100},
            max_pages,
        )

    def fetch_campaigns(self, max_pages: Optional[int] = None) -> List[Dict[str, Any]]:
        return self._fetch_all(
            f"{self.ad_account_id}/campaigns",
            {"fields": "id,name,status,effective_status,objective,created_time", "limit": 100},
            max_pages,
        )

    def fetch_ad_sets(
        self,
        campaign_id: Optional[str] = None,
        max_pages: Optional[int] = None,
    ) -> List[Dict[str, Any]]:
        parent_id = (
            self._validate_object_id(campaign_id)
            if campaign_id is not None
            else self.ad_account_id
        )
        return self._fetch_all(
            f"{parent_id}/adsets",
            {
                "fields": (
                    "id,name,campaign_id,status,effective_status,start_time,end_time,"
                    "daily_budget,lifetime_budget,optimization_goal"
                ),
                "limit": 100,
            },
            max_pages,
        )

    def fetch_ads(
        self,
        ad_set_id: Optional[str] = None,
        max_pages: Optional[int] = None,
    ) -> List[Dict[str, Any]]:
        parent_id = (
            self._validate_object_id(ad_set_id)
            if ad_set_id is not None
            else self.ad_account_id
        )
        return self._fetch_all(
            f"{parent_id}/ads",
            {"fields": "id,name,adset_id,campaign_id,status,effective_status,creative", "limit": 100},
            max_pages,
        )

    def fetch_ad_creatives(self, max_pages: Optional[int] = None) -> List[Dict[str, Any]]:
        return self._fetch_all(
            f"{self.ad_account_id}/adcreatives",
            {"fields": "id,name,object_story_spec,asset_feed_spec", "limit": 100},
            max_pages,
        )

    def fetch_insights(
        self,
        *,
        level: str,
        since: str,
        until: str,
        time_increment: Optional[Any] = None,
        max_pages: Optional[int] = None,
    ) -> List[Dict[str, Any]]:
        if level not in {"account", "campaign", "adset", "ad"}:
            raise ValueError("level must be account, campaign, adset, or ad.")
        try:
            since_date = date.fromisoformat(since)
            until_date = date.fromisoformat(until)
        except (TypeError, ValueError):
            raise ValueError("since and until must be valid YYYY-MM-DD dates.") from None
        if until_date < since_date:
            raise ValueError("A valid since/until date range is required.")
        if time_increment is not None and not (
            time_increment in {"monthly", "all_days"}
            or (
                isinstance(time_increment, int)
                and not isinstance(time_increment, bool)
                and 1 <= time_increment <= 90
            )
        ):
            raise ValueError("time_increment must be monthly, all_days, or an integer from 1 to 90.")

        params: Dict[str, Any] = {
            "level": level,
            "fields": self.INSIGHTS_FIELDS,
            "time_range": json.dumps(
                {"since": since, "until": until}, separators=(",", ":")
            ),
            "limit": 100,
        }
        if time_increment is not None:
            params["time_increment"] = time_increment
        return self._fetch_all(
            f"{self.ad_account_id}/insights", params, max_pages
        )
