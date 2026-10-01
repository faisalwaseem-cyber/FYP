"""
Production-Ready Meta Marketing API (Graph API) Client for BRIDGE FLOW.

Documentation: https://developers.facebook.com/docs/marketing-api/
Architecture:
- Current verified API Version: v26.0
- Runtime credential injection (access_token, ad_account_id, app_secret)
- Dynamic HMAC-SHA256 appsecret_proof generation for enhanced Graph API security
- Integration with auth.credential_manager (reuses CredentialManager, core.platform_connection, tenant resolution)
- Comprehensive rate-limit monitoring via X-Business-Use-Case-Usage and X-App-Usage headers
- Cursor-based pagination generator supporting after/next paging
- Automatic transient retry with exponential backoff on HTTP 5xx and network drops
- Strict credential safety: token & secret masking in __repr__ and exception logs
"""
import hashlib
import hmac
import json
import logging
import time
import urllib.parse
from typing import Any, Dict, Generator, List, Optional

import requests

logger = logging.getLogger(__name__)

# Verified official Meta Marketing API version
META_API_VERSION = "v26.0"
META_GRAPH_BASE_URL = "https://graph.facebook.com"


class MetaAPIError(RuntimeError):
    """Raised when Meta Graph API returns an error response."""

    def __init__(
        self,
        message: str,
        code: Optional[int] = None,
        error_subcode: Optional[int] = None,
        error_type: Optional[str] = None,
        fbtrace_id: Optional[str] = None,
        status_code: Optional[int] = None,
    ):
        self.code = code
        self.error_subcode = error_subcode
        self.error_type = error_type
        self.fbtrace_id = fbtrace_id
        self.status_code = status_code
        self.error_message = message

        details = []
        if code is not None:
            details.append(f"code={code}")
        if error_subcode is not None:
            details.append(f"subcode={error_subcode}")
        if error_type is not None:
            details.append(f"type={error_type}")
        if fbtrace_id is not None:
            details.append(f"fbtrace_id={fbtrace_id}")

        suffix = f" [{', '.join(details)}]" if details else ""
        super().__init__(f"Meta API Error: {message}{suffix}")


class MetaAuthenticationError(MetaAPIError):
    """Raised when access token is invalid, expired, revoked, or lacks required scopes."""
    pass


class MetaRateLimitError(MetaAPIError):
    """Raised when Meta Marketing API rate limit or throttling threshold is hit."""

    def __init__(
        self,
        message: str,
        code: Optional[int] = None,
        error_subcode: Optional[int] = None,
        estimated_time_to_regain_access: Optional[int] = None,
        usage_info: Optional[Dict[str, Any]] = None,
    ):
        self.estimated_time_to_regain_access = estimated_time_to_regain_access
        self.usage_info = usage_info or {}
        super().__init__(
            message=message,
            code=code,
            error_subcode=error_subcode,
            error_type="OAuthRateLimitException",
        )


class MetaClient:
    """
    Modular Meta Marketing API Client for multi-tenant SaaS integration.

    Directly interacts with Graph API v26.0 endpoints.
    Receives credentials dynamically at runtime or resolves via CredentialManager.
    """

    def __init__(
        self,
        access_token: str,
        ad_account_id: str,
        app_secret: Optional[str] = None,
        api_version: str = META_API_VERSION,
        timeout: float = 30.0,
        max_retries: int = 3,
    ):
        if not access_token:
            raise ValueError("access_token is required to initialize MetaClient.")
        if not ad_account_id:
            raise ValueError("ad_account_id is required to initialize MetaClient.")

        self._access_token = access_token.strip()
        self._app_secret = app_secret.strip() if app_secret else None
        self.api_version = api_version.strip()
        self.timeout = timeout
        self.max_retries = max_retries

        # Ensure account ID has strict 'act_' prefix per Graph API specification
        raw_id = str(ad_account_id).replace("act_", "").strip()
        if not raw_id:
            raise ValueError(f"Invalid ad_account_id provided: {ad_account_id}")
        self.ad_account_id = f"act_{raw_id}"

        self.base_url = f"{META_GRAPH_BASE_URL}/{self.api_version}"
        self.session = requests.Session()
        self.session.headers.update({
            "Authorization": f"Bearer {self._access_token}",
            "Accept": "application/json",
            "User-Agent": "BridgeFlow-MetaClient/1.0",
        })

    def __repr__(self) -> str:
        # Credential safety: never expose access_token or app_secret
        return (
            f"<MetaClient ad_account_id='{self.ad_account_id}' "
            f"api_version='{self.api_version}' token='***'>"
        )

    # ------------------------------------------------------------------
    # Security: appsecret_proof generation
    # ------------------------------------------------------------------

    def get_appsecret_proof(self) -> Optional[str]:
        """
        Calculates HMAC-SHA256 hash of the access token using the app secret.
        Required for secured Graph API calls when app secret proof is enabled.
        """
        if not self._app_secret or not self._access_token:
            return None
        return hmac.new(
            self._app_secret.encode("utf-8"),
            self._access_token.encode("utf-8"),
            hashlib.sha256,
        ).hexdigest()

    # ------------------------------------------------------------------
    # Rate Limit Header Inspection
    # ------------------------------------------------------------------

    @staticmethod
    def parse_rate_limit_headers(headers: requests.structures.CaseInsensitiveDict) -> Dict[str, Any]:
        """
        Extracts usage percentages and cooldown windows from Meta API response headers.
        """
        usage_data: Dict[str, Any] = {}

        # 1. Business Use Case Usage (marketing API specific)
        buc_header = headers.get("x-business-use-case-usage")
        if buc_header:
            try:
                usage_data["business_use_case"] = json.loads(buc_header)
            except Exception:
                usage_data["business_use_case"] = buc_header

        # 2. App-level Usage
        app_header = headers.get("x-app-usage")
        if app_header:
            try:
                usage_data["app_usage"] = json.loads(app_header)
            except Exception:
                usage_data["app_usage"] = app_header

        # 3. Ad-Account-level Usage
        acct_header = headers.get("x-ad-account-usage")
        if acct_header:
            try:
                usage_data["ad_account_usage"] = json.loads(acct_header)
            except Exception:
                usage_data["ad_account_usage"] = acct_header

        return usage_data

    # ------------------------------------------------------------------
    # Core Request Execution with Retries & Rate-Limit Handling
    # ------------------------------------------------------------------

    def request(
        self,
        method: str,
        path: str,
        params: Optional[Dict[str, Any]] = None,
        data: Optional[Dict[str, Any]] = None,
        json_body: Optional[Dict[str, Any]] = None,
    ) -> Dict[str, Any]:
        """
        Executes HTTP request against Meta Graph API with transient retry and rate-limit parsing.
        """
        # Build normalized URL
        clean_path = path.lstrip("/")
        if clean_path.startswith("http://") or clean_path.startswith("https://"):
            url = clean_path
        else:
            url = f"{self.base_url}/{clean_path}"

        req_params = dict(params or {})

        # Automatically inject appsecret_proof if secret is available
        proof = self.get_appsecret_proof()
        if proof and "appsecret_proof" not in req_params:
            req_params["appsecret_proof"] = proof

        last_exception: Optional[Exception] = None

        for attempt in range(1, self.max_retries + 1):
            try:
                response = self.session.request(
                    method=method.upper(),
                    url=url,
                    params=req_params,
                    data=data,
                    json=json_body,
                    timeout=self.timeout,
                )

                usage_info = self.parse_rate_limit_headers(response.headers)

                # Attempt to parse json payload
                try:
                    payload = response.json()
                except Exception:
                    payload = {}

                # Check for Meta Graph API error payload
                if not response.ok or "error" in payload:
                    err_dict = payload.get("error", {})
                    err_msg = err_dict.get("message", f"HTTP {response.status_code}")
                    err_code = err_dict.get("code")
                    err_subcode = err_dict.get("error_subcode")
                    err_type = err_dict.get("type")
                    fbtrace_id = err_dict.get("fbtrace_id")
                    is_transient = err_dict.get("is_transient", False)

                    # 1. Rate Limit Checks (HTTP 429 or codes 4, 17, 613, 80004)
                    if response.status_code == 429 or err_code in (4, 17, 613, 80004):
                        est_time = None
                        buc = usage_info.get("business_use_case")
                        if isinstance(buc, dict):
                            for v in buc.values():
                                if isinstance(v, list) and v and isinstance(v[0], dict):
                                    est_time = v[0].get("estimated_time_to_regain_access")
                                    break

                        raise MetaRateLimitError(
                            message=err_msg,
                            code=err_code,
                            error_subcode=err_subcode,
                            estimated_time_to_regain_access=est_time,
                            usage_info=usage_info,
                        )

                    # 2. Authentication / Authorization Failure (Code 190 or OAuthException)
                    if response.status_code == 401 or err_code == 190 or err_type == "OAuthException":
                        raise MetaAuthenticationError(
                            message=err_msg,
                            code=err_code,
                            error_subcode=err_subcode,
                            error_type=err_type,
                            fbtrace_id=fbtrace_id,
                            status_code=response.status_code,
                        )

                    # 3. Server-side Transient Retries (HTTP 5xx or is_transient)
                    if (response.status_code >= 500 or is_transient) and attempt < self.max_retries:
                        sleep_s = 2 ** attempt
                        logger.warning(
                            "Meta API transient error (%s: %s). Retrying in %ss (attempt %s/%s)...",
                            response.status_code,
                            err_msg,
                            sleep_s,
                            attempt,
                            self.max_retries,
                        )
                        time.sleep(sleep_s)
                        continue

                    # 4. General API Error
                    raise MetaAPIError(
                        message=err_msg,
                        code=err_code,
                        error_subcode=err_subcode,
                        error_type=err_type,
                        fbtrace_id=fbtrace_id,
                        status_code=response.status_code,
                    )

                return payload

            except (requests.exceptions.ConnectionError, requests.exceptions.Timeout) as exc:
                last_exception = exc
                if attempt < self.max_retries:
                    sleep_s = 2 ** attempt
                    logger.warning(
                        "Meta API network error (%s). Retrying in %ss (attempt %s/%s)...",
                        exc.__class__.__name__,
                        sleep_s,
                        attempt,
                        self.max_retries,
                    )
                    time.sleep(sleep_s)
                else:
                    raise RuntimeError(
                        f"Failed communicating with Meta API after {self.max_retries} attempts: {exc}"
                    ) from exc
            except (MetaAPIError, MetaRateLimitError, MetaAuthenticationError):
                raise

        if last_exception:
            raise last_exception
        raise RuntimeError("Meta API request failed with unknown state.")

    def get(self, path: str, params: Optional[Dict[str, Any]] = None) -> Dict[str, Any]:
        """Convenience wrapper for GET request."""
        return self.request("GET", path, params=params)

    def post(
        self,
        path: str,
        params: Optional[Dict[str, Any]] = None,
        data: Optional[Dict[str, Any]] = None,
        json_body: Optional[Dict[str, Any]] = None,
    ) -> Dict[str, Any]:
        """Convenience wrapper for POST request."""
        return self.request("POST", path, params=params, data=data, json_body=json_body)

    # ------------------------------------------------------------------
    # Cursor-based Pagination
    # ------------------------------------------------------------------

    def paginate(
        self,
        path: str,
        params: Optional[Dict[str, Any]] = None,
        limit: int = 100,
        max_pages: Optional[int] = None,
    ) -> Generator[Dict[str, Any], None, None]:
        """
        Cursor-based pagination generator for Meta Graph API collections.

        Yields page dictionaries:
        {
            "page": int,
            "count": int,
            "data": List[Dict[str, Any]],
            "paging": Dict[str, Any],
            "raw_response": Dict[str, Any]
        }
        """
        current_params = dict(params or {})
        current_params["limit"] = limit
        page_num = 0

        while True:
            page_num += 1
            response = self.get(path, params=current_params)
            data_items = response.get("data", [])
            paging = response.get("paging", {})

            yield {
                "page": page_num,
                "count": len(data_items),
                "data": data_items,
                "paging": paging,
                "raw_response": response,
            }

            if not data_items:
                break

            if max_pages is not None and page_num >= max_pages:
                break

            # Check next cursor
            cursors = paging.get("cursors", {})
            after_cursor = cursors.get("after")

            # Meta Graph API can provide either paging.next URL or cursors.after
            if after_cursor:
                current_params["after"] = after_cursor
            elif paging.get("next"):
                # If cursors.after isn't present, check if there's a next link
                next_url = paging["next"]
                # If next url is present, continue with next page via next cursor query
                parsed = urllib.parse.urlparse(next_url)
                qs = urllib.parse.parse_qs(parsed.query)
                if "after" in qs:
                    current_params["after"] = qs["after"][0]
                else:
                    break
            else:
                break

    # ------------------------------------------------------------------
    # Tenant & Credential Manager Factories
    # ------------------------------------------------------------------

    @classmethod
    def from_credentials(
        cls,
        credentials: Dict[str, Any],
        api_version: str = META_API_VERSION,
        timeout: float = 30.0,
        max_retries: int = 3,
    ) -> "MetaClient":
        """
        Instantiates MetaClient from a decrypted credentials dictionary
        (e.g., returned by CredentialManager or ConnectionService).
        """
        access_token = credentials.get("access_token")
        if not access_token:
            raise ValueError("Missing 'access_token' in credentials dictionary.")

        # Resolve ad account identifier
        additional_creds = credentials.get("additional_credentials") or {}
        ad_account_id = (
            credentials.get("account_identifier")
            or credentials.get("ad_account_id")
            or additional_creds.get("ad_account_id")
        )
        if not ad_account_id:
            raise ValueError(
                "Missing 'ad_account_id' or 'account_identifier' in credentials dictionary."
            )

        app_secret = credentials.get("app_secret") or additional_creds.get("app_secret")

        return cls(
            access_token=access_token,
            ad_account_id=str(ad_account_id),
            app_secret=str(app_secret) if app_secret else None,
            api_version=api_version,
            timeout=timeout,
            max_retries=max_retries,
        )

    @classmethod
    def for_tenant(
        cls,
        *,
        business_id: int,
        account_identifier: str,
        credential_manager: Optional[Any] = None,
        platform: str = "meta",
        api_version: str = META_API_VERSION,
        timeout: float = 30.0,
        max_retries: int = 3,
    ) -> "MetaClient":
        """
        Authoritative tenant-resolution factory.
        Uses CredentialManager to retrieve decrypted credentials from core.platform_connection.
        """
        if credential_manager is None:
            from auth.credential_manager import CredentialManager
            credential_manager = CredentialManager()

        credentials = credential_manager.get_valid_credentials(
            business_id=business_id,
            platform=platform,
            account_identifier=account_identifier,
        )

        return cls.from_credentials(
            credentials=credentials,
            api_version=api_version,
            timeout=timeout,
            max_retries=max_retries,
        )

