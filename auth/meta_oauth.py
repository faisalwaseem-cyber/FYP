"""Server-side Meta OAuth helpers for the Marketing API."""

from collections.abc import Mapping
from typing import Any, Dict, List
from urllib.parse import urlencode

import requests

from auth.credential_manager import CredentialManager
from config.settings import (
    META_API_VERSION,
    META_APP_ID,
    META_APP_SECRET,
    META_GRAPH_API_BASE_URL,
    META_OAUTH_SCOPES,
    META_REDIRECT_URI,
)
from connectors.meta_ads.client import MetaAdsClient
from connectors.meta_ads.services import MetaAdAccountService


class MetaOAuthError(RuntimeError):
    """Provider OAuth failure with no token, code, or secret in its message."""


class _OAuthTokenProvider(CredentialManager):
    """Short-lived in-memory credential adapter used only before account storage."""

    def __init__(self, access_token: str):
        self._access_token = access_token

    def __repr__(self) -> str:
        return f"{type(self).__name__}(credentials=<redacted>)"

    def get_valid_credentials(self, **_: Any) -> Dict[str, str]:
        return {"access_token": self._access_token}


def build_meta_authorization_url(state: str) -> str:
    if not META_APP_ID or not META_REDIRECT_URI:
        raise MetaOAuthError("Meta OAuth configuration is incomplete.")
    params = {
        "client_id": META_APP_ID,
        "redirect_uri": META_REDIRECT_URI,
        "state": state,
        "scope": META_OAUTH_SCOPES,
        "response_type": "code",
    }
    return (
        f"https://www.facebook.com/{META_API_VERSION}/dialog/oauth?"
        f"{urlencode(params)}"
    )


def exchange_meta_authorization_code(code: str) -> Dict[str, Any]:
    """Exchange a code server-side and preserve Meta's token lifetime for reauth handling.

    Meta documents long-lived exchange as optional and requires the existing
    access token in a query parameter, so BRIDGE FLOW does not perform it.
    """
    if not code or not META_APP_ID or not META_APP_SECRET or not META_REDIRECT_URI:
        raise MetaOAuthError("Meta OAuth configuration or authorization code is missing.")

    try:
        response = requests.get(
            f"{META_GRAPH_API_BASE_URL}/{META_API_VERSION}/oauth/access_token",
            params={
                "client_id": META_APP_ID,
                "redirect_uri": META_REDIRECT_URI,
                "client_secret": META_APP_SECRET,
                "code": code,
            },
            headers={"Accept": "application/json"},
            timeout=30,
        )
        payload = response.json()
    except (requests.RequestException, ValueError):
        raise MetaOAuthError("Meta authorization code exchange failed.") from None

    if (
        response.status_code >= 400
        or not isinstance(payload, Mapping)
        or payload.get("error")
        or not isinstance(payload.get("access_token"), str)
        or not payload.get("access_token")
    ):
        raise MetaOAuthError("Meta authorization code exchange failed.")

    result: Dict[str, Any] = {"access_token": payload["access_token"]}
    expires_in = payload.get("expires_in")
    if expires_in is not None:
        try:
            expires_in = int(expires_in)
        except (TypeError, ValueError, OverflowError):
            raise MetaOAuthError("Meta returned an invalid access token lifetime.") from None
        if expires_in <= 0:
            raise MetaOAuthError("Meta returned an invalid access token lifetime.")
        result["expires_in"] = expires_in
    return result


def fetch_meta_accessible_ad_accounts(
    business_id: int,
    access_token: str,
) -> List[Dict[str, Any]]:
    """Use the existing Meta service before persisting the OAuth token per account."""
    client = MetaAdsClient(
        business_id=business_id,
        account_identifier="oauth-bootstrap",
        ad_account_id="0",
        credential_manager=_OAuthTokenProvider(access_token),
        base_url=META_GRAPH_API_BASE_URL,
    )
    return MetaAdAccountService(client).fetch_accounts()