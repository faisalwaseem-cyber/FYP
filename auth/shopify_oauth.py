"""
Shopify OAuth 2.0 Authorization Code Grant Service for BRIDGE FLOW.

Handles:
1. Shop domain validation and sanitization.
2. Cryptographically secure state generation with server-side business_id binding.
3. Authorization URL generation.
4. Timing-safe HMAC-SHA256 signature verification matching Shopify's exact algorithm.
5. Server-to-server authorization code exchange for expiring offline tokens.
"""
import hmac
import hashlib
import re
import secrets
import urllib.parse
from datetime import datetime, timezone
from typing import Dict, Any, Optional

import requests
from sqlalchemy import text

from config.database import get_db_connection
from config.settings import (
    SHOPIFY_CLIENT_ID,
    SHOPIFY_CLIENT_SECRET,
    SHOPIFY_REDIRECT_URI,
    SHOPIFY_SCOPES,
)

# Regex to strictly validate myshopify.com domain format
_SHOP_REGEX = re.compile(r"^[a-zA-Z0-9][a-zA-Z0-9\-]*\.myshopify\.com$")


def sanitize_shop_domain(shop: str) -> str:
    """
    Sanitizes and validates a Shopify domain.
    E.g. 'https://my-store.myshopify.com/' -> 'my-store.myshopify.com'
         'my-store' -> 'my-store.myshopify.com'
    Raises ValueError if invalid.
    """
    if not shop:
        raise ValueError("Shop domain cannot be empty.")

    cleaned = shop.strip().lower()
    cleaned = cleaned.replace("https://", "").replace("http://", "").strip("/")

    if not cleaned.endswith(".myshopify.com"):
        cleaned = f"{cleaned}.myshopify.com"

    if not _SHOP_REGEX.match(cleaned):
        raise ValueError(
            f"Invalid Shopify domain '{shop}'. Domain must match 'subdomain.myshopify.com' format."
        )

    return cleaned


def generate_signed_oauth_state(
    business_id: int,
    ttl_minutes: int = 15,
    *,
    platform: str = "shopify",
) -> str:
    """
    Generates a cryptographically secure random state nonce, binds it to the
    authenticated business_id in core.oauth_state, and returns the state token.
    State expires automatically after ttl_minutes (default: 15 minutes).
    Also opportunistically cleans up expired oauth states to prevent table bloat.
    """
    nonce = secrets.token_urlsafe(32)

    cleanup_sql = text("""
        DELETE FROM core.oauth_state
        WHERE expires_at < now();
    """)

    insert_sql = text("""
        INSERT INTO core.oauth_state (business_id, state_nonce, platform, created_at, expires_at)
        VALUES (:b_id, :nonce, :platform, now(), now() + (:ttl || ' minutes')::INTERVAL);
    """)

    with get_db_connection() as conn:
        conn.execute(cleanup_sql)
        conn.execute(insert_sql, {
            "b_id": business_id,
            "nonce": nonce,
            "platform": platform,
            "ttl": str(ttl_minutes),
        })
        conn.commit()

    return nonce


def verify_oauth_state(
    state_nonce: str,
    *,
    platform: str = "shopify",
) -> Optional[int]:
    """
    Verifies that the state nonce exists in core.oauth_state and has not expired.
    Deletes the used state nonce to prevent replay/CSRF attacks.
    Returns the bound business_id, or None if invalid/expired.
    """
    if not state_nonce:
        return None

    consume_sql = text("""
        DELETE FROM core.oauth_state
        WHERE state_nonce = :nonce
          AND platform = :platform
          AND expires_at > now()
        RETURNING business_id;
    """)

    with get_db_connection() as conn:
        row = conn.execute(consume_sql, {
            "nonce": state_nonce,
            "platform": platform,
        }).fetchone()
        if row is None:
            return None
        conn.commit()
        return int(row[0])


def build_authorization_url(
    shop_domain: str,
    state: str,
    client_id: Optional[str] = None,
    scopes: Optional[str] = None,
    redirect_uri: Optional[str] = None,
) -> str:
    """
    Constructs the Shopify OAuth authorization URL for standalone apps.
    """
    cid = client_id or SHOPIFY_CLIENT_ID
    sc = scopes or SHOPIFY_SCOPES
    r_uri = redirect_uri or SHOPIFY_REDIRECT_URI

    if not cid:
        raise ValueError("SHOPIFY_CLIENT_ID is not configured.")

    params = {
        "client_id": cid,
        "scope": sc,
        "redirect_uri": r_uri,
        "state": state,
    }
    encoded_params = urllib.parse.urlencode(params)
    return f"https://{shop_domain}/admin/oauth/authorize?{encoded_params}"


def verify_shopify_hmac(
    params: Dict[str, str],
    client_secret: Optional[str] = None,
) -> bool:
    """
    Verifies the HMAC signature sent by Shopify in the callback query parameters.
    Uses timing-safe comparison to prevent side-channel timing attacks.
    """
    secret = client_secret or SHOPIFY_CLIENT_SECRET
    if not secret:
        raise ValueError("SHOPIFY_CLIENT_SECRET is not configured.")

    provided_hmac = params.get("hmac", "")
    if not provided_hmac:
        return False

    # Extract all query parameters except 'hmac' and 'signature'
    filtered_params = {
        k: v for k, v in params.items() if k not in ("hmac", "signature")
    }

    # Sort lexicographically by key and build 'k1=v1&k2=v2'
    sorted_pairs = sorted(filtered_params.items())
    message = "&".join(f"{k}={v}" for k, v in sorted_pairs)

    # Compute HMAC-SHA256
    computed_hmac = hmac.new(
        secret.encode("utf-8"),
        message.encode("utf-8"),
        hashlib.sha256,
    ).hexdigest()

    return hmac.compare_digest(computed_hmac, provided_hmac)


def exchange_authorization_code(
    shop_domain: str,
    code: str,
    client_id: Optional[str] = None,
    client_secret: Optional[str] = None,
) -> Dict[str, Any]:
    """
    Exchanges a temporary authorization code for an expiring offline access token
    and refresh token via POST /admin/oauth/access_token.

    Returns dict containing:
      - access_token (str)
      - scope (str)
      - expires_in (int seconds, optional in older apps, standard in modern offline tokens)
      - refresh_token (str, optional)
      - refresh_token_expires_in (int seconds, optional)
    """
    cid = client_id or SHOPIFY_CLIENT_ID
    secret = client_secret or SHOPIFY_CLIENT_SECRET

    if not cid or not secret:
        raise ValueError("SHOPIFY_CLIENT_ID and SHOPIFY_CLIENT_SECRET must be configured.")

    token_url = f"https://{shop_domain}/admin/oauth/access_token"
    payload = {
        "client_id": cid,
        "client_secret": secret,
        "code": code,
    }

    response = requests.post(
        token_url,
        json=payload,
        headers={"Content-Type": "application/json", "Accept": "application/json"},
        timeout=30,
    )
    response.raise_for_status()
    return response.json()


def refresh_shopify_token_network_call(
    shop_domain: str,
    refresh_token: str,
    client_id: Optional[str] = None,
    client_secret: Optional[str] = None,
) -> Dict[str, Any]:
    """
    Exchanges an expiring offline refresh token for a new access token and rotated
    refresh token via POST https://{shop_domain}/admin/oauth/access_token.

    Returns dict containing:
      - access_token (str)
      - expires_in (int seconds)
      - refresh_token (str, optional)
      - refresh_token_expires_in (int seconds, optional)
    """
    cid = client_id or SHOPIFY_CLIENT_ID
    secret = client_secret or SHOPIFY_CLIENT_SECRET

    if not cid or not secret:
        raise ValueError("SHOPIFY_CLIENT_ID and SHOPIFY_CLIENT_SECRET must be configured.")

    if not refresh_token:
        raise ValueError("Cannot refresh token without a valid refresh_token.")

    token_url = f"https://{shop_domain}/admin/oauth/access_token"
    payload = {
        "client_id": cid,
        "client_secret": secret,
        "grant_type": "refresh_token",
        "refresh_token": refresh_token,
    }

    response = requests.post(
        token_url,
        json=payload,
        headers={"Content-Type": "application/json", "Accept": "application/json"},
        timeout=30,
    )
    response.raise_for_status()
    return response.json()


def make_shopify_refresh_callback(shop_domain: str):
    """
    Factory creating a refresh_callback compatible with CredentialManager.
    Accepts current decrypted credentials dict, extracts refresh_token,
    executes server-to-server refresh network call, and returns the response.
    """
    def _callback(current_creds: Dict[str, Any]) -> Dict[str, Any]:
        rt = current_creds.get("refresh_token")
        if not rt:
            raise ValueError(
                f"No refresh_token found for Shopify store '{shop_domain}'. "
                "Cannot perform automatic refresh."
            )
        return refresh_shopify_token_network_call(
            shop_domain=shop_domain,
            refresh_token=rt,
        )
    return _callback

