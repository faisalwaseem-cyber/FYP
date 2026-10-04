"""
BRIDGE FLOW Backend API Service.

Exposes:
- GET  /health
- GET  /integrations/shopify/connect     (Session-authenticated OAuth initiation)
- GET  /integrations/shopify/callback    (HMAC & State verified OAuth callback)
- GET  /integrations/shopify/status      (Safe connection status)
- POST /integrations/shopify/sync        (Session-authenticated ingestion trigger)
- POST /integrations/shopify/disconnect  (Session-authenticated disconnection)

Backward-compatible routes:
- GET  /auth/shopify/login
- GET  /auth/shopify/callback
- POST /api/sync/shopify
"""
import json
import logging
import re
from typing import Any, Dict, Optional
from urllib.parse import parse_qsl, urlencode

from fastapi import Depends, FastAPI, HTTPException, Query, Request, status
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import RedirectResponse
from pydantic import BaseModel, ConfigDict, ValidationError

from api.dependencies import get_current_business_id
from auth.connection_service import (
    create_or_update_connection,
    list_connections,
    setup_jazzcash_sandbox_connection,
    update_status,
)
from auth.meta_oauth import (
    MetaOAuthError,
    build_meta_authorization_url,
    exchange_meta_authorization_code,
    fetch_meta_accessible_ad_accounts,
)
from auth.shopify_oauth import (
    build_authorization_url,
    exchange_authorization_code,
    generate_signed_oauth_state,
    sanitize_shop_domain,
    verify_oauth_state,
    verify_shopify_hmac,
)
from auth.token_manager import save_initial_store_connection
from config.settings import BRIDGE_FLOW_FRONTEND_ORIGIN, META_OAUTH_SCOPES
from pipeline.run_shopify_sync import run_shopify_sync
from pipeline.run_meta_ads_sync import run_meta_ads_sync

logger = logging.getLogger(__name__)


class JazzCashSandboxSetupRequest(BaseModel):
    model_config = ConfigDict(extra="forbid")

    merchant_id: str
    password: str
    integrity_salt: str


app = FastAPI(
    title="BRIDGE FLOW Backend API",
    description="Multi-Tenant E-Commerce Intelligence Platform Backend",
    version="1.0.0",
)
app.add_middleware(
    CORSMiddleware,
    allow_origins=[BRIDGE_FLOW_FRONTEND_ORIGIN],
    allow_methods=["GET", "POST", "OPTIONS"],
    allow_headers=["Authorization", "Content-Type"],
)


@app.get("/health")
def health_check():
    return {"status": "healthy", "service": "bridge-flow-api"}


async def _receive_jazzcash_notification(request: Request, notification_type: str):
    content_type = request.headers.get("content-type", "")
    media_type = content_type.split(";", 1)[0].strip().lower()
    fields_present = set()

    if media_type == "application/json":
        try:
            payload = json.loads(await request.body())
        except (json.JSONDecodeError, UnicodeDecodeError):
            raise HTTPException(
                status_code=status.HTTP_400_BAD_REQUEST,
                detail="Malformed JazzCash return payload.",
            ) from None
        if not isinstance(payload, dict):
            raise HTTPException(
                status_code=status.HTTP_400_BAD_REQUEST,
                detail="JazzCash return payload must be an object.",
            )
        fields_present = set(payload)
    elif media_type == "application/x-www-form-urlencoded":
        try:
            body = (await request.body()).decode("utf-8")
            fields_present = {
                key
                for key, _ in parse_qsl(
                    body,
                    keep_blank_values=True,
                    max_num_fields=1000,
                )
            }
        except (UnicodeDecodeError, ValueError):
            raise HTTPException(
                status_code=status.HTTP_400_BAD_REQUEST,
                detail="Malformed JazzCash return payload.",
            ) from None

    logger.info(
        "JazzCash %s received method=%s content_type=%s "
        "has_txn_ref=%s has_response_code=%s has_secure_hash=%s",
        notification_type,
        request.method,
        media_type or "unknown",
        "pp_TxnRefNo" in fields_present,
        "pp_ResponseCode" in fields_present,
        "pp_SecureHash" in fields_present,
    )
    return {"status": "received"}


@app.post("/integrations/jazzcash/return")
async def jazzcash_return(request: Request):
    """Acknowledge JazzCash return data without trusting or persisting it."""
    return await _receive_jazzcash_notification(request, "return")


@app.post("/integrations/jazzcash/ipn")
async def jazzcash_ipn(request: Request):
    """Acknowledge JazzCash IPN data without processing or persisting it."""
    return await _receive_jazzcash_notification(request, "IPN")


@app.post("/integrations/jazzcash/connect")
async def jazzcash_sandbox_connect(
    request: Request,
    business_id: int = Depends(get_current_business_id),
):
    """Configure JazzCash Sandbox credentials for the authenticated business."""
    try:
        payload = await request.json()
        setup = JazzCashSandboxSetupRequest.model_validate(payload)
    except (ValueError, ValidationError):
        raise HTTPException(
            status_code=422,
            detail="Invalid JazzCash Sandbox setup request.",
        ) from None

    if any(
        not value.strip()
        for value in (setup.merchant_id, setup.password, setup.integrity_salt)
    ):
        raise HTTPException(
            status_code=422,
            detail="Invalid JazzCash Sandbox setup request.",
        )

    connection_id = setup_jazzcash_sandbox_connection(
        business_id=business_id,
        merchant_id=setup.merchant_id,
        password=setup.password,
        integrity_salt=setup.integrity_salt,
    )
    return {
        "connection_id": connection_id,
        "platform": "jazzcash",
        "account_name": "JazzCash Sandbox",
        "status": "active",
    }


# ---------------------------------------------------------------------------
# 1. Connect / Login Endpoints
# ---------------------------------------------------------------------------

def _shopify_authorization_url(shop: str, business_id: int) -> str:
    try:
        clean_shop = sanitize_shop_domain(shop)
    except ValueError as exc:
        raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail=str(exc)) from exc

    state = generate_signed_oauth_state(business_id)
    return build_authorization_url(clean_shop, state)


def _shopify_frontend_redirect(outcome: str, reason: Optional[str] = None) -> RedirectResponse:
    query = {"shopify": outcome}
    if reason:
        query["reason"] = reason
    return RedirectResponse(
        url=f"{BRIDGE_FLOW_FRONTEND_ORIGIN}/?{urlencode(query)}",
        status_code=status.HTTP_303_SEE_OTHER,
    )

@app.get("/integrations/shopify/connect")
@app.get("/auth/shopify/login")
def shopify_connect(
    shop: str = Query(..., description="Target Shopify store domain (e.g. my-store.myshopify.com)"),
    business_id: int = Depends(get_current_business_id),
):
    """
    Initiates the Shopify OAuth 2.0 flow for the authenticated business.
    business_id is determined server-side from the authenticated session.
    """
    auth_url = _shopify_authorization_url(shop, business_id)
    return RedirectResponse(url=auth_url, status_code=status.HTTP_302_FOUND)


@app.post("/integrations/shopify/connect")
def shopify_connect_start(
    shop: str = Query(..., description="Target Shopify store domain (e.g. my-store.myshopify.com)"),
    business_id: int = Depends(get_current_business_id),
):
    """Returns an authenticated Shopify authorization URL for browser navigation."""
    return {"authorization_url": _shopify_authorization_url(shop, business_id)}


# ---------------------------------------------------------------------------
# 2. Callback Endpoints
# ---------------------------------------------------------------------------

@app.get("/integrations/shopify/callback")
@app.get("/auth/shopify/callback")
def shopify_callback(request: Request):
    """
    Shopify OAuth callback endpoint.
    1. Validates HMAC signature using SHOPIFY_CLIENT_SECRET.
    2. Validates state nonce and extracts the verified business_id.
    3. Exchanges authorization code for expiring offline access token & refresh token.
    4. Encrypts tokens with Fernet and stores in core.platform_connection.
    """
    params = dict(request.query_params)
    shop = params.get("shop")
    code = params.get("code")
    state = params.get("state")

    if not shop or not code or not state:
        return _shopify_frontend_redirect("error", "invalid_request")

    # 1. Timing-safe HMAC verification
    try:
        valid_hmac = verify_shopify_hmac(params)
    except Exception:
        valid_hmac = False
    if not valid_hmac:
        return _shopify_frontend_redirect("error", "invalid_hmac")

    # 2. State verification & business_id resolution (consumed once)
    try:
        business_id = verify_oauth_state(state)
    except Exception:
        business_id = None
    if not business_id:
        return _shopify_frontend_redirect("error", "invalid_state")

    try:
        clean_shop = sanitize_shop_domain(shop)
    except ValueError:
        return _shopify_frontend_redirect("error", "invalid_shop")

    # 3. Server-to-server code exchange for expiring offline tokens
    try:
        token_response = exchange_authorization_code(clean_shop, code)
    except Exception:
        return _shopify_frontend_redirect("error", "token_exchange_failed")

    access_token = token_response.get("access_token")
    scopes = token_response.get("scope", "")
    expires_in = token_response.get("expires_in")
    refresh_token = token_response.get("refresh_token")
    rt_expires_in = token_response.get("refresh_token_expires_in")

    if not access_token:
        return _shopify_frontend_redirect("error", "token_exchange_failed")

    # 4. Authoritative storage in core.platform_connection via ConnectionService
    try:
        create_or_update_connection(
            business_id=business_id,
            platform="shopify",
            platform_category="ecommerce",
            account_identifier=clean_shop,
            account_name=clean_shop,
            auth_type="oauth2",
            access_token=access_token,
            refresh_token=refresh_token,
            expires_in=expires_in,
            refresh_token_expires_in=rt_expires_in,
            scopes=scopes,
        )
    except Exception:
        return _shopify_frontend_redirect("error", "connection_storage_failed")

    # Dual-write to legacy core.store_connection for backward-compatibility with older tests
    try:
        save_initial_store_connection(
            business_id=business_id,
            store_domain=clean_shop,
            access_token=access_token,
            scopes=scopes,
            expires_in=expires_in,
            refresh_token=refresh_token,
            refresh_token_expires_in=rt_expires_in,
        )
    except Exception:
        pass

    return _shopify_frontend_redirect("connected")


# ---------------------------------------------------------------------------
# 3. Status Endpoint
# ---------------------------------------------------------------------------

@app.get("/integrations/shopify/status")
def shopify_status(
    business_id: int = Depends(get_current_business_id),
):
    """
    Returns connection status for the authenticated business.
    Never returns secrets, tokens, or credentials.
    """
    conns = list_connections(business_id)
    shopify_conn = next((c for c in conns if c["platform"] == "shopify"), None)

    if not shopify_conn:
        return {
            "connected": False,
            "platform": "shopify",
            "status": "not_connected",
        }

    return {
        "connected": (shopify_conn["status"] == "active"),
        "platform": "shopify",
        "store": shopify_conn["account_identifier"],
        "status": shopify_conn["status"],
        "installed_at": shopify_conn["installed_at"].isoformat() if shopify_conn.get("installed_at") else None,
        "last_synced_at": shopify_conn["last_synced_at"].isoformat() if shopify_conn.get("last_synced_at") else None,
    }


# ---------------------------------------------------------------------------
# 4. Disconnect Endpoint
# ---------------------------------------------------------------------------

@app.post("/integrations/shopify/disconnect")
def shopify_disconnect(
    business_id: int = Depends(get_current_business_id),
):
    """
    Disconnects the Shopify store for the authenticated business.
    Transitions connection status to 'disconnected'.
    """
    conns = list_connections(business_id)
    shopify_conn = next((c for c in conns if c["platform"] == "shopify"), None)

    if not shopify_conn:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail="No Shopify connection found for this business.",
        )

    update_status(shopify_conn["connection_id"], "disconnected")

    return {
        "status": "success",
        "message": f"Shopify store '{shopify_conn['account_identifier']}' disconnected successfully.",
        "business_id": business_id,
    }


# ---------------------------------------------------------------------------
# 5. Ingestion Sync Endpoint
# ---------------------------------------------------------------------------

@app.post("/integrations/shopify/sync")
@app.post("/api/sync/shopify")
def trigger_shopify_sync(
    business_id: int = Depends(get_current_business_id),
):
    """
    Triggers Shopify data synchronization for the authenticated business.
    business_id is strictly derived from the authenticated session.
    """
    try:
        metrics = run_shopify_sync(business_id)
        return {
            "status": "success",
            "business_id": business_id,
            "metrics": metrics,
        }
    except Exception as e:
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Shopify sync failed: {e}",
        )


# ---------------------------------------------------------------------------
# 6. Meta Ads OAuth and ingestion endpoints
# ---------------------------------------------------------------------------

def _meta_frontend_redirect(outcome: str, reason: Optional[str] = None, accounts: Optional[int] = None):
    query = {"meta": outcome}
    if reason:
        query["reason"] = reason
    if accounts is not None:
        query["accounts"] = str(accounts)
    return RedirectResponse(
        url=f"{BRIDGE_FLOW_FRONTEND_ORIGIN}/?{urlencode(query)}",
        status_code=status.HTTP_303_SEE_OTHER,
    )


@app.post("/integrations/meta/connect")
def meta_connect(business_id: int = Depends(get_current_business_id)):
    """Return a server-built OAuth URL for browser navigation after authenticated fetch."""
    try:
        state_nonce = generate_signed_oauth_state(
            business_id,
            platform="meta_ads",
        )
        authorization_url = build_meta_authorization_url(state_nonce)
    except MetaOAuthError:
        raise HTTPException(
            status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
            detail="Meta OAuth is not configured.",
        ) from None
    return {"authorization_url": authorization_url}


@app.get("/integrations/meta/callback")
def meta_callback(request: Request):
    """Consume a tenant-bound state, exchange the code, and encrypt per-account tokens."""
    params = request.query_params
    state_nonce = params.get("state")
    if not state_nonce:
        return _meta_frontend_redirect("error", "invalid_state")

    try:
        business_id = verify_oauth_state(state_nonce, platform="meta_ads")
    except Exception:
        business_id = None
    if business_id is None:
        return _meta_frontend_redirect("error", "invalid_state")

    if params.get("error"):
        return _meta_frontend_redirect("error", "authorization_denied")
    code = params.get("code")
    if not code:
        return _meta_frontend_redirect("error", "invalid_request")

    try:
        token_data = exchange_meta_authorization_code(code)
        access_token = token_data["access_token"]
        accounts = fetch_meta_accessible_ad_accounts(business_id, access_token)
    except Exception:
        return _meta_frontend_redirect("error", "authorization_failed")

    connected_account_ids = set()
    for account in accounts:
        account_identifier = account.get("id")
        if (
            not isinstance(account_identifier, str)
            or re.fullmatch(r"act_[0-9]+", account_identifier) is None
            or account_identifier in connected_account_ids
        ):
            continue
        try:
            create_or_update_connection(
                business_id=business_id,
                platform="meta_ads",
                platform_category="marketing",
                account_identifier=account_identifier,
                account_name=account.get("name") if isinstance(account.get("name"), str) else None,
                auth_type="oauth2",
                access_token=access_token,
                expires_in=token_data.get("expires_in"),
                scopes=META_OAUTH_SCOPES,
            )
        except Exception:
            return _meta_frontend_redirect("error", "connection_storage_failed")
        connected_account_ids.add(account_identifier)

    if not connected_account_ids:
        return _meta_frontend_redirect("error", "no_accessible_ad_accounts")
    return _meta_frontend_redirect("connected", accounts=len(connected_account_ids))


@app.get("/integrations/meta/status")
def meta_status(business_id: int = Depends(get_current_business_id)):
    """Return safe connection metadata for all Meta accounts owned by this tenant."""
    connections = [
        connection
        for connection in list_connections(business_id)
        if connection["platform"] == "meta_ads"
    ]
    accounts = []
    for connection in connections:
        accounts.append({
            "account_identifier": connection["account_identifier"],
            "account_name": connection.get("account_name"),
            "status": connection["status"],
            "scopes": connection.get("scopes"),
            "installed_at": connection["installed_at"].isoformat()
            if connection.get("installed_at") else None,
            "last_synced_at": connection["last_synced_at"].isoformat()
            if connection.get("last_synced_at") else None,
        })
    return {
        "connected": any(account["status"] == "active" for account in accounts),
        "platform": "meta_ads",
        "accounts": accounts,
    }


@app.post("/integrations/meta/disconnect")
def meta_disconnect(
    account_identifier: Optional[str] = Query(None),
    business_id: int = Depends(get_current_business_id),
):
    """Disconnect one selected account or all Meta accounts within the authenticated tenant."""
    connections = [
        connection
        for connection in list_connections(business_id)
        if connection["platform"] == "meta_ads"
        and (account_identifier is None or connection["account_identifier"] == account_identifier)
    ]
    if not connections:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail="No matching Meta ad account connection was found.",
        )
    for connection in connections:
        update_status(connection["connection_id"], "disconnected")
    return {"status": "disconnected", "accounts_disconnected": len(connections)}


@app.post("/integrations/meta/sync")
def trigger_meta_sync(
    account_identifier: Optional[str] = Query(None),
    days_back: int = Query(7, ge=1, le=90),
    business_id: int = Depends(get_current_business_id),
):
    """Sync one selected or all active Meta ad accounts for the authenticated business."""
    try:
        result = run_meta_ads_sync(
            business_id=business_id,
            days_back=days_back,
            account_identifier=account_identifier,
        )
    except ValueError as exc:
        raise HTTPException(
            status_code=status.HTTP_409_CONFLICT,
            detail=str(exc),
        ) from None
    except Exception:
        raise HTTPException(
            status_code=status.HTTP_502_BAD_GATEWAY,
            detail="Meta Ads synchronization failed.",
        ) from None
    return result
