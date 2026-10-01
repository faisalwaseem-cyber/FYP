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
from typing import Any, Dict, Optional

from fastapi import Depends, FastAPI, HTTPException, Query, Request, status
from fastapi.responses import JSONResponse, RedirectResponse

from api.dependencies import get_current_business_id
from auth.connection_service import (
    create_or_update_connection,
    list_connections,
    update_status,
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
from pipeline.run_shopify_sync import run_shopify_sync

app = FastAPI(
    title="BRIDGE FLOW Backend API",
    description="Multi-Tenant E-Commerce Intelligence Platform Backend",
    version="1.0.0",
)


@app.get("/health")
def health_check():
    return {"status": "healthy", "service": "bridge-flow-api"}


# ---------------------------------------------------------------------------
# 1. Connect / Login Endpoints
# ---------------------------------------------------------------------------

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
    try:
        clean_shop = sanitize_shop_domain(shop)
    except ValueError as e:
        raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail=str(e))

    # Generate secure random nonce bound to this business_id in core.oauth_state
    state = generate_signed_oauth_state(business_id)

    # Build Shopify authorization URL
    auth_url = build_authorization_url(clean_shop, state)

    return RedirectResponse(url=auth_url, status_code=status.HTTP_302_FOUND)


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
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="Missing required query parameters: 'shop', 'code', or 'state'.",
        )

    # 1. Timing-safe HMAC verification
    if not verify_shopify_hmac(params):
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="Invalid HMAC signature: Request may have been tampered with.",
        )

    # 2. State verification & business_id resolution (consumed once)
    business_id = verify_oauth_state(state)
    if not business_id:
        raise HTTPException(
            status_code=status.HTTP_403_FORBIDDEN,
            detail="Invalid or expired OAuth state nonce. Please initiate authorization again.",
        )

    clean_shop = sanitize_shop_domain(shop)

    # 3. Server-to-server code exchange for expiring offline tokens
    try:
        token_response = exchange_authorization_code(clean_shop, code)
    except Exception as e:
        raise HTTPException(
            status_code=status.HTTP_502_BAD_GATEWAY,
            detail=f"Shopify token exchange failed: {e}",
        )

    access_token = token_response.get("access_token")
    scopes = token_response.get("scope", "")
    expires_in = token_response.get("expires_in")
    refresh_token = token_response.get("refresh_token")
    rt_expires_in = token_response.get("refresh_token_expires_in")

    if not access_token:
        raise HTTPException(
            status_code=status.HTTP_502_BAD_GATEWAY,
            detail="Shopify response did not contain an access_token.",
        )

    # 4. Authoritative storage in core.platform_connection via ConnectionService
    conn_id = create_or_update_connection(
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

    return JSONResponse(content={
        "status": "success",
        "message": "Shopify store successfully connected.",
        "connection_id": conn_id,
        "business_id": business_id,
        "store_domain": clean_shop,
        "scopes": scopes,
    })


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
