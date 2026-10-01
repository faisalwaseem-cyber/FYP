"""
Multi-Tenant Token Storage and Refresh Manager for BRIDGE FLOW.

Handles:
1. Secure initial storage of expiring offline access tokens and refresh tokens.
2. Authenticated symmetric encryption at rest using Fernet.
3. Concurrency-safe token refresh with PostgreSQL row-level locking (SELECT ... FOR UPDATE).
4. Retaining existing refresh tokens if Shopify does not rotate on refresh.
5. Providing valid, decrypted access tokens to the ingestion pipeline.
"""
from datetime import datetime, timedelta, timezone
from typing import Dict, Any, Optional, Tuple

import requests
from sqlalchemy import text

from config.database import get_db_connection
from config.settings import (
    SHOPIFY_CLIENT_ID,
    SHOPIFY_CLIENT_SECRET,
    SHOPIFY_API_VERSION,
)
from security.encryption import encrypt_token, decrypt_token


def save_initial_store_connection(
    business_id: int,
    store_domain: str,
    access_token: str,
    scopes: str,
    expires_in: Optional[int] = None,
    refresh_token: Optional[str] = None,
    refresh_token_expires_in: Optional[int] = None,
    platform: str = "shopify",
) -> int:
    """
    Saves or updates a tenant's store connection in core.store_connection.
    Encrypts access_token and refresh_token at rest using Fernet.
    Returns connection_id.
    """
    now_utc = datetime.now(timezone.utc)
    access_expires_at = (
        now_utc + timedelta(seconds=expires_in) if expires_in else None
    )
    refresh_expires_at = (
        now_utc + timedelta(seconds=refresh_token_expires_in)
        if refresh_token_expires_in
        else None
    )

    encrypted_at = encrypt_token(access_token)
    encrypted_rt = encrypt_token(refresh_token or "")

    upsert_sql = text("""
        INSERT INTO core.store_connection (
            business_id,
            platform,
            store_domain,
            access_token,
            refresh_token,
            access_token_expires_at,
            refresh_token_expires_at,
            scopes,
            status,
            installed_at,
            updated_at
        )
        VALUES (
            :business_id,
            :platform,
            :store_domain,
            :access_token,
            :refresh_token,
            :access_token_expires_at,
            :refresh_token_expires_at,
            :scopes,
            'active',
            now(),
            now()
        )
        ON CONFLICT (business_id, platform) DO UPDATE SET
            store_domain = EXCLUDED.store_domain,
            access_token = EXCLUDED.access_token,
            refresh_token = CASE
                WHEN EXCLUDED.refresh_token <> '' THEN EXCLUDED.refresh_token
                ELSE core.store_connection.refresh_token
            END,
            access_token_expires_at = EXCLUDED.access_token_expires_at,
            refresh_token_expires_at = COALESCE(
                EXCLUDED.refresh_token_expires_at,
                core.store_connection.refresh_token_expires_at
            ),
            scopes = EXCLUDED.scopes,
            status = 'active',
            updated_at = now()
        RETURNING connection_id;
    """)

    with get_db_connection() as conn:
        result = conn.execute(upsert_sql, {
            "business_id": business_id,
            "platform": platform,
            "store_domain": store_domain,
            "access_token": encrypted_at,
            "refresh_token": encrypted_rt,
            "access_token_expires_at": access_expires_at,
            "refresh_token_expires_at": refresh_expires_at,
            "scopes": scopes,
        })
        conn_id = result.scalar()
        conn.commit()
        return conn_id


def refresh_shopify_token_network_call(
    store_domain: str,
    decrypted_refresh_token: str,
    client_id: Optional[str] = None,
    client_secret: Optional[str] = None,
) -> Dict[str, Any]:
    """
    Performs server-to-server POST to exchange an expiring offline refresh token
    for a new access token and rotated refresh token.
    """
    cid = client_id or SHOPIFY_CLIENT_ID
    secret = client_secret or SHOPIFY_CLIENT_SECRET
    if not cid or not secret:
        raise ValueError("SHOPIFY_CLIENT_ID and SHOPIFY_CLIENT_SECRET must be configured.")

    token_url = f"https://{store_domain}/admin/oauth/access_token"
    payload = {
        "client_id": cid,
        "client_secret": secret,
        "grant_type": "refresh_token",
        "refresh_token": decrypted_refresh_token,
    }

    response = requests.post(
        token_url,
        json=payload,
        headers={"Content-Type": "application/json", "Accept": "application/json"},
        timeout=30,
    )
    response.raise_for_status()
    return response.json()


def get_valid_shopify_access_token(
    business_id: int,
    safety_buffer_minutes: int = 5,
) -> Tuple[str, str]:
    """
    Retrieves a valid, decrypted Shopify access token for a business.
    
    Concurrency Protection:
    Uses PostgreSQL row-level locking (SELECT ... FOR UPDATE) to ensure that if
    two background workers attempt to sync at the same time, only one performs the
    token refresh, and the other safely receives the refreshed token without race conditions.

    Returns:
        (store_domain, decrypted_access_token)
    """
    select_lock_sql = text("""
        SELECT
            connection_id,
            store_domain,
            access_token,
            refresh_token,
            access_token_expires_at,
            refresh_token_expires_at,
            status
        FROM core.store_connection
        WHERE business_id = :business_id AND platform = 'shopify'
        FOR UPDATE;
    """)

    update_tokens_sql = text("""
        UPDATE core.store_connection
        SET access_token = :access_token,
            refresh_token = :refresh_token,
            access_token_expires_at = :access_token_expires_at,
            refresh_token_expires_at = :refresh_token_expires_at,
            updated_at = now()
        WHERE connection_id = :connection_id;
    """)

    with get_db_connection() as conn:
        row = conn.execute(select_lock_sql, {"business_id": business_id}).fetchone()
        if not row:
            raise ValueError(
                f"No Shopify store connection found for business_id={business_id}. "
                f"Please connect a Shopify store first."
            )

        conn_id = row[0]
        store_domain = row[1]
        enc_access_token = row[2]
        enc_refresh_token = row[3]
        access_expires_at = row[4]
        refresh_expires_at = row[5]
        status = row[6]

        if status != "active":
            raise ValueError(
                f"Shopify connection for business_id={business_id} ({store_domain}) is '{status}'."
            )

        now_utc = datetime.now(timezone.utc)
        needs_refresh = False

        # Check if access token is expired or within the safety buffer window
        if access_expires_at is not None:
            threshold = now_utc + timedelta(minutes=safety_buffer_minutes)
            if access_expires_at <= threshold:
                needs_refresh = True

        if not needs_refresh:
            # Token is valid and healthy
            conn.commit()
            return store_domain, decrypt_token(enc_access_token)

        # Token needs refresh
        if not enc_refresh_token:
            # If no refresh token exists, fall back to current access token
            conn.commit()
            return store_domain, decrypt_token(enc_access_token)

        # Decrypt refresh token
        decrypted_rt = decrypt_token(enc_refresh_token)

        # Perform token refresh call
        refresh_data = refresh_shopify_token_network_call(store_domain, decrypted_rt)

        new_access_token = refresh_data["access_token"]
        new_expires_in = refresh_data.get("expires_in")
        new_refresh_token = refresh_data.get("refresh_token")
        new_rt_expires_in = refresh_data.get("refresh_token_expires_in")

        # Technical Rule 1:
        # If Shopify returns a new refresh_token, encrypt and store it.
        # If Shopify does NOT return a new refresh_token, retain the existing encrypted one.
        if new_refresh_token:
            final_enc_rt = encrypt_token(new_refresh_token)
        else:
            final_enc_rt = enc_refresh_token

        final_enc_at = encrypt_token(new_access_token)
        final_at_expires_at = (
            now_utc + timedelta(seconds=new_expires_in) if new_expires_in else None
        )
        final_rt_expires_at = (
            now_utc + timedelta(seconds=new_rt_expires_in)
            if new_rt_expires_in
            else refresh_expires_at
        )

        conn.execute(update_tokens_sql, {
            "connection_id": conn_id,
            "access_token": final_enc_at,
            "refresh_token": final_enc_rt,
            "access_token_expires_at": final_at_expires_at,
            "refresh_token_expires_at": final_rt_expires_at,
        })
        conn.commit()

        return store_domain, new_access_token
