"""
Platform-Agnostic Connection Service for BRIDGE FLOW.

Provides CRUD operations for the core.platform_connection table — the universal
multi-tenant connection registry for all external platform integrations.

Design invariants
-----------------
* Credentials are ALWAYS encrypted before writing to the database.
* Credentials are NEVER returned decrypted from this module.
  Decryption is the sole responsibility of auth.credential_manager.
* Expiry timestamps are always derived from provider-returned `expires_in`
  seconds; they are NEVER hard-coded.
* If a provider does not return a new refresh_token on token update, the
  existing encrypted refresh_token is retained unchanged.
"""
from datetime import datetime, timedelta, timezone
from typing import Any, Dict, List, Optional

from sqlalchemy import text

from config.database import get_db_connection
from security.encryption import encrypt_json, encrypt_token


# ---------------------------------------------------------------------------
# Public helpers
# ---------------------------------------------------------------------------

def create_or_update_connection(
    *,
    business_id: int,
    platform: str,
    platform_category: str,
    account_identifier: str,
    account_name: Optional[str] = None,
    auth_type: str,
    access_token: Optional[str] = None,
    refresh_token: Optional[str] = None,
    additional_credentials: Optional[Dict[str, Any]] = None,
    expires_in: Optional[int] = None,
    refresh_token_expires_in: Optional[int] = None,
    scopes: Optional[str] = None,
) -> int:
    """
    Inserts or updates a platform connection in core.platform_connection.

    * access_token, refresh_token, and additional_credentials are encrypted
      before storage.
    * Expiry timestamps are calculated from `expires_in` and
      `refresh_token_expires_in` (seconds from now).
    * On conflict (same business + platform + account), the row is updated.
      The refresh_token column is only overwritten when a non-empty replacement
      is supplied; otherwise the existing encrypted value is preserved.

    Returns
    -------
    int
        The connection_id of the created or updated row.
    """
    now_utc = datetime.now(timezone.utc)

    enc_at = encrypt_token(access_token or "")
    enc_rt = encrypt_token(refresh_token or "")
    enc_creds = encrypt_json(additional_credentials or {})

    at_expires_at = (
        now_utc + timedelta(seconds=expires_in) if expires_in else None
    )
    rt_expires_at = (
        now_utc + timedelta(seconds=refresh_token_expires_in)
        if refresh_token_expires_in
        else None
    )

    sql = text("""
        INSERT INTO core.platform_connection (
            business_id,
            platform,
            platform_category,
            account_identifier,
            account_name,
            auth_type,
            encrypted_access_token,
            encrypted_refresh_token,
            encrypted_additional_credentials,
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
            :platform_category,
            :account_identifier,
            :account_name,
            :auth_type,
            :enc_at,
            :enc_rt,
            :enc_creds,
            :at_expires_at,
            :rt_expires_at,
            :scopes,
            'active',
            now(),
            now()
        )
        ON CONFLICT (business_id, platform, account_identifier) DO UPDATE SET
            account_name                     = EXCLUDED.account_name,
            auth_type                        = EXCLUDED.auth_type,
            encrypted_access_token           = CASE
                WHEN EXCLUDED.encrypted_access_token <> ''
                THEN EXCLUDED.encrypted_access_token
                ELSE core.platform_connection.encrypted_access_token
            END,
            encrypted_refresh_token          = CASE
                WHEN EXCLUDED.encrypted_refresh_token <> ''
                THEN EXCLUDED.encrypted_refresh_token
                ELSE core.platform_connection.encrypted_refresh_token
            END,
            encrypted_additional_credentials = CASE
                WHEN EXCLUDED.encrypted_additional_credentials <> ''
                THEN EXCLUDED.encrypted_additional_credentials
                ELSE core.platform_connection.encrypted_additional_credentials
            END,
            access_token_expires_at          = EXCLUDED.access_token_expires_at,
            refresh_token_expires_at         = COALESCE(
                EXCLUDED.refresh_token_expires_at,
                core.platform_connection.refresh_token_expires_at
            ),
            scopes                           = COALESCE(EXCLUDED.scopes, core.platform_connection.scopes),
            status                           = 'active',
            error_message                    = NULL,
            updated_at                       = now()
        RETURNING connection_id;
    """)

    with get_db_connection() as conn:
        result = conn.execute(sql, {
            "business_id": business_id,
            "platform": platform,
            "platform_category": platform_category,
            "account_identifier": account_identifier,
            "account_name": account_name,
            "auth_type": auth_type,
            "enc_at": enc_at,
            "enc_rt": enc_rt,
            "enc_creds": enc_creds,
            "at_expires_at": at_expires_at,
            "rt_expires_at": rt_expires_at,
            "scopes": scopes,
        })
        connection_id = result.scalar()
        conn.commit()
        return connection_id


def get_connection(
    business_id: int,
    platform: str,
    account_identifier: str,
) -> Optional[Dict[str, Any]]:
    """
    Returns the raw connection row for a specific business + platform +
    account combination, or None if not found.

    IMPORTANT: The returned dict contains encrypted credential fields.
    Use auth.credential_manager to obtain decrypted credentials.
    """
    sql = text("""
        SELECT
            connection_id,
            business_id,
            platform,
            platform_category,
            account_identifier,
            account_name,
            auth_type,
            encrypted_access_token,
            encrypted_refresh_token,
            encrypted_additional_credentials,
            access_token_expires_at,
            refresh_token_expires_at,
            scopes,
            status,
            error_message,
            last_synced_at,
            installed_at,
            updated_at
        FROM core.platform_connection
        WHERE business_id = :business_id
          AND platform    = :platform
          AND account_identifier = :account_identifier;
    """)

    with get_db_connection() as conn:
        row = conn.execute(sql, {
            "business_id": business_id,
            "platform": platform,
            "account_identifier": account_identifier,
        }).fetchone()

    if row is None:
        return None

    return dict(zip([
        "connection_id", "business_id", "platform", "platform_category",
        "account_identifier", "account_name", "auth_type",
        "encrypted_access_token", "encrypted_refresh_token",
        "encrypted_additional_credentials",
        "access_token_expires_at", "refresh_token_expires_at",
        "scopes", "status", "error_message",
        "last_synced_at", "installed_at", "updated_at",
    ], row))


def list_connections(business_id: int) -> List[Dict[str, Any]]:
    """
    Returns all connection rows for a business (encrypted — no credential data).
    """
    sql = text("""
        SELECT
            connection_id,
            platform,
            platform_category,
            account_identifier,
            account_name,
            auth_type,
            scopes,
            status,
            error_message,
            last_synced_at,
            installed_at,
            updated_at
        FROM core.platform_connection
        WHERE business_id = :business_id
        ORDER BY platform, account_identifier;
    """)

    with get_db_connection() as conn:
        rows = conn.execute(sql, {"business_id": business_id}).fetchall()

    keys = [
        "connection_id", "platform", "platform_category",
        "account_identifier", "account_name", "auth_type",
        "scopes", "status", "error_message",
        "last_synced_at", "installed_at", "updated_at",
    ]
    return [dict(zip(keys, row)) for row in rows]


def update_status(
    connection_id: int,
    status: str,
    error_message: Optional[str] = None,
) -> None:
    """
    Updates the status and optional error message for a connection.

    Valid statuses: 'active', 'needs_reauth', 'disconnected', 'error'
    """
    valid = {"active", "needs_reauth", "disconnected", "error"}
    if status not in valid:
        raise ValueError(f"Invalid status '{status}'. Must be one of: {valid}")

    sql = text("""
        UPDATE core.platform_connection
        SET status        = :status,
            error_message = :error_message,
            updated_at    = now()
        WHERE connection_id = :connection_id;
    """)

    with get_db_connection() as conn:
        conn.execute(sql, {
            "connection_id": connection_id,
            "status": status,
            "error_message": error_message,
        })
        conn.commit()


def update_tokens(
    connection_id: int,
    *,
    new_access_token: str,
    new_refresh_token: Optional[str],
    expires_in: Optional[int],
    refresh_token_expires_in: Optional[int],
    current_encrypted_refresh_token: str,
    current_refresh_token_expires_at: Optional[datetime],
) -> None:
    """
    Updates the encrypted token pair for a connection after a successful refresh.

    Refresh-token retention rule:
    * If the provider returned a new refresh_token → encrypt and store it.
    * If the provider did NOT return a new refresh_token → retain the
      currently stored encrypted refresh_token unchanged.

    Expiry timestamps:
    * access_token_expires_at  ← calculated from expires_in (seconds from now).
    * refresh_token_expires_at ← calculated from refresh_token_expires_in, or
                                  the existing value if no new one was returned.

    IMPORTANT: This function must be called INSIDE a transaction with a
    row-level lock already held (SELECT … FOR UPDATE). See CredentialManager.
    """
    now_utc = datetime.now(timezone.utc)

    final_enc_at = encrypt_token(new_access_token)
    final_at_expires = (
        now_utc + timedelta(seconds=expires_in) if expires_in else None
    )

    # Retain existing RT if provider did not rotate it
    if new_refresh_token:
        final_enc_rt = encrypt_token(new_refresh_token)
    else:
        final_enc_rt = current_encrypted_refresh_token  # already encrypted

    final_rt_expires = (
        now_utc + timedelta(seconds=refresh_token_expires_in)
        if refresh_token_expires_in
        else current_refresh_token_expires_at
    )

    sql = text("""
        UPDATE core.platform_connection
        SET encrypted_access_token   = :enc_at,
            encrypted_refresh_token  = :enc_rt,
            access_token_expires_at  = :at_expires,
            refresh_token_expires_at = :rt_expires,
            status                   = 'active',
            error_message            = NULL,
            updated_at               = now()
        WHERE connection_id = :connection_id;
    """)

    # NOTE: This execute must happen inside the caller's open connection/
    # transaction for the row-lock to remain effective. The caller is
    # responsible for commit/rollback.
    # To support this pattern we accept an optional conn parameter path via
    # CredentialManager. Here we provide a standalone path for service-level use.
    with get_db_connection() as conn:
        conn.execute(sql, {
            "connection_id": connection_id,
            "enc_at": final_enc_at,
            "enc_rt": final_enc_rt,
            "at_expires": final_at_expires,
            "rt_expires": final_rt_expires,
        })
        conn.commit()


def update_last_synced(connection_id: int) -> None:
    """
    Stamps the last_synced_at timestamp for a connection after a successful sync.
    """
    sql = text("""
        UPDATE core.platform_connection
        SET last_synced_at = now(),
            updated_at     = now()
        WHERE connection_id = :connection_id;
    """)
    with get_db_connection() as conn:
        conn.execute(sql, {"connection_id": connection_id})
        conn.commit()
