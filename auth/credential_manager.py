"""
Unified Credential Manager for BRIDGE FLOW.

This is the single authoritative entry point for any backend pipeline or service
that needs to access decrypted platform credentials at runtime.

Design invariants
-----------------
* Credentials are decrypted ONLY inside this module and returned ONLY to
  trusted backend callers (never to the frontend or external clients).
* Concurrent token-refresh is protected with PostgreSQL SELECT … FOR UPDATE
  row-level locking so that two simultaneous sync workers never create a
  duplicate refresh race.
* If a provider network call times out ambiguously, the transaction is rolled
  back and the original credentials are left intact (no credential overwrite
  on ambiguous failure).
* If the provider does not return a new refresh_token, the existing one is
  retained.  The caller injects the platform-specific refresh function; static-
  key platforms (API key, Daraz credentials) never trigger a refresh.

Typical usage
-------------
    from auth.credential_manager import CredentialManager

    mgr = CredentialManager()
    creds = mgr.get_valid_credentials(
        business_id=42,
        platform="shopify",
        account_identifier="my-store.myshopify.com",
        refresh_callback=shopify_refresh_fn,   # optional
        safety_buffer_seconds=300,
    )
    access_token = creds["access_token"]
"""
from datetime import datetime, timedelta, timezone
from typing import Any, Callable, Dict, Optional

from sqlalchemy import text

from config.database import get_db_connection
from security.encryption import decrypt_json, decrypt_token, encrypt_token


class TokenRevokedError(RuntimeError):
    """Raised when the provider explicitly rejects the refresh token as invalid or revoked."""
    pass


def is_revocation_error(exc: Exception) -> bool:
    """
    Determines whether an exception represents an explicit token revocation or invalid grant,
    as opposed to a transient network timeout or 5xx server error.
    """
    if isinstance(exc, TokenRevokedError):
        return True
    exc_str = str(exc).lower()
    if any(k in exc_str for k in ("invalid_grant", "token revoked", "revoked", "invalid_token", "unauthorized_client")):
        return True
    response = getattr(exc, "response", None)
    if response is not None:
        status_code = getattr(response, "status_code", None)
        if status_code in (400, 401):
            text = (getattr(response, "text", "") or "").lower()
            if any(k in text for k in ("invalid_grant", "revoked", "invalid_token", "unauthorized_client")):
                return True
    return False


class CredentialManager:
    """
    Manages per-tenant, per-platform credential retrieval with automatic
    token refresh and concurrency safety.
    """

    # ------------------------------------------------------------------
    # Public API
    # ------------------------------------------------------------------

    def get_valid_credentials(
        self,
        *,
        business_id: int,
        platform: str,
        account_identifier: str,
        refresh_callback: Optional[
            Callable[[Dict[str, Any]], Dict[str, Any]]
        ] = None,
        safety_buffer_seconds: int = 300,
    ) -> Dict[str, Any]:
        """
        Returns a dict of **decrypted** credentials for the specified connection.

        For OAuth2 platforms with expiring tokens the method checks whether the
        access token will expire within `safety_buffer_seconds` and, if so,
        calls `refresh_callback` to obtain a new token pair before returning.

        Parameters
        ----------
        business_id : int
            Server-resolved tenant identifier.
        platform : str
            Platform name, e.g. "shopify", "meta", "daraz".
        account_identifier : str
            Platform-specific unique account key (e.g. Shopify store domain,
            Daraz seller ID, Meta ad account ID).
        refresh_callback : callable, optional
            A function that accepts the current decrypted credentials dict and
            returns a dict with the refreshed token data from the provider:

                {
                    "access_token": "...",
                    "expires_in": 3600,          # optional
                    "refresh_token": "...",       # optional (retain old if absent)
                    "refresh_token_expires_in": 7776000,  # optional
                }

            If None and the token is expired, a ValueError is raised.
            Static-key platforms should pass None (no refresh needed).
        safety_buffer_seconds : int
            Refresh the access token this many seconds before it actually expires.
            Default: 300 (5 minutes).

        Returns
        -------
        dict
            Decrypted credentials. Guaranteed keys depend on auth_type:
            - oauth2         → "access_token", optionally "refresh_token"
            - api_key        → "access_token"
            - token          → "access_token"
            - credentials    → "additional_credentials" (dict)

        Raises
        ------
        ValueError
            If no connection exists, status is not 'active', or token is
            expired and no refresh_callback is provided.
        RuntimeError
            If the token refresh network call fails.
        """
        with get_db_connection() as conn:
            return self._resolve_credentials(
                conn=conn,
                business_id=business_id,
                platform=platform,
                account_identifier=account_identifier,
                refresh_callback=refresh_callback,
                safety_buffer_seconds=safety_buffer_seconds,
            )

    # ------------------------------------------------------------------
    # Internal implementation
    # ------------------------------------------------------------------

    def _resolve_credentials(
        self,
        conn: Any,
        *,
        business_id: int,
        platform: str,
        account_identifier: str,
        refresh_callback: Optional[Callable],
        safety_buffer_seconds: int,
    ) -> Dict[str, Any]:
        """
        Core credential resolution logic executed inside a single DB connection.
        Uses SELECT … FOR UPDATE to prevent concurrent refresh races.
        """
        row = conn.execute(
            text("""
                SELECT
                    connection_id,
                    auth_type,
                    encrypted_access_token,
                    encrypted_refresh_token,
                    encrypted_additional_credentials,
                    access_token_expires_at,
                    refresh_token_expires_at,
                    status
                FROM core.platform_connection
                WHERE business_id        = :business_id
                  AND platform           = :platform
                  AND account_identifier = :account_identifier
                FOR UPDATE;
            """),
            {
                "business_id": business_id,
                "platform": platform,
                "account_identifier": account_identifier,
            },
        ).fetchone()

        if row is None:
            raise ValueError(
                f"No connection found for business_id={business_id}, "
                f"platform='{platform}', account='{account_identifier}'. "
                "Please connect this platform first."
            )

        (
            connection_id,
            auth_type,
            enc_at,
            enc_rt,
            enc_creds,
            at_expires_at,
            rt_expires_at,
            status,
        ) = row

        if status != "active":
            raise ValueError(
                f"Connection {connection_id} for business_id={business_id}, "
                f"platform='{platform}' is '{status}'. "
                "Re-authentication required."
            )

        # Decrypt what we have
        access_token = decrypt_token(enc_at)
        refresh_token = decrypt_token(enc_rt)
        additional_credentials = decrypt_json(enc_creds)

        # Static-key platforms: return immediately without expiry check
        if auth_type in ("api_key", "credentials"):
            conn.commit()
            return self._build_result(
                auth_type=auth_type,
                access_token=access_token,
                refresh_token=refresh_token,
                additional_credentials=additional_credentials,
            )

        # OAuth2 / token: check expiry
        now_utc = datetime.now(timezone.utc)
        needs_refresh = False
        if at_expires_at is not None:
            threshold = now_utc + timedelta(seconds=safety_buffer_seconds)
            if at_expires_at <= threshold:
                needs_refresh = True

        if not needs_refresh:
            conn.commit()
            return self._build_result(
                auth_type=auth_type,
                access_token=access_token,
                refresh_token=refresh_token,
                additional_credentials=additional_credentials,
            )

        # Token needs refresh
        if refresh_callback is None:
            raise ValueError(
                f"Access token for connection {connection_id} "
                f"(business_id={business_id}, platform='{platform}') "
                "is expired and no refresh_callback was provided."
            )

        # Perform refresh — rollback on ambiguous network failure
        try:
            current_creds = self._build_result(
                auth_type=auth_type,
                access_token=access_token,
                refresh_token=refresh_token,
                additional_credentials=additional_credentials,
            )
            refresh_data = refresh_callback(current_creds)
            if not isinstance(refresh_data, dict) or not refresh_data.get("access_token"):
                raise ValueError("Refresh callback returned malformed response or missing 'access_token'.")
        except Exception as exc:
            if is_revocation_error(exc):
                # Permanent rejection: update connection status to 'needs_reauth'
                conn.rollback()
                resp_text = getattr(getattr(exc, "response", None), "text", "")
                err_detail = f"{exc} - {resp_text}" if resp_text else str(exc)
                conn.execute(
                    text("""
                        UPDATE core.platform_connection
                        SET status        = 'needs_reauth',
                            error_message = :err,
                            updated_at    = now()
                        WHERE connection_id = :connection_id;
                    """),
                    {
                        "connection_id": connection_id,
                        "err": f"Refresh token revoked or invalid: {err_detail}",
                    },
                )
                conn.commit()
                raise TokenRevokedError(
                    f"Refresh token revoked or rejected by provider for connection {connection_id} "
                    f"(business_id={business_id}, platform='{platform}'): {err_detail}. "
                    "Connection marked 'needs_reauth'."
                ) from exc

            # Ambiguous/transient failure (network timeout, 5xx): roll back, leave stored credentials intact
            conn.rollback()
            raise RuntimeError(
                f"Token refresh failed for connection {connection_id} "
                f"(business_id={business_id}, platform='{platform}'). "
                "Stored credentials have been preserved. "
                f"Underlying error: {exc}"
            ) from exc

        new_access_token = refresh_data["access_token"]
        new_expires_in = refresh_data.get("expires_in")
        new_refresh_token = refresh_data.get("refresh_token")
        new_rt_expires_in = refresh_data.get("refresh_token_expires_in")

        # Compute new expiry timestamps from provider-returned durations
        new_at_expires = (
            now_utc + timedelta(seconds=new_expires_in) if new_expires_in else None
        )
        new_rt_expires = (
            now_utc + timedelta(seconds=new_rt_expires_in)
            if new_rt_expires_in
            else rt_expires_at  # retain existing
        )

        # Retention rule: keep existing encrypted RT if provider did not rotate it
        if new_refresh_token:
            final_enc_rt = encrypt_token(new_refresh_token)
        else:
            final_enc_rt = enc_rt  # already encrypted, no change

        final_enc_at = encrypt_token(new_access_token)

        conn.execute(
            text("""
                UPDATE core.platform_connection
                SET encrypted_access_token   = :enc_at,
                    encrypted_refresh_token  = :enc_rt,
                    access_token_expires_at  = :at_expires,
                    refresh_token_expires_at = :rt_expires,
                    status                   = 'active',
                    error_message            = NULL,
                    updated_at               = now()
                WHERE connection_id = :connection_id;
            """),
            {
                "connection_id": connection_id,
                "enc_at": final_enc_at,
                "enc_rt": final_enc_rt,
                "at_expires": new_at_expires,
                "rt_expires": new_rt_expires,
            },
        )
        conn.commit()

        return self._build_result(
            auth_type=auth_type,
            access_token=new_access_token,
            refresh_token=new_refresh_token or refresh_token,
            additional_credentials=additional_credentials,
        )

    @staticmethod
    def _build_result(
        *,
        auth_type: str,
        access_token: str,
        refresh_token: str,
        additional_credentials: Dict[str, Any],
    ) -> Dict[str, Any]:
        """
        Builds the decrypted credential dict returned to the caller.
        Never includes raw encrypted values.
        """
        result: Dict[str, Any] = {"auth_type": auth_type}
        if access_token:
            result["access_token"] = access_token
        if refresh_token:
            result["refresh_token"] = refresh_token
        if additional_credentials:
            result["additional_credentials"] = additional_credentials
        return result
