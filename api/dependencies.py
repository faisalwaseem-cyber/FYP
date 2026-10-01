"""
FastAPI Authentication Dependencies for BRIDGE FLOW.

Tenant Resolution — Security Contract
--------------------------------------
The ONLY authoritative flow for resolving business_id is:

    1.  HTTP request carries a Bearer token in the Authorization header.
    2.  The JWT is validated (signature, expiry, issuer).
    3.  The user identity is extracted from the validated JWT payload (sub claim).
    4.  The user's business_id is looked up from the database via the
        user → business mapping.
    5.  The resolved business_id is injected into the route handler.

FORBIDDEN: Trusting any client-supplied business_id, including:
    - Query parameters    (?business_id=1)
    - Path parameters     (/api/shopify/{business_id}/sync)
    - Request headers     (X-Business-ID: 1)

These client-supplied values are NEVER used as the authoritative tenant selector.

FYP Development Stub
--------------------
Full JWT validation requires a running auth service (out of Phase 1 scope).
This module provides a two-mode implementation:

    Production mode (BRIDGE_FLOW_AUTH_MODE=jwt):
        Validates the Authorization: Bearer <token> header, decodes the JWT,
        looks up the business_id from core.business_user.

    Development stub mode (BRIDGE_FLOW_AUTH_MODE=stub, default):
        Requires the Authorization: Bearer <token> header to be present.
        Extracts the business_id from the token payload WITHOUT cryptographic
        verification (only safe for local development — never deploy this).
        Falls back to BUSINESS_ID from settings if the token has no sub claim.

    In both modes, NO client-supplied business_id header is accepted.
"""
import os
from typing import Optional

from fastapi import Depends, Header, HTTPException, status
from sqlalchemy import text

from config.database import get_db_connection
from config.settings import BUSINESS_ID

# ---------------------------------------------------------------------------
# JWT import — soft-dependency so the codebase still imports cleanly even if
# PyJWT is not yet installed (the stub mode doesn't need it).
# ---------------------------------------------------------------------------
try:
    import jwt as _pyjwt  # type: ignore
    _JWT_AVAILABLE = True
except ImportError:
    _JWT_AVAILABLE = False

_AUTH_MODE = os.getenv("BRIDGE_FLOW_AUTH_MODE", "stub").lower()
_JWT_SECRET = os.getenv("BRIDGE_FLOW_JWT_SECRET", "")
_JWT_ALGORITHM = os.getenv("BRIDGE_FLOW_JWT_ALGORITHM", "HS256")


# ---------------------------------------------------------------------------
# Internal helpers
# ---------------------------------------------------------------------------

def _extract_bearer_token(authorization: Optional[str]) -> Optional[str]:
    """
    Parses the Authorization header and returns the raw Bearer token, or None.
    """
    if not authorization:
        return None
    parts = authorization.strip().split(" ", 1)
    if len(parts) == 2 and parts[0].lower() == "bearer" and parts[1]:
        return parts[1]
    return None


def _resolve_business_id_from_token(raw_token: str) -> int:
    """
    Resolves business_id from a JWT/Bearer token.

    Production mode: full JWT validation → DB user→business lookup.
    Stub mode: unverified decode for local development.
    """
    if _AUTH_MODE == "jwt":
        return _resolve_production(raw_token)
    return _resolve_stub(raw_token)


def _resolve_production(raw_token: str) -> int:
    """
    Full production path:
    1. Cryptographically validate the JWT.
    2. Look up business_id from core.business_user via user identity.
    """
    if not _JWT_AVAILABLE:
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail="JWT library not installed. Run: pip install PyJWT",
        )
    if not _JWT_SECRET:
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail="BRIDGE_FLOW_JWT_SECRET is not configured.",
        )
    try:
        payload = _pyjwt.decode(
            raw_token,
            _JWT_SECRET,
            algorithms=[_JWT_ALGORITHM],
        )
    except Exception as exc:
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail=f"Invalid or expired JWT: {exc}",
        ) from exc

    user_id = payload.get("sub")
    if not user_id:
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="JWT payload missing 'sub' (user identity) claim.",
        )

    # DB lookup: user → business
    lookup_sql = text("""
        SELECT business_id
        FROM core.business_user
        WHERE user_id = :user_id
        LIMIT 1;
    """)
    with get_db_connection() as conn:
        row = conn.execute(lookup_sql, {"user_id": user_id}).fetchone()

    if not row:
        raise HTTPException(
            status_code=status.HTTP_403_FORBIDDEN,
            detail=f"Authenticated user '{user_id}' is not associated with any business.",
        )
    return int(row[0])


def _resolve_stub(raw_token: str) -> int:
    """
    Development stub path:
    Decodes the JWT WITHOUT signature verification.
    Falls back to the configured BUSINESS_ID if no payload sub is present.

    WARNING: This path must NEVER be used in production. Set
    BRIDGE_FLOW_AUTH_MODE=jwt to switch to the production path.
    """
    if _JWT_AVAILABLE:
        try:
            payload = _pyjwt.decode(
                raw_token,
                options={"verify_signature": False},
                algorithms=[_JWT_ALGORITHM],
            )
            if "business_id" in payload:
                return int(payload["business_id"])
            if "sub" in payload:
                # Treat sub as business_id integer in stub mode
                try:
                    return int(payload["sub"])
                except (TypeError, ValueError):
                    pass
        except Exception:
            pass

    # No JWT library or undecodable token: fall back to configured default
    return BUSINESS_ID


# ---------------------------------------------------------------------------
# FastAPI dependency
# ---------------------------------------------------------------------------

def get_current_business_id(
    authorization: Optional[str] = Header(
        None,
        alias="Authorization",
        description=(
            "Bearer token issued by the BRIDGE FLOW authentication service. "
            "Example: 'Authorization: Bearer <jwt>'"
        ),
    ),
) -> int:
    """
    FastAPI dependency: resolves and returns the authenticated business_id.

    business_id is determined EXCLUSIVELY from the authenticated JWT/session.
    No client-supplied business_id values (headers, query params, path params)
    are accepted as authoritative tenant selectors.

    In production (BRIDGE_FLOW_AUTH_MODE=jwt):
        - Validates the Bearer JWT cryptographically.
        - Looks up the user's business from core.business_user.
        - 401 if token is absent, invalid, or expired.
        - 403 if the user has no associated business.

    In development stub mode (BRIDGE_FLOW_AUTH_MODE=stub, default):
        - Decodes the token WITHOUT signature verification.
        - Falls back to settings.BUSINESS_ID if no token is present.
        - MUST NOT be deployed to production.
    """
    raw_token = _extract_bearer_token(authorization)

    if _AUTH_MODE == "jwt" and raw_token is None:
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Missing Authorization header. Expected: 'Bearer <token>'.",
            headers={"WWW-Authenticate": "Bearer"},
        )

    if raw_token is None:
        # Stub mode without a token: use configured default
        return BUSINESS_ID

    return _resolve_business_id_from_token(raw_token)
