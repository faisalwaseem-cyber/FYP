from .shopify_oauth import (
    sanitize_shop_domain,
    generate_signed_oauth_state,
    verify_oauth_state,
    build_authorization_url,
    verify_shopify_hmac,
    exchange_authorization_code,
)
from .token_manager import (
    save_initial_store_connection,
    get_valid_shopify_access_token,
    refresh_shopify_token_network_call,
)
from .connection_service import (
    create_or_update_connection,
    get_connection,
    list_connections,
    update_status,
    update_tokens,
    update_last_synced,
)
from .credential_manager import CredentialManager

__all__ = [
    # Shopify OAuth (Phase 2 wiring)
    "sanitize_shop_domain",
    "generate_signed_oauth_state",
    "verify_oauth_state",
    "build_authorization_url",
    "verify_shopify_hmac",
    "exchange_authorization_code",
    # Legacy store_connection helpers (kept for existing tests)
    "save_initial_store_connection",
    "get_valid_shopify_access_token",
    "refresh_shopify_token_network_call",
    # Phase 1: generic connection infrastructure
    "create_or_update_connection",
    "get_connection",
    "list_connections",
    "update_status",
    "update_tokens",
    "update_last_synced",
    "CredentialManager",
]
