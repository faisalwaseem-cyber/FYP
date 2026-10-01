"""
Cryptographic Security Layer for BRIDGE FLOW.

Provides authenticated symmetric encryption for sensitive credentials at rest
(access tokens, refresh tokens, and multi-part API credential blobs) using
Python's cryptography.fernet.

Fernet guarantees:
- Confidentiality: 128-bit AES in CBC mode with PKCS7 padding.
- Integrity & Authenticity: HMAC using SHA-256 to prevent message tampering.
- Key: 256-bit URL-safe base64-encoded key held strictly in server environment.

Public API
----------
encrypt_token(plaintext)          -> ciphertext str   (single token/key)
decrypt_token(ciphertext)         -> plaintext str

encrypt_json(data: dict)          -> ciphertext str   (multi-part credentials)
decrypt_json(ciphertext)          -> dict
"""
import json
import os
from typing import Any, Dict, Optional

from cryptography.fernet import Fernet, InvalidToken

from config.settings import STORE_TOKEN_ENCRYPTION_KEY


_cached_fernet: Optional[Fernet] = None


def get_fernet_instance(key: Optional[str] = None) -> Fernet:
    """
    Returns a singleton Fernet instance initialized with the configured key.
    Pass a custom *key* only in unit tests; production code should omit it.
    """
    global _cached_fernet
    active_key = key or STORE_TOKEN_ENCRYPTION_KEY
    if not active_key:
        raise ValueError(
            "STORE_TOKEN_ENCRYPTION_KEY is not configured in .env. "
            "Generate one with: "
            "python -c 'from cryptography.fernet import Fernet; print(Fernet.generate_key().decode())'"
        )

    if key is not None:
        # Custom key passed for testing — always create a fresh instance.
        raw = active_key.encode() if isinstance(active_key, str) else active_key
        return Fernet(raw)

    if _cached_fernet is None:
        raw = active_key.encode() if isinstance(active_key, str) else active_key
        _cached_fernet = Fernet(raw)
    return _cached_fernet


# ---------------------------------------------------------------------------
# Single-token helpers (access_token / refresh_token columns)
# ---------------------------------------------------------------------------

def encrypt_token(plaintext: str, key: Optional[str] = None) -> str:
    """
    Encrypts a plaintext token string and returns an ASCII ciphertext string.
    Returns an empty string unchanged (no-op for optional fields).
    """
    if not plaintext:
        return ""
    f = get_fernet_instance(key)
    return f.encrypt(plaintext.encode("utf-8")).decode("utf-8")


def decrypt_token(ciphertext: str, key: Optional[str] = None) -> str:
    """
    Decrypts an ASCII ciphertext string and returns the original plaintext.
    Returns an empty string unchanged for optional fields.
    Raises ValueError if the ciphertext has been tampered with or the key is wrong.
    """
    if not ciphertext:
        return ""
    f = get_fernet_instance(key)
    try:
        return f.decrypt(ciphertext.encode("utf-8")).decode("utf-8")
    except InvalidToken as exc:
        raise ValueError(
            "Failed to decrypt token: invalid ciphertext or mismatched key."
        ) from exc


# ---------------------------------------------------------------------------
# JSON credential-blob helpers (encrypted_additional_credentials column)
# Used for multi-part credentials such as Daraz app_key + app_secret.
# ---------------------------------------------------------------------------

def encrypt_json(data: Dict[str, Any], key: Optional[str] = None) -> str:
    """
    Serialises *data* to compact JSON, then Fernet-encrypts it.
    Returns an ASCII ciphertext string safe for TEXT column storage.

    Usage:
        ciphertext = encrypt_json({"app_key": "...", "app_secret": "..."})
    """
    if not data:
        return ""
    json_bytes = json.dumps(data, separators=(",", ":")).encode("utf-8")
    f = get_fernet_instance(key)
    return f.encrypt(json_bytes).decode("utf-8")


def decrypt_json(ciphertext: str, key: Optional[str] = None) -> Dict[str, Any]:
    """
    Fernet-decrypts *ciphertext* and deserialises the JSON payload.
    Returns an empty dict for empty/None input.
    Raises ValueError if the ciphertext is invalid or the key is wrong.

    Usage:
        creds = decrypt_json(row.encrypted_additional_credentials)
        app_key = creds["app_key"]
    """
    if not ciphertext:
        return {}
    f = get_fernet_instance(key)
    try:
        plaintext_bytes = f.decrypt(ciphertext.encode("utf-8"))
        return json.loads(plaintext_bytes.decode("utf-8"))
    except InvalidToken as exc:
        raise ValueError(
            "Failed to decrypt credentials blob: invalid ciphertext or mismatched key."
        ) from exc
    except json.JSONDecodeError as exc:
        raise ValueError(
            "Decrypted credentials blob is not valid JSON."
        ) from exc
