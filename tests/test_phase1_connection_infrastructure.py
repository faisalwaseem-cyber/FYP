"""
Phase 1 Test Suite — Common Multi-Tenant Connection Infrastructure

Tests cover:
    T01  Encryption / decryption round-trip (token)
    T02  Tamper detection (token)
    T03  JSON credential blob encryption / decryption round-trip
    T04  Tamper detection (JSON blob)
    T05  Encrypted-at-rest storage (plaintext must not appear in DB)
    T06  Cross-tenant isolation (Business A cannot read Business B's credentials)
    T07  Unauthorized tenant returns no connection (unknown business_id)
    T08  Dynamic token expiry derived from provider expires_in
    T09  Connection CRUD (create → read → list → status update → last_synced)
    T10  Connection status transitions (active → error → needs_reauth → disconnected → active)
    T11  CredentialManager: valid token returned without refresh
    T12  CredentialManager: expired token triggers refresh_callback
    T13  CredentialManager: retain existing RT when provider does not return new one
    T14  CredentialManager: rollback on ambiguous refresh network failure
    T15  CredentialManager: concurrent credential access (sequential simulation)
    T16  CredentialManager: static API-key platform — no refresh triggered

All tests are fully self-contained and clean up after themselves.
Requires a live PostgreSQL 18 connection to ecommerce_intelligence.
"""
import sys
import json
import time
import traceback
from datetime import datetime, timedelta, timezone
from typing import Any, Dict, Optional
from unittest.mock import patch

# Ensure project root is on path when run directly
sys.path.insert(0, r"d:\FYP\FYP_CODE\Data_Base")

from cryptography.fernet import Fernet

from security.encryption import (
    decrypt_json,
    decrypt_token,
    encrypt_json,
    encrypt_token,
)
from auth.connection_service import (
    create_or_update_connection,
    get_connection,
    list_connections,
    update_last_synced,
    update_status,
)
from auth.credential_manager import CredentialManager
from config.database import get_db_connection
from sqlalchemy import text


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

PASS = "PASS"
FAIL = "FAIL"

_results: list[tuple[str, str, str]] = []  # (test_id, label, status)

TEST_KEY = Fernet.generate_key().decode()  # isolated key for every run

# Use business_id = 1 (guaranteed to exist in the test DB)
TEST_BUSINESS_A = 1
TEST_BUSINESS_B = 2
TEST_PLATFORM = "test_platform"          # will be cleaned up
TEST_ACCOUNT_A = "account-tenant-A"
TEST_ACCOUNT_B = "account-tenant-B"


def _cleanup():
    """Remove any platform_connection rows written by this test run."""
    sql = text("""
        DELETE FROM core.platform_connection
        WHERE platform = :platform;
    """)
    with get_db_connection() as conn:
        conn.execute(sql, {"platform": TEST_PLATFORM})
        conn.commit()


def _create_test_connection(
    business_id: int = TEST_BUSINESS_A,
    account_identifier: str = TEST_ACCOUNT_A,
    access_token: str = "test_access_token_123",
    refresh_token: str = "test_refresh_token_456",
    additional_credentials: Optional[Dict] = None,
    expires_in: Optional[int] = None,
    refresh_token_expires_in: Optional[int] = None,
    auth_type: str = "oauth2",
) -> int:
    return create_or_update_connection(
        business_id=business_id,
        platform=TEST_PLATFORM,
        platform_category="ecommerce",
        account_identifier=account_identifier,
        account_name="Test Store",
        auth_type=auth_type,
        access_token=access_token,
        refresh_token=refresh_token,
        additional_credentials=additional_credentials,
        expires_in=expires_in,
        refresh_token_expires_in=refresh_token_expires_in,
        scopes="read_orders,read_products",
    )


def _run(test_id: str, label: str, fn):
    try:
        fn()
        _results.append((test_id, label, PASS))
        print(f"  [{PASS}] {test_id}: {label}")
    except Exception as exc:
        _results.append((test_id, label, FAIL))
        print(f"  [{FAIL}] {test_id}: {label}")
        traceback.print_exc()


# ---------------------------------------------------------------------------
# T01 — Encryption / decryption round-trip (token)
# ---------------------------------------------------------------------------

def t01_encrypt_decrypt_token():
    plaintext = "shpss_test_access_token_value_abc123"
    ct = encrypt_token(plaintext, key=TEST_KEY)
    assert ct != plaintext, "Ciphertext must differ from plaintext"
    assert "shpss_" not in ct, "Plaintext must not appear in ciphertext"
    recovered = decrypt_token(ct, key=TEST_KEY)
    assert recovered == plaintext, f"Decrypted value mismatch: {recovered!r}"


# ---------------------------------------------------------------------------
# T02 — Tamper detection (token)
# ---------------------------------------------------------------------------

def t02_tamper_detection_token():
    ct = encrypt_token("genuine_token", key=TEST_KEY)
    tampered = ct[:-4] + "XXXX"
    try:
        decrypt_token(tampered, key=TEST_KEY)
        raise AssertionError("Expected ValueError on tampered ciphertext")
    except ValueError:
        pass  # Correct


# ---------------------------------------------------------------------------
# T03 — JSON credential blob encryption / decryption round-trip
# ---------------------------------------------------------------------------

def t03_encrypt_decrypt_json():
    creds = {"app_key": "daraz_app_key_999", "app_secret": "top_secret_xyz"}
    ct = encrypt_json(creds, key=TEST_KEY)
    assert ct, "Ciphertext must not be empty"
    assert "daraz_app_key" not in ct, "Plaintext key must not appear in ciphertext"
    recovered = decrypt_json(ct, key=TEST_KEY)
    assert recovered == creds, f"Decrypted JSON mismatch: {recovered!r}"


# ---------------------------------------------------------------------------
# T04 — Tamper detection (JSON blob)
# ---------------------------------------------------------------------------

def t04_tamper_detection_json():
    ct = encrypt_json({"secret": "value"}, key=TEST_KEY)
    tampered = ct[:-4] + "YYYY"
    try:
        decrypt_json(tampered, key=TEST_KEY)
        raise AssertionError("Expected ValueError on tampered JSON blob")
    except ValueError:
        pass  # Correct


# ---------------------------------------------------------------------------
# T05 — Encrypted-at-rest: plaintext must not appear in database
# ---------------------------------------------------------------------------

def t05_encrypted_at_rest():
    plaintext_at = "PLAINTEXT_TOKEN_SHOULD_NOT_APPEAR_IN_DB"
    plaintext_rt = "PLAINTEXT_REFRESH_TOKEN_SHOULD_NOT_APPEAR"

    _create_test_connection(
        access_token=plaintext_at,
        refresh_token=plaintext_rt,
    )

    # Read raw DB bytes
    sql = text("""
        SELECT encrypted_access_token, encrypted_refresh_token
        FROM core.platform_connection
        WHERE business_id = :bid AND platform = :p AND account_identifier = :acc;
    """)
    with get_db_connection() as conn:
        row = conn.execute(sql, {
            "bid": TEST_BUSINESS_A,
            "p": TEST_PLATFORM,
            "acc": TEST_ACCOUNT_A,
        }).fetchone()

    assert row is not None, "Connection row not found"
    raw_at, raw_rt = row[0], row[1]

    assert plaintext_at not in raw_at, "Plaintext access token found in DB column!"
    assert plaintext_rt not in raw_rt, "Plaintext refresh token found in DB column!"

    # Verify we can still decrypt
    recovered_at = decrypt_token(raw_at)
    assert recovered_at == plaintext_at, "access_token round-trip failed"


# ---------------------------------------------------------------------------
# T06 — Cross-tenant isolation
# ---------------------------------------------------------------------------

def t06_cross_tenant_isolation():
    # Create connection for Business A
    _create_test_connection(
        business_id=TEST_BUSINESS_A,
        account_identifier=TEST_ACCOUNT_A,
        access_token="token_for_business_A",
    )
    # Create connection for Business B
    _create_test_connection(
        business_id=TEST_BUSINESS_B,
        account_identifier=TEST_ACCOUNT_B,
        access_token="token_for_business_B",
    )

    # Business B should NOT see Business A's account
    connections_b = list_connections(TEST_BUSINESS_B)
    account_ids_b = [c["account_identifier"] for c in connections_b
                     if c["platform"] == TEST_PLATFORM]
    assert TEST_ACCOUNT_A not in account_ids_b, (
        f"Business B can see Business A's account! accounts_b={account_ids_b}"
    )
    assert TEST_ACCOUNT_B in account_ids_b, (
        f"Business B cannot see its own account. accounts_b={account_ids_b}"
    )


# ---------------------------------------------------------------------------
# T07 — Unauthorized tenant returns no connection
# ---------------------------------------------------------------------------

def t07_unauthorized_tenant():
    mgr = CredentialManager()
    try:
        mgr.get_valid_credentials(
            business_id=999999,
            platform=TEST_PLATFORM,
            account_identifier=TEST_ACCOUNT_A,
        )
        raise AssertionError("Expected ValueError for unknown business_id")
    except ValueError as exc:
        assert "No connection found" in str(exc), f"Unexpected error: {exc}"


# ---------------------------------------------------------------------------
# T08 — Dynamic token expiry derived from provider expires_in
# ---------------------------------------------------------------------------

def t08_dynamic_token_expiry():
    expires_in = 3600          # Shopify: 1 hour
    rt_expires_in = 7776000    # Shopify: 90 days

    before = datetime.now(timezone.utc)
    _create_test_connection(
        expires_in=expires_in,
        refresh_token_expires_in=rt_expires_in,
    )
    after = datetime.now(timezone.utc)

    row = get_connection(TEST_BUSINESS_A, TEST_PLATFORM, TEST_ACCOUNT_A)
    assert row is not None

    at_exp = row["access_token_expires_at"]
    rt_exp = row["refresh_token_expires_at"]

    # at_exp must be approximately now + 1 hour
    expected_at_min = before + timedelta(seconds=expires_in - 5)
    expected_at_max = after + timedelta(seconds=expires_in + 5)
    assert expected_at_min <= at_exp <= expected_at_max, (
        f"access_token_expires_at out of range: {at_exp}"
    )

    # rt_exp must be approximately now + 90 days
    expected_rt_min = before + timedelta(seconds=rt_expires_in - 5)
    expected_rt_max = after + timedelta(seconds=rt_expires_in + 5)
    assert expected_rt_min <= rt_exp <= expected_rt_max, (
        f"refresh_token_expires_at out of range: {rt_exp}"
    )


# ---------------------------------------------------------------------------
# T09 — Connection CRUD lifecycle
# ---------------------------------------------------------------------------

def t09_connection_crud():
    # CREATE
    conn_id = _create_test_connection()
    assert isinstance(conn_id, int) and conn_id > 0

    # READ
    row = get_connection(TEST_BUSINESS_A, TEST_PLATFORM, TEST_ACCOUNT_A)
    assert row is not None
    assert row["platform"] == TEST_PLATFORM
    assert row["account_identifier"] == TEST_ACCOUNT_A
    assert row["status"] == "active"

    # LIST
    all_conns = list_connections(TEST_BUSINESS_A)
    test_conns = [c for c in all_conns if c["platform"] == TEST_PLATFORM]
    assert len(test_conns) >= 1

    # UPDATE STATUS
    update_status(conn_id, "error", error_message="Test error message")
    row2 = get_connection(TEST_BUSINESS_A, TEST_PLATFORM, TEST_ACCOUNT_A)
    assert row2["status"] == "error"
    assert row2["error_message"] == "Test error message"

    # LAST SYNCED
    before_sync = datetime.now(timezone.utc)
    update_last_synced(conn_id)
    row3 = get_connection(TEST_BUSINESS_A, TEST_PLATFORM, TEST_ACCOUNT_A)
    assert row3["last_synced_at"] is not None
    assert row3["last_synced_at"] >= before_sync


# ---------------------------------------------------------------------------
# T10 — Status transitions
# ---------------------------------------------------------------------------

def t10_status_transitions():
    conn_id = _create_test_connection()

    for target_status in ("error", "needs_reauth", "disconnected", "active"):
        update_status(conn_id, target_status)
        row = get_connection(TEST_BUSINESS_A, TEST_PLATFORM, TEST_ACCOUNT_A)
        assert row["status"] == target_status, (
            f"Expected status='{target_status}', got='{row['status']}'"
        )

    # Invalid status must be rejected
    try:
        update_status(conn_id, "invalid_status")
        raise AssertionError("Expected ValueError for invalid status")
    except ValueError:
        pass


# ---------------------------------------------------------------------------
# T11 — CredentialManager: valid token returned without refresh
# ---------------------------------------------------------------------------

def t11_valid_token_no_refresh():
    # Token expires far in the future
    _create_test_connection(
        access_token="valid_at_111",
        refresh_token="valid_rt_111",
        expires_in=7200,  # 2 hours from now
    )

    refresh_called = {"count": 0}

    def should_not_be_called(creds):
        refresh_called["count"] += 1
        return {}

    mgr = CredentialManager()
    creds = mgr.get_valid_credentials(
        business_id=TEST_BUSINESS_A,
        platform=TEST_PLATFORM,
        account_identifier=TEST_ACCOUNT_A,
        refresh_callback=should_not_be_called,
    )

    assert creds["access_token"] == "valid_at_111", f"Wrong AT: {creds}"
    assert refresh_called["count"] == 0, "Refresh callback was invoked unnecessarily"


# ---------------------------------------------------------------------------
# T12 — CredentialManager: expired token triggers refresh_callback
# ---------------------------------------------------------------------------

def t12_expired_token_triggers_refresh():
    # Insert a connection with an already-expired access token
    conn_id = _create_test_connection(
        access_token="old_expired_at",
        refresh_token="old_rt_for_refresh",
        expires_in=None,   # null → no expiry stored initially
    )

    # Backdate the expiry so the token looks expired
    with get_db_connection() as conn:
        conn.execute(text("""
            UPDATE core.platform_connection
            SET access_token_expires_at = :exp
            WHERE connection_id = :cid;
        """), {
            "exp": datetime.now(timezone.utc) - timedelta(hours=2),
            "cid": conn_id,
        })
        conn.commit()

    def mock_refresh(creds):
        assert creds.get("refresh_token") == "old_rt_for_refresh"
        return {
            "access_token": "new_fresh_at",
            "expires_in": 3600,
            "refresh_token": "new_rotated_rt",
            "refresh_token_expires_in": 7776000,
        }

    mgr = CredentialManager()
    creds = mgr.get_valid_credentials(
        business_id=TEST_BUSINESS_A,
        platform=TEST_PLATFORM,
        account_identifier=TEST_ACCOUNT_A,
        refresh_callback=mock_refresh,
        safety_buffer_seconds=300,
    )

    assert creds["access_token"] == "new_fresh_at", f"Expected refreshed AT, got: {creds}"

    # Verify DB was updated
    row = get_connection(TEST_BUSINESS_A, TEST_PLATFORM, TEST_ACCOUNT_A)
    stored_at = decrypt_token(row["encrypted_access_token"])
    assert stored_at == "new_fresh_at", f"DB access_token not updated: {stored_at}"

    stored_rt = decrypt_token(row["encrypted_refresh_token"])
    assert stored_rt == "new_rotated_rt", f"DB refresh_token not updated: {stored_rt}"


# ---------------------------------------------------------------------------
# T13 — CredentialManager: retain existing RT when provider does not return new one
# ---------------------------------------------------------------------------

def t13_retain_existing_refresh_token():
    conn_id = _create_test_connection(
        access_token="old_at_retain",
        refresh_token="precious_rt_do_not_lose",
        expires_in=None,
    )

    # Backdate access token expiry
    with get_db_connection() as conn:
        conn.execute(text("""
            UPDATE core.platform_connection
            SET access_token_expires_at = :exp
            WHERE connection_id = :cid;
        """), {
            "exp": datetime.now(timezone.utc) - timedelta(hours=1),
            "cid": conn_id,
        })
        conn.commit()

    def mock_refresh_no_rt(creds):
        # Provider returns new access_token but NO refresh_token
        return {
            "access_token": "new_at_after_refresh",
            "expires_in": 3600,
            # refresh_token deliberately absent
        }

    mgr = CredentialManager()
    creds = mgr.get_valid_credentials(
        business_id=TEST_BUSINESS_A,
        platform=TEST_PLATFORM,
        account_identifier=TEST_ACCOUNT_A,
        refresh_callback=mock_refresh_no_rt,
        safety_buffer_seconds=300,
    )

    assert creds["access_token"] == "new_at_after_refresh"

    # Verify the original refresh token is still in DB
    row = get_connection(TEST_BUSINESS_A, TEST_PLATFORM, TEST_ACCOUNT_A)
    stored_rt = decrypt_token(row["encrypted_refresh_token"])
    assert stored_rt == "precious_rt_do_not_lose", (
        f"Existing RT was overwritten! Stored: {stored_rt!r}"
    )


# ---------------------------------------------------------------------------
# T14 — CredentialManager: rollback on ambiguous refresh network failure
# ---------------------------------------------------------------------------

def t14_rollback_on_refresh_failure():
    original_at = "original_at_must_survive"
    original_rt = "original_rt_must_survive"

    conn_id = _create_test_connection(
        access_token=original_at,
        refresh_token=original_rt,
        expires_in=None,
    )

    # Backdate access token expiry to force refresh attempt
    with get_db_connection() as conn:
        conn.execute(text("""
            UPDATE core.platform_connection
            SET access_token_expires_at = :exp
            WHERE connection_id = :cid;
        """), {
            "exp": datetime.now(timezone.utc) - timedelta(hours=1),
            "cid": conn_id,
        })
        conn.commit()

    def failing_refresh(creds):
        raise ConnectionError("Simulated network timeout — ambiguous failure")

    mgr = CredentialManager()
    try:
        mgr.get_valid_credentials(
            business_id=TEST_BUSINESS_A,
            platform=TEST_PLATFORM,
            account_identifier=TEST_ACCOUNT_A,
            refresh_callback=failing_refresh,
            safety_buffer_seconds=300,
        )
        raise AssertionError("Expected RuntimeError from failing refresh")
    except RuntimeError as exc:
        assert "Token refresh failed" in str(exc)

    # Verify original credentials are still intact in DB
    row = get_connection(TEST_BUSINESS_A, TEST_PLATFORM, TEST_ACCOUNT_A)
    stored_at = decrypt_token(row["encrypted_access_token"])
    stored_rt = decrypt_token(row["encrypted_refresh_token"])

    assert stored_at == original_at, (
        f"Original AT was overwritten after failed refresh! Stored: {stored_at!r}"
    )
    assert stored_rt == original_rt, (
        f"Original RT was overwritten after failed refresh! Stored: {stored_rt!r}"
    )


# ---------------------------------------------------------------------------
# T15 — Concurrent credential access (sequential simulation)
# ---------------------------------------------------------------------------

def t15_concurrent_access_simulation():
    """
    Simulates two background workers requesting credentials simultaneously.
    Because both workers hold a DB connection with SELECT ... FOR UPDATE,
    only one refreshes, and the second gets the already-refreshed value.

    Note: True concurrency testing requires threads. Here we simulate the
    second read AFTER the first worker has committed its refresh to verify
    the DB state is consistent.
    """
    refresh_call_count = {"count": 0}

    conn_id = _create_test_connection(
        access_token="concurrent_old_at",
        refresh_token="concurrent_old_rt",
        expires_in=None,
    )

    # Backdate expiry
    with get_db_connection() as conn:
        conn.execute(text("""
            UPDATE core.platform_connection
            SET access_token_expires_at = :exp
            WHERE connection_id = :cid;
        """), {
            "exp": datetime.now(timezone.utc) - timedelta(hours=1),
            "cid": conn_id,
        })
        conn.commit()

    def counting_refresh(creds):
        refresh_call_count["count"] += 1
        return {
            "access_token": f"refreshed_at_v{refresh_call_count['count']}",
            "expires_in": 3600,
        }

    mgr = CredentialManager()

    # Worker 1 — performs the refresh
    creds1 = mgr.get_valid_credentials(
        business_id=TEST_BUSINESS_A,
        platform=TEST_PLATFORM,
        account_identifier=TEST_ACCOUNT_A,
        refresh_callback=counting_refresh,
        safety_buffer_seconds=300,
    )

    # Worker 2 — the new token is now stored and valid; refresh must NOT fire again
    # (Token was just refreshed with expires_in=3600 → well within buffer)
    creds2 = mgr.get_valid_credentials(
        business_id=TEST_BUSINESS_A,
        platform=TEST_PLATFORM,
        account_identifier=TEST_ACCOUNT_A,
        refresh_callback=counting_refresh,
        safety_buffer_seconds=300,
    )

    assert creds2["access_token"] == creds1["access_token"], (
        f"Worker 2 got a different token than worker 1: {creds2['access_token']!r}"
    )
    assert refresh_call_count["count"] == 1, (
        f"Refresh called {refresh_call_count['count']} times; expected exactly 1"
    )


# ---------------------------------------------------------------------------
# T16 — Static API-key platform: no refresh triggered
# ---------------------------------------------------------------------------

def t16_static_api_key_no_refresh():
    # Daraz-style credentials: multi-part, no expiry
    daraz_creds = {"app_key": "daraz_key_xyz", "app_secret": "daraz_secret_abc"}

    create_or_update_connection(
        business_id=TEST_BUSINESS_A,
        platform=TEST_PLATFORM,
        platform_category="ecommerce",
        account_identifier="daraz-seller-123",
        account_name="Daraz Test Seller",
        auth_type="credentials",
        additional_credentials=daraz_creds,
    )

    refresh_called = {"count": 0}

    def should_not_refresh(creds):
        refresh_called["count"] += 1
        return {}

    mgr = CredentialManager()
    result = mgr.get_valid_credentials(
        business_id=TEST_BUSINESS_A,
        platform=TEST_PLATFORM,
        account_identifier="daraz-seller-123",
        refresh_callback=should_not_refresh,
    )

    assert refresh_called["count"] == 0, (
        f"Refresh callback invoked for static-key platform (count={refresh_called['count']})"
    )
    assert "additional_credentials" in result, f"Missing additional_credentials: {result}"
    assert result["additional_credentials"]["app_key"] == "daraz_key_xyz"
    assert result["additional_credentials"]["app_secret"] == "daraz_secret_abc"

    # Cleanup the extra row
    with get_db_connection() as conn:
        conn.execute(text("""
            DELETE FROM core.platform_connection
            WHERE business_id = :bid AND platform = :p AND account_identifier = 'daraz-seller-123';
        """), {"bid": TEST_BUSINESS_A, "p": TEST_PLATFORM})
        conn.commit()


# ---------------------------------------------------------------------------
# Test runner
# ---------------------------------------------------------------------------

def run_all():
    print("\n" + "=" * 70)
    print(" BRIDGE FLOW — Phase 1: Common Connection Infrastructure Tests")
    print("=" * 70)

    tests = [
        ("T01", "Encryption / decryption round-trip (token)",        t01_encrypt_decrypt_token),
        ("T02", "Tamper detection (token)",                           t02_tamper_detection_token),
        ("T03", "JSON credential blob encrypt / decrypt round-trip",  t03_encrypt_decrypt_json),
        ("T04", "Tamper detection (JSON blob)",                       t04_tamper_detection_json),
        ("T05", "Encrypted-at-rest storage (no plaintext in DB)",     t05_encrypted_at_rest),
        ("T06", "Cross-tenant isolation",                             t06_cross_tenant_isolation),
        ("T07", "Unauthorized tenant returns no connection",          t07_unauthorized_tenant),
        ("T08", "Dynamic token expiry from provider expires_in",      t08_dynamic_token_expiry),
        ("T09", "Connection CRUD lifecycle",                          t09_connection_crud),
        ("T10", "Status transitions (active/error/needs_reauth/...)", t10_status_transitions),
        ("T11", "CredentialManager: valid token, no refresh needed",  t11_valid_token_no_refresh),
        ("T12", "CredentialManager: expired token triggers refresh",  t12_expired_token_triggers_refresh),
        ("T13", "CredentialManager: retain existing RT when absent",  t13_retain_existing_refresh_token),
        ("T14", "CredentialManager: rollback on refresh failure",     t14_rollback_on_refresh_failure),
        ("T15", "CredentialManager: concurrent access simulation",    t15_concurrent_access_simulation),
        ("T16", "Static API-key platform: no refresh triggered",      t16_static_api_key_no_refresh),
    ]

    _cleanup()  # Clean up any leftover rows from previous runs

    for test_id, label, fn in tests:
        _cleanup()  # Each test starts from a clean slate
        _run(test_id, label, fn)

    _cleanup()  # Final cleanup

    # Summary
    passed = sum(1 for _, _, s in _results if s == PASS)
    failed = sum(1 for _, _, s in _results if s == FAIL)
    total = len(_results)

    print("\n" + "-" * 70)
    print(f" Results: {passed}/{total} passed  |  {failed}/{total} failed")
    print("-" * 70)

    if failed == 0:
        print(" ALL PHASE 1 TESTS PASSED.")
    else:
        print(" SOME TESTS FAILED. Review output above.")
        for tid, label, s in _results:
            if s == FAIL:
                print(f"   {tid}: {label}")

    print("=" * 70 + "\n")
    return failed == 0


if __name__ == "__main__":
    success = run_all()
    sys.exit(0 if success else 1)
