"""Focused tests for encrypted JazzCash Sandbox connection setup."""
from __future__ import annotations

import logging
import sys
import uuid
from pathlib import Path

from sqlalchemy import text

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from auth.connection_service import setup_jazzcash_sandbox_connection
from auth.credential_manager import CredentialManager
from config.database import get_db_connection
from security.encryption import decrypt_json


def test_jazzcash_sandbox_credentials_are_encrypted_and_retrievable(caplog):
    business_name = f"JazzCash connection test {uuid.uuid4()}"
    business_id = None
    account_identifier = "sandbox"
    first_credentials = {
        "merchant_id": "fake-merchant-one",
        "password": "fake-password-one",
        "integrity_salt": "fake-integrity-salt-one",
    }
    updated_credentials = {
        "merchant_id": "fake-merchant-two",
        "password": "fake-password-two",
        "integrity_salt": "fake-integrity-salt-two",
    }

    try:
        with get_db_connection() as conn:
            business_id = conn.execute(text("""
                INSERT INTO core.business (business_name)
                VALUES (:business_name)
                RETURNING business_id;
            """), {"business_name": business_name}).scalar_one()
            conn.commit()

        with caplog.at_level(logging.INFO):
            connection_id = setup_jazzcash_sandbox_connection(
                business_id=business_id,
                **first_credentials,
            )
            updated_connection_id = setup_jazzcash_sandbox_connection(
                business_id=business_id,
                **updated_credentials,
            )

        assert isinstance(connection_id, int)
        assert isinstance(updated_connection_id, int)
        assert updated_connection_id == connection_id
        with get_db_connection() as conn:
            row = conn.execute(text("""
                SELECT platform, platform_category, account_identifier,
                       account_name, auth_type, encrypted_additional_credentials
                FROM core.platform_connection
                WHERE business_id = :business_id
                  AND platform = 'jazzcash'
                  AND account_identifier = :account_identifier;
            """), {
                "business_id": business_id,
                "account_identifier": account_identifier,
            }).one()

        assert tuple(row[:5]) == (
            "jazzcash", "payment", "sandbox", "JazzCash Sandbox", "credentials",
        )
        encrypted_credentials = row[5]
        assert all(
            secret not in encrypted_credentials
            for secret in updated_credentials.values()
        )
        assert decrypt_json(encrypted_credentials) == updated_credentials

        retrieved = CredentialManager().get_valid_credentials(
            business_id=business_id,
            platform="jazzcash",
            account_identifier=account_identifier,
        )
        assert retrieved["additional_credentials"] == updated_credentials
        assert all(secret not in caplog.text for secret in (
            *first_credentials.values(),
            *updated_credentials.values(),
        ))
    finally:
        if business_id is not None:
            with get_db_connection() as conn:
                conn.execute(text("""
                    DELETE FROM core.platform_connection
                    WHERE business_id = :business_id
                      AND platform = 'jazzcash'
                      AND account_identifier = :account_identifier;
                """), {
                    "business_id": business_id,
                    "account_identifier": account_identifier,
                })
                conn.execute(text("""
                    DELETE FROM core.business
                    WHERE business_id = :business_id;
                """), {"business_id": business_id})
                conn.commit()
