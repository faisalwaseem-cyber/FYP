"""Focused tests for the JazzCash return-receipt endpoint."""
from __future__ import annotations

import logging
import sys
from pathlib import Path
from urllib.parse import urlencode

from fastapi.testclient import TestClient

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from api.main import app

client = TestClient(app)


def test_jazzcash_return_accepts_json_without_trusting_payment_status(caplog):
    secret_hash = "sensitive-test-hash"
    with caplog.at_level(logging.INFO, logger="api.main"):
        response = client.post(
            "/integrations/jazzcash/return",
            json={
                "pp_TxnRefNo": "test-ref-123",
                "pp_ResponseCode": "000",
                "pp_SecureHash": secret_hash,
                "pp_Password": "sensitive-test-password",
            },
        )

    assert response.status_code == 200
    assert response.json() == {"status": "received"}
    assert "has_txn_ref=True" in caplog.text
    assert "has_response_code=True" in caplog.text
    assert "has_secure_hash=True" in caplog.text
    assert secret_hash not in caplog.text
    assert "sensitive-test-password" not in caplog.text


def test_jazzcash_return_accepts_url_encoded_form_data():
    response = client.post(
        "/integrations/jazzcash/return",
        content=urlencode({
            "pp_TxnRefNo": "test-ref-456",
            "pp_ResponseCode": "000",
            "pp_SecureHash": "sensitive-test-hash",
        }),
        headers={"content-type": "application/x-www-form-urlencoded"},
    )

    assert response.status_code == 200
    assert response.json() == {"status": "received"}


def test_jazzcash_ipn_accepts_json_and_does_not_log_secret_values(caplog):
    secret_hash = "sensitive-ipn-hash"
    password = "sensitive-ipn-password"
    with caplog.at_level(logging.INFO, logger="api.main"):
        response = client.post(
            "/integrations/jazzcash/ipn",
            json={
                "pp_TxnRefNo": "ipn-ref-123",
                "pp_ResponseCode": "000",
                "pp_SecureHash": secret_hash,
                "pp_Password": password,
            },
        )

    assert response.status_code == 200
    assert response.json() == {"status": "received"}
    assert "JazzCash IPN received" in caplog.text
    assert "has_txn_ref=True" in caplog.text
    assert "has_response_code=True" in caplog.text
    assert "has_secure_hash=True" in caplog.text
    assert secret_hash not in caplog.text
    assert password not in caplog.text


def test_jazzcash_ipn_accepts_url_encoded_form_data():
    response = client.post(
        "/integrations/jazzcash/ipn",
        content=urlencode({
            "pp_TxnRefNo": "ipn-ref-456",
            "pp_ResponseCode": "000",
            "pp_SecureHash": "sensitive-ipn-hash",
        }),
        headers={"content-type": "application/x-www-form-urlencoded"},
    )

    assert response.status_code == 200
    assert response.json() == {"status": "received"}
