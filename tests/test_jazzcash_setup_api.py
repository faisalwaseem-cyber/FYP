"""Focused tests for the authenticated JazzCash Sandbox setup endpoint."""
from __future__ import annotations

import logging
import sys
from pathlib import Path
from unittest.mock import patch

from fastapi.testclient import TestClient

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from api.dependencies import get_current_business_id
from api.main import app

client = TestClient(app)


def _setup_payload():
    return {
        "merchant_id": "fake-merchant-id",
        "password": "fake-merchant-password",
        "integrity_salt": "fake-integrity-salt",
    }


def test_jazzcash_setup_uses_authenticated_tenant_and_returns_safe_metadata(caplog):
    app.dependency_overrides[get_current_business_id] = lambda: 42
    try:
        with (
            patch(
                "api.main.setup_jazzcash_sandbox_connection",
                return_value=17,
            ) as setup_connection,
            caplog.at_level(logging.INFO, logger="api.main"),
        ):
            response = client.post(
                "/integrations/jazzcash/connect",
                json=_setup_payload(),
            )
    finally:
        app.dependency_overrides.pop(get_current_business_id, None)

    assert response.status_code == 200
    assert response.json() == {
        "connection_id": 17,
        "platform": "jazzcash",
        "account_name": "JazzCash Sandbox",
        "status": "active",
    }
    setup_connection.assert_called_once_with(
        business_id=42,
        merchant_id="fake-merchant-id",
        password="fake-merchant-password",
        integrity_salt="fake-integrity-salt",
    )
    for secret in _setup_payload().values():
        assert secret not in response.text
        assert secret not in caplog.text


def test_jazzcash_setup_is_scoped_to_each_resolved_business():
    setup_payload = _setup_payload()
    with patch(
        "api.main.setup_jazzcash_sandbox_connection",
        return_value=17,
    ) as setup_connection:
        try:
            app.dependency_overrides[get_current_business_id] = lambda: 42
            first_response = client.post(
                "/integrations/jazzcash/connect",
                json=setup_payload,
            )
            app.dependency_overrides[get_current_business_id] = lambda: 84
            second_response = client.post(
                "/integrations/jazzcash/connect",
                json=setup_payload,
            )
        finally:
            app.dependency_overrides.pop(get_current_business_id, None)

    assert first_response.status_code == 200
    assert second_response.status_code == 200
    assert [
        call.kwargs["business_id"]
        for call in setup_connection.call_args_list
    ] == [42, 84]


def test_jazzcash_setup_rejects_client_selected_business_id():
    app.dependency_overrides[get_current_business_id] = lambda: 42
    try:
        with patch("api.main.setup_jazzcash_sandbox_connection") as setup_connection:
            response = client.post(
                "/integrations/jazzcash/connect",
                json={**_setup_payload(), "business_id": 84},
            )
    finally:
        app.dependency_overrides.pop(get_current_business_id, None)

    assert response.status_code == 422
    assert response.json() == {
        "detail": "Invalid JazzCash Sandbox setup request."
    }
    assert "fake-merchant-password" not in response.text
    assert "fake-integrity-salt" not in response.text
    setup_connection.assert_not_called()
