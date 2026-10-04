"""Focused checks for the active provider registry and incomplete M&P scope."""
from unittest.mock import MagicMock, patch

import pytest

from connectors.courier import (
    ACTIVE_COURIER_PLATFORMS,
    M_AND_P_IMPLEMENTATION_STATUS,
)
from connectors.payment import provider_factory
from connectors.payment.provider_factory import get_payment_provider
from pipeline.run_courier_sync import resolve_courier_client, run_courier_sync


def test_courier_scope_contains_only_m_and_p_and_is_marked_incomplete():
    assert ACTIVE_COURIER_PLATFORMS == ("m&p",)
    assert M_AND_P_IMPLEMENTATION_STATUS == "incomplete"


def test_m_and_p_dispatch_reports_incomplete_without_sync_side_effects():
    connection = {
        "connection_id": 42,
        "platform": "m&p",
        "account_identifier": "main",
        "status": "active",
    }
    with patch("pipeline.run_courier_sync.list_connections", return_value=[connection]):
        result = run_courier_sync(business_id=1)

    assert result == {
        "status": "incomplete",
        "business_id": 1,
        "connections_synced": 0,
        "results": [{
            "status": "incomplete",
            "reason": "m_and_p_client_not_implemented",
            "connection_id": 42,
            "platform": "m&p",
        }],
    }


def test_m_and_p_client_resolution_does_not_read_credentials():
    credential_manager = MagicMock()
    with pytest.raises(NotImplementedError, match="API contract has not been verified"):
        resolve_courier_client(
            business_id=1,
            platform="m&p",
            account_identifier="main",
            credential_manager=credential_manager,
        )
    credential_manager.get_valid_credentials.assert_not_called()


def test_payment_registry_keeps_jazzcash_and_shared_import_modes_only():
    assert set(provider_factory._REGISTRY) == {
        "jazzcash",
        "cod",
        "manual",
        "bank_transfer",
    }
    assert get_payment_provider("jazzcash").PROVIDER_NAME == "jazzcash"
    assert get_payment_provider("manual").PROVIDER_NAME == "manual"
    with pytest.raises(ValueError, match="Unsupported payment provider"):
        get_payment_provider("unavailable")
