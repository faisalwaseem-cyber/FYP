"""Focused tests for JazzCash Sandbox MWALLET request construction."""
from __future__ import annotations

import logging
import sys
from pathlib import Path
from unittest.mock import Mock, patch

import requests

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from connectors.payment.jazzcash_provider import (
    JAZZCASH_SANDBOX_MWALLET_URL,
    JazzCashPaymentProvider,
    generate_secure_hash,
)


FAKE_CREDENTIALS = {
    "merchant_id": "fake-merchant-id",
    "password": "fake-password",
    "integrity_salt": "fake-integrity-salt",
}
RETURN_URL = "https://bridge-flow.example/integrations/jazzcash/return"


def _credential_manager_mock():
    manager = Mock()
    manager.get_valid_credentials.return_value = {
        "additional_credentials": dict(FAKE_CREDENTIALS),
    }
    return manager


def _response(status_code, payload):
    response = Mock()
    response.status_code = status_code
    response.json.return_value = payload
    return response


def test_secure_hash_sorts_ascii_names_and_uses_values_only():
    fields = {
        "ppmpf_1": "five",
        "pp_B": "two",
        "pp_SecureHash": "must-not-be-signed",
        "pp_A": "one",
    }

    assert generate_secure_hash(fields, "integrity-salt") == (
        "147248e5cf999e76a5981cbb2a781faa32281ea805cb88a51596bf92d95c2e14"
    )


def test_secure_hash_uses_utf8_payload_bytes_and_integrity_salt_key():
    fields = {"pp_Value": "café"}
    expected = "f0946f30ff2a4aaff4d96ec1d1f277c158d4985ddae6cf95a01272a667938821"

    assert generate_secure_hash(fields, "salt") == expected


def test_mwallet_builds_unique_signed_sandbox_json_request_and_redacts_response(caplog):
    echoed_response = {
        "pp_ResponseCode": "000",
        "pp_MerchantID": FAKE_CREDENTIALS["merchant_id"],
        "pp_Password": FAKE_CREDENTIALS["password"],
        "pp_SecureHash": "echoed-hash",
        "message": f"salt={FAKE_CREDENTIALS['integrity_salt']}",
    }
    manager = _credential_manager_mock()
    with (
        patch(
            "connectors.payment.jazzcash_provider.CredentialManager",
            return_value=manager,
        ),
        patch(
            "connectors.payment.jazzcash_provider.requests.post",
            return_value=_response(200, echoed_response),
        ) as post,
        caplog.at_level(logging.DEBUG),
    ):
        first = JazzCashPaymentProvider().initiate_sandbox_mwallet_test(
            business_id=42,
            return_url=RETURN_URL,
        )
        first_fields = post.call_args.kwargs["json"]
        second = JazzCashPaymentProvider().initiate_sandbox_mwallet_test(
            business_id=42,
            return_url=RETURN_URL,
        )
        second_fields = post.call_args.kwargs["json"]

    manager.get_valid_credentials.assert_called_with(
        business_id=42,
        platform="jazzcash",
        account_identifier="sandbox",
    )
    assert post.call_count == 2
    assert post.call_args.args == (JAZZCASH_SANDBOX_MWALLET_URL,)
    assert post.call_args.kwargs["timeout"] == 30
    assert first_fields["pp_TxnRefNo"] != second_fields["pp_TxnRefNo"]
    assert first_fields["pp_TxnType"] == "MWALLET"
    assert first_fields["pp_TxnCurrency"] == "PKR"
    assert first_fields["pp_Amount"] == "100"
    assert first_fields["pp_ReturnURL"] == RETURN_URL
    assert first_fields["ppmpf_1"] == "03001234567"
    assert first_fields["pp_BankID"] == ""
    assert first_fields["pp_ProductID"] == ""
    assert [first_fields[f"ppmpf_{number}"] for number in range(2, 6)] == [
        "", "", "", "",
    ]
    assert first_fields["pp_SecureHash"] == generate_secure_hash(
        {key: value for key, value in first_fields.items() if key != "pp_SecureHash"},
        FAKE_CREDENTIALS["integrity_salt"],
    )
    assert "pp_Password" in first_fields
    assert first["http_status"] == 200
    assert first["provider_response"]["pp_ResponseCode"] == "000"
    assert first["provider_response"]["pp_Password"] == "[REDACTED]"
    assert first["provider_response"]["pp_MerchantID"] == "[REDACTED]"
    assert "[REDACTED]" in first["provider_response"]["message"]
    assert "payment_status" not in first
    for secret in (
        *FAKE_CREDENTIALS.values(),
        first_fields["pp_SecureHash"],
        "echoed-hash",
    ):
        assert secret not in caplog.text
        assert secret not in str(first)


def test_mwallet_preserves_provider_error_without_interpreting_payment_status():
    manager = _credential_manager_mock()
    error_payload = {"pp_ResponseCode": "ERR", "message": "sandbox rejected request"}
    with (
        patch(
            "connectors.payment.jazzcash_provider.CredentialManager",
            return_value=manager,
        ),
        patch(
            "connectors.payment.jazzcash_provider.requests.post",
            return_value=_response(422, error_payload),
        ),
    ):
        result = JazzCashPaymentProvider().initiate_sandbox_mwallet_test(
            business_id=42,
            return_url=RETURN_URL,
        )

    assert result == {
        "http_status": 422,
        "provider_response": error_payload,
    }
    assert "payment_status" not in result


def test_mwallet_request_exception_does_not_leak_secrets():
    manager = _credential_manager_mock()
    with (
        patch(
            "connectors.payment.jazzcash_provider.CredentialManager",
            return_value=manager,
        ),
        patch(
            "connectors.payment.jazzcash_provider.requests.post",
            side_effect=requests.ConnectionError(FAKE_CREDENTIALS["password"]),
        ),
        patch("connectors.payment.jazzcash_provider.logger") as logger,
    ):
        try:
            JazzCashPaymentProvider().initiate_sandbox_mwallet_test(
                business_id=42,
                return_url=RETURN_URL,
            )
        except RuntimeError as exc:
            assert str(exc) == "JazzCash Sandbox request failed."
            assert FAKE_CREDENTIALS["password"] not in str(exc)
        else:
            raise AssertionError("Expected a sanitized request failure.")

    assert FAKE_CREDENTIALS["password"] not in str(logger.warning.call_args)
