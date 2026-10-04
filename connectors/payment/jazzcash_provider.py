"""Offline parsing of supplied JazzCash transaction rows."""
from __future__ import annotations

import csv
import hashlib
import hmac
import io
import logging
import re
import uuid
from datetime import datetime, timedelta, timezone
from typing import Any, Dict, List, Union

import requests

from auth.credential_manager import CredentialManager
from connectors.payment.base import (
    BasePaymentProvider,
    PaymentRecord,
    normalize_method,
    normalize_status,
    normalize_type,
    safe_parse_money,
)

logger = logging.getLogger(__name__)

JAZZCASH_SANDBOX_MWALLET_URL = (
    "https://sandbox.jazzcash.com.pk/ApplicationAPI/API/Payment/DoTransaction"
)
JAZZCASH_SANDBOX_TEST_AMOUNT = "100"


def generate_secure_hash(
    transaction_fields: Dict[str, Any],
    integrity_salt: str,
) -> str:
    """Generate the JazzCash v4.2 SecureHash for transaction fields."""
    if not isinstance(integrity_salt, str) or not integrity_salt:
        raise ValueError("A non-empty JazzCash Integrity Salt is required.")

    fields = []
    for name, value in transaction_fields.items():
        if not isinstance(name, str):
            raise ValueError("JazzCash transaction field names must be strings.")
        try:
            name_bytes = name.encode("ascii")
        except UnicodeEncodeError:
            raise ValueError("JazzCash transaction field names must be ASCII.") from None
        if name.upper().startswith("PP") and name.lower() != "pp_securehash":
            fields.append((name_bytes, str(value)))

    fields.sort(key=lambda item: item[0])
    values = "&".join(value for _, value in fields)
    canonical = f"{integrity_salt}&{values}"

    # The ISO-8859-1 round-trip preserves the UTF-8 byte sequence in the
    # representation required by the documented JazzCash hashing procedure.
    payload = canonical.encode("utf-8").decode("iso-8859-1").encode("iso-8859-1")
    digest = hmac.new(
        integrity_salt.encode("utf-8"),
        payload,
        hashlib.sha256,
    )
    return digest.hexdigest()


def _redact_provider_response(value: Any, secret_values: List[str]) -> Any:
    if isinstance(value, dict):
        redacted = {}
        for key, item in value.items():
            normalized_key = re.sub(r"[^a-z0-9]", "", str(key).lower())
            if any(secret_name in normalized_key for secret_name in (
                "password",
                "integritysalt",
                "securehash",
                "merchantid",
                "authorization",
            )):
                redacted[key] = "[REDACTED]"
            else:
                redacted[key] = _redact_provider_response(item, secret_values)
        return redacted
    if isinstance(value, list):
        return [_redact_provider_response(item, secret_values) for item in value]
    if isinstance(value, str):
        for secret in secret_values:
            if secret:
                value = value.replace(secret, "[REDACTED]")
        return value
    return value


class JazzCashPaymentProvider(BasePaymentProvider):
    PROVIDER_NAME = "jazzcash"
    SUPPORTED_METHODS = {"csv", "settlement"}

    def initiate_sandbox_mwallet_test(
        self,
        *,
        business_id: int,
        return_url: str,
    ) -> Dict[str, Any]:
        """Submit one explicitly sandbox-only MWALLET test request.

        This method returns a redacted provider response. It does not interpret
        payment status or write payment, order, or settlement records.
        """
        if not isinstance(business_id, int) or isinstance(business_id, bool) or business_id <= 0:
            raise ValueError("business_id must be a positive server-resolved integer.")
        if not isinstance(return_url, str) or not return_url.strip():
            raise ValueError("A JazzCash Sandbox Return URL is required.")

        credentials = CredentialManager().get_valid_credentials(
            business_id=business_id,
            platform="jazzcash",
            account_identifier="sandbox",
        ).get("additional_credentials")
        if not isinstance(credentials, dict):
            raise RuntimeError("Active JazzCash Sandbox credentials are unavailable.")

        merchant_id = credentials.get("merchant_id")
        password = credentials.get("password")
        integrity_salt = credentials.get("integrity_salt")
        if any(
            not isinstance(value, str) or not value.strip()
            for value in (merchant_id, password, integrity_salt)
        ):
            raise RuntimeError("Active JazzCash Sandbox credentials are incomplete.")

        now = datetime.now(timezone.utc)
        expires = now + timedelta(minutes=30)
        timestamp_format = "%Y%m%d%H%M%S"
        reference = f"T{uuid.uuid4().hex[:19].upper()}"
        fields = {
            "pp_Version": "1.1",
            "pp_TxnType": "MWALLET",
            "pp_Language": "EN",
            "pp_MerchantID": merchant_id,
            "pp_SubMerchantID": "",
            "pp_Password": password,
            "pp_BankID": "",
            "pp_ProductID": "",
            "pp_TxnRefNo": reference,
            "pp_Amount": JAZZCASH_SANDBOX_TEST_AMOUNT,
            "pp_TxnCurrency": "PKR",
            "pp_TxnDateTime": now.strftime(timestamp_format),
            "pp_BillReference": "BFTEST",
            "pp_Description": "BRIDGE FLOW JazzCash Sandbox MWALLET test",
            "pp_TxnExpiryDateTime": expires.strftime(timestamp_format),
            "pp_ReturnURL": return_url.strip(),
            "ppmpf_1": "03001234567",
            "ppmpf_2": "",
            "ppmpf_3": "",
            "ppmpf_4": "",
            "ppmpf_5": "",
        }
        fields["pp_SecureHash"] = generate_secure_hash(fields, integrity_salt)

        try:
            response = requests.post(
                JAZZCASH_SANDBOX_MWALLET_URL,
                json=fields,
                timeout=30,
            )
        except requests.RequestException:
            logger.warning("JazzCash Sandbox MWALLET request failed.")
            raise RuntimeError("JazzCash Sandbox request failed.") from None

        try:
            provider_response = response.json()
        except (ValueError, requests.exceptions.JSONDecodeError):
            provider_response = {"error": "non_json_response"}

        safe_response = _redact_provider_response(
            provider_response,
            [merchant_id, password, integrity_salt, fields["pp_SecureHash"]],
        )
        return {
            "http_status": response.status_code,
            "provider_response": safe_response,
        }

    def parse_transactions(self, raw_data: Union[str, List[Dict[str, Any]]]) -> List[PaymentRecord]:
        rows = list(csv.DictReader(io.StringIO(raw_data.strip()))) if isinstance(raw_data, str) else raw_data
        if not isinstance(rows, list):
            return []
        records = []
        for row in rows:
            payment_id = row.get("external_payment_id")
            order_id = row.get("external_order_id")
            amount = row.get("canonical_amount")
            fee = row.get("gateway_fee")
            status = row.get("payment_status")
            transaction_type = row.get("payment_type")
            date = row.get("paid_at")
            currency = row.get("currency")
            account = row.get("provider_account_identifier")
            records.append(PaymentRecord(
                external_order_id=str(order_id).strip() if order_id and str(order_id).strip() else None,
                amount=safe_parse_money(amount),
                gateway_fee=safe_parse_money(fee),
                external_payment_id=str(payment_id).strip() if payment_id and str(payment_id).strip() else None,
                payment_method=normalize_method(row.get("payment_method")),
                payment_type=normalize_type(transaction_type),
                payment_status=normalize_status(status),
                paid_at=str(date).strip() if date else None,
                currency=str(currency).strip().upper() if currency and len(str(currency).strip()) == 3 else None,
                provider=self.PROVIDER_NAME,
                provider_account_identifier=str(account).strip() if account and str(account).strip() else None,
                original_external_payment_id=row.get("original_payment_id") or None,
                raw_metadata=dict(row),
            ))
        return records
