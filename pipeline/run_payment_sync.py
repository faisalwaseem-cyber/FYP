"""Offline payment ingestion orchestration with tenant-scoped raw auditing."""
from __future__ import annotations

import csv
import io
import json
import re
import sys
from datetime import date, datetime
from decimal import Decimal
from pathlib import Path
from typing import Any, Dict, List, Optional, Sequence

sys.path.append(str(Path(__file__).resolve().parent.parent))

from config.settings import BUSINESS_ID
from connectors.payment.base import PaymentRecord
from connectors.payment.provider_factory import get_payment_provider
from etl.payment_transformer import PaymentTransformer
from loaders.payment_loader import PaymentLoader
from pipeline.sync_logger import SyncLogger
from raw_layer.raw_repository import save_raw_payload

_SENSITIVE_KEY_PARTS = (
    "token", "secret", "password", "authorization", "apikey",
    "clientkey", "signature", "credential",
)
_SENSITIVE_TEXT = re.compile(
    r"(?i)((?:api[_-]?key|access[_-]?token|refresh[_-]?token|client[_-]?secret|"
    r"authorization|password|webhook[_-]?secret)\s*[:=]\s*)"
    r"((?:bearer\s+)?[^,\s;&]+)"
)


def _sensitive_key(key: Any) -> bool:
    normalized = re.sub(r"[^a-z0-9]", "", str(key).lower())
    return any(part in normalized for part in _SENSITIVE_KEY_PARTS)


def _redact_raw_payload(value: Any) -> Any:
    """Preserve the source structure while removing credential-shaped fields."""
    if isinstance(value, dict):
        return {
            str(key): "[REDACTED]" if _sensitive_key(key) else _redact_raw_payload(item)
            for key, item in value.items()
        }
    if isinstance(value, (list, tuple)):
        return [_redact_raw_payload(item) for item in value]
    if isinstance(value, (datetime, date)):
        return value.isoformat()
    if isinstance(value, Decimal):
        return str(value)
    if isinstance(value, bytes):
        value = value.decode("utf-8", errors="replace")
    if isinstance(value, str):
        stripped = value.strip()
        if stripped.startswith(("{", "[")):
            try:
                return _redact_raw_payload(json.loads(stripped))
            except json.JSONDecodeError:
                pass
        if "\n" in value and "," in value:
            reader = csv.DictReader(io.StringIO(value))
            if reader.fieldnames and any(_sensitive_key(name) for name in reader.fieldnames):
                return {
                    "format": "redacted_csv",
                    "records": _redact_raw_payload(list(reader)),
                }
        return _SENSITIVE_TEXT.sub(r"\1[REDACTED]", value)
    if value is None or isinstance(value, (bool, int, float)):
        return value
    return str(value)


def run_payment_sync(
    provider_name: str,
    raw_payload: Any,
    sync_type: str = "statement_sync",
    business_id: int = BUSINESS_ID,
    settlement_record: Optional[Dict[str, Any]] = None,
    settlement_entries: Optional[Sequence[Dict[str, Any]]] = None,
) -> Dict[str, Any]:
    """Import supplied payment data; provider network calls are not performed here."""
    if not isinstance(business_id, int) or business_id <= 0:
        raise ValueError("business_id must be resolved by trusted server-side code.")
    provider = get_payment_provider(provider_name)
    logger = SyncLogger(business_id, f"payment_{provider_name}")
    logger.start_sync(sync_type=sync_type)
    loader = PaymentLoader(business_id)
    transformer = PaymentTransformer(business_id)

    try:
        # Keep the original response in the tenant-scoped raw layer before parsing.
        audited_payload: Dict[str, Any] = {
            "provider_response": _redact_raw_payload(raw_payload)
        }
        if settlement_record is not None:
            audited_payload["settlement_response"] = _redact_raw_payload(settlement_record)
        if settlement_entries is not None:
            audited_payload["settlement_entries_response"] = _redact_raw_payload(
                settlement_entries
            )
        raw_payload_id = save_raw_payload(
            business_id=business_id,
            source="payment_gateway",
            data_type=f"{provider_name}_response",
            payload=audited_payload,
        )

        records: List[PaymentRecord] = provider.parse_transactions(raw_payload)
        order_map = loader.get_order_id_map()
        payment_context = loader.get_payment_context_map()
        canonical_payments = transformer.transform_payment_records(
            records,
            order_map,
            original_payment_map=payment_context,
        )
        inserted, updated = loader.load_payments(canonical_payments)

        settlement_id = None
        if settlement_record is not None:
            settlement_id, _ = loader.load_settlement(
                settlement_record,
                list(settlement_entries or []),
            )

        logger.finish_success(
            fetched=len(records),
            inserted=inserted,
            updated=updated,
        )
        return {
            "status": "success",
            "business_id": business_id,
            "provider": provider_name,
            "raw_id": raw_payload_id,
            "records_fetched": len(records),
            "records_inserted": inserted,
            "records_updated": updated,
            "records_retained_raw_only": len(transformer.rejected_records),
            "settlement_id": settlement_id,
        }
    except Exception as error:
        safe_error = _SENSITIVE_TEXT.sub(r"\1[REDACTED]", str(error))
        logger.finish_failure(error_msg=safe_error)
        print(f"[ERROR] Payment sync failed for {provider_name}: {safe_error}")
        raise


def run_payment_csv_sync(
    csv_content: str,
    gateway_name: str,
    sync_type: str = "csv_import",
    business_id: int = BUSINESS_ID,
) -> Dict[str, Any]:
    return run_payment_sync(
        provider_name=gateway_name,
        raw_payload=csv_content,
        sync_type=sync_type,
        business_id=business_id,
    )
