"""Tenant-scoped persistence for canonical payments and settlements."""
from __future__ import annotations

import re
from dataclasses import asdict, is_dataclass
from datetime import datetime
from decimal import Decimal, InvalidOperation
from typing import Any, Dict, Mapping, Optional, Tuple

import pandas as pd
from sqlalchemy import text

from config.database import get_db_connection
from connectors.payment.base import (
    VALID_PAYMENT_METHODS,
    VALID_PAYMENT_STATUSES,
    VALID_PAYMENT_TYPES,
    VALID_SETTLEMENT_ENTRY_TYPES,
    VALID_SETTLEMENT_STATUSES,
)

_CURRENCY = re.compile(r"^[A-Z]{3}$")


def _record_dict(record: Any) -> Dict[str, Any]:
    if is_dataclass(record) and not isinstance(record, type):
        return asdict(record)
    if isinstance(record, Mapping):
        return dict(record)
    raise TypeError("Payment and settlement records must be mappings or dataclasses.")


def _decimal(value: Any, field: str, *, optional: bool = False) -> Optional[Decimal]:
    if value is None or (not isinstance(value, (str, bytes)) and pd.isna(value)):
        if optional:
            return None
        raise ValueError(f"{field} is required.")
    try:
        amount = Decimal(str(value).strip())
    except (InvalidOperation, ValueError):
        raise ValueError(f"{field} must be a valid decimal amount.") from None
    if not amount.is_finite():
        raise ValueError(f"{field} must be finite.")
    return amount


def _required_text(value: Any, field: str) -> str:
    if value is None or pd.isna(value) or not str(value).strip():
        raise ValueError(f"{field} is required.")
    return str(value).strip()


def _currency(value: Any, field: str) -> str:
    normalized = _required_text(value, field).upper()
    if not _CURRENCY.fullmatch(normalized):
        raise ValueError(f"{field} must be an explicit three-letter currency code.")
    return normalized


def _fits_numeric(value: Decimal, precision: int, scale: int, field: str) -> None:
    try:
        rounded = value.quantize(Decimal(1).scaleb(-scale))
    except ArithmeticError:
        raise ValueError(f"{field} exceeds NUMERIC({precision},{scale}).") from None
    if rounded != value or abs(value) >= Decimal(10) ** (precision - scale):
        raise ValueError(f"{field} exceeds NUMERIC({precision},{scale}).")


class PaymentLoader:
    def __init__(self, business_id: int):
        if not isinstance(business_id, int) or business_id <= 0:
            raise ValueError("business_id must be a positive server-resolved integer.")
        self.business_id = business_id

    def get_order_id_map(self) -> Dict[str, int]:
        query = text("""
            SELECT external_order_id, order_id
            FROM core.orders
            WHERE business_id = :business_id;
        """)
        with get_db_connection() as conn:
            result = conn.execute(query, {"business_id": self.business_id})
            return {str(row[0]): int(row[1]) for row in result.fetchall()}

    def get_payment_id_map(self) -> Dict[str, int]:
        query = text("""
            SELECT external_payment_id, payment_id
            FROM core.payment
            WHERE business_id = :business_id
              AND external_payment_id IS NOT NULL;
        """)
        with get_db_connection() as conn:
            result = conn.execute(query, {"business_id": self.business_id})
            return {str(row[0]): int(row[1]) for row in result.fetchall()}

    def get_payment_context_map(self) -> Dict[str, Dict[str, Any]]:
        query = text("""
            SELECT external_payment_id, payment_id, order_id,
                   provider, provider_account_identifier, currency, payment_type
            FROM core.payment
            WHERE business_id = :business_id
              AND external_payment_id IS NOT NULL;
        """)
        with get_db_connection() as conn:
            result = conn.execute(query, {"business_id": self.business_id})
            return {
                str(row[0]): {
                    "payment_id": int(row[1]),
                    "order_id": int(row[2]),
                    "provider": row[3],
                    "provider_account_identifier": row[4],
                    "currency": row[5].strip() if row[5] else None,
                    "payment_type": row[6],
                }
                for row in result.fetchall()
            }

    def load_payments(self, df: pd.DataFrame) -> Tuple[int, int]:
        """Insert/update complete payment facts on the retained legacy key."""
        if df.empty:
            return 0, 0

        upsert_sql = text("""
            INSERT INTO core.payment (
                business_id, order_id, external_payment_id, payment_method,
                payment_type, payment_status, amount, gateway_fee, paid_at,
                provider, provider_account_identifier, currency,
                original_payment_id, created_at
            )
            VALUES (
                :business_id, :order_id, :external_payment_id, :payment_method,
                :payment_type, :payment_status, :amount, :gateway_fee, :paid_at,
                :provider, :provider_account_identifier, :currency,
                :original_payment_id, now()
            )
            ON CONFLICT (business_id, external_payment_id) DO UPDATE SET
                payment_status = EXCLUDED.payment_status,
                paid_at = EXCLUDED.paid_at
            WHERE core.payment.order_id = EXCLUDED.order_id
              AND core.payment.payment_method = EXCLUDED.payment_method
              AND core.payment.payment_type = EXCLUDED.payment_type
              AND core.payment.amount = EXCLUDED.amount
              AND core.payment.gateway_fee = EXCLUDED.gateway_fee
              AND core.payment.provider = EXCLUDED.provider
              AND core.payment.provider_account_identifier = EXCLUDED.provider_account_identifier
              AND core.payment.currency = EXCLUDED.currency
              AND core.payment.original_payment_id IS NOT DISTINCT FROM EXCLUDED.original_payment_id
            RETURNING (xmax = 0) AS is_inserted;
        """)
        find_existing = text("""
            SELECT provider, provider_account_identifier, currency, order_id,
                   payment_method, payment_type, amount, gateway_fee,
                   original_payment_id
            FROM core.payment
            WHERE business_id = :business_id
              AND external_payment_id = :external_payment_id
            FOR UPDATE;
        """)

        inserted = updated = 0
        with get_db_connection() as conn:
            with conn.begin():
                for _, row in df.iterrows():
                    params = self._validate_payment_row(row)
                    if params["original_payment_id"] is not None:
                        original_type = conn.execute(text("""
                            SELECT payment_type
                            FROM core.payment
                            WHERE payment_id = :payment_id
                              AND business_id = :business_id;
                        """), {
                            "payment_id": params["original_payment_id"],
                            "business_id": self.business_id,
                        }).scalar_one_or_none()
                        if original_type is None:
                            raise ValueError(
                                "original_payment_id must reference a payment in the same tenant."
                            )
                        if original_type != "payment":
                            raise ValueError(
                                "original_payment_id must reference a non-refund payment."
                            )
                    existing = conn.execute(find_existing, params).fetchone()
                    if existing is not None:
                        old_provider, old_account, old_currency = (
                            existing[0], existing[1], existing[2]
                        )
                        if (
                            old_provider is None
                            or old_account is None
                            or old_currency is None
                        ):
                            raise ValueError(
                                "Cannot safely replay a legacy payment without verified provider, "
                                "account, and currency provenance."
                            )
                        if (
                            old_provider != params["provider"]
                            or old_account != params["provider_account_identifier"]
                            or old_currency.strip() != params["currency"]
                        ):
                            raise ValueError(
                                "External payment ID already belongs to a different provider identity."
                            )
                        expected_existing_values = (
                            params["order_id"],
                            params["payment_method"],
                            params["payment_type"],
                            params["amount"],
                            params["gateway_fee"],
                            params["original_payment_id"],
                        )
                        for old, expected in zip(existing[3:], expected_existing_values):
                            if old != expected:
                                raise ValueError(
                                    "Payment replay conflicts with existing transaction facts."
                                )

                    result = conn.execute(upsert_sql, params)
                    row_result = result.fetchone()
                    if row_result and row_result[0]:
                        inserted += 1
                    elif row_result:
                        updated += 1
                    else:
                        raise ValueError(
                            "Concurrent payment replay conflicts with existing transaction facts."
                        )
        return inserted, updated

    def _validate_payment_row(self, row: Any) -> Dict[str, Any]:
        business_id = row.get("business_id")
        if business_id is not None and not pd.isna(business_id) and int(business_id) != self.business_id:
            raise ValueError("Payment business_id does not match the server-resolved tenant.")
        order_id = row.get("order_id")
        if order_id is None or pd.isna(order_id) or int(order_id) <= 0:
            raise ValueError("A valid tenant-scoped order_id is required.")
        payment_id = _required_text(row.get("external_payment_id"), "external_payment_id")
        method = _required_text(row.get("payment_method"), "payment_method")
        payment_type = _required_text(row.get("payment_type"), "payment_type")
        status = _required_text(row.get("payment_status"), "payment_status")
        if method not in VALID_PAYMENT_METHODS:
            raise ValueError(f"Unsupported payment_method: {method}")
        if payment_type not in VALID_PAYMENT_TYPES:
            raise ValueError(f"Unsupported payment_type: {payment_type}")
        if status not in VALID_PAYMENT_STATUSES:
            raise ValueError(f"Unsupported payment_status: {status}")
        amount = _decimal(row.get("amount"), "amount")
        fee = _decimal(row.get("gateway_fee"), "gateway_fee")
        if amount is None or fee is None:
            raise ValueError("Payment amount and gateway_fee are required.")
        if amount < 0 or fee < 0:
            raise ValueError("Payment amount and gateway_fee must be nonnegative.")
        _fits_numeric(amount, 12, 2, "amount")
        _fits_numeric(fee, 12, 2, "gateway_fee")
        provider = _required_text(row.get("provider"), "provider").lower()
        account = _required_text(row.get("provider_account_identifier"), "provider_account_identifier")
        currency = _currency(row.get("currency"), "currency")

        original_id_value = row.get("original_payment_id")
        original_id = None
        if original_id_value is not None and not pd.isna(original_id_value):
            if payment_type != "refund":
                raise ValueError("original_payment_id is valid only for a refund.")
            original_id = int(original_id_value)
            if original_id <= 0:
                raise ValueError("original_payment_id must be a positive canonical payment ID.")

        return {
            "business_id": self.business_id,
            "order_id": int(order_id),
            "external_payment_id": payment_id,
            "payment_method": method,
            "payment_type": payment_type,
            "payment_status": status,
            "amount": amount,
            "gateway_fee": fee,
            "paid_at": row.get("paid_at") if pd.notna(row.get("paid_at")) else None,
            "provider": provider,
            "provider_account_identifier": account,
            "currency": currency,
            "original_payment_id": original_id,
        }

    def load_settlement(
        self,
        settlement: Any,
        entries: list[Any],
    ) -> Tuple[int, int]:
        """Persist a verified settlement and its entries atomically and idempotently."""
        source = _record_dict(settlement)
        supplied_business = source.get("business_id")
        if (
            supplied_business is not None
            and not pd.isna(supplied_business)
            and int(supplied_business) != self.business_id
        ):
            raise ValueError("Settlement business_id does not match the server-resolved tenant.")
        settlement_row = self._validate_settlement(source)
        entry_rows = [self._validate_settlement_entry(_record_dict(entry)) for entry in entries]

        with get_db_connection() as conn:
            with conn.begin():
                settlement_id, _ = self._upsert_settlement(conn, settlement_row)
                inserted_entries = 0
                for entry in entry_rows:
                    entry["business_id"] = self.business_id
                    entry["settlement_id"] = settlement_id
                    inserted_entries += self._upsert_settlement_entry(conn, entry)
        return settlement_id, inserted_entries

    def _validate_settlement(self, source: Dict[str, Any]) -> Dict[str, Any]:
        provider = _required_text(source.get("provider"), "provider").lower()
        account = _required_text(source.get("provider_account_identifier"), "provider_account_identifier")
        currency = _currency(source.get("currency"), "settlement currency")
        status = _required_text(source.get("status"), "settlement status")
        if status not in VALID_SETTLEMENT_STATUSES:
            raise ValueError(f"Unsupported settlement status: {status}")
        amounts: Dict[str, Optional[Decimal]] = {}
        for key in ("gross_amount", "fee_amount", "adjustment_amount", "net_amount"):
            amounts[key] = _decimal(source.get(key), key, optional=True)
        for key in ("gross_amount", "fee_amount"):
            amount = amounts[key]
            if amount is not None and amount < 0:
                raise ValueError(f"{key} must be nonnegative.")
        for key, amount in amounts.items():
            if amount is not None:
                _fits_numeric(amount, 20, 6, key)
        external_id = source.get("external_settlement_id")
        external_id = str(external_id).strip() if external_id is not None and str(external_id).strip() else None
        return {
            **amounts,
            "provider": provider,
            "provider_account_identifier": account,
            "external_settlement_id": external_id,
            "currency": currency,
            "status": status,
            "settled_at": self._timestamp(source.get("settled_at"), "settled_at"),
            "expected_arrival_at": self._timestamp(
                source.get("expected_arrival_at"), "expected_arrival_at"
            ),
            "provider_status": source.get("provider_status"),
        }

    def _validate_settlement_entry(self, source: Dict[str, Any]) -> Dict[str, Any]:
        entry_type = _required_text(source.get("entry_type"), "entry_type")
        if entry_type not in VALID_SETTLEMENT_ENTRY_TYPES:
            raise ValueError(f"Unsupported settlement entry_type: {entry_type}")
        payment_id = source.get("payment_id")
        if payment_id is not None and not pd.isna(payment_id):
            payment_id = int(payment_id)
            if payment_id <= 0:
                raise ValueError("payment_id must be a positive canonical payment ID.")
        else:
            payment_id = None
        signed_amount = _decimal(source.get("signed_amount"), "signed_amount")
        if signed_amount is None:
            raise ValueError("signed_amount is required.")
        _fits_numeric(signed_amount, 20, 6, "signed_amount")
        return {
            "payment_id": payment_id,
            "external_entry_id": self._optional_text(source.get("external_entry_id")),
            "external_transaction_id": self._optional_text(source.get("external_transaction_id")),
            "entry_type": entry_type,
            "signed_amount": signed_amount,
            "currency": _currency(source.get("currency"), "entry currency"),
            "provider_status": self._optional_text(source.get("provider_status")),
        }

    @staticmethod
    def _optional_text(value: Any) -> Optional[str]:
        if value is None or pd.isna(value) or not str(value).strip():
            return None
        return str(value).strip()

    @staticmethod
    def _timestamp(value: Any, field: str) -> Optional[datetime]:
        if value is None or pd.isna(value) or not str(value).strip():
            return None
        try:
            parsed = value if isinstance(value, datetime) else datetime.fromisoformat(str(value))
        except ValueError:
            raise ValueError(f"{field} must be an ISO-8601 timestamp.") from None
        if parsed.tzinfo is None:
            raise ValueError(f"{field} must include an explicit timezone.")
        return parsed

    def _upsert_settlement(self, conn: Any, row: Dict[str, Any]) -> Tuple[int, bool]:
        insert = text("""
            INSERT INTO core.payment_settlement (
                business_id, provider, provider_account_identifier,
                external_settlement_id, currency, gross_amount, fee_amount,
                adjustment_amount, net_amount, status, settled_at,
                expected_arrival_at, provider_status
            )
            VALUES (
                :business_id, :provider, :provider_account_identifier,
                :external_settlement_id, :currency, :gross_amount, :fee_amount,
                :adjustment_amount, :net_amount, :status, :settled_at,
                :expected_arrival_at, :provider_status
            )
            ON CONFLICT (
                business_id, provider, provider_account_identifier, external_settlement_id
            ) WHERE provider_account_identifier IS NOT NULL
                  AND external_settlement_id IS NOT NULL
            DO NOTHING
            RETURNING settlement_id;
        """)
        select = text("""
            SELECT settlement_id, currency, gross_amount, fee_amount,
                   adjustment_amount, net_amount, status, settled_at,
                   expected_arrival_at, provider_status
            FROM core.payment_settlement
            WHERE business_id = :business_id
              AND provider = :provider
              AND provider_account_identifier = :provider_account_identifier
              AND external_settlement_id = :external_settlement_id
            FOR UPDATE;
        """)
        params = {"business_id": self.business_id, **row}
        result = conn.execute(insert, params).fetchone()
        if result:
            return int(result[0]), True
        if row["external_settlement_id"] is None:
            raise RuntimeError("Settlement insert did not return an ID.")
        existing = conn.execute(select, params).fetchone()
        if existing is None:
            raise RuntimeError("Conflicting settlement disappeared during idempotent upsert.")
        keys = (
            "currency", "gross_amount", "fee_amount", "adjustment_amount",
            "net_amount", "status", "settled_at", "expected_arrival_at",
            "provider_status",
        )
        for key, existing_value in zip(keys, existing[1:]):
            expected = row[key]
            if key.endswith("_amount") and existing_value is not None and expected is not None:
                if Decimal(existing_value) != expected:
                    raise ValueError(f"Conflicting settlement replay changes provider fact {key}.")
            elif existing_value != expected:
                raise ValueError(f"Conflicting settlement replay changes provider fact {key}.")
        return int(existing[0]), False

    def _upsert_settlement_entry(self, conn: Any, row: Dict[str, Any]) -> int:
        if row["external_entry_id"] is None:
            conn.execute(text("""
                INSERT INTO core.payment_settlement_entry (
                    business_id, settlement_id, payment_id, external_entry_id,
                    external_transaction_id, entry_type, signed_amount, currency,
                    provider_status
                )
                VALUES (
                    :business_id, :settlement_id, :payment_id, :external_entry_id,
                    :external_transaction_id, :entry_type, :signed_amount, :currency,
                    :provider_status
                );
            """), row)
            return 1

        insert = text("""
            INSERT INTO core.payment_settlement_entry (
                business_id, settlement_id, payment_id, external_entry_id,
                external_transaction_id, entry_type, signed_amount, currency,
                provider_status
            )
            VALUES (
                :business_id, :settlement_id, :payment_id, :external_entry_id,
                :external_transaction_id, :entry_type, :signed_amount, :currency,
                :provider_status
            )
            ON CONFLICT (business_id, settlement_id, external_entry_id)
                WHERE external_entry_id IS NOT NULL
            DO NOTHING
            RETURNING settlement_entry_id;
        """)
        result = conn.execute(insert, row).fetchone()
        if result:
            return 1
        existing = conn.execute(text("""
            SELECT payment_id, external_transaction_id, entry_type, signed_amount,
                   currency, provider_status
            FROM core.payment_settlement_entry
            WHERE business_id = :business_id
              AND settlement_id = :settlement_id
              AND external_entry_id = :external_entry_id
            FOR UPDATE;
        """), row).fetchone()
        if existing is None:
            raise RuntimeError("Conflicting settlement entry disappeared during idempotent upsert.")
        expected_values = (
            row["payment_id"], row["external_transaction_id"], row["entry_type"],
            row["signed_amount"], row["currency"], row["provider_status"],
        )
        for old, new in zip(existing, expected_values):
            if old != new:
                raise ValueError("Conflicting settlement-entry replay changes provider facts.")
        return 0
