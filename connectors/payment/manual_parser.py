"""
Manual Payment Reconciliation Client.

For COD orders and any payment records that don't come from an online gateway,
this module reads structured JSON or CSV files provided manually by the merchant.

Supports two input formats:
1. CSV: transaction_id, order_id, amount, payment_method, status, paid_at
2. JSON list of payment objects

This is the fallback for Pakistani businesses using COD (Cash on Delivery),
which is the dominant payment method (~70-80% of e-commerce transactions in Pakistan).
"""
import csv
import io
import json
from typing import List, Dict, Any


class ManualPaymentParser:
    """
    Parses manually uploaded payment records (COD collections, bank transfers, etc.)
    """

    REQUIRED_FIELDS = ["external_order_id", "amount", "payment_method", "payment_status"]

    @staticmethod
    def _text(row: Dict[str, Any], key: str) -> str:
        value = row.get(key)
        return str(value).strip() if value is not None else ""

    def parse_json(self, json_content: str, gateway_name: str = "manual") -> List[Dict[str, Any]]:
        """
        Parses a JSON string (list of objects) into standardized records.
        Example JSON format:
        [
            {
                "external_payment_id": "MAN-001",
                "external_order_id": "ORD-999",
                "amount": 2500.00,
                "payment_method": "cod",
                "payment_status": "paid",
                "paid_at": "2024-01-15 18:00:00"
            }
        ]
        """
        records = json.loads(json_content)
        if not isinstance(records, list):
            raise ValueError("JSON content must be a list of payment objects.")

        if not all(isinstance(record, dict) for record in records):
            raise ValueError("Every JSON payment record must be an object.")
        return [dict(record) for record in records]

    def parse_csv(self, csv_content: str, gateway_name: str = "manual") -> List[Dict[str, Any]]:
        """
        Parses a CSV string. Expected columns:
        external_payment_id, external_order_id, amount, payment_method, payment_status, paid_at

        payment_method must be: cod, card, bank_transfer, wallet, other
        payment_status must be: pending, paid, failed
        """
        reader = csv.DictReader(io.StringIO(csv_content.strip()))
        records = []
        for row in reader:
            records.append({
                "provider": self._text(row, "provider") or None,
                "external_payment_id": self._text(row, "external_payment_id") or None,
                "external_order_id": self._text(row, "external_order_id") or None,
                "amount": self._text(row, "amount") or None,
                "gateway_fee": self._text(row, "gateway_fee") or None,
                "payment_method": self._text(row, "payment_method") or None,
                "payment_status": self._text(row, "payment_status") or None,
                "payment_type": self._text(row, "payment_type") or None,
                "paid_at": self._text(row, "paid_at") or None,
                "currency": self._text(row, "currency") or None,
                "provider_account_identifier": self._text(row, "provider_account_identifier") or None,
                "original_external_payment_id": self._text(row, "original_external_payment_id") or None,
            })
        return records
