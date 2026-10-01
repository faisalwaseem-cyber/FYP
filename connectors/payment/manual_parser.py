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

        for r in records:
            r.setdefault("gateway_name", gateway_name)
            r.setdefault("external_payment_id", None)
            r.setdefault("gateway_fee", 0)
            r.setdefault("payment_type", "payment")
        return records

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
                "gateway_name":        gateway_name,
                "external_payment_id": row.get("external_payment_id", "").strip() or None,
                "external_order_id":   row.get("external_order_id", "").strip(),
                "amount":              row.get("amount", "0").strip(),
                "gateway_fee":         row.get("gateway_fee", "0").strip(),
                "payment_method":      row.get("payment_method", "other").strip(),
                "payment_status":      row.get("payment_status", "paid").strip(),
                "payment_type":        row.get("payment_type", "payment").strip(),
                "paid_at":             row.get("paid_at", "").strip(),
            })
        return records
