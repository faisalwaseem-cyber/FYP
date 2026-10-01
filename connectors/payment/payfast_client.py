"""
PayFast Pakistan API Client (JazzCash / EasyPaisa / PayFast).

NOTE: PayFast (Pvt. Ltd., Pakistan) and EasyPaisa/JazzCash provide
settlement statements as CSV files through their merchant portals rather
than a real-time REST API in most cases.

This client provides:
1. A CSV parser for PayFast/EasyPaisa merchant settlement exports
2. A generic template for future webhook or API integration

PayFast CSV export columns (typical):
transaction_id, order_id, datetime, type, amount, status, fee, description
"""
import csv
import io
from typing import List, Dict, Any

class PayFastCSVParser:
    """
    Parses a PayFast / EasyPaisa merchant settlement CSV export file.
    """

    # Map column names from the CSV to our internal standard keys
    COLUMN_MAP = {
        "transaction_id": ["transaction_id", "txn_id", "txnid", "Transaction ID"],
        "order_id":       ["order_id", "order_ref", "orderid", "Order ID", "Reference"],
        "amount":         ["amount", "Amount", "total"],
        "fee":            ["fee", "Fee", "gateway_fee", "charges"],
        "status":         ["status", "Status", "payment_status"],
        "type":           ["type", "Type", "txn_type", "transaction_type"],
        "paid_at":        ["datetime", "date", "Date", "created_at", "Timestamp"],
    }

    def _resolve_column(self, headers: List[str], aliases: List[str]) -> str | None:
        for alias in aliases:
            if alias in headers:
                return alias
        return None

    def parse_csv(self, csv_content: str, gateway_name: str = "payfast") -> List[Dict[str, Any]]:
        """
        Parses CSV content (string) into a list of standardized payment dicts.
        """
        reader = csv.DictReader(io.StringIO(csv_content.strip()))
        headers = reader.fieldnames or []

        col_map = {
            internal_key: self._resolve_column(headers, aliases)
            for internal_key, aliases in self.COLUMN_MAP.items()
        }

        records = []
        for row in reader:
            records.append({
                "gateway_name":        gateway_name,
                "external_payment_id": row.get(col_map["transaction_id"], "").strip(),
                "external_order_id":   row.get(col_map["order_id"], "").strip(),
                "amount":              row.get(col_map["amount"], "0").replace(",", "").strip(),
                "gateway_fee":         row.get(col_map["fee"], "0").replace(",", "").strip(),
                "payment_status":      row.get(col_map["status"], "paid").strip(),
                "payment_type":        row.get(col_map["type"], "payment").strip(),
                "paid_at":             row.get(col_map["paid_at"], "").strip(),
            })
        return records
