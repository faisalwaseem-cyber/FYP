"""
PayFast Pakistan API Client (JazzCash / EasyPaisa / PayFast).

NOTE: PayFast (Pvt. Ltd., Pakistan) and EasyPaisa/JazzCash provide
settlement statements as CSV files through their merchant portals rather
than a real-time REST API in most cases.

This client provides:
1. A CSV parser for PayFast/EasyPaisa merchant settlement exports
2. A generic template for future webhook or API integration

Canonical CSV input fields include provider_account_identifier, currency, and
gateway_fee when those facts are explicitly verified in the source.
"""
import csv
import io
from typing import Any, Dict, List, Sequence

class PayFastCSVParser:
    """
    Parses a PayFast / EasyPaisa merchant settlement CSV export file.
    """

    COLUMN_MAP = {
        "external_payment_id": ["external_payment_id"],
        "external_order_id": ["external_order_id"],
        "amount": ["amount"],
        "gateway_fee": ["gateway_fee"],
        "payment_status": ["payment_status"],
        "payment_type": ["payment_type"],
        "payment_method": ["payment_method"],
        "paid_at": ["paid_at"],
        "currency": ["currency"],
        "provider_account_identifier": ["provider_account_identifier"],
    }

    def _resolve_column(self, headers: Sequence[str], aliases: List[str]) -> str | None:
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
                "provider": gateway_name,
                "external_payment_id": (row.get(col_map["external_payment_id"]) or "").strip() or None,
                "external_order_id":   (row.get(col_map["external_order_id"]) or "").strip() or None,
                "amount":              (row.get(col_map["amount"]) or "").replace(",", "").strip() or None,
                "gateway_fee":         (row.get(col_map["gateway_fee"]) or "").replace(",", "").strip() or None,
                "payment_status":      (row.get(col_map["payment_status"]) or "").strip() or None,
                "payment_type":        (row.get(col_map["payment_type"]) or "").strip() or None,
                "payment_method":      (row.get(col_map["payment_method"]) or "").strip() or None,
                "paid_at":             (row.get(col_map["paid_at"]) or "").strip() or None,
                "currency":            (row.get(col_map["currency"]) or "").strip() or None,
                "provider_account_identifier": (
                    row.get(col_map["provider_account_identifier"]) or ""
                ).strip() or None,
            })
        return records
