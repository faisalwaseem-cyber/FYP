"""
PostgreSQL Payment Loader.
Upserts payment records into core.payment and tracks inserted/updated metrics.
"""
from typing import Dict, Any, Tuple
import pandas as pd
from sqlalchemy import text
from config.database import get_db_connection


class PaymentLoader:
    def __init__(self, business_id: int):
        self.business_id = business_id

    def get_order_id_map(self) -> Dict[str, int]:
        """
        Retrieves a mapping of external_order_id -> internal order_id
        for the given business.
        """
        query = text("""
            SELECT external_order_id, order_id
            FROM core.orders
            WHERE business_id = :business_id;
        """)
        with get_db_connection() as conn:
            result = conn.execute(query, {"business_id": self.business_id})
            return {str(row[0]): int(row[1]) for row in result.fetchall()}

    def load_payments(self, df: pd.DataFrame) -> Tuple[int, int]:
        """
        Loads payments DataFrame into core.payment.
        Returns:
            (records_inserted, records_updated)
        """
        if df.empty:
            return 0, 0

        upsert_sql = text("""
            INSERT INTO core.payment (
                business_id,
                order_id,
                external_payment_id,
                payment_method,
                payment_type,
                payment_status,
                amount,
                gateway_fee,
                paid_at,
                created_at
            )
            VALUES (
                :business_id,
                :order_id,
                :external_payment_id,
                :payment_method,
                :payment_type,
                :payment_status,
                :amount,
                :gateway_fee,
                :paid_at,
                now()
            )
            ON CONFLICT (business_id, external_payment_id) DO UPDATE SET
                payment_method = EXCLUDED.payment_method,
                payment_type = EXCLUDED.payment_type,
                payment_status = EXCLUDED.payment_status,
                amount = EXCLUDED.amount,
                gateway_fee = EXCLUDED.gateway_fee,
                paid_at = EXCLUDED.paid_at
            RETURNING (xmax = 0) AS is_inserted;
        """)

        inserted = 0
        updated = 0

        with get_db_connection() as conn:
            for _, row in df.iterrows():
                ext_payment_id = row.get("external_payment_id")
                if not ext_payment_id:
                    # Deterministic fallback ID for manual/untracked payments per order
                    ext_payment_id = f"PAY-ORD-{row['order_id']}-{row['payment_type']}"

                params = {
                    "business_id": self.business_id,
                    "order_id": int(row["order_id"]),
                    "external_payment_id": str(ext_payment_id),
                    "payment_method": row["payment_method"],
                    "payment_type": row.get("payment_type", "payment"),
                    "payment_status": row.get("payment_status", "paid"),
                    "amount": float(row["amount"]),
                    "gateway_fee": float(row.get("gateway_fee", 0.0)),
                    "paid_at": row.get("paid_at") if pd.notnull(row.get("paid_at")) else None,
                }

                result = conn.execute(upsert_sql, params)
                row_res = result.fetchone()
                if row_res and row_res[0]:
                    inserted += 1
                else:
                    updated += 1

            conn.commit()

        return inserted, updated
