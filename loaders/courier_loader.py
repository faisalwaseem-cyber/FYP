"""
PostgreSQL Courier Loader.
Handles:
- Upserting couriers into core.courier
- Upserting shipments into core.shipment
- Cascading status updates to core.orders (e.g. marked as 'delivered' or 'rto')
- Recording Return-to-Origin records in core.rto_return
"""
from typing import Dict, Any, Optional
from sqlalchemy import text
from config.database import get_db_connection

class CourierLoader:
    def __init__(self, business_id: int):
        self.business_id = business_id

    def get_or_create_courier(self, courier_name: str) -> int:
        """
        Ensures courier exists in core.courier and returns courier_id.
        """
        clean_name = courier_name.strip().lower()
        query = text("""
            INSERT INTO core.courier (business_id, courier_name, is_active, created_at)
            VALUES (:business_id, :courier_name, TRUE, now())
            ON CONFLICT (business_id, courier_name) DO UPDATE
            SET is_active = TRUE
            RETURNING courier_id;
        """)
        with get_db_connection() as conn:
            result = conn.execute(query, {
                "business_id": self.business_id,
                "courier_name": clean_name
            })
            courier_id = result.scalar()
            conn.commit()
            return courier_id

    def load_shipment(self, shipment_data: Dict[str, Any], default_rto_penalty: float = 150.0) -> Optional[int]:
        """
        Loads shipment, updates order status, and logs RTO if returned.
        Returns:
            shipment_id if successfully loaded, None otherwise.
        """
        ext_order_id = shipment_data["external_order_id"]
        courier_name = shipment_data["courier_name"]
        tracking_num = shipment_data["tracking_number"]
        status = shipment_data["shipment_status"]

        if not tracking_num or not ext_order_id:
            print(f"[!] Skipping shipment: Missing tracking number or external order ID.")
            return None

        courier_id = self.get_or_create_courier(courier_name)

        find_order_sql = text("""
            SELECT order_id, order_status FROM core.orders
            WHERE business_id = :business_id AND external_order_id = :ext_order_id;
        """)

        shipment_upsert_sql = text("""
            INSERT INTO core.shipment (
                business_id, order_id, courier_id, tracking_number,
                shipment_status, shipped_at, delivered_at, shipping_cost,
                cod_amount, delivery_attempts, created_at
            )
            VALUES (
                :business_id, :order_id, :courier_id, :tracking_number,
                :shipment_status, CAST(:shipped_at AS TIMESTAMPTZ), CAST(:delivered_at AS TIMESTAMPTZ),
                :shipping_cost, :cod_amount, :delivery_attempts, now()
            )
            ON CONFLICT (business_id, courier_id, tracking_number) DO UPDATE
            SET shipment_status = EXCLUDED.shipment_status,
                delivered_at = COALESCE(EXCLUDED.delivered_at, core.shipment.delivered_at),
                shipped_at = COALESCE(EXCLUDED.shipped_at, core.shipment.shipped_at),
                shipping_cost = CASE WHEN EXCLUDED.shipping_cost > 0 THEN EXCLUDED.shipping_cost ELSE core.shipment.shipping_cost END,
                delivery_attempts = GREATEST(EXCLUDED.delivery_attempts, core.shipment.delivery_attempts)
            RETURNING shipment_id;
        """)

        update_order_sql = text("""
            UPDATE core.orders
            SET order_status = :new_order_status
            WHERE order_id = :order_id;
        """)

        insert_rto_sql = text("""
            INSERT INTO core.rto_return (
                business_id, order_id, shipment_id, return_type,
                return_reason, return_date, return_shipping_cost, refund_amount, created_at
            )
            SELECT :business_id, :order_id, :shipment_id, 'rto',
                   'Failed Delivery / RTO by Courier', now(), :return_cost, 0, now()
            WHERE NOT EXISTS (
                SELECT 1 FROM core.rto_return WHERE shipment_id = :shipment_id
            );
        """)

        with get_db_connection() as conn:
            # 1. Resolve order_id
            order_row = conn.execute(find_order_sql, {
                "business_id": self.business_id,
                "ext_order_id": ext_order_id
            }).fetchone()

            if not order_row:
                print(f"[!] Warning: Order '{ext_order_id}' not found in core.orders. Cannot link shipment {tracking_num}.")
                return None

            order_id = order_row[0]

            # 2. Upsert shipment
            result = conn.execute(shipment_upsert_sql, {
                "business_id": self.business_id,
                "order_id": order_id,
                "courier_id": courier_id,
                "tracking_number": tracking_num,
                "shipment_status": status,
                "shipped_at": shipment_data.get("shipped_at"),
                "delivered_at": shipment_data.get("delivered_at"),
                "shipping_cost": shipment_data.get("shipping_cost", 0.0),
                "cod_amount": shipment_data.get("cod_amount", 0.0),
                "delivery_attempts": shipment_data.get("delivery_attempts", 0)
            })
            shipment_id = result.scalar()

            # 3. Cascading Order Status Updates
            if status == "delivered":
                conn.execute(update_order_sql, {
                    "new_order_status": "delivered",
                    "order_id": order_id
                })
            elif status in ("returning", "returned"):
                conn.execute(update_order_sql, {
                    "new_order_status": "rto",
                    "order_id": order_id
                })
                # If courier marked returned, calculate and record RTO return cost
                if status == "returned":
                    conn.execute(insert_rto_sql, {
                        "business_id": self.business_id,
                        "order_id": order_id,
                        "shipment_id": shipment_id,
                        "return_cost": default_rto_penalty
                    })

            conn.commit()
            return shipment_id
