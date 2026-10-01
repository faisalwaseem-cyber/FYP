import pandas as pd
from typing import Tuple
from sqlalchemy import text
from config.database import get_db_connection

class PostgresLoader:
    def __init__(self, business_id: int):
        self.business_id = business_id

    def load_customers(self, df_customers: pd.DataFrame):
        if df_customers.empty:
            return

        query = text("""
            INSERT INTO core.customer (
                business_id, external_customer_id, full_name, phone, email, city, created_at
            )
            VALUES (
                :business_id, :external_customer_id, :full_name, :phone, :email, :city, now()
            )
            ON CONFLICT (business_id, phone) DO UPDATE
            SET full_name = EXCLUDED.full_name,
                email = COALESCE(EXCLUDED.email, core.customer.email),
                city = COALESCE(EXCLUDED.city, core.customer.city);
        """)

        with get_db_connection() as conn:
            for _, r in df_customers.iterrows():
                conn.execute(query, {
                    "business_id": self.business_id,
                    "external_customer_id": r["external_customer_id"],
                    "full_name": r["full_name"],
                    "phone": r["phone"],
                    "email": r["email"] if pd.notnull(r["email"]) else None,
                    "city": r["city"] if pd.notnull(r["city"]) else None
                })
            conn.commit()

    def load_products(self, df_products: pd.DataFrame):
        if df_products.empty:
            return

        query = text("""
            INSERT INTO core.product (
                business_id, external_product_id, sku, product_name, selling_price, unit_cost, is_active, created_at, updated_at
            )
            VALUES (
                :business_id, :external_product_id, :sku, :product_name, :selling_price, :unit_cost, :is_active, now(), now()
            )
            ON CONFLICT (business_id, external_product_id) DO UPDATE
            SET product_name = EXCLUDED.product_name,
                sku = EXCLUDED.sku,
                selling_price = EXCLUDED.selling_price,
                updated_at = now();
        """)

        with get_db_connection() as conn:
            for _, r in df_products.iterrows():
                conn.execute(query, {
                    "business_id": self.business_id,
                    "external_product_id": r["external_product_id"],
                    "sku": r["sku"],
                    "product_name": r["product_name"],
                    "selling_price": float(r["selling_price"]),
                    "unit_cost": float(r["unit_cost"]),
                    "is_active": bool(r["is_active"])
                })
            conn.commit()

    def load_orders(self, df_orders: pd.DataFrame, df_items: pd.DataFrame) -> Tuple[int, int]:
        """
        Inserts or updates orders and associated items.
        Returns:
            (inserted_count, updated_count)
        """
        if df_orders.empty:
            return 0, 0

        order_upsert_query = text("""
            INSERT INTO core.orders (
                business_id, customer_id, external_order_id, order_date, order_status,
                payment_method, subtotal_amount, discount_amount, shipping_charged,
                total_amount, currency, shipping_city, shipping_province, source_platform, created_at
            )
            VALUES (
                :business_id,
                (SELECT customer_id FROM core.customer WHERE business_id = :business_id AND phone = :customer_phone LIMIT 1),
                :external_order_id,
                CAST(:order_date AS TIMESTAMPTZ),
                :order_status,
                :payment_method,
                :subtotal_amount,
                :discount_amount,
                :shipping_charged,
                :total_amount,
                :currency,
                :shipping_city,
                :shipping_province,
                :source_platform,
                now()
            )
            ON CONFLICT (business_id, external_order_id) DO UPDATE
            SET order_status = EXCLUDED.order_status,
                subtotal_amount = EXCLUDED.subtotal_amount,
                discount_amount = EXCLUDED.discount_amount,
                shipping_charged = EXCLUDED.shipping_charged,
                total_amount = EXCLUDED.total_amount,
                source_platform = EXCLUDED.source_platform
            RETURNING (xmax = 0) AS is_inserted;
        """)

        clean_items_query = text("""
            DELETE FROM core.order_item
            WHERE order_id = (
                SELECT order_id FROM core.orders 
                WHERE business_id = :business_id AND external_order_id = :external_order_id
            );
        """)

        item_insert_query = text("""
            INSERT INTO core.order_item (
                business_id, order_id, product_id, quantity, unit_price, discount_amount, unit_cost_at_sale, created_at
            )
            VALUES (
                :business_id,
                (SELECT order_id FROM core.orders WHERE business_id = :business_id AND external_order_id = :external_order_id LIMIT 1),
                (SELECT product_id FROM core.product WHERE business_id = :business_id AND external_product_id = :external_product_id LIMIT 1),
                :quantity,
                :unit_price,
                :discount_amount,
                :unit_cost_at_sale,
                now()
            );
        """)

        inserted = 0
        updated = 0

        with get_db_connection() as conn:
            for _, r in df_orders.iterrows():
                res = conn.execute(order_upsert_query, {
                    "business_id": self.business_id,
                    "customer_phone": r["customer_phone"],
                    "external_order_id": r["external_order_id"],
                    "order_date": r["order_date"],
                    "order_status": r["order_status"],
                    "payment_method": r["payment_method"],
                    "subtotal_amount": float(r["subtotal_amount"]),
                    "discount_amount": float(r["discount_amount"]),
                    "shipping_charged": float(r["shipping_charged"]),
                    "total_amount": float(r["total_amount"]),
                    "currency": r["currency"],
                    "shipping_city": r["shipping_city"] if pd.notnull(r["shipping_city"]) else None,
                    "shipping_province": r["shipping_province"] if pd.notnull(r["shipping_province"]) else None,
                    "source_platform": r.get("source_platform", "shopify")
                })
                is_ins = res.scalar()
                if is_ins:
                    inserted += 1
                else:
                    updated += 1

                # Refresh order items
                conn.execute(clean_items_query, {
                    "business_id": self.business_id,
                    "external_order_id": r["external_order_id"]
                })

                matching_items = df_items[df_items["external_order_id"] == r["external_order_id"]]
                for _, item_row in matching_items.iterrows():
                    conn.execute(item_insert_query, {
                        "business_id": self.business_id,
                        "external_order_id": item_row["external_order_id"],
                        "external_product_id": item_row["external_product_id"],
                        "quantity": int(item_row["quantity"]),
                        "unit_price": float(item_row["unit_price"]),
                        "discount_amount": float(item_row["discount_amount"]),
                        "unit_cost_at_sale": float(item_row["unit_cost_at_sale"])
                    })

            conn.commit()

        return inserted, updated

    def load_inventory(self, df_inventory: pd.DataFrame) -> Tuple[int, int]:
        """
        Idempotently inserts or updates inventory snapshot records into core.inventory.
        Links product_id via (business_id, external_product_id).

        Parameters
        ----------
        df_inventory : pd.DataFrame
            DataFrame with columns:
            ['business_id', 'external_product_id', 'location_name', 'snapshot_date', 'quantity_available']

        Returns
        -------
        Tuple[int, int]
            (inserted_count, updated_count)
        """
        if df_inventory.empty:
            return 0, 0

        find_product_sql = text("""
            SELECT product_id FROM core.product
            WHERE business_id = :business_id AND external_product_id = :external_product_id
            LIMIT 1;
        """)

        inventory_upsert_sql = text("""
            INSERT INTO core.inventory (
                business_id, product_id, location_name, snapshot_date, quantity_available, created_at
            )
            VALUES (
                :business_id, :product_id, :location_name, CAST(:snapshot_date AS DATE), :quantity_available, now()
            )
            ON CONFLICT (business_id, product_id, location_name, snapshot_date) DO UPDATE
            SET quantity_available = EXCLUDED.quantity_available
            RETURNING (xmax = 0) AS is_inserted;
        """)

        inserted = 0
        updated = 0

        with get_db_connection() as conn:
            for _, r in df_inventory.iterrows():
                ext_prod_id = str(r["external_product_id"])
                # Resolve product_id foreign key
                prod_row = conn.execute(find_product_sql, {
                    "business_id": self.business_id,
                    "external_product_id": ext_prod_id,
                }).fetchone()

                if not prod_row:
                    # Product not yet loaded in core.product, skip to maintain FK integrity
                    continue

                prod_id = prod_row[0]
                loc_name = str(r.get("location_name") or "Default Location").strip()
                if not loc_name:
                    loc_name = "Default Location"

                res = conn.execute(inventory_upsert_sql, {
                    "business_id": self.business_id,
                    "product_id": prod_id,
                    "location_name": loc_name,
                    "snapshot_date": r["snapshot_date"],
                    "quantity_available": int(r["quantity_available"]),
                })
                row = res.fetchone()
                if row:
                    if row[0]:
                        inserted += 1
                    else:
                        updated += 1

            conn.commit()

        return inserted, updated

