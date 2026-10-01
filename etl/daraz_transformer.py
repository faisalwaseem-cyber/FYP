"""
Transforms Daraz Open Platform API responses into canonical DataFrames.

Canonical Models Mapped:
1. core.customer:
   - Derives full_name, phone, email, city from order shipping address.
   - Enforces PostgreSQL E.164 phone constraint (^+92[0-9]{10}$) by generating
     a deterministic synthetic phone proxy when Daraz masks phone (+92300*****67).
2. core.product:
   - Standardizes external_product_id (daraz_prod_{sku}), sku, product_name, price.
3. core.orders:
   - Normalizes Daraz order statuses to canonical enum ('pending', 'confirmed', 'fulfilled', 'delivered', 'cancelled', 'returned', 'rto').
   - Normalizes payment methods ('cod', 'card', 'bank_transfer', 'wallet', 'other').
   - Preserves exact 2-decimal monetary precision matching NUMERIC(12,2).
4. core.order_item:
   - Standardizes quantities (>= 1), unit prices, discounts.
5. core.inventory:
   - Standardizes stock snapshot levels per SKU/product for Daraz fulfillment locations.
"""
import hashlib
from datetime import date, datetime
from decimal import Decimal
from typing import Any, Dict, List, Optional, Tuple

import pandas as pd

from etl.normalizers import normalize_pakistan_phone
from etl.validator import DataValidator


def normalize_daraz_status(raw_status: Optional[str]) -> str:
    """
    Standardizes Daraz order status to canonical database enum:
    ('pending', 'confirmed', 'fulfilled', 'delivered', 'cancelled', 'returned', 'rto')
    """
    s = str(raw_status or "").strip().lower()
    if s == "unpaid":
        return "pending"
    elif s in ("pending", "ready_to_ship"):
        return "confirmed"
    elif s in ("shipped", "toship"):
        return "fulfilled"
    elif s == "delivered":
        return "delivered"
    elif s in ("canceled", "cancelled"):
        return "cancelled"
    elif s in ("failed", "failed_delivery"):
        return "rto"
    elif s in ("returned", "returned_to_shipper"):
        return "returned"
    return "pending"


def normalize_daraz_payment_method(raw_method: Optional[str]) -> str:
    """
    Standardizes Daraz payment method to canonical enum:
    ('cod', 'card', 'bank_transfer', 'wallet', 'other')
    """
    s = str(raw_method or "").strip().lower()
    if any(k in s for k in ("wallet", "easypaisa", "jazzcash", "jazz cash", "easy paisa")):
        return "wallet"
    elif any(k in s for k in ("card", "visa", "master", "credit", "debit")):
        return "card"
    elif any(k in s for k in ("bank", "transfer", "ibft", "wire")):
        return "bank_transfer"
    elif "cod" in s or "cash" in s:
        return "cod"
    return "other"


def parse_daraz_money(val: Any) -> float:
    """
    Parses a monetary value into a safe float rounded to 2 decimals,
    preserving NUMERIC(12,2) database precision.
    """
    if val is None:
        return 0.0
    try:
        dec = Decimal(str(val).strip().replace(",", ""))
        return max(0.0, float(round(dec, 2)))
    except Exception:
        return 0.0


class DarazTransformer:
    """
    Transforms Daraz API payloads into clean canonical Pandas DataFrames.
    Maintains business_id isolation throughout all transformations.
    """

    def __init__(self, business_id: int):
        self.business_id = business_id

    # ------------------------------------------------------------------
    # Orders Transformation
    # ------------------------------------------------------------------

    def transform(
        self,
        raw_orders: List[Dict[str, Any]],
    ) -> Tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame, pd.DataFrame]:
        """Alias for transform_orders for backward compatibility."""
        return self.transform_orders(raw_orders)

    def transform_orders(
        self,
        raw_orders: List[Dict[str, Any]],
    ) -> Tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame, pd.DataFrame]:
        """
        Transforms Daraz order objects into canonical DataFrames:
        (df_customers, df_products, df_orders, df_items).
        """
        customers_list = []
        products_list = []
        orders_list = []
        order_items_list = []

        for o in raw_orders:
            ext_order_id = str(o.get("order_id") or o.get("order_number") or "").strip()
            if not ext_order_id:
                continue

            order_date = o.get("created_at")
            address_info = o.get("address_shipping") or {}

            # 1. Customer Normalization
            raw_phone = address_info.get("phone") or o.get("customer_phone")
            phone = normalize_pakistan_phone(raw_phone)

            # Daraz often masks phone numbers (+92300*****67).
            # Generate deterministic synthetic E.164 phone to satisfy constraint ^\+92[0-9]{10}$
            if not phone:
                phone_hash = hashlib.sha256(ext_order_id.encode("utf-8")).hexdigest()
                synthetic_digits = f"{int(phone_hash, 16) % 1000000000:09d}"
                phone = f"+923{synthetic_digits}"

            first_name = address_info.get("first_name") or o.get("customer_first_name") or ""
            last_name = address_info.get("last_name") or o.get("customer_last_name") or ""
            full_name = f"{first_name} {last_name}".strip() or "Daraz Customer"
            city = address_info.get("city") or "Pakistan"

            customers_list.append({
                "business_id": self.business_id,
                "external_customer_id": f"daraz_cust_{ext_order_id}",
                "full_name": full_name,
                "phone": phone,
                "email": o.get("customer_email"),
                "city": city,
            })

            # 2. Order Normalization
            statuses = o.get("statuses")
            raw_status = next((status for status in statuses if status), None) if isinstance(statuses, list) else o.get("status")
            order_status = normalize_daraz_status(raw_status)
            payment_method = normalize_daraz_payment_method(o.get("payment_method", "cod"))

            total_amount = parse_daraz_money(o.get("price"))
            shipping_fee = parse_daraz_money(o.get("shipping_fee"))
            subtotal = max(0.0, float(round(Decimal(str(total_amount)) - Decimal(str(shipping_fee)), 2)))

            orders_list.append({
                "business_id": self.business_id,
                "customer_phone": phone,
                "external_order_id": f"daraz_{ext_order_id}",
                "order_date": order_date,
                "order_status": order_status,
                "payment_method": payment_method,
                "subtotal_amount": subtotal,
                "discount_amount": 0.0,
                "shipping_charged": shipping_fee,
                "total_amount": total_amount,
                "currency": "PKR",
                "shipping_city": city,
                "shipping_province": address_info.get("country", "Pakistan"),
                "source_platform": "daraz",
            })

            # 3. Line Items & Products
            items = o.get("items") or o.get("order_items") or []
            for item in items:
                sku = item.get("sku") or f"DARAZ_SKU_{item.get('sku_id', 'UNKNOWN')}"
                prod_name = item.get("name") or "Daraz Marketplace Product"
                item_price = parse_daraz_money(item.get("item_price") or item.get("paid_price"))
                ext_prod_id = f"daraz_prod_{sku}"

                products_list.append({
                    "business_id": self.business_id,
                    "external_product_id": ext_prod_id,
                    "sku": sku,
                    "product_name": prod_name,
                    "selling_price": item_price,
                    "unit_cost": 0.0,
                    "is_active": True,
                })

                order_items_list.append({
                    "business_id": self.business_id,
                    "external_order_id": f"daraz_{ext_order_id}",
                    "external_product_id": ext_prod_id,
                    "quantity": max(1, int(item.get("quantity") or item.get("qty") or 1)),
                    "unit_price": item_price,
                    "discount_amount": 0.0,
                    "unit_cost_at_sale": 0.0,
                })

        df_c = pd.DataFrame(customers_list).drop_duplicates(subset=["business_id", "phone"]) if customers_list else pd.DataFrame(columns=["business_id", "external_customer_id", "full_name", "phone", "email", "city"])
        df_p = pd.DataFrame(products_list).drop_duplicates(subset=["business_id", "external_product_id"]) if products_list else pd.DataFrame(columns=["business_id", "external_product_id", "sku", "product_name", "selling_price", "unit_cost", "is_active"])
        df_o = pd.DataFrame(orders_list).drop_duplicates(subset=["business_id", "external_order_id"]) if orders_list else pd.DataFrame(columns=["business_id", "customer_phone", "external_order_id", "order_date", "order_status", "payment_method", "subtotal_amount", "discount_amount", "shipping_charged", "total_amount", "currency", "shipping_city", "shipping_province", "source_platform"])
        df_i = pd.DataFrame(order_items_list) if order_items_list else pd.DataFrame(columns=["business_id", "external_order_id", "external_product_id", "quantity", "unit_price", "discount_amount", "unit_cost_at_sale"])

        if not df_c.empty:
            df_c = DataValidator.validate_customers(df_c)
        if not df_p.empty:
            df_p = DataValidator.validate_products(df_p)
        if not df_o.empty:
            df_o = DataValidator.validate_orders(df_o)

        return df_c, df_p, df_o, df_i

    # ------------------------------------------------------------------
    # Products Catalog Transformation
    # ------------------------------------------------------------------

    def transform_products(
        self,
        raw_products: List[Dict[str, Any]],
    ) -> pd.DataFrame:
        """
        Transforms Daraz product catalog items (/products/get) into canonical df_products.
        """
        products_list = []

        for p in raw_products:
            prod_name = p.get("attributes", {}).get("name") or p.get("name") or "Daraz Product"
            skus = p.get("skus") or p.get("Skus") or []

            if skus:
                for sku in skus:
                    seller_sku = str(sku.get("SellerSku") or sku.get("seller_sku") or sku.get("ShopSku") or "").strip()
                    if not seller_sku:
                        seller_sku = f"DARAZ_SKU_{p.get('item_id', 'UNKNOWN')}"

                    price = parse_daraz_money(sku.get("price") or sku.get("special_price"))
                    status = str(sku.get("Status") or sku.get("status") or p.get("status") or "active").lower()
                    is_active = (status in ("active", "published"))

                    products_list.append({
                        "business_id": self.business_id,
                        "external_product_id": f"daraz_prod_{seller_sku}",
                        "sku": seller_sku,
                        "product_name": prod_name,
                        "selling_price": price,
                        "unit_cost": 0.0,
                        "is_active": is_active,
                    })
            else:
                item_id = str(p.get("item_id") or p.get("id") or "UNKNOWN")
                products_list.append({
                    "business_id": self.business_id,
                    "external_product_id": f"daraz_prod_{item_id}",
                    "sku": f"DARAZ_ITEM_{item_id}",
                    "product_name": prod_name,
                    "selling_price": 0.0,
                    "unit_cost": 0.0,
                    "is_active": True,
                })

        df = pd.DataFrame(products_list).drop_duplicates(subset=["business_id", "external_product_id"]) if products_list else pd.DataFrame(columns=["business_id", "external_product_id", "sku", "product_name", "selling_price", "unit_cost", "is_active"])
        if not df.empty:
            df = DataValidator.validate_products(df)
        return df

    # ------------------------------------------------------------------
    # Inventory Transformation
    # ------------------------------------------------------------------

    def transform_inventory(
        self,
        raw_inventory_items: List[Dict[str, Any]],
        location_name: str = "Daraz Warehouse",
        snapshot_date: Optional[date] = None,
    ) -> pd.DataFrame:
        """
        Transforms Daraz inventory level records into canonical df_inventory.
        Columns: ['business_id', 'external_product_id', 'location_name', 'snapshot_date', 'quantity_available']
        """
        records = []
        snap_date = snapshot_date or date.today()

        for inv in raw_inventory_items:
            seller_sku = str(inv.get("seller_sku") or inv.get("SellerSku") or inv.get("sku") or "").strip()
            if not seller_sku:
                continue

            qty = max(0, int(inv.get("quantity") or inv.get("Available") or 0))
            ext_prod_id = f"daraz_prod_{seller_sku}"

            records.append({
                "business_id": self.business_id,
                "external_product_id": ext_prod_id,
                "location_name": location_name,
                "snapshot_date": snap_date,
                "quantity_available": qty,
            })

        return (
            pd.DataFrame(records).drop_duplicates(
                subset=["business_id", "external_product_id", "location_name", "snapshot_date"]
            )
            if records
            else pd.DataFrame(columns=["business_id", "external_product_id", "location_name", "snapshot_date", "quantity_available"])
        )
