from datetime import date
from decimal import Decimal
from typing import Any, Dict, List, Optional, Tuple

import pandas as pd

from etl.normalizers import (
    normalize_order_status,
    normalize_currency_code,
    normalize_location,
    normalize_money,
    normalize_pakistan_phone,
    normalize_payment_method,
    normalize_timestamp,
    synthetic_pakistan_phone,
)
from etl.validator import DataValidator


def extract_numeric_id(gid_or_id: Any) -> str:
    """
    Extracts the source numeric identifier from a Shopify Global ID (GID)
    e.g. 'gid://shopify/Order/5589123456789' -> '5589123456789'
    or returns string as-is if already a clean identifier.
    """
    if not gid_or_id:
        return ""
    val = str(gid_or_id).strip()
    if val.startswith("gid://shopify/"):
        return val.rsplit("/", 1)[-1]
    return val


def extract_money_amount(money_set: Any) -> Decimal:
    """
    Extracts monetary decimal amount from a Shopify GraphQL MoneyV2 or PriceSet dict.
    e.g. {'shopMoney': {'amount': '4250.00', 'currencyCode': 'PKR'}} -> 4250.0
    Returns a Decimal matching NUMERIC(12,2).
    """
    if not money_set or not isinstance(money_set, dict):
        return Decimal("0.00")

    inner = money_set.get("shopMoney") or money_set
    if isinstance(inner, dict):
        amt_str = inner.get("amount")
        if amt_str is not None:
            return normalize_money(amt_str)
    return Decimal("0.00")


def normalize_graphql_order_status(order: Dict[str, Any]) -> str:
    """
    Maps Shopify GraphQL order attributes to canonical order_status enum:
    ('pending', 'confirmed', 'fulfilled', 'delivered', 'cancelled', 'returned', 'rto')
    """
    if order.get("cancelledAt") is not None:
        return "cancelled"

    fulfill = str(order.get("displayFulfillmentStatus") or "").upper()
    if fulfill == "FULFILLED":
        return "fulfilled"
    elif fulfill in ("PARTIALLY_FULFILLED", "IN_PROGRESS"):
        return "confirmed"

    finance = str(order.get("displayFinancialStatus") or "").upper()
    if finance in ("PAID", "AUTHORIZED"):
        return "confirmed"
    elif finance in ("REFUNDED", "PARTIALLY_REFUNDED"):
        return "returned"
    elif finance == "VOIDED":
        return "cancelled"

    return "pending"


class ShopifyTransformer:
    def __init__(self, business_id: int):
        self.business_id = business_id

    # ------------------------------------------------------------------
    # Legacy REST Transformer (Preserved 100% for backward-compatibility)
    # ------------------------------------------------------------------

    def transform(self, raw_orders: List[Dict[str, Any]]) -> Tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame, pd.DataFrame]:
        customers_list = []
        products_list = []
        orders_list = []
        order_items_list = []

        for o in raw_orders:
            ext_order_id = str(o["id"])
            order_date = normalize_timestamp(o.get("created_at"))
            shipping_addr = o.get("shipping_address") or {}
            cust = o.get("customer") or {}

            # --- 1. Customer Normalization ---
            raw_phone = cust.get("phone") or shipping_addr.get("phone") or o.get("phone")
            phone = normalize_pakistan_phone(raw_phone)

            # If phone is absent, generate a unique deterministic synthetic number
            # to satisfy core.customer NOT NULL and regex constraint: ^\+92[0-9]{10}$
            if not phone:
                phone = synthetic_pakistan_phone("shopify", ext_order_id)

            ext_customer_id = str(cust.get("id")) if cust.get("id") else f"guest_{ext_order_id}"
            full_name = f"{cust.get('first_name') or ''} {cust.get('last_name') or ''}".strip()
            if not full_name:
                full_name = shipping_addr.get("name") or "Guest Customer"

            customers_list.append({
                "business_id": self.business_id,
                "external_customer_id": ext_customer_id,
                "full_name": full_name,
                "phone": phone,
                "email": cust.get("email"),
                "city": shipping_addr.get("city")
            })

            # --- 2. Order Normalization ---
            order_status = normalize_order_status(o)
            payment_method = normalize_payment_method(o)

            subtotal = normalize_money(o.get("subtotal_price"))
            discounts = normalize_money(o.get("total_discounts"))

            shipping_charged = 0.0
            shipping_lines = o.get("shipping_lines") or []
            if shipping_lines:
                shipping_charged = sum((normalize_money(line.get("price")) for line in shipping_lines), Decimal("0.00"))
            elif o.get("total_shipping_price_set"):
                shipping_charged = normalize_money(o["total_shipping_price_set"].get("shop_money", {}).get("amount"))

            total_amount = normalize_money(o.get("total_price"))

            orders_list.append({
                "business_id": self.business_id,
                "customer_phone": phone,
                "external_order_id": ext_order_id,
                "order_date": order_date,
                "order_status": order_status,
                "payment_method": payment_method,
                "subtotal_amount": subtotal,
                "discount_amount": discounts,
                "shipping_charged": shipping_charged,
                "total_amount": total_amount,
                "currency": normalize_currency_code(o.get("currency")),
                "shipping_city": normalize_location(shipping_addr.get("city")),
                "shipping_province": normalize_location(shipping_addr.get("province")),
                "source_platform": "shopify"
            })

            # --- 3. Line Items & Products ---
            for item in o.get("line_items", []):
                prod_id = item.get("product_id")
                var_id = item.get("variant_id")
                ext_prod_id = str(prod_id) if prod_id else f"var_{var_id}"
                unit_price = normalize_money(item.get("price"))

                products_list.append({
                    "business_id": self.business_id,
                    "external_product_id": ext_prod_id,
                    "sku": item.get("sku") or f"NO_SKU_{ext_prod_id}",
                    "product_name": item.get("title") or item.get("name") or "Unknown Product",
                    "selling_price": unit_price,
                    "unit_cost": 0.0,
                    "is_active": True
                })

                order_items_list.append({
                    "business_id": self.business_id,
                    "external_order_id": ext_order_id,
                    "external_product_id": ext_prod_id,
                    "quantity": max(1, int(item.get("quantity") or 1)),
                    "unit_price": unit_price,
                    "discount_amount": normalize_money(item.get("total_discount")),
                    "unit_cost_at_sale": 0.0
                })

        df_customers = pd.DataFrame(customers_list).drop_duplicates(subset=["business_id", "phone"])
        df_products = pd.DataFrame(products_list).drop_duplicates(subset=["business_id", "external_product_id"])
        df_orders = pd.DataFrame(orders_list).drop_duplicates(subset=["business_id", "external_order_id"])
        df_items = pd.DataFrame(order_items_list)

        # Validate constraints
        df_customers = DataValidator.validate_customers(df_customers)
        df_products = DataValidator.validate_products(df_products)
        df_orders = DataValidator.validate_orders(df_orders)

        return df_customers, df_products, df_orders, df_items

    # ------------------------------------------------------------------
    # GraphQL Transformers (Phase 2)
    # ------------------------------------------------------------------

    def transform_graphql_orders(
        self,
        raw_orders: List[Dict[str, Any]],
    ) -> Tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame, pd.DataFrame]:
        """
        Transforms Shopify GraphQL order nodes into canonical DataFrames:
        (df_customers, df_products, df_orders, df_items).
        """
        customers_list = []
        products_list = []
        orders_list = []
        order_items_list = []

        for o in raw_orders:
            ext_order_id = extract_numeric_id(o.get("id"))
            order_date = normalize_timestamp(o.get("createdAt"))
            shipping_addr = o.get("shippingAddress") or {}
            cust = o.get("customer") or {}

            # 1. Customer Normalization
            raw_phone = cust.get("phone") or shipping_addr.get("phone")
            phone = normalize_pakistan_phone(raw_phone)

            # If phone is absent, generate deterministic synthetic phone for guest checkout
            if not phone:
                phone = synthetic_pakistan_phone("shopify", ext_order_id)

            cust_gid = cust.get("id")
            ext_customer_id = (
                extract_numeric_id(cust_gid) if cust_gid else f"guest_{ext_order_id}"
            )

            full_name = f"{cust.get('firstName') or ''} {cust.get('lastName') or ''}".strip()
            if not full_name:
                full_name = shipping_addr.get("name") or "Guest Customer"

            if not full_name or full_name == "":
                full_name = "Guest Customer"

            customers_list.append({
                "business_id": self.business_id,
                "external_customer_id": ext_customer_id,
                "full_name": full_name,
                "phone": phone,
                "email": cust.get("email"),
                "city": normalize_location(shipping_addr.get("city")),
            })

            # 2. Order Normalization
            order_status = normalize_graphql_order_status(o)
            payment_method = normalize_payment_method({
                "payment_gateway_names": o.get("paymentGatewayNames", []),
                "gateway": (o.get("paymentGatewayNames") or [""])[0]
                if o.get("paymentGatewayNames")
                else "",
            })

            subtotal = extract_money_amount(o.get("currentSubtotalPriceSet"))
            discounts = extract_money_amount(o.get("currentTotalDiscountsSet"))
            shipping_charged = extract_money_amount(o.get("totalShippingPriceSet"))
            total_amount = extract_money_amount(o.get("currentTotalPriceSet"))

            currency = normalize_currency_code(o.get("currencyCode"))

            orders_list.append({
                "business_id": self.business_id,
                "customer_phone": phone,
                "external_order_id": ext_order_id,
                "order_date": order_date,
                "order_status": order_status,
                "payment_method": payment_method,
                "subtotal_amount": subtotal,
                "discount_amount": discounts,
                "shipping_charged": shipping_charged,
                "total_amount": total_amount,
                "currency": currency,
                "shipping_city": normalize_location(shipping_addr.get("city")),
                "shipping_province": normalize_location(shipping_addr.get("province")),
                "source_platform": "shopify",
            })

            # 3. Line Items & Products
            # Support both edges and direct nodes
            line_items_container = o.get("lineItems") or {}
            items_edges = (
                line_items_container.get("edges")
                if isinstance(line_items_container, dict)
                else []
            ) or []

            raw_items = [
                e.get("node") if isinstance(e, dict) and "node" in e else e
                for e in items_edges
            ] if items_edges else (
                line_items_container.get("nodes", [])
                if isinstance(line_items_container, dict)
                else []
            )

            for item in raw_items:
                prod = item.get("product") or {}
                var = item.get("variant") or {}

                prod_gid = prod.get("id")
                var_gid = var.get("id")

                ext_prod_id = (
                    extract_numeric_id(prod_gid)
                    if prod_gid
                    else (extract_numeric_id(var_gid) if var_gid else f"line_{extract_numeric_id(item.get('id'))}")
                )

                unit_price = extract_money_amount(item.get("originalUnitPriceSet"))
                item_discount = extract_money_amount(item.get("totalDiscountSet"))
                sku = item.get("sku") or (var.get("sku") if var else None) or f"NO_SKU_{ext_prod_id}"
                title = item.get("title") or "Unknown Product"

                products_list.append({
                    "business_id": self.business_id,
                    "external_product_id": ext_prod_id,
                    "sku": sku,
                    "product_name": title,
                    "selling_price": unit_price,
                    "unit_cost": 0.0,
                    "is_active": True,
                })

                order_items_list.append({
                    "business_id": self.business_id,
                    "external_order_id": ext_order_id,
                    "external_product_id": ext_prod_id,
                    "quantity": max(1, int(item.get("quantity") or 1)),
                    "unit_price": unit_price,
                    "discount_amount": item_discount,
                    "unit_cost_at_sale": 0.0,
                })

        df_customers = (
            pd.DataFrame(customers_list).drop_duplicates(subset=["business_id", "phone"])
            if customers_list
            else pd.DataFrame(columns=["business_id", "external_customer_id", "full_name", "phone", "email", "city"])
        )
        df_products = (
            pd.DataFrame(products_list).drop_duplicates(subset=["business_id", "external_product_id"])
            if products_list
            else pd.DataFrame(columns=["business_id", "external_product_id", "sku", "product_name", "selling_price", "unit_cost", "is_active"])
        )
        df_orders = (
            pd.DataFrame(orders_list).drop_duplicates(subset=["business_id", "external_order_id"])
            if orders_list
            else pd.DataFrame(columns=["business_id", "customer_phone", "external_order_id", "order_date", "order_status", "payment_method", "subtotal_amount", "discount_amount", "shipping_charged", "total_amount", "currency", "shipping_city", "shipping_province", "source_platform"])
        )
        df_items = (
            pd.DataFrame(order_items_list)
            if order_items_list
            else pd.DataFrame(columns=["business_id", "external_order_id", "external_product_id", "quantity", "unit_price", "discount_amount", "unit_cost_at_sale"])
        )

        # Validate constraints
        if not df_customers.empty:
            df_customers = DataValidator.validate_customers(df_customers)
        if not df_products.empty:
            df_products = DataValidator.validate_products(df_products)
        if not df_orders.empty:
            df_orders = DataValidator.validate_orders(df_orders)

        return df_customers, df_products, df_orders, df_items

    def transform_graphql_products(
        self,
        raw_products: List[Dict[str, Any]],
    ) -> pd.DataFrame:
        """
        Transforms standalone Shopify GraphQL product nodes into canonical df_products.
        """
        products_list = []
        for p in raw_products:
            ext_prod_id = extract_numeric_id(p.get("id"))
            title = p.get("title") or "Unknown Product"
            status = str(p.get("status") or "ACTIVE").upper()
            is_active = (status == "ACTIVE")

            variants_container = p.get("variants") or {}
            var_edges = variants_container.get("edges") or []
            var_nodes = [
                e.get("node") if isinstance(e, dict) and "node" in e else e
                for e in var_edges
            ] if var_edges else variants_container.get("nodes", [])

            if var_nodes:
                for v in var_nodes:
                    sku = v.get("sku") or f"NO_SKU_{ext_prod_id}"
                    price = normalize_money(v.get("price"))
                    products_list.append({
                        "business_id": self.business_id,
                        "external_product_id": ext_prod_id,
                        "sku": sku,
                        "product_name": title,
                        "selling_price": price,
                        "unit_cost": 0.0,
                        "is_active": is_active,
                    })
            else:
                products_list.append({
                    "business_id": self.business_id,
                    "external_product_id": ext_prod_id,
                    "sku": f"NO_SKU_{ext_prod_id}",
                    "product_name": title,
                    "selling_price": 0.0,
                    "unit_cost": 0.0,
                    "is_active": is_active,
                })

        df = (
            pd.DataFrame(products_list).drop_duplicates(subset=["business_id", "external_product_id"])
            if products_list
            else pd.DataFrame(columns=["business_id", "external_product_id", "sku", "product_name", "selling_price", "unit_cost", "is_active"])
        )
        if not df.empty:
            df = DataValidator.validate_products(df)
        return df

    def transform_graphql_customers(
        self,
        raw_customers: List[Dict[str, Any]],
    ) -> pd.DataFrame:
        """
        Transforms standalone Shopify GraphQL customer nodes into canonical df_customers.
        """
        customers_list = []
        for c in raw_customers:
            ext_cust_id = extract_numeric_id(c.get("id"))
            raw_phone = c.get("phone")
            phone = normalize_pakistan_phone(raw_phone)
            if not phone:
                # Need valid E.164 phone to satisfy core.customer NOT NULL
                phone = synthetic_pakistan_phone("shopify", ext_cust_id)

            first = c.get("firstName") or ""
            last = c.get("lastName") or ""
            full_name = f"{first} {last}".strip() or "Shopify Customer"

            addr = c.get("defaultAddress") or {}
            city = addr.get("city")

            customers_list.append({
                "business_id": self.business_id,
                "external_customer_id": ext_cust_id,
                "full_name": full_name,
                "phone": phone,
                "email": c.get("email"),
                "city": city,
            })

        df = (
            pd.DataFrame(customers_list).drop_duplicates(subset=["business_id", "phone"])
            if customers_list
            else pd.DataFrame(columns=["business_id", "external_customer_id", "full_name", "phone", "email", "city"])
        )
        if not df.empty:
            df = DataValidator.validate_customers(df)
        return df

    def transform_graphql_inventory(
        self,
        raw_inventory_levels: List[Dict[str, Any]],
        location_name: str,
        snapshot_date: Optional[date] = None,
    ) -> pd.DataFrame:
        """
        Transforms Shopify GraphQL inventory level nodes into canonical df_inventory.
        Columns: ['business_id', 'external_product_id', 'location_name', 'snapshot_date', 'quantity_available']
        """
        records = []
        snap_date = snapshot_date or date.today()

        for inv in raw_inventory_levels:
            quantities = inv.get("quantities") or []
            available = 0
            for q in quantities:
                if q.get("name") == "available":
                    available = int(q.get("quantity") or 0)
                    break

            item = inv.get("item") or {}
            item_gid = item.get("id")
            sku = item.get("sku") or ""
            # Product reference
            ext_prod_id = extract_numeric_id(item_gid) if item_gid else sku

            if ext_prod_id:
                records.append({
                    "business_id": self.business_id,
                    "external_product_id": ext_prod_id,
                    "location_name": location_name,
                    "snapshot_date": snap_date,
                    "quantity_available": max(0, available),
                })

        return (
            pd.DataFrame(records).drop_duplicates(
                subset=["business_id", "external_product_id", "location_name", "snapshot_date"]
            )
            if records
            else pd.DataFrame(columns=["business_id", "external_product_id", "location_name", "snapshot_date", "quantity_available"])
        )
