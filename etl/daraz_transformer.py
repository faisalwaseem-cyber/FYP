"""Transform verified Daraz fields into schema-constrained canonical frames."""
import re
from collections.abc import Mapping
from datetime import datetime, timezone
from decimal import Decimal, InvalidOperation
from typing import Any, Dict, List, Optional, Tuple

import pandas as pd

from etl.normalizers import normalize_pakistan_phone

_CENT = Decimal("0.01")
_MAX_NUMERIC_12_2 = Decimal("9999999999.99")
_PAKISTAN_PHONE = re.compile(r"^\+92[0-9]{10}$")
_CURRENCY = re.compile(r"^[A-Z]{3}$")

CUSTOMER_COLUMNS = ["business_id", "external_customer_id", "full_name", "phone", "email", "city"]
PRODUCT_COLUMNS = ["business_id", "external_product_id", "sku", "product_name", "selling_price", "is_active"]
ORDER_COLUMNS = [
    "business_id", "customer_phone", "external_order_id", "order_date", "order_status",
    "payment_method", "subtotal_amount", "discount_amount", "shipping_charged",
    "total_amount", "currency", "shipping_city", "shipping_province", "source_platform",
]
ORDER_ITEM_COLUMNS = [
    "business_id", "external_order_id", "external_product_id", "quantity",
    "unit_price", "discount_amount",
]
INVENTORY_COLUMNS = [
    "business_id", "external_product_id", "location_name", "snapshot_date", "quantity_available",
]

_ORDER_STATUS_MAP = {
    "unpaid": "pending",
    "pending": "pending",
    "ready_to_ship": "confirmed",
    "topack": "confirmed",
    "toship": "confirmed",
    "shipped": "fulfilled",
    "delivered": "delivered",
    "canceled": "cancelled",
    "cancelled": "cancelled",
    "returned": "returned",
}
_PRODUCT_STATUS_MAP = {
    "active": True,
    "live": True,
    "inactive": False,
    "deleted": False,
    "image-missing": False,
    "pending": False,
    "pending qc": False,
    "rejected": False,
    "sold-out": False,
    "suspended": False,
}


def _empty_frame(columns: List[str], integer_columns: Tuple[str, ...] = ()) -> pd.DataFrame:
    return pd.DataFrame({
        column: pd.Series(dtype="int64" if column in integer_columns else "object")
        for column in columns
    })


def _make_frame(
    rows: List[Dict[str, Any]],
    columns: List[str],
    integer_columns: Tuple[str, ...] = (),
) -> pd.DataFrame:
    return pd.DataFrame(rows, columns=columns) if rows else _empty_frame(columns, integer_columns)


def _clean_text(value: Any) -> Optional[str]:
    if not isinstance(value, str):
        return None
    cleaned = value.strip()
    return cleaned or None


def _parse_timestamp(value: Any) -> Optional[datetime]:
    if isinstance(value, datetime):
        parsed = value
    elif isinstance(value, str) and value.strip():
        text_value = value.strip()
        if text_value.endswith("Z"):
            text_value = f"{text_value[:-1]}+00:00"
        try:
            parsed = datetime.fromisoformat(text_value)
        except ValueError:
            return None
    else:
        return None
    if parsed.tzinfo is None or parsed.utcoffset() is None:
        return None
    return parsed.astimezone(timezone.utc)


def _valid_phone(value: Any) -> Optional[str]:
    phone = _clean_text(value)
    return phone if phone is not None and _PAKISTAN_PHONE.fullmatch(phone) else None


def normalize_daraz_status(raw_status: Optional[str]) -> Optional[str]:
    """Map known Daraz order states; do not coerce unknown states."""
    if not isinstance(raw_status, str):
        return None
    return _ORDER_STATUS_MAP.get(raw_status.strip().lower())


def normalize_daraz_payment_method(raw_method: Optional[str]) -> str:
    """Normalize exact supported payment labels to the canonical constraint."""
    value = raw_method.strip().lower() if isinstance(raw_method, str) else ""
    return {
        "cod": "cod",
        "cash on delivery": "cod",
        "cash on delivery (cod)": "cod",
        "card": "card",
        "credit card": "card",
        "debit card": "card",
        "visa card": "card",
        "daraz wallet": "wallet",
        "easypaisa": "wallet",
        "jazzcash": "wallet",
        "bank transfer": "bank_transfer",
        "wallet": "wallet",
        "other": "other",
    }.get(value, "other")


def parse_daraz_money(value: Any) -> Optional[Decimal]:
    """Parse a non-negative NUMERIC(12,2) value without substituting zero."""
    if value is None or isinstance(value, bool) or (isinstance(value, str) and not value.strip()):
        return None
    try:
        amount = Decimal(str(value).strip())
        if not amount.is_finite() or amount < 0:
            return None
        quantized = amount.quantize(_CENT)
    except (InvalidOperation, ValueError):
        return None
    if amount != quantized or quantized > _MAX_NUMERIC_12_2:
        return None
    return quantized


class DarazTransformer:
    """Provider-to-canonical transformer; tenant identity is supplied by the caller."""

    def __init__(self, business_id: int):
        if isinstance(business_id, bool) or not isinstance(business_id, int) or business_id <= 0:
            raise ValueError("business_id must be a positive server-resolved integer.")
        self.business_id = business_id
        self.rejected_records: List[Dict[str, Optional[str]]] = []

    def _reject(self, source_type: str, external_id: Any, reason: str) -> None:
        self.rejected_records.append({
            "source_type": source_type,
            "external_id": str(external_id) if external_id is not None else None,
            "reason": reason,
        })

    def transform(
        self,
        raw_orders: List[Dict[str, Any]],
    ) -> Tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame, pd.DataFrame]:
        return self.transform_orders(raw_orders)

    def transform_orders(
        self,
        raw_orders: List[Dict[str, Any]],
    ) -> Tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame, pd.DataFrame]:
        """Return customer, order, and item frames; FK join keys await a loader."""
        self.rejected_records = []
        customers: Dict[str, Dict[str, Any]] = {}
        orders: List[Dict[str, Any]] = []
        items_out: List[Dict[str, Any]] = []

        for order in raw_orders:
            if not isinstance(order, Mapping):
                self._reject("order", None, "provider order is not an object")
                continue
            raw_order_id = order.get("order_id") or order.get("order_number")
            order_id = _clean_text(str(raw_order_id)) if raw_order_id is not None else None
            if not order_id:
                self._reject("order", None, "missing order identifier")
                continue

            created_at = _parse_timestamp(order.get("created_at"))
            raw_status = order.get("status")
            if not isinstance(raw_status, str):
                statuses = order.get("statuses")
                if isinstance(statuses, list) and len(statuses) == 1 and isinstance(statuses[0], str):
                    raw_status = statuses[0]
            status = normalize_daraz_status(raw_status)
            if created_at is None or status is None:
                self._reject("order", order_id, "missing or unsupported order date/status")
                continue

            address = order.get("address_shipping")
            address = address if isinstance(address, Mapping) else {}
            raw_phone = address.get("phone") or order.get("customer_phone")
            phone = normalize_pakistan_phone(raw_phone)
            if phone is None:
                self._reject("order", order_id, "customer phone is missing, masked, or violates the Pakistan phone constraint")
                continue

            total = parse_daraz_money(order.get("total_amount", order.get("price")))
            subtotal = parse_daraz_money(order.get("subtotal_amount"))
            shipping = parse_daraz_money(order.get("shipping_charged", order.get("shipping_fee")))
            if total is None or subtotal is None or shipping is None:
                self._reject(
                    "order",
                    order_id,
                    "required total/subtotal/shipping amounts are not all verified and valid",
                )
                continue

            discount = parse_daraz_money(order.get("discount_amount"))
            if discount is None:
                voucher = parse_daraz_money(order.get("voucher")) if "voucher" in order else None
                if voucher not in (None, Decimal("0.00")):
                    self._reject("order", order_id, "Daraz voucher breakdown is not verified for canonical discounts")
                    continue
                discount = Decimal("0.00")

            currency = _clean_text(order.get("currency") or order.get("currency_code")) or "PKR"
            if not _CURRENCY.fullmatch(currency):
                self._reject("order", order_id, "currency does not fit core.orders.currency")
                continue

            first_name = _clean_text(address.get("first_name") or order.get("customer_first_name"))
            last_name = _clean_text(address.get("last_name") or order.get("customer_last_name"))
            full_name = " ".join(name for name in (first_name, last_name) if name) or None
            city = _clean_text(address.get("city"))
            customer_id = order.get("customer_id")
            customers.setdefault(phone, {
                "business_id": self.business_id,
                "external_customer_id": str(customer_id) if customer_id is not None else None,
                "full_name": full_name,
                "phone": phone,
                "email": _clean_text(order.get("customer_email")),
                "city": city,
            })

            orders.append({
                "business_id": self.business_id,
                "customer_phone": phone,
                "external_order_id": order_id,
                "order_date": created_at,
                "order_status": status,
                "payment_method": normalize_daraz_payment_method(order.get("payment_method")),
                "subtotal_amount": subtotal,
                "discount_amount": discount,
                "shipping_charged": shipping,
                "total_amount": total,
                "currency": currency.upper(),
                "shipping_city": city,
                "shipping_province": _clean_text(
                    address.get("province") or address.get("state") or address.get("region")
                ),
                "source_platform": "daraz",
            })

            raw_items = order.get("fetched_order_items")
            if raw_items is None:
                raw_items = order.get("items") or order.get("order_items") or []
            if not isinstance(raw_items, list):
                self._reject("order_items", order_id, "order item payload is not a list")
                continue
            for item in raw_items:
                if not isinstance(item, Mapping):
                    self._reject("order_item", order_id, "provider order item is not an object")
                    continue
                sku_id = item.get("SkuId") or item.get("sku_id")
                try:
                    quantity = int(item.get("quantity"))
                except (TypeError, ValueError):
                    quantity = 0
                unit_price = parse_daraz_money(item.get("item_price", item.get("paid_price")))
                if sku_id is None or quantity <= 0 or unit_price is None:
                    self._reject("order_item", order_id, "missing SKU ID, positive quantity, or valid item price")
                    continue
                item_discount = parse_daraz_money(item.get("discount_amount"))
                items_out.append({
                    "business_id": self.business_id,
                    "external_order_id": order_id,
                    "external_product_id": str(sku_id),
                    "quantity": quantity,
                    "unit_price": unit_price,
                    "discount_amount": item_discount if item_discount is not None else Decimal("0.00"),
                })

        return (
            _make_frame(list(customers.values()), CUSTOMER_COLUMNS, ("business_id",)),
            _empty_frame(PRODUCT_COLUMNS, ("business_id",)),
            _make_frame(orders, ORDER_COLUMNS, ("business_id",)),
            _make_frame(items_out, ORDER_ITEM_COLUMNS, ("business_id", "quantity")),
        )

    def transform_products(self, raw_products: List[Dict[str, Any]]) -> pd.DataFrame:
        """Represent each Daraz SKU as a core.product row; retain parent only in raw data."""
        self.rejected_records = []
        products: List[Dict[str, Any]] = []
        for product in raw_products:
            if not isinstance(product, Mapping):
                self._reject("product", None, "provider product is not an object")
                continue
            item_id = product.get("item_id")
            attributes = product.get("attributes")
            attributes = attributes if isinstance(attributes, Mapping) else {}
            name = _clean_text(attributes.get("name"))
            skus = product.get("skus")
            if not isinstance(skus, list) or not skus:
                self._reject("product", item_id, "no documented SKU row is available to map")
                continue
            if name is None:
                self._reject("product", item_id, "missing required product name")
                continue

            for sku in skus:
                if not isinstance(sku, Mapping):
                    self._reject("product_sku", item_id, "provider SKU is not an object")
                    continue
                sku_id = sku.get("SkuId") or sku.get("sku_id")
                seller_sku = _clean_text(sku.get("SellerSku") or sku.get("ShopSku"))
                price = parse_daraz_money(sku.get("price"))
                raw_status = sku.get("Status") or product.get("status")
                active = _PRODUCT_STATUS_MAP.get(str(raw_status).strip().lower()) if raw_status is not None else None
                if sku_id is None and len(skus) == 1:
                    sku_id = item_id
                if sku_id is None or price is None or active is None:
                    self._reject("product_sku", item_id, "missing stable SKU/item ID, price, or recognized status")
                    continue
                products.append({
                    "business_id": self.business_id,
                    "external_product_id": str(sku_id),
                    "sku": seller_sku,
                    "product_name": name,
                    "selling_price": price,
                    "is_active": active,
                })

        unique_products = {}
        for product in products:
            unique_products[(product["business_id"], product["external_product_id"])] = product
        return _make_frame(list(unique_products.values()), PRODUCT_COLUMNS, ("business_id",))

    def transform_inventory(self, raw_inventory_items: List[Dict[str, Any]]) -> pd.DataFrame:
        """Keep stock in raw provider data until Daraz quantity semantics are verified."""
        self.rejected_records = []
        for item in raw_inventory_items:
            item_id = item.get("item_id") if isinstance(item, Mapping) else None
            self._reject(
                "inventory",
                item_id,
                "Daraz stock quantities and warehouse codes are preserved in raw data; canonical meaning is unverified",
            )
        return _empty_frame(INVENTORY_COLUMNS, ("business_id", "quantity_available"))
