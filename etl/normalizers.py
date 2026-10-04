import hashlib
import re
from datetime import date, datetime, timezone
from decimal import Decimal, InvalidOperation, ROUND_HALF_UP
from typing import Any, Optional


def normalize_money(value: Any) -> Decimal:
    """Return a non-negative NUMERIC(12,2)-ready decimal value."""
    if value is None or str(value).strip() == "":
        return Decimal("0.00")
    try:
        amount = Decimal(str(value).strip().replace(",", ""))
        if not amount.is_finite():
            return Decimal("0.00")
        if amount < 0:
            return Decimal("0.00")
        return amount.quantize(Decimal("0.01"), rounding=ROUND_HALF_UP)
    except (InvalidOperation, ValueError):
        return Decimal("0.00")


def normalize_currency_code(value: Any, default: str = "PKR") -> str:
    """Normalize an ISO 4217 alphabetic currency code for CHAR(3)."""
    if value is None or str(value).strip() == "":
        return default
    code = str(value).strip().upper()
    if not re.fullmatch(r"[A-Z]{3}", code):
        raise ValueError(f"Invalid currency code: {value!r}")
    return code


def normalize_timestamp(value: Any) -> Optional[datetime]:
    """Return an aware UTC datetime suitable for a TIMESTAMPTZ column."""
    if value is None or str(value).strip() == "":
        return None
    if isinstance(value, datetime):
        parsed = value
    else:
        raw = str(value).strip()
        if raw.endswith(("Z", "z")):
            raw = f"{raw[:-1]}+00:00"
        try:
            parsed = datetime.fromisoformat(raw)
        except ValueError as exc:
            raise ValueError(f"Invalid timestamp: {value!r}") from exc
    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=timezone.utc)
    return parsed.astimezone(timezone.utc)


def normalize_date(value: Any) -> Optional[date]:
    """Return a Python date suitable for a DATE column."""
    if value is None or str(value).strip() == "":
        return None
    if isinstance(value, datetime):
        timestamp = normalize_timestamp(value)
        return timestamp.date() if timestamp else None
    if isinstance(value, date):
        return value
    try:
        return date.fromisoformat(str(value).strip())
    except ValueError as exc:
        raise ValueError(f"Invalid date: {value!r}") from exc


def normalize_location(value: Any) -> Optional[str]:
    if value is None:
        return None
    clean = str(value).strip()
    return clean or None


def synthetic_pakistan_phone(namespace: str, identifier: Any) -> str:
    """Create a stable, valid Pakistan phone proxy for records without a phone."""
    stable_key = f"{namespace}:{identifier}".encode("utf-8")
    digits = f"{int(hashlib.sha256(stable_key).hexdigest(), 16) % 1_000_000_000:09d}"
    return f"+923{digits}"

def normalize_pakistan_phone(phone: Optional[str]) -> Optional[str]:
    """
    Normalizes Pakistani phone numbers to strictly match:
    ^+92[0-9]{10}$
    
    Accepts:
    - 03001234567
    - +92-300-1234567
    - 923001234567
    - 0300 1234567
    """
    if not phone:
        return None

    # Remove all non-digit characters except leading plus
    digits = re.sub(r"[^\d]", "", str(phone).strip())

    if digits.startswith("92") and len(digits) == 12:
        return f"+{digits}"
    elif digits.startswith("03") and len(digits) == 11:
        return f"+92{digits[1:]}"
    elif digits.startswith("3") and len(digits) == 10:
        return f"+92{digits}"

    return None

def normalize_order_status(shopify_order: dict) -> str:
    """
    Maps Shopify order attributes to canonical order_status enum:
    ('pending', 'confirmed', 'fulfilled', 'delivered', 'cancelled', 'returned', 'rto')
    """
    if shopify_order.get("cancelled_at") is not None:
        return "cancelled"

    financial_status = str(shopify_order.get("financial_status") or "").lower()
    if financial_status in ("refunded", "partially_refunded"):
        return "returned"
    if financial_status == "voided":
        return "cancelled"

    fulfillment_status = shopify_order.get("fulfillment_status")
    if fulfillment_status == "fulfilled":
        return "fulfilled"
    elif fulfillment_status == "partial":
        return "confirmed"

    if financial_status in ("paid", "authorized"):
        return "confirmed"

    return "pending"

def normalize_payment_method(shopify_order: dict) -> str:
    """
    Maps Shopify payment gateway names to canonical payment_method enum:
    ('cod', 'card', 'bank_transfer', 'wallet', 'other')
    """
    gateways = [str(g).lower() for g in shopify_order.get("payment_gateway_names", [])]
    single_gateway = str(shopify_order.get("gateway") or "").lower()
    combined = " ".join(gateways + [single_gateway])

    if any(k in combined for k in ["cod", "cash on delivery", "cash_on_delivery", "manual"]):
        return "cod"
    if any(k in combined for k in ["card", "visa", "mastercard", "stripe", "checkout", "payfast", "credit"]):
        return "card"
    if any(k in combined for k in ["bank", "transfer", "ibft", "kuickpay"]):
        return "bank_transfer"
    if any(k in combined for k in ["easypaisa", "jazzcash", "nayapay", "sadapay", "wallet"]):
        return "wallet"

    return "other"
