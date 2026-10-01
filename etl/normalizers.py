import re
from typing import Optional

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

    fulfillment_status = shopify_order.get("fulfillment_status")
    if fulfillment_status == "fulfilled":
        return "fulfilled"
    elif fulfillment_status == "partial":
        return "confirmed"

    financial_status = shopify_order.get("financial_status")
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
