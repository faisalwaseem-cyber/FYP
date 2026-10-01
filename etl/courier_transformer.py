"""
Courier ETL Transformer — BRIDGE FLOW Phase 5.3
================================================

Transforms raw courier API responses (PostEx, Trax, Leopards or any dict-based
payload) into the canonical shipment and RTO record structures that the
CourierLoader writes to the database.

Schema mapping (verified against ecommerce_intelligence_schema.sql)
-------------------------------------------------------------------
core.shipment
  shipment_id        — DB-generated; NOT set here
  business_id        — injected at transformer construction
  order_id           — resolved by loader via external_order_id
  courier_id         — resolved by loader via courier_name
  tracking_number    — TEXT NOT NULL
  shipment_status    — CHECK IN ('booked','in_transit','delivered',
                                 'failed_delivery','returning','returned')
  shipped_at         — TIMESTAMPTZ, nullable
  delivered_at       — TIMESTAMPTZ, nullable; set ONLY when status='delivered'
  shipping_cost      — NUMERIC(12,2) >= 0  (actual courier charge)
  cod_amount         — NUMERIC(12,2) >= 0  (COD amount collected or expected)
  delivery_attempts  — INTEGER >= 0

core.rto_return
  return_id             — DB-generated
  business_id           — from transformer
  order_id              — resolved by loader
  shipment_id           — resolved by loader
  return_type           — CHECK IN ('rto','customer_return')
  return_reason         — TEXT nullable
  return_date           — TIMESTAMPTZ NOT NULL
  return_shipping_cost  — NUMERIC(12,2) >= 0
  refund_amount         — NUMERIC(12,2) >= 0

core.orders.order_status  (cascade driven by loader, based on shipment_status)
  'delivered'    ← shipment_status == 'delivered'
  'rto'          ← shipment_status in ('returning', 'returned')
  'cancelled'    ← shipment_status == 'cancelled'  (held in orders; not in shipment)
  'fulfilled'    ← shipment_status in ('booked', 'in_transit', 'failed_delivery')

Order identifier reconciliation
--------------------------------
Shopify orders use a numeric external_order_id (e.g. '5589123456789').
Daraz orders are prefixed: 'daraz_{order_id}'.
The transformer stores whichever value the payload exposes and leaves
the loader to resolve order_id via (business_id, external_order_id).
No prefix is added here — callers must pass the correct identifier.

Design invariants
-----------------
* No database interaction in this module.
* No invented fields — every output key maps to a verified schema column.
* All monetary values are clamped to >= 0.0 and rounded to 2 decimals.
* `normalize_shipment_status` is a pure function, easily unit-tested.
* `normalize_rto_reason` is a pure function that maps provider statuses
  to human-readable reason strings stored in rto_return.return_reason.
"""
from __future__ import annotations

import re
from datetime import datetime, timezone
from decimal import Decimal, InvalidOperation
from typing import Any, Dict, List, Optional


# ---------------------------------------------------------------------------
# Valid enum sets — verified against schema CHECK constraints
# ---------------------------------------------------------------------------

VALID_SHIPMENT_STATUSES = frozenset(
    {"booked", "in_transit", "delivered", "failed_delivery", "returning", "returned"}
)

def normalize_shipment_status(raw_status: Optional[str]) -> str:
    """
    Normalises a courier provider's raw status string to the canonical
    core.shipment.shipment_status enum value.

    Parameters
    ----------
    raw_status : str | None
        Status string as returned by the courier API.

    Returns
    -------
    str
        One of: 'booked' | 'in_transit' | 'delivered' |
                'failed_delivery' | 'returning' | 'returned'

    Notes
    -----
    Falls back to 'booked' for any unrecognised or null input, matching the
    database column default for freshly booked shipments.
    """
    if not raw_status:
        return "booked"

    s = str(raw_status).strip().lower()

    if s in VALID_SHIPMENT_STATUSES:
        return s

    # 1. Returned / RTO (Evaluated before delivered so 'undelivered' or 'return completed' won't match 'delivered'/'complete')
    if s in {"rto", "undelivered", "not delivered", "return completed", "shipment returned"} \
       or any(phrase in s for phrase in ("returned to", "return to", "return completed", "returned")):
        return "returned"

    # 2. Returning (return in transit / return in progress)
    if any(phrase in s for phrase in ("returning", "return in progress", "return in transit", "return initiated", "return transit", "return picked up")):
        return "returning"

    # 3. Failed delivery
    if any(phrase in s for phrase in ("failed", "unreachable", "refused", "incomplete address", "reattempt", "attempted", "delivery attempted")):
        return "failed_delivery"

    # 4. Delivered
    if s in {"delivered", "complete", "completed", "successful", "cash collected"} \
       or any(phrase in s for phrase in ("delivery successful", "shipment delivered", "cash collected")):
        return "delivered"
    if re.search(r"\b(delivered|complete|completed|successful)\b", s):
        return "delivered"

    # 5. In transit
    if any(phrase in s for phrase in ("in transit", "out for delivery", "dispatched", "arrival", "hub", "transit", "on route", "picked", "picked up", "en route")):
        return "in_transit"

    # 6. Booked / Cancelled (cancelled shipments before dispatch stay 'booked' or 'failed_delivery')
    if any(phrase in s for phrase in ("booked", "booking", "manifest", "created")):
        return "booked"

    return "booked"


def normalize_rto_reason(raw_status: Optional[str]) -> str:
    """
    Returns a human-readable RTO reason string for rto_return.return_reason,
    derived from the courier's raw status.

    This is the only text stored in return_reason; no other fields are invented.
    """
    if not raw_status:
        return "Returned by courier"

    s = str(raw_status).strip()
    return f"Courier status: {s}"


def _parse_money(value: Any) -> float:
    """
    Safely converts any numeric-like value to a non-negative float
    rounded to 2 decimal places, matching NUMERIC(12,2).
    Returns 0.0 on any parse failure.
    """
    if value is None:
        return 0.0
    try:
        dec = Decimal(str(value).strip().replace(",", ""))
        return max(0.0, float(round(dec, 2)))
    except (InvalidOperation, ValueError):
        return 0.0


def _parse_int_non_negative(value: Any, default: int = 0) -> int:
    """Parses an integer, clamping to >= 0."""
    try:
        return max(0, int(value))
    except (TypeError, ValueError):
        return default


def _parse_timestamp(value: Any) -> Optional[str]:
    """
    Returns the value as-is if it looks like a timestamp string, or None.
    Passes through ISO 8601 strings and datetime objects.
    SQLAlchemy / psycopg2 handles casting from strings via CAST(:x AS TIMESTAMPTZ).
    """
    if value is None:
        return None
    if isinstance(value, datetime):
        return value.isoformat()
    s = str(value).strip()
    return s if s else None


# ---------------------------------------------------------------------------
# Provider-specific field extraction helpers
# ---------------------------------------------------------------------------

def _extract_postex_fields(payload: Dict[str, Any]) -> Dict[str, Any]:
    """
    Extracts fields from a PostEx tracking API response.

    PostEx wraps data under a 'dist' key:
      {
        "statusCode": "200",
        "dist": {
          "trackingNumber": "...",
          "orderRefNumber": "...",
          "orderStatus": "...",
          "deliveryCharges": 250.0,
          "orderAmount": 4250.0,
          "attemptCount": 1,
          "bookingDate": "2026-09-29T11:00:00+05:00",
          "deliveryDate": "2026-09-29T16:30:00+05:00"
        }
      }
    Callers may also pass the unwrapped 'dist' dict directly.
    """
    dist = payload.get("dist") if isinstance(payload.get("dist"), dict) else payload
    return {
        "tracking_number":   str(dist.get("trackingNumber") or dist.get("tracking_number") or "").strip(),
        "external_order_id": str(dist.get("orderRefNumber") or dist.get("external_order_id") or dist.get("order_id") or "").strip(),
        "raw_status":        dist.get("orderStatus") or dist.get("status") or dist.get("transactionStatus"),
        "shipping_cost":     _parse_money(dist.get("deliveryCharges") or dist.get("shipping_cost")),
        "cod_amount":        _parse_money(dist.get("orderAmount") or dist.get("cod_amount") or dist.get("invoicePayment")),
        "delivery_attempts": _parse_int_non_negative(dist.get("attemptCount") or dist.get("delivery_attempts")),
        "shipped_at":        _parse_timestamp(dist.get("bookingDate") or dist.get("shipped_at")),
        "delivered_at":      _parse_timestamp(dist.get("deliveryDate") or dist.get("delivered_at")),
    }


def _extract_trax_fields(payload: Dict[str, Any]) -> Dict[str, Any]:
    """
    Extracts fields from a Trax Pakistan tracking API response.

      {
        "cn": "TRAX-001-PK",
        "reference": "ORD-12345",
        "current_status": "Delivered",
        "charges": 180.0,
        "cod_amount": 3000.0,
        "attempts": 1,
        "pickup_date": "...",
        "delivery_date": "..."
      }
    """
    return {
        "tracking_number":   str(payload.get("cn") or payload.get("tracking_number") or "").strip(),
        "external_order_id": str(payload.get("reference") or payload.get("order_reference") or payload.get("external_order_id") or "").strip(),
        "raw_status":        payload.get("current_status") or payload.get("status"),
        "shipping_cost":     _parse_money(payload.get("charges") or payload.get("shipping_cost") or payload.get("freight")),
        "cod_amount":        _parse_money(payload.get("cod_amount") or payload.get("amount")),
        "delivery_attempts": _parse_int_non_negative(payload.get("attempts") or payload.get("delivery_attempts")),
        "shipped_at":        _parse_timestamp(payload.get("pickup_date") or payload.get("shipped_at")),
        "delivered_at":      _parse_timestamp(payload.get("delivery_date") or payload.get("delivered_at")),
    }


def _extract_leopards_fields(payload: Dict[str, Any]) -> Dict[str, Any]:
    """
    Extracts fields from a Leopards Courier API response.

    Leopards wraps tracking data under 'packet_list':
      {
        "status": 1,
        "packet_list": [{
          "booked_packet_cn": "LEP-9001",
          "booked_packet_order_id": "ORD-789",
          "booked_packet_status": "Delivered",
          "booked_packet_vol_weight_lbs": "...",
          "booked_packet_collect_amount": "2500",
          "booked_packet_no_of_attempts": "2",
          "cn_date": "...",
          "cn_delivered_date": "..."
        }]
      }
    Callers may also pass the packet dict directly.
    """
    # Unwrap packet_list if present
    if "packet_list" in payload and isinstance(payload["packet_list"], list):
        packet = payload["packet_list"][0] if payload["packet_list"] else {}
    else:
        packet = payload

    return {
        "tracking_number":   str(packet.get("booked_packet_cn") or packet.get("tracking_number") or "").strip(),
        "external_order_id": str(packet.get("booked_packet_order_id") or packet.get("order_id") or packet.get("external_order_id") or "").strip(),
        "raw_status":        packet.get("booked_packet_status") or packet.get("status"),
        "shipping_cost":     _parse_money(packet.get("booked_packet_freight") or packet.get("shipping_cost")),
        "cod_amount":        _parse_money(packet.get("booked_packet_collect_amount") or packet.get("cod_amount")),
        "delivery_attempts": _parse_int_non_negative(packet.get("booked_packet_no_of_attempts") or packet.get("delivery_attempts")),
        "shipped_at":        _parse_timestamp(packet.get("cn_date") or packet.get("shipped_at")),
        "delivered_at":      _parse_timestamp(packet.get("cn_delivered_date") or packet.get("delivered_at")),
    }


# Registry: provider_name → extraction function
_PROVIDER_EXTRACTORS = {
    "postex":   _extract_postex_fields,
    "trax":     _extract_trax_fields,
    "leopards": _extract_leopards_fields,
}


# ---------------------------------------------------------------------------
# Main transformer class
# ---------------------------------------------------------------------------

class CourierTransformer:
    """
    Transforms raw courier API response payloads into canonical shipment and
    RTO dicts ready for the CourierLoader.

    The transformer is stateless except for business_id; it can be
    instantiated once per sync run and reused for all payloads.

    Parameters
    ----------
    business_id : int
        Tenant identifier, always included in output records.
    """

    def __init__(self, business_id: int):
        self.business_id = business_id

    # ------------------------------------------------------------------
    # Primary public method
    # ------------------------------------------------------------------

    def transform_shipment(
        self,
        courier_name: str,
        payload: Dict[str, Any],
        *,
        source_platform: str = "shopify",
    ) -> Dict[str, Any]:
        """
        Transforms a raw courier API response into a canonical shipment dict.

        The dict contains exactly the fields that core.shipment requires,
        plus `external_order_id` (used by the loader to resolve order_id)
        and `courier_name` (used by the loader to resolve courier_id).

        Parameters
        ----------
        courier_name : str
            Provider name: 'postex', 'trax', 'leopards', or any custom name.
            Stored normalised (lowercase) in core.courier.courier_name.
        payload : dict
            Raw API response dict from the courier client.
        source_platform : str
            'shopify' | 'daraz' — used only for diagnostic logging by the
            caller; not stored in core.shipment (no such column).

        Returns
        -------
        dict with keys:
            business_id       int
            courier_name      str  (lowercase)
            external_order_id str  (matches core.orders.external_order_id)
            tracking_number   str
            shipment_status   str  (validated enum)
            shipping_cost     float  NUMERIC(12,2) >= 0
            cod_amount        float  NUMERIC(12,2) >= 0
            delivery_attempts int    >= 0
            shipped_at        str | None  (ISO 8601 or None)
            delivered_at      str | None  (only set when status='delivered')

        Raises
        ------
        ValueError
            If tracking_number is empty after extraction.
        """
        provider = courier_name.strip().lower()

        extractor = _PROVIDER_EXTRACTORS.get(provider)
        if extractor is not None:
            fields = extractor(payload)
        else:
            # Generic fallback: try common field names from any provider
            fields = self._extract_generic_fields(payload)

        # Honour caller-supplied overrides (allow pipelines to inject
        # external_order_id from the database query context)
        if payload.get("external_order_id"):
            fields["external_order_id"] = str(payload["external_order_id"]).strip()
        if payload.get("tracking_number") and not fields.get("tracking_number"):
            fields["tracking_number"] = str(payload["tracking_number"]).strip()

        tracking_num = fields.get("tracking_number", "").strip()
        if not tracking_num:
            raise ValueError(
                f"CourierTransformer: cannot transform shipment — "
                f"tracking_number is empty for provider='{provider}', "
                f"payload keys={list(payload.keys())}"
            )

        canonical_status = normalize_shipment_status(fields.get("raw_status"))

        # delivered_at is only set when the shipment was actually delivered.
        # Schema: CONSTRAINT chk_shipment_dates CHECK (shipped_at IS NULL OR delivered_at IS NULL OR delivered_at >= shipped_at)
        delivered_at = (
            fields.get("delivered_at") if canonical_status == "delivered" else None
        )

        return {
            "business_id":       self.business_id,
            "courier_name":      provider,
            "external_order_id": fields.get("external_order_id", ""),
            "tracking_number":   tracking_num,
            "shipment_status":   canonical_status,
            "shipping_cost":     fields.get("shipping_cost", 0.0),
            "cod_amount":        fields.get("cod_amount", 0.0),
            "delivery_attempts": fields.get("delivery_attempts", 0),
            "shipped_at":        fields.get("shipped_at"),
            "delivered_at":      delivered_at,
        }

    # ------------------------------------------------------------------
    # RTO record builder
    # ------------------------------------------------------------------

    def build_rto_record(
        self,
        shipment_data: Dict[str, Any],
        *,
        return_shipping_cost: float = 0.0,
        refund_amount: float = 0.0,
        return_type: str = "rto",
        raw_status: Optional[str] = None,
    ) -> Dict[str, Any]:
        """
        Builds a canonical core.rto_return dict from an already-transformed
        shipment record.

        This is called by the loader (or pipeline) when shipment_status is
        'returned', to create the companion RTO record.

        Parameters
        ----------
        shipment_data : dict
            Output of transform_shipment().
        return_shipping_cost : float
            Actual cost of the return leg (default 0 — updated when known).
        refund_amount : float
            Amount refunded to customer (default 0 for COD RTO).
        return_type : str
            'rto' (courier-initiated) or 'customer_return' (buyer-initiated).
            Validated against core.rto_return CHECK constraint.
        raw_status : str, optional
            Raw courier status string used to build return_reason.

        Returns
        -------
        dict with keys:
            business_id          int
            external_order_id    str   (loader resolves to order_id)
            return_type          str   ('rto' | 'customer_return')
            return_reason        str
            return_date          str   (ISO 8601 UTC now)
            return_shipping_cost float NUMERIC(12,2) >= 0
            refund_amount        float NUMERIC(12,2) >= 0

        Raises
        ------
        ValueError
            If return_type is not a valid enum value.
        """
        valid_return_types = {"rto", "customer_return"}
        if return_type not in valid_return_types:
            raise ValueError(
                f"Invalid return_type '{return_type}'. "
                f"Must be one of: {valid_return_types}"
            )

        reason = normalize_rto_reason(
            raw_status or shipment_data.get("_raw_status")
        )

        return {
            "business_id":          self.business_id,
            "external_order_id":    shipment_data["external_order_id"],
            "return_type":          return_type,
            "return_reason":        reason,
            "return_date":          datetime.now(timezone.utc).isoformat(),
            "return_shipping_cost": max(0.0, round(float(return_shipping_cost), 2)),
            "refund_amount":        max(0.0, round(float(refund_amount), 2)),
        }

    # ------------------------------------------------------------------
    # COD settlement transformer
    # ------------------------------------------------------------------

    def transform_cod_records(
        self,
        records: List[Dict[str, Any]],
        courier_name: str,
    ) -> List[Dict[str, Any]]:
        """
        Transforms a list of COD settlement records (from get_cod_settlement)
        into lightweight dicts suitable for reconciliation.

        These records do NOT target a specific table — they are used by the
        pipeline to reconcile cod_amount on existing shipment rows.

        Parameters
        ----------
        records : list[dict]
            Raw COD records from the courier API.
        courier_name : str
            Provider name.

        Returns
        -------
        list[dict] with keys:
            business_id      int
            courier_name     str
            tracking_number  str
            cod_amount       float
            settlement_date  str | None
        """
        provider = courier_name.strip().lower()
        result = []

        for rec in records:
            # Provider-agnostic field extraction for COD settlement
            tracking = str(
                rec.get("trackingNumber")
                or rec.get("booked_packet_cn")
                or rec.get("cn")
                or rec.get("tracking_number")
                or ""
            ).strip()

            if not tracking:
                continue  # Skip incomplete records

            cod = _parse_money(
                rec.get("orderAmount")
                or rec.get("booked_packet_collect_amount")
                or rec.get("amount")
                or rec.get("cod_amount")
            )

            settlement_date = _parse_timestamp(
                rec.get("settlementDate")
                or rec.get("settled_date")
                or rec.get("settlement_date")
            )

            result.append({
                "business_id":     self.business_id,
                "courier_name":    provider,
                "tracking_number": tracking,
                "cod_amount":      cod,
                "settlement_date": settlement_date,
            })

        return result

    # ------------------------------------------------------------------
    # Generic fallback extractor
    # ------------------------------------------------------------------

    @staticmethod
    def _extract_generic_fields(payload: Dict[str, Any]) -> Dict[str, Any]:
        """
        Generic field extraction for unregistered providers.
        Tries all known field aliases in priority order.
        """
        return {
            "tracking_number": str(
                payload.get("tracking_number")
                or payload.get("trackingNumber")
                or payload.get("cn")
                or ""
            ).strip(),
            "external_order_id": str(
                payload.get("external_order_id")
                or payload.get("orderRefNumber")
                or payload.get("order_id")
                or payload.get("reference")
                or ""
            ).strip(),
            "raw_status": (
                payload.get("status")
                or payload.get("orderStatus")
                or payload.get("current_status")
                or payload.get("transactionStatus")
                or payload.get("shipment_status")
            ),
            "shipping_cost": _parse_money(
                payload.get("shipping_cost")
                or payload.get("deliveryCharges")
                or payload.get("charges")
                or payload.get("freight")
            ),
            "cod_amount": _parse_money(
                payload.get("cod_amount")
                or payload.get("orderAmount")
                or payload.get("amount")
                or payload.get("invoicePayment")
            ),
            "delivery_attempts": _parse_int_non_negative(
                payload.get("delivery_attempts")
                or payload.get("attemptCount")
                or payload.get("attempts")
            ),
            "shipped_at": _parse_timestamp(
                payload.get("shipped_at")
                or payload.get("bookingDate")
                or payload.get("pickup_date")
            ),
            "delivered_at": _parse_timestamp(
                payload.get("delivered_at")
                or payload.get("deliveryDate")
                or payload.get("delivery_date")
            ),
        }
