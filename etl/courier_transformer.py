"""
Courier ETL Transformer — BRIDGE FLOW Phase 5.3
================================================

Transforms generic courier payloads into the canonical shipment and RTO
record structures that the
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
from decimal import Decimal
from typing import Any, Dict, List, Optional

from etl.normalizers import normalize_money, normalize_timestamp


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


def _parse_money(value: Any) -> Decimal:
    """
    Safely converts any numeric-like value to a non-negative Decimal
    rounded to 2 places, matching NUMERIC(12,2).
    """
    return normalize_money(value)


def _parse_int_non_negative(value: Any, default: int = 0) -> int:
    """Parses an integer, clamping to >= 0."""
    try:
        return max(0, int(value))
    except (TypeError, ValueError):
        return default


def _parse_timestamp(value: Any) -> Optional[datetime]:
    """Return an aware UTC datetime for TIMESTAMPTZ fields."""
    return normalize_timestamp(value)


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
            Provider identifier stored lowercased in core.courier.courier_name.
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
            "return_date":          datetime.now(timezone.utc),
            "return_shipping_cost": normalize_money(return_shipping_cost),
            "refund_amount":        normalize_money(refund_amount),
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
                rec.get("tracking_number")
                or rec.get("trackingNumber")
                or rec.get("cn")
                or ""
            ).strip()

            if not tracking:
                continue  # Skip incomplete records

            cod = _parse_money(
                rec.get("cod_amount")
                or rec.get("amount")
            )

            settlement_date = _parse_timestamp(
                rec.get("settlement_date")
                or rec.get("settled_date")
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
        Extract common courier fields without provider-specific mappings.
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
                or payload.get("order_id")
                or payload.get("reference")
                or ""
            ).strip(),
            "raw_status": (
                payload.get("status")
                or payload.get("current_status")
                or payload.get("shipment_status")
            ),
            "shipping_cost": _parse_money(
                payload.get("shipping_cost")
                or payload.get("charges")
                or payload.get("freight")
            ),
            "cod_amount": _parse_money(
                payload.get("cod_amount")
                or payload.get("amount")
            ),
            "delivery_attempts": _parse_int_non_negative(
                payload.get("delivery_attempts")
                or payload.get("attempts")
            ),
            "shipped_at": _parse_timestamp(
                payload.get("shipped_at")
                or payload.get("pickup_date")
            ),
            "delivered_at": _parse_timestamp(
                payload.get("delivered_at")
                or payload.get("delivery_date")
            ),
        }
