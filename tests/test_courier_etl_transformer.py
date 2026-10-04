"""
Unit Test Suite — Courier ETL Transformer (BRIDGE FLOW Phase 5.3)

All tests are pure unit tests: no database, no network.

High-value coverage:

Status normalisation (normalize_shipment_status)
  1.  All 6 canonical shipment_status values map correctly
  2.  Delivered variants: 'Delivered', 'COMPLETE', 'Cash Collected', 'Delivery Successful'
  3.  Returned variants: 'Returned to Shipper', 'RTO', 'Return Completed'
  4.  Returning variants: 'Returning', 'Return In Progress', 'Return Transit'
  5.  Failed delivery variants: 'Failed', 'Unreachable', 'Refused', 'Attempted'
  6.  In-transit variants: 'In Transit', 'Out for Delivery', 'Dispatched', 'On Route'
  7.  Unknown/null status → 'booked' (safe default)

PostEx extraction (transform_shipment, provider='postex')
  8.  Standard PostEx 'dist'-wrapped response → correct canonical fields
  9.  Flat PostEx dict (pre-unwrapped) → correct fields
  10. COD amount extracted from orderAmount
  11. shipping_cost extracted from deliveryCharges
  12. delivery_attempts from attemptCount
  13. shipped_at from bookingDate; delivered_at only set when delivered
  14. delivered_at is None when status is NOT 'delivered'

Trax extraction (provider='trax')
  15. Trax response fields mapped correctly
  16. charges → shipping_cost; cod_amount → cod_amount
  17. pickup_date → shipped_at; delivery_date → delivered_at

Leopards extraction (provider='leopards')
  18. packet_list-wrapped response → correct canonical fields
  19. booked_packet_collect_amount → cod_amount
  20. booked_packet_freight → shipping_cost
  21. cn_date → shipped_at; cn_delivered_date → delivered_at (only when delivered)

Generic fallback (unknown provider)
  22. Generic extraction with common field aliases
  23. Provider name stored lowercased as courier_name

Order identifier reconciliation
  24. Shopify numeric external_order_id preserved as-is
  25. Daraz-prefixed 'daraz_{id}' preserved as-is
  26. caller-injected external_order_id overrides payload value

RTO record builder (build_rto_record)
  27. Valid RTO record produced from a 'returned' shipment
  28. return_type 'customer_return' accepted
  29. Invalid return_type raises ValueError
  30. return_shipping_cost clamped to >= 0
  31. return_date is a non-empty ISO 8601 string

COD settlement transformer (transform_cod_records)
  32. PostEx COD records → list of settlement dicts with correct fields
  33. Trax COD records mapped correctly
  34. Leopards COD records (booked_packet_collect_amount) mapped correctly
  35. Records with empty tracking_number are silently skipped

Monetary parsing (_parse_money)
  36. String amounts with commas and decimals parsed correctly
  37. Negative values clamped to 0.0
  38. None → 0.0; unparseable → 0.0

Validation / edge cases
  39. Empty tracking_number raises ValueError
  40. All output monetary values are >= 0
  41. business_id always present in output
  42. delivery_attempts always >= 0
  43. Unknown status fields: uppercase/mixed-case normalised correctly
"""
import sys
import unittest
from datetime import datetime, timezone
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from etl.courier_transformer import (
    CourierTransformer,
    _parse_money,
    normalize_rto_reason,
    normalize_shipment_status,
)

BUSINESS_ID = 1


class TestNormalizeShipmentStatus(unittest.TestCase):
    """Tests for the pure normalize_shipment_status function."""

    # ------------------------------------------------------------------
    # 1. All 6 canonical values covered
    # ------------------------------------------------------------------

    def test_booked_on_unknown_input(self):
        self.assertEqual(normalize_shipment_status("unknown_xyz"), "booked")

    def test_booked_on_none(self):
        self.assertEqual(normalize_shipment_status(None), "booked")

    def test_booked_on_empty_string(self):
        self.assertEqual(normalize_shipment_status(""), "booked")

    # ------------------------------------------------------------------
    # 2. Delivered variants
    # ------------------------------------------------------------------

    def test_delivered_exact(self):
        self.assertEqual(normalize_shipment_status("Delivered"), "delivered")

    def test_delivered_complete(self):
        self.assertEqual(normalize_shipment_status("COMPLETE"), "delivered")

    def test_delivered_cash_collected(self):
        self.assertEqual(normalize_shipment_status("Cash Collected"), "delivered")

    def test_delivered_successful(self):
        self.assertEqual(normalize_shipment_status("Delivery Successful"), "delivered")

    def test_delivered_case_insensitive(self):
        self.assertEqual(normalize_shipment_status("DELIVERED"), "delivered")

    # ------------------------------------------------------------------
    # 3. Returned variants
    # ------------------------------------------------------------------

    def test_returned_to_shipper(self):
        self.assertEqual(normalize_shipment_status("Returned to Shipper"), "returned")

    def test_returned_to_origin(self):
        self.assertEqual(normalize_shipment_status("Returned to Origin"), "returned")

    def test_returned_rto_keyword(self):
        self.assertEqual(normalize_shipment_status("RTO"), "returned")

    def test_returned_undelivered(self):
        self.assertEqual(normalize_shipment_status("Undelivered"), "returned")

    def test_returned_return_completed(self):
        self.assertEqual(normalize_shipment_status("Return Completed"), "returned")

    # ------------------------------------------------------------------
    # 4. Returning variants
    # ------------------------------------------------------------------

    def test_returning_exact(self):
        self.assertEqual(normalize_shipment_status("Returning"), "returning")

    def test_returning_in_progress(self):
        self.assertEqual(normalize_shipment_status("Return In Progress"), "returning")

    def test_returning_transit(self):
        self.assertEqual(normalize_shipment_status("Return Transit"), "returning")

    # ------------------------------------------------------------------
    # 5. Failed delivery variants
    # ------------------------------------------------------------------

    def test_failed_delivery_failed(self):
        self.assertEqual(normalize_shipment_status("Failed"), "failed_delivery")

    def test_failed_delivery_unreachable(self):
        self.assertEqual(normalize_shipment_status("Unreachable"), "failed_delivery")

    def test_failed_delivery_refused(self):
        self.assertEqual(normalize_shipment_status("Refused"), "failed_delivery")

    def test_failed_delivery_attempted(self):
        self.assertEqual(normalize_shipment_status("Delivery Attempted"), "failed_delivery")

    def test_failed_delivery_incomplete_address(self):
        self.assertEqual(normalize_shipment_status("Incomplete Address"), "failed_delivery")

    # ------------------------------------------------------------------
    # 6. In-transit variants
    # ------------------------------------------------------------------

    def test_in_transit_exact(self):
        self.assertEqual(normalize_shipment_status("In Transit"), "in_transit")

    def test_in_transit_out_for_delivery(self):
        self.assertEqual(normalize_shipment_status("Out for Delivery"), "in_transit")

    def test_in_transit_dispatched(self):
        self.assertEqual(normalize_shipment_status("Dispatched"), "in_transit")

    def test_in_transit_on_route(self):
        self.assertEqual(normalize_shipment_status("On Route"), "in_transit")

    def test_in_transit_picked_up(self):
        self.assertEqual(normalize_shipment_status("Picked Up"), "in_transit")

    # ------------------------------------------------------------------
    # 7. Canonical values remain unchanged when passed directly
    # ------------------------------------------------------------------

    def test_canonical_values_pass_through(self):
        for status in ("booked", "in_transit", "delivered",
                       "failed_delivery", "returning", "returned"):
            with self.subTest(status=status):
                self.assertEqual(normalize_shipment_status(status), status)


class TestPostExTransformer(unittest.TestCase):

    def setUp(self):
        self.t = CourierTransformer(business_id=BUSINESS_ID)

    # ------------------------------------------------------------------
    # 8. Standard PostEx 'dist'-wrapped response
    # ------------------------------------------------------------------

    def test_postex_dist_wrapped_response(self):
        payload = {
            "statusCode": "200",
            "dist": {
                "trackingNumber":  "POSTEX-PK-001",
                "orderRefNumber":  "5589123456789",
                "orderStatus":     "Delivered",
                "deliveryCharges": 250.0,
                "orderAmount":     4250.0,
                "attemptCount":    1,
                "bookingDate":     "2026-09-29T11:00:00+05:00",
                "deliveryDate":    "2026-09-29T16:30:00+05:00",
            }
        }
        result = self.t.transform_shipment("postex", payload)

        self.assertEqual(result["business_id"],       BUSINESS_ID)
        self.assertEqual(result["courier_name"],      "postex")
        self.assertEqual(result["tracking_number"],   "POSTEX-PK-001")
        self.assertEqual(result["external_order_id"], "5589123456789")
        self.assertEqual(result["shipment_status"],   "delivered")
        self.assertEqual(result["shipping_cost"],     250.0)
        self.assertEqual(result["cod_amount"],        4250.0)
        self.assertEqual(result["delivery_attempts"], 1)

    # ------------------------------------------------------------------
    # 9. Flat PostEx dict (pre-unwrapped 'dist')
    # ------------------------------------------------------------------

    def test_postex_flat_dict(self):
        payload = {
            "trackingNumber":  "POSTEX-PK-002",
            "orderRefNumber":  "5589000002",
            "orderStatus":     "In Transit",
            "deliveryCharges": 200.0,
            "orderAmount":     3000.0,
            "attemptCount":    0,
            "bookingDate":     "2026-09-28T09:00:00+05:00",
        }
        result = self.t.transform_shipment("postex", payload)

        self.assertEqual(result["tracking_number"],   "POSTEX-PK-002")
        self.assertEqual(result["external_order_id"], "5589000002")
        self.assertEqual(result["shipment_status"],   "in_transit")

    # ------------------------------------------------------------------
    # 10-12. PostEx specific field mappings
    # ------------------------------------------------------------------

    def test_postex_cod_amount_from_orderAmount(self):
        payload = {"dist": {"trackingNumber": "TN-003", "orderRefNumber": "REF-003",
                            "orderStatus": "Delivered", "orderAmount": 1500.0,
                            "deliveryCharges": 180.0}}
        result = self.t.transform_shipment("postex", payload)
        self.assertEqual(result["cod_amount"], 1500.0)

    def test_postex_shipping_cost_from_deliveryCharges(self):
        payload = {"dist": {"trackingNumber": "TN-004", "orderRefNumber": "REF-004",
                            "orderStatus": "Booked", "deliveryCharges": 220.0}}
        result = self.t.transform_shipment("postex", payload)
        self.assertEqual(result["shipping_cost"], 220.0)

    def test_postex_delivery_attempts_from_attemptCount(self):
        payload = {"dist": {"trackingNumber": "TN-005", "orderRefNumber": "REF-005",
                            "orderStatus": "Failed", "attemptCount": 3}}
        result = self.t.transform_shipment("postex", payload)
        self.assertEqual(result["delivery_attempts"], 3)
        self.assertEqual(result["shipment_status"], "failed_delivery")

    # ------------------------------------------------------------------
    # 13. shipped_at from bookingDate; delivered_at only when delivered
    # ------------------------------------------------------------------

    def test_shipped_at_from_bookingDate(self):
        payload = {"dist": {"trackingNumber": "TN-006", "orderRefNumber": "REF-006",
                            "orderStatus": "In Transit",
                            "bookingDate": "2026-09-25T08:00:00+05:00"}}
        result = self.t.transform_shipment("postex", payload)
        self.assertIsNotNone(result["shipped_at"])
        self.assertEqual(result["shipped_at"], datetime(2026, 9, 25, 3, 0, tzinfo=timezone.utc))

    def test_delivered_at_set_when_status_delivered(self):
        payload = {"dist": {"trackingNumber": "TN-007", "orderRefNumber": "REF-007",
                            "orderStatus": "Delivered",
                            "bookingDate": "2026-09-25T08:00:00+05:00",
                            "deliveryDate": "2026-09-29T16:00:00+05:00"}}
        result = self.t.transform_shipment("postex", payload)
        self.assertEqual(result["shipment_status"], "delivered")
        self.assertIsNotNone(result["delivered_at"])

    # ------------------------------------------------------------------
    # 14. delivered_at is None when NOT delivered
    # ------------------------------------------------------------------

    def test_delivered_at_none_when_not_delivered(self):
        """Schema constraint: delivered_at must only be set when actually delivered."""
        for status in ("In Transit", "Returning", "Failed", "Booked"):
            with self.subTest(status=status):
                payload = {"dist": {"trackingNumber": "TN-X", "orderRefNumber": "REF-X",
                                    "orderStatus": status,
                                    "deliveryDate": "2026-09-29T16:00:00+05:00"}}
                result = self.t.transform_shipment("postex", payload)
                self.assertIsNone(
                    result["delivered_at"],
                    f"delivered_at should be None when status='{status}'"
                )


class TestTraxTransformer(unittest.TestCase):

    def setUp(self):
        self.t = CourierTransformer(business_id=BUSINESS_ID)

    # ------------------------------------------------------------------
    # 15. Trax standard response
    # ------------------------------------------------------------------

    def test_trax_standard_response(self):
        payload = {
            "cn":             "TRAX-001-PK",
            "reference":      "ORD-12345",
            "current_status": "Delivered",
            "charges":        180.0,
            "cod_amount":     3000.0,
            "attempts":       2,
            "pickup_date":    "2026-09-27T10:00:00+05:00",
            "delivery_date":  "2026-09-29T14:00:00+05:00",
        }
        result = self.t.transform_shipment("trax", payload)

        self.assertEqual(result["tracking_number"],   "TRAX-001-PK")
        self.assertEqual(result["external_order_id"], "ORD-12345")
        self.assertEqual(result["shipment_status"],   "delivered")
        self.assertEqual(result["courier_name"],      "trax")

    # ------------------------------------------------------------------
    # 16. charges → shipping_cost; cod_amount → cod_amount
    # ------------------------------------------------------------------

    def test_trax_charges_to_shipping_cost(self):
        payload = {"cn": "TRAX-002", "reference": "REF-002",
                   "current_status": "In Transit", "charges": 150.0, "cod_amount": 2500.0}
        result = self.t.transform_shipment("trax", payload)
        self.assertEqual(result["shipping_cost"], 150.0)
        self.assertEqual(result["cod_amount"],    2500.0)

    # ------------------------------------------------------------------
    # 17. pickup_date → shipped_at; delivery_date → delivered_at
    # ------------------------------------------------------------------

    def test_trax_date_fields(self):
        payload = {"cn": "TRAX-003", "reference": "REF-003",
                   "current_status": "Delivered",
                   "pickup_date": "2026-09-26T08:00:00+05:00",
                   "delivery_date": "2026-09-30T12:00:00+05:00"}
        result = self.t.transform_shipment("trax", payload)
        self.assertEqual(result["shipped_at"], datetime(2026, 9, 26, 3, 0, tzinfo=timezone.utc))
        self.assertEqual(result["delivered_at"], datetime(2026, 9, 30, 7, 0, tzinfo=timezone.utc))


class TestLeopardsTransformer(unittest.TestCase):

    def setUp(self):
        self.t = CourierTransformer(business_id=BUSINESS_ID)

    # ------------------------------------------------------------------
    # 18. Leopards packet_list-wrapped response
    # ------------------------------------------------------------------

    def test_leopards_packet_list_wrapped(self):
        payload = {
            "status": 1,
            "packet_list": [{
                "booked_packet_cn":              "LEP-9001",
                "booked_packet_order_id":        "ORD-789",
                "booked_packet_status":          "Delivered",
                "booked_packet_collect_amount":  "2500",
                "booked_packet_freight":         "180",
                "booked_packet_no_of_attempts":  "1",
                "cn_date":                       "2026-09-26",
                "cn_delivered_date":             "2026-09-30",
            }]
        }
        result = self.t.transform_shipment("leopards", payload)

        self.assertEqual(result["tracking_number"],   "LEP-9001")
        self.assertEqual(result["external_order_id"], "ORD-789")
        self.assertEqual(result["shipment_status"],   "delivered")
        self.assertEqual(result["courier_name"],      "leopards")

    # ------------------------------------------------------------------
    # 19-20. Leopards specific field mappings
    # ------------------------------------------------------------------

    def test_leopards_cod_amount_from_collect_amount(self):
        payload = {"packet_list": [{"booked_packet_cn": "LEP-002",
                                    "booked_packet_order_id": "REF-002",
                                    "booked_packet_status": "Delivered",
                                    "booked_packet_collect_amount": "3200"}]}
        result = self.t.transform_shipment("leopards", payload)
        self.assertEqual(result["cod_amount"], 3200.0)

    def test_leopards_shipping_cost_from_freight(self):
        payload = {"packet_list": [{"booked_packet_cn": "LEP-003",
                                    "booked_packet_order_id": "REF-003",
                                    "booked_packet_status": "In Transit",
                                    "booked_packet_freight": "220"}]}
        result = self.t.transform_shipment("leopards", payload)
        self.assertEqual(result["shipping_cost"], 220.0)

    # ------------------------------------------------------------------
    # 21. cn_date → shipped_at; cn_delivered_date → delivered_at (delivered only)
    # ------------------------------------------------------------------

    def test_leopards_dates_only_delivered_at_when_delivered(self):
        payload = {"packet_list": [{"booked_packet_cn": "LEP-004",
                                    "booked_packet_order_id": "REF-004",
                                    "booked_packet_status": "Returning",
                                    "cn_date": "2026-09-25",
                                    "cn_delivered_date": "2026-09-29"}]}
        result = self.t.transform_shipment("leopards", payload)
        # Not delivered → delivered_at must be None
        self.assertIsNone(result["delivered_at"])
        self.assertIsNotNone(result["shipped_at"])


class TestGenericFallback(unittest.TestCase):

    def setUp(self):
        self.t = CourierTransformer(business_id=BUSINESS_ID)

    # ------------------------------------------------------------------
    # 22. Generic extraction with known field aliases
    # ------------------------------------------------------------------

    def test_generic_provider_fallback(self):
        payload = {
            "tracking_number":   "CUSTOM-001",
            "external_order_id": "5589999999999",
            "status":            "Delivered",
            "shipping_cost":     200.0,
            "cod_amount":        3500.0,
            "delivery_attempts": 1,
        }
        result = self.t.transform_shipment("custom_courier", payload)

        self.assertEqual(result["tracking_number"],   "CUSTOM-001")
        self.assertEqual(result["external_order_id"], "5589999999999")
        self.assertEqual(result["shipment_status"],   "delivered")

    # ------------------------------------------------------------------
    # 23. Provider name stored lowercase
    # ------------------------------------------------------------------

    def test_courier_name_stored_lowercase(self):
        payload = {"tracking_number": "ABC-001", "external_order_id": "REF-001",
                   "status": "Booked"}
        result = self.t.transform_shipment("MyProvider", payload)
        self.assertEqual(result["courier_name"], "myprovider")


class TestOrderIdentifierReconciliation(unittest.TestCase):

    def setUp(self):
        self.t = CourierTransformer(business_id=BUSINESS_ID)

    # ------------------------------------------------------------------
    # 24. Shopify numeric external_order_id preserved
    # ------------------------------------------------------------------

    def test_shopify_numeric_order_id_preserved(self):
        payload = {"dist": {"trackingNumber": "TN-SH", "orderRefNumber": "5589123456789",
                            "orderStatus": "Delivered"}}
        result = self.t.transform_shipment("postex", payload)
        self.assertEqual(result["external_order_id"], "5589123456789")

    # ------------------------------------------------------------------
    # 25. Daraz-prefixed order id preserved
    # ------------------------------------------------------------------

    def test_daraz_prefixed_order_id_preserved(self):
        payload = {"tracking_number": "TRAX-DARAZ-001",
                   "reference": "daraz_123456789",
                   "current_status": "In Transit"}
        result = self.t.transform_shipment("trax", payload)
        self.assertEqual(result["external_order_id"], "daraz_123456789")

    # ------------------------------------------------------------------
    # 26. Caller-injected external_order_id overrides payload value
    # ------------------------------------------------------------------

    def test_injected_external_order_id_overrides_payload(self):
        """Pipeline context injection takes priority over API response value."""
        payload = {
            "dist": {
                "trackingNumber": "TN-INJ",
                "orderRefNumber": "PAYLOAD_REF",
                "orderStatus": "Delivered",
            },
            "external_order_id": "INJECTED_FROM_DB",
        }
        result = self.t.transform_shipment("postex", payload)
        self.assertEqual(result["external_order_id"], "INJECTED_FROM_DB")


class TestRTORecordBuilder(unittest.TestCase):

    def setUp(self):
        self.t = CourierTransformer(business_id=BUSINESS_ID)
        self.shipment = {
            "business_id":       BUSINESS_ID,
            "courier_name":      "postex",
            "external_order_id": "5589000100",
            "tracking_number":   "POSTEX-RTO-001",
            "shipment_status":   "returned",
            "shipping_cost":     200.0,
            "cod_amount":        3000.0,
            "delivery_attempts": 3,
            "shipped_at":        "2026-09-25T10:00:00+05:00",
            "delivered_at":      None,
        }

    # ------------------------------------------------------------------
    # 27. Valid RTO record produced
    # ------------------------------------------------------------------

    def test_rto_record_correct_fields(self):
        rto = self.t.build_rto_record(
            self.shipment,
            return_shipping_cost=150.0,
            refund_amount=0.0,
            return_type="rto",
            raw_status="Returned to Shipper",
        )

        self.assertEqual(rto["business_id"],          BUSINESS_ID)
        self.assertEqual(rto["external_order_id"],    "5589000100")
        self.assertEqual(rto["return_type"],          "rto")
        self.assertEqual(rto["return_shipping_cost"], 150.0)
        self.assertEqual(rto["refund_amount"],        0.0)
        self.assertIn("Returned to Shipper", rto["return_reason"])
        self.assertIsNotNone(rto["return_date"])

    # ------------------------------------------------------------------
    # 28. customer_return type accepted
    # ------------------------------------------------------------------

    def test_customer_return_type_accepted(self):
        rto = self.t.build_rto_record(
            self.shipment, return_type="customer_return"
        )
        self.assertEqual(rto["return_type"], "customer_return")

    # ------------------------------------------------------------------
    # 29. Invalid return_type raises ValueError
    # ------------------------------------------------------------------

    def test_invalid_return_type_raises(self):
        with self.assertRaises(ValueError):
            self.t.build_rto_record(self.shipment, return_type="partial_rto")

    # ------------------------------------------------------------------
    # 30. return_shipping_cost clamped to >= 0
    # ------------------------------------------------------------------

    def test_return_shipping_cost_clamped(self):
        rto = self.t.build_rto_record(self.shipment, return_shipping_cost=-999.0)
        self.assertGreaterEqual(rto["return_shipping_cost"], 0.0)

    # ------------------------------------------------------------------
    # 31. return_date is a valid non-empty ISO 8601 string
    # ------------------------------------------------------------------

    def test_return_date_is_aware_datetime(self):
        rto = self.t.build_rto_record(self.shipment)
        self.assertIsInstance(rto["return_date"], datetime)
        self.assertEqual(rto["return_date"].tzinfo, timezone.utc)


class TestCODSettlementTransformer(unittest.TestCase):

    def setUp(self):
        self.t = CourierTransformer(business_id=BUSINESS_ID)

    # ------------------------------------------------------------------
    # 32. PostEx COD records
    # ------------------------------------------------------------------

    def test_postex_cod_records(self):
        records = [
            {"trackingNumber": "POSTEX-001", "orderAmount": 1000.0, "settlementDate": "2026-09-30"},
            {"trackingNumber": "POSTEX-002", "orderAmount": 2500.0, "settlementDate": "2026-09-30"},
        ]
        result = self.t.transform_cod_records(records, "postex")
        self.assertEqual(len(result), 2)
        self.assertEqual(result[0]["tracking_number"], "POSTEX-001")
        self.assertEqual(result[0]["cod_amount"], 1000.0)
        self.assertEqual(result[0]["business_id"], BUSINESS_ID)
        self.assertEqual(result[0]["courier_name"], "postex")

    # ------------------------------------------------------------------
    # 33. Trax COD records
    # ------------------------------------------------------------------

    def test_trax_cod_records(self):
        records = [
            {"cn": "TRAX-001", "amount": 1500.0, "settlement_date": "2026-09-30"},
        ]
        result = self.t.transform_cod_records(records, "trax")
        self.assertEqual(result[0]["tracking_number"], "TRAX-001")
        self.assertEqual(result[0]["cod_amount"],      1500.0)

    # ------------------------------------------------------------------
    # 34. Leopards COD records (booked_packet_collect_amount)
    # ------------------------------------------------------------------

    def test_leopards_cod_records(self):
        records = [
            {"booked_packet_cn": "LEP-001", "booked_packet_collect_amount": "3200"},
        ]
        result = self.t.transform_cod_records(records, "leopards")
        self.assertEqual(result[0]["tracking_number"], "LEP-001")
        self.assertEqual(result[0]["cod_amount"],      3200.0)

    # ------------------------------------------------------------------
    # 35. Records with empty tracking_number are skipped
    # ------------------------------------------------------------------

    def test_empty_tracking_skipped(self):
        records = [
            {"trackingNumber": "", "orderAmount": 1000.0},
            {"orderAmount": 2000.0},  # no tracking at all
            {"trackingNumber": "POSTEX-OK", "orderAmount": 500.0},
        ]
        result = self.t.transform_cod_records(records, "postex")
        self.assertEqual(len(result), 1)
        self.assertEqual(result[0]["tracking_number"], "POSTEX-OK")


class TestMonetaryParsing(unittest.TestCase):
    """Tests for _parse_money (pure function)."""

    # ------------------------------------------------------------------
    # 36. String amounts with commas and decimals
    # ------------------------------------------------------------------

    def test_comma_separated_amount(self):
        self.assertAlmostEqual(_parse_money("1,500.50"), 1500.50, places=2)

    def test_plain_decimal_string(self):
        self.assertAlmostEqual(_parse_money("3250.75"), 3250.75, places=2)

    def test_integer_string(self):
        self.assertAlmostEqual(_parse_money("200"), 200.0, places=2)

    def test_float_value(self):
        self.assertAlmostEqual(_parse_money(4250.0), 4250.0, places=2)

    # ------------------------------------------------------------------
    # 37. Negative values clamped to 0.0
    # ------------------------------------------------------------------

    def test_negative_clamped_to_zero(self):
        self.assertEqual(_parse_money(-500.0), 0.0)
        self.assertEqual(_parse_money("-100"), 0.0)

    # ------------------------------------------------------------------
    # 38. None and unparseable values → 0.0
    # ------------------------------------------------------------------

    def test_none_returns_zero(self):
        self.assertEqual(_parse_money(None), 0.0)

    def test_empty_string_returns_zero(self):
        self.assertEqual(_parse_money(""), 0.0)

    def test_unparseable_returns_zero(self):
        self.assertEqual(_parse_money("N/A"), 0.0)
        self.assertEqual(_parse_money("PKR 1500"), 0.0)


class TestEdgeCases(unittest.TestCase):

    def setUp(self):
        self.t = CourierTransformer(business_id=BUSINESS_ID)

    # ------------------------------------------------------------------
    # 39. Empty tracking_number raises ValueError
    # ------------------------------------------------------------------

    def test_empty_tracking_raises_value_error(self):
        payload = {"external_order_id": "REF-001", "status": "Booked"}
        with self.assertRaises(ValueError):
            self.t.transform_shipment("postex", payload)

    def test_tracking_number_in_dist_empty_raises(self):
        payload = {"dist": {"trackingNumber": "", "orderRefNumber": "REF-001",
                            "orderStatus": "Booked"}}
        with self.assertRaises(ValueError):
            self.t.transform_shipment("postex", payload)

    # ------------------------------------------------------------------
    # 40. All monetary values >= 0
    # ------------------------------------------------------------------

    def test_monetary_values_non_negative(self):
        payload = {"dist": {"trackingNumber": "TN-001", "orderRefNumber": "REF-001",
                            "orderStatus": "Delivered",
                            "deliveryCharges": -100.0,  # invalid negative
                            "orderAmount":     -500.0}}
        result = self.t.transform_shipment("postex", payload)
        self.assertGreaterEqual(result["shipping_cost"], 0.0)
        self.assertGreaterEqual(result["cod_amount"],    0.0)

    # ------------------------------------------------------------------
    # 41. business_id always present in output
    # ------------------------------------------------------------------

    def test_business_id_always_present(self):
        payload = {"dist": {"trackingNumber": "TN-002", "orderRefNumber": "REF-002",
                            "orderStatus": "Booked"}}
        result = self.t.transform_shipment("postex", payload)
        self.assertEqual(result["business_id"], BUSINESS_ID)

    # ------------------------------------------------------------------
    # 42. delivery_attempts always >= 0
    # ------------------------------------------------------------------

    def test_delivery_attempts_non_negative(self):
        payload = {"dist": {"trackingNumber": "TN-003", "orderRefNumber": "REF-003",
                            "orderStatus": "Failed", "attemptCount": -2}}
        result = self.t.transform_shipment("postex", payload)
        self.assertGreaterEqual(result["delivery_attempts"], 0)

    # ------------------------------------------------------------------
    # 43. Mixed-case/uppercase status normalised correctly
    # ------------------------------------------------------------------

    def test_uppercase_status_normalised(self):
        for raw, expected in [
            ("IN TRANSIT", "in_transit"),
            ("DELIVERED",  "delivered"),
            ("RETURNED TO SHIPPER", "returned"),
        ]:
            with self.subTest(raw=raw):
                self.assertEqual(normalize_shipment_status(raw), expected)


if __name__ == "__main__":
    unittest.main(verbosity=2)
