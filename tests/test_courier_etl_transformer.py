"""Provider-neutral unit tests for generic courier ETL behavior."""
import unittest
from datetime import datetime, timezone
from decimal import Decimal

from etl.courier_transformer import (
    CourierTransformer,
    _parse_money,
    normalize_shipment_status,
)


class TestNormalizeShipmentStatus(unittest.TestCase):
    def test_canonical_statuses_pass_through(self):
        statuses = (
            "booked",
            "in_transit",
            "delivered",
            "failed_delivery",
            "returning",
            "returned",
        )
        for status in statuses:
            with self.subTest(status=status):
                self.assertEqual(normalize_shipment_status(status), status)

    def test_common_status_variants_normalize(self):
        cases = {
            "Delivered": "delivered",
            "Out for Delivery": "in_transit",
            "Returned to Shipper": "returned",
            "Refused": "failed_delivery",
            "unrecognized": "booked",
        }
        for raw, expected in cases.items():
            with self.subTest(raw=raw):
                self.assertEqual(normalize_shipment_status(raw), expected)


class TestGenericCourierTransformer(unittest.TestCase):
    def setUp(self):
        self.transformer = CourierTransformer(business_id=1)

    def test_common_fields_transform_to_canonical_shipment(self):
        shipment = self.transformer.transform_shipment("custom_courier", {
            "tracking_number": "TN-1",
            "external_order_id": "ORDER-1",
            "status": "Delivered",
            "shipping_cost": "125.40",
            "cod_amount": "1500.00",
            "delivery_attempts": "2",
            "shipped_at": "2026-10-01T12:00:00+05:00",
            "delivery_date": "2026-10-01T16:00:00+05:00",
        })

        self.assertEqual(shipment["business_id"], 1)
        self.assertEqual(shipment["courier_name"], "custom_courier")
        self.assertEqual(shipment["tracking_number"], "TN-1")
        self.assertEqual(shipment["external_order_id"], "ORDER-1")
        self.assertEqual(shipment["shipment_status"], "delivered")
        self.assertEqual(shipment["shipping_cost"], Decimal("125.40"))
        self.assertEqual(shipment["cod_amount"], Decimal("1500.00"))
        self.assertEqual(shipment["delivery_attempts"], 2)
        self.assertEqual(
            shipment["shipped_at"],
            datetime(2026, 10, 1, 7, 0, tzinfo=timezone.utc),
        )
        self.assertEqual(
            shipment["delivered_at"],
            datetime(2026, 10, 1, 11, 0, tzinfo=timezone.utc),
        )

    def test_caller_order_and_tracking_values_override_payload(self):
        shipment = self.transformer.transform_shipment("custom_courier", {
            "trackingNumber": "TN-OLD",
            "order_id": "ORDER-OLD",
            "status": "In Transit",
            "tracking_number": "TN-NEW",
            "external_order_id": "ORDER-NEW",
        })
        self.assertEqual(shipment["tracking_number"], "TN-NEW")
        self.assertEqual(shipment["external_order_id"], "ORDER-NEW")

    def test_delivered_at_is_omitted_for_non_delivered_shipments(self):
        shipment = self.transformer.transform_shipment("custom_courier", {
            "tracking_number": "TN-2",
            "status": "In Transit",
            "delivery_date": "2026-10-01T16:00:00Z",
        })
        self.assertIsNone(shipment["delivered_at"])

    def test_missing_tracking_number_is_rejected(self):
        with self.assertRaisesRegex(ValueError, "tracking_number is empty"):
            self.transformer.transform_shipment("custom_courier", {
                "external_order_id": "ORDER-1",
                "status": "Booked",
            })

    def test_rto_record_uses_shared_shape_and_validates_type(self):
        shipment = self.transformer.transform_shipment("custom_courier", {
            "tracking_number": "TN-RTO",
            "external_order_id": "ORDER-RTO",
            "status": "Returned",
        })
        rto = self.transformer.build_rto_record(
            shipment,
            return_shipping_cost="50.25",
            return_type="rto",
        )

        self.assertEqual(rto["business_id"], 1)
        self.assertEqual(rto["external_order_id"], "ORDER-RTO")
        self.assertEqual(rto["return_type"], "rto")
        self.assertEqual(rto["return_shipping_cost"], 50.25)
        self.assertEqual(rto["return_date"].tzinfo, timezone.utc)
        with self.assertRaisesRegex(ValueError, "Invalid return_type"):
            self.transformer.build_rto_record(shipment, return_type="invalid")

    def test_generic_cod_settlement_records_skip_missing_tracking(self):
        records = self.transformer.transform_cod_records([
            {
                "tracking_number": "TN-COD",
                "cod_amount": "1200.00",
                "settlement_date": "2026-10-01T12:00:00+05:00",
            },
            {"cod_amount": "300.00"},
        ], "custom_courier")

        self.assertEqual(len(records), 1)
        self.assertEqual(records[0]["tracking_number"], "TN-COD")
        self.assertEqual(records[0]["cod_amount"], Decimal("1200.00"))
        self.assertEqual(records[0]["courier_name"], "custom_courier")
        self.assertEqual(
            records[0]["settlement_date"],
            datetime(2026, 10, 1, 7, 0, tzinfo=timezone.utc),
        )

    def test_money_parser_clamps_invalid_and_negative_values(self):
        self.assertEqual(_parse_money("1,500.50"), Decimal("1500.50"))
        self.assertEqual(_parse_money("-100"), Decimal("0.00"))
        self.assertEqual(_parse_money("not a number"), Decimal("0.00"))


if __name__ == "__main__":
    unittest.main()
