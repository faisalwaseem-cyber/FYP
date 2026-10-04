import unittest
from datetime import date, datetime, timezone
from decimal import Decimal

from connectors.payment.base import safe_parse_money
from etl.courier_transformer import CourierTransformer
from etl.daraz_transformer import DarazTransformer
from etl.meta_ads_transformer import MetaAdsTransformer
from etl.normalizers import normalize_money, normalize_timestamp, synthetic_pakistan_phone
from etl.payment_transformer import PaymentTransformer
from etl.shopify_transformer import ShopifyTransformer, normalize_order_status


class TestPhase71Standardization(unittest.TestCase):
    def test_shared_money_timestamp_and_stable_phone_formats(self):
        self.assertEqual(normalize_money("1,234.565"), Decimal("1234.57"))
        self.assertEqual(safe_parse_money("1.005"), 1.01)
        self.assertEqual(
            normalize_timestamp("2026-10-01T12:00:00+05:00"),
            datetime(2026, 10, 1, 7, 0, tzinfo=timezone.utc),
        )
        phone = synthetic_pakistan_phone("shopify", "guest-100")
        self.assertEqual(phone, synthetic_pakistan_phone("shopify", "guest-100"))
        self.assertRegex(phone, r"^\+92[0-9]{10}$")

    def test_shopify_and_daraz_orders_emit_canonical_fields(self):
        shopify_order = {
            "id": "gid://shopify/Order/1001",
            "createdAt": "2026-10-01T12:00:00Z",
            "currencyCode": "pkr",
            "displayFinancialStatus": "REFUNDED",
            "currentSubtotalPriceSet": {"shopMoney": {"amount": "12.34"}},
            "currentTotalPriceSet": {"shopMoney": {"amount": "12.34"}},
            "customer": None,
            "shippingAddress": {"city": " Karachi ", "province": " Sindh "},
            "lineItems": {"nodes": []},
        }
        _, _, shopify_orders, _ = ShopifyTransformer(1).transform_graphql_orders([shopify_order])
        shopify = shopify_orders.iloc[0]
        self.assertEqual(shopify["external_order_id"], "1001")
        self.assertIsInstance(shopify["order_date"], datetime)
        self.assertEqual(shopify["order_date"].tzinfo, timezone.utc)
        self.assertIsInstance(shopify["total_amount"], Decimal)
        self.assertEqual(shopify["currency"], "PKR")
        self.assertEqual(shopify["order_status"], "returned")
        self.assertEqual(shopify["shipping_city"], "Karachi")
        self.assertEqual(normalize_order_status({"financial_status": "partially_refunded"}), "returned")

        daraz_order = {
            "order_id": 2002,
            "created_at": "2026-10-01T12:00:00+05:00",
            "price": "1,250.50",
            "shipping_fee": "50.25",
            "address_shipping": {
                "phone": "03001234567",
                "city": "Lahore",
                "province": "Punjab",
                "country": "Pakistan",
            },
        }
        _, _, daraz_orders, _ = DarazTransformer(1).transform_orders([daraz_order])
        daraz = daraz_orders.iloc[0]
        self.assertIsInstance(daraz["order_date"], datetime)
        self.assertIsInstance(daraz["total_amount"], Decimal)
        self.assertIsInstance(daraz["shipping_charged"], Decimal)
        self.assertEqual(daraz["shipping_province"], "Punjab")
        self.assertNotEqual(daraz["shipping_province"], "Pakistan")
        self.assertEqual(daraz["currency"], "PKR")

    def test_meta_ads_preserves_large_counts_and_database_date_money_types(self):
        _, spend_df = MetaAdsTransformer(1).transform_daily_insights([{
            "campaign_id": "0012",
            "date_start": "2026-10-01",
            "spend": "12.34",
            "impressions": "9007199254740993",
            "clicks": "12",
        }])
        row = spend_df.iloc[0]
        self.assertEqual(row["external_campaign_id"], "0012")
        self.assertEqual(row["spend_date"], date(2026, 10, 1))
        self.assertEqual(row["spend_amount"], Decimal("12.34"))
        self.assertEqual(row["impressions"], 9007199254740993)

    def test_courier_and_payment_emit_decimal_and_aware_timestamp_values(self):
        shipment = CourierTransformer(1).transform_shipment("postex", {
            "trackingNumber": "TN-1",
            "orderRefNumber": "1001",
            "orderStatus": "Delivered",
            "deliveryCharges": "125.40",
            "bookingDate": "2026-10-01T12:00:00+05:00",
            "deliveryDate": "2026-10-01T16:00:00+05:00",
        })
        self.assertIsInstance(shipment["shipping_cost"], Decimal)
        self.assertEqual(shipment["shipped_at"], datetime(2026, 10, 1, 7, 0, tzinfo=timezone.utc))
        self.assertEqual(shipment["delivered_at"], datetime(2026, 10, 1, 11, 0, tzinfo=timezone.utc))

        payments = PaymentTransformer(1).transform_provider_data("manual", [{
            "external_order_id": "1001",
            "external_payment_id": "pay-1",
            "amount": "1.005",
            "gateway_fee": "0.005",
            "paid_at": "2026-10-01T12:00:00+05:00",
        }], {"1001": 10})
        payment_row = payments.iloc[0]
        self.assertEqual(payment_row["amount"], Decimal("1.01"))
        self.assertEqual(payment_row["gateway_fee"], Decimal("0.01"))
        self.assertEqual(payment_row["paid_at"], datetime(2026, 10, 1, 7, 0, tzinfo=timezone.utc))


if __name__ == "__main__":
    unittest.main()