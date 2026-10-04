"""
Unit Test Suite for Daraz Services & ETL Transformation (Phase 3.3).

Validates:
1. Order transformation (Orders, Customers, Products, Items DataFrames)
2. Product transformation (Standalone /products/get to df_products)
3. Inventory transformation (SKU stock to df_inventory)
4. Customer transformation with Daraz masked phone handling (+92300*****67)
5. Malformed and null data edge cases
6. Status mapping across all Daraz order statuses
7. Monetary parsing with exact 2-decimal precision
8. DarazOrderService, DarazProductService, and DarazInventoryService
"""
import sys
import unittest
from datetime import date
from decimal import Decimal
from pathlib import Path
from unittest.mock import MagicMock, patch

import pandas as pd

# Ensure root directory is on Python path
sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from connectors.daraz.client import DarazClient
from connectors.daraz.services import (
    DarazInventoryService,
    DarazOrderService,
    DarazProductService,
)
from etl.daraz_transformer import (
    DarazTransformer,
    normalize_daraz_payment_method,
    normalize_daraz_status,
    parse_daraz_money,
)


class TestDarazETLAndServices(unittest.TestCase):
    def setUp(self):
        self.b_id = 1
        self.transformer = DarazTransformer(business_id=self.b_id)

    # ------------------------------------------------------------------
    # 1. Status & Payment Method Mapping
    # ------------------------------------------------------------------

    def test_status_mapping(self):
        self.assertEqual(normalize_daraz_status("unpaid"), "pending")
        self.assertEqual(normalize_daraz_status("pending"), "pending")
        self.assertEqual(normalize_daraz_status("ready_to_ship"), "confirmed")
        self.assertEqual(normalize_daraz_status("shipped"), "fulfilled")
        self.assertEqual(normalize_daraz_status("toship"), "confirmed")
        self.assertEqual(normalize_daraz_status("topack"), "confirmed")
        self.assertEqual(normalize_daraz_status("delivered"), "delivered")
        self.assertEqual(normalize_daraz_status("canceled"), "cancelled")
        self.assertEqual(normalize_daraz_status("cancelled"), "cancelled")
        self.assertIsNone(normalize_daraz_status("failed"))
        self.assertEqual(normalize_daraz_status("returned"), "returned")
        self.assertIsNone(normalize_daraz_status("unknown_status_xyz"))
        self.assertIsNone(normalize_daraz_status(None))

    def test_payment_method_mapping(self):
        self.assertEqual(normalize_daraz_payment_method("Cash on Delivery (COD)"), "cod")
        self.assertEqual(normalize_daraz_payment_method("Credit Card"), "card")
        self.assertEqual(normalize_daraz_payment_method("Visa Card"), "card")
        self.assertEqual(normalize_daraz_payment_method("Daraz Wallet"), "wallet")
        self.assertEqual(normalize_daraz_payment_method("Easypaisa"), "wallet")
        self.assertEqual(normalize_daraz_payment_method("JazzCash"), "wallet")
        self.assertEqual(normalize_daraz_payment_method("Bank Transfer"), "bank_transfer")
        self.assertEqual(normalize_daraz_payment_method("Unknown"), "other")
        self.assertEqual(normalize_daraz_payment_method(None), "other")

    # ------------------------------------------------------------------
    # 2. Monetary Precision Parsing
    # ------------------------------------------------------------------

    def test_money_conversion(self):
        self.assertEqual(parse_daraz_money("1250.50"), Decimal("1250.50"))
        self.assertIsNone(parse_daraz_money("1,250.50"))
        self.assertEqual(parse_daraz_money(1250), Decimal("1250.00"))
        self.assertEqual(parse_daraz_money(0), Decimal("0.00"))
        self.assertIsNone(parse_daraz_money(None))
        self.assertIsNone(parse_daraz_money("invalid"))
        self.assertIsNone(parse_daraz_money(-50))
        self.assertIsNone(parse_daraz_money("1.001"))

    # ------------------------------------------------------------------
    # 3. Order Transformation & Customer Masked Phone Normalization
    # ------------------------------------------------------------------

    def test_order_transformation(self):
        raw_orders = [
            {
                "order_id": 987654321,
                "created_at": "2026-10-01T14:00:00+05:00",
                "status": "ready_to_ship",
                "payment_method": "COD",
                "price": "3500.00",
                "subtotal_amount": "3300.00",
                "shipping_fee": "200.00",
                "customer_first_name": "Tariq",
                "customer_last_name": "Jamil",
                "address_shipping": {
                    "first_name": "Tariq",
                    "last_name": "Jamil",
                    "phone": "+923001234567",
                    "city": "Rawalpindi",
                    "country": "Pakistan",
                },
                "fetched_order_items": [
                    {
                        "sku_id": "112233",
                        "item_price": "3300.00",
                        "quantity": 1,
                    }
                ],
            }
        ]

        df_c, df_p, df_o, df_i = self.transformer.transform_orders(raw_orders)

        # 1. Customers
        self.assertEqual(len(df_c), 1)
        cust_row = df_c.iloc[0]
        self.assertEqual(cust_row["full_name"], "Tariq Jamil")
        self.assertEqual(cust_row["phone"], "+923001234567")
        self.assertIsNone(cust_row["external_customer_id"])
        self.assertEqual(cust_row["city"], "Rawalpindi")

        # Order items require products to be loaded from the catalog separately.
        self.assertTrue(df_p.empty)

        # 2. Orders
        self.assertEqual(len(df_o), 1)
        ord_row = df_o.iloc[0]
        self.assertEqual(ord_row["external_order_id"], "987654321")
        self.assertEqual(ord_row["order_status"], "confirmed")
        self.assertEqual(ord_row["payment_method"], "cod")
        self.assertEqual(ord_row["total_amount"], Decimal("3500.00"))
        self.assertEqual(ord_row["shipping_charged"], Decimal("200.00"))
        self.assertEqual(ord_row["subtotal_amount"], Decimal("3300.00"))
        self.assertEqual(ord_row["source_platform"], "daraz")
        self.assertEqual(ord_row["business_id"], self.b_id)

        # 3. Items retain external join keys for the later loader.
        self.assertEqual(len(df_i), 1)
        item_row = df_i.iloc[0]
        self.assertEqual(item_row["external_order_id"], "987654321")
        self.assertEqual(item_row["external_product_id"], "112233")
        self.assertEqual(item_row["quantity"], 1)
        self.assertEqual(item_row["unit_price"], Decimal("3300.00"))

    def test_order_without_verified_subtotal_is_rejected(self):
        order = {
            "order_id": "order-1",
            "created_at": "2026-10-01T10:00:00Z",
            "status": "pending",
            "price": "100.00",
            "shipping_fee": "0.00",
            "address_shipping": {"phone": "+923001234567"},
        }
        _, _, orders, _ = self.transformer.transform_orders([order])
        self.assertTrue(orders.empty)
        self.assertEqual(self.transformer.rejected_records[0]["external_id"], "order-1")
        self.assertIn("subtotal", str(self.transformer.rejected_records[0]["reason"]))

    # ------------------------------------------------------------------
    # 4. Product Catalog Transformation
    # ------------------------------------------------------------------

    def test_product_catalog_transformation(self):
        raw_products = [
            {
                "item_id": 551122,
                "attributes": {"name": "Men Casual T-Shirt"},
                "skus": [
                    {
                        "SkuId": 101,
                        "SellerSku": "TSHIRT-M-BLK",
                        "ShopSku": "DARAZ_TSHIRT_M",
                        "price": "999.00",
                        "Status": "active",
                    },
                    {
                        "SkuId": 102,
                        "SellerSku": "TSHIRT-L-BLK",
                        "ShopSku": "DARAZ_TSHIRT_L",
                        "price": "1099.00",
                        "Status": "active",
                    },
                ],
            }
        ]

        df_prod = self.transformer.transform_products(raw_products)
        self.assertEqual(len(df_prod), 2)
        skus = list(df_prod["sku"])
        self.assertIn("TSHIRT-M-BLK", skus)
        self.assertIn("TSHIRT-L-BLK", skus)
        prices = list(df_prod["selling_price"])
        self.assertIn(Decimal("999.00"), prices)
        self.assertIn(Decimal("1099.00"), prices)
        self.assertEqual(set(df_prod["external_product_id"]), {"101", "102"})
        self.assertNotIn("item_id", df_prod.columns)
        self.assertTrue((df_prod["business_id"] == self.b_id).all())

    def test_multiple_skus_have_no_invented_parent_relationship(self):
        rows = self.transformer.transform_products([{
            "item_id": "parent-7",
            "attributes": {"name": "Variant product"},
            "skus": [
                {"SkuId": 701, "SellerSku": "SKU-701", "price": "10.00", "Status": "active"},
                {"SkuId": 702, "SellerSku": "SKU-702", "price": "11.00", "Status": "active"},
            ],
        }])
        self.assertEqual(list(rows["external_product_id"]), ["701", "702"])
        self.assertNotIn("parent_product_id", rows.columns)

    # ------------------------------------------------------------------
    # 5. Inventory Transformation
    # ------------------------------------------------------------------

    def test_inventory_transformation_does_not_guess_stock_semantics(self):
        raw_inventory_items = [
            {
                "item_id": "parent-7",
                "skus": [{
                    "SellerSku": "SKU-701",
                    "Available": 75,
                    "RealTimeStock": 80,
                    "ReservedStock": 5,
                    "multiWarehouseInventories": [{"warehouseCode": "WH-1", "quantity": 75}],
                }],
            },
        ]

        df_inv = self.transformer.transform_inventory(raw_inventory_items)
        self.assertTrue(df_inv.empty)
        self.assertEqual(len(self.transformer.rejected_records), 1)
        self.assertIn("unverified", str(self.transformer.rejected_records[0]["reason"]))

    # ------------------------------------------------------------------
    # 6. Malformed and Null Edge Cases
    # ------------------------------------------------------------------

    def test_malformed_and_null_data_is_rejected_without_fabrication(self):
        malformed_orders = [
            # Missing address, null items, null prices
            {
                "order_id": 111000,
                "created_at": "2026-10-01T15:00:00Z",
                "status": None,
                "price": None,
                "shipping_fee": None,
                "address_shipping": None,
                "items": None,
            },
            # Empty dictionary
            {},
        ]

        df_c, df_p, df_o, df_i = self.transformer.transform_orders(malformed_orders)
        self.assertEqual(len(df_o), 0)
        self.assertEqual(len(df_c), 0)
        self.assertEqual(len(df_i), 0)
        self.assertTrue(self.transformer.rejected_records)

    def test_empty_responses_have_schema_columns(self):
        customers, products, orders, items = self.transformer.transform_orders([])
        self.assertEqual(list(customers.columns), ["business_id", "external_customer_id", "full_name", "phone", "email", "city"])
        self.assertEqual(list(products.columns), ["business_id", "external_product_id", "sku", "product_name", "selling_price", "is_active"])
        self.assertEqual(list(orders.columns)[0], "business_id")
        self.assertEqual(list(items.columns)[0], "business_id")
        self.assertTrue(self.transformer.transform_products([]).empty)
        self.assertTrue(self.transformer.transform_inventory([]).empty)

    def test_provider_business_id_is_ignored(self):
        _, _, orders, _ = self.transformer.transform_orders([{
            "business_id": 999,
            "order_id": "order-tenant",
            "created_at": "2026-10-01T10:00:00Z",
            "status": "delivered",
            "payment_method": "COD",
            "total_amount": "20.00",
            "subtotal_amount": "20.00",
            "shipping_fee": "0.00",
            "address_shipping": {"phone": "+923001234567"},
        }])
        self.assertEqual(len(orders), 1)
        self.assertEqual(orders.iloc[0]["business_id"], self.b_id)

    # ------------------------------------------------------------------
    # 7. Modular Services Layer Tests
    # ------------------------------------------------------------------

    def test_daraz_order_service(self):
        mock_client = MagicMock(spec=DarazClient)
        mock_client.paginate.return_value = [
            {
                "page": 1,
                "offset": 0,
                "count": 1,
                "records": [{"order_id": 12345}],
                "raw_response": {"raw": "order_data"},
            }
        ]
        mock_client.fetch_order_items.return_value = [{"sku": "SKU-99", "item_price": 500}]

        service = DarazOrderService(mock_client)
        pages = list(service.fetch_orders(created_after="2026-10-01", hydrate_items=True))

        self.assertEqual(len(pages), 1)
        self.assertEqual(pages[0]["orders"][0]["order_id"], 12345)
        self.assertEqual(pages[0]["orders"][0]["fetched_order_items"][0]["sku"], "SKU-99")
        mock_client.fetch_order_items.assert_called_once_with("12345")

    def test_daraz_product_service(self):
        mock_client = MagicMock(spec=DarazClient)
        mock_client.paginate.return_value = [
            {
                "page": 1,
                "offset": 0,
                "count": 1,
                "records": [{"item_id": 9988}],
                "raw_response": {"raw": "prod_data"},
            }
        ]

        service = DarazProductService(mock_client)
        pages = list(service.fetch_products(filter_status="live"))
        self.assertEqual(len(pages), 1)
        self.assertEqual(pages[0]["products"][0]["item_id"], 9988)

    def test_daraz_product_item_service(self):
        mock_client = MagicMock(spec=DarazClient)
        mock_client.execute.return_value = {"item_id": "item-1", "skus": []}
        service = DarazProductService(mock_client)
        result = service.fetch_product_item("item-1", seller_sku="SELLER-1")
        self.assertEqual(result["item_id"], "item-1")
        mock_client.execute.assert_called_once_with(
            "/product/item/get",
            params={"item_id": "item-1", "seller_sku": "SELLER-1"},
            method="GET",
        )

    def test_daraz_inventory_service_preserves_raw_stock_fields(self):
        mock_client = MagicMock(spec=DarazClient)
        mock_client.paginate.return_value = [
            {
                "page": 1,
                "offset": 0,
                "count": 1,
                "records": [{"item_id": 7711, "skus": [{"SellerSku": "SKU-LIVE-01", "Available": 100}]}],
                "raw_response": {"raw": "inv_data"},
            }
        ]

        service = DarazInventoryService(mock_client)
        pages = list(service.fetch_inventory())
        self.assertEqual(len(pages), 1)
        self.assertEqual(pages[0]["products"][0]["skus"][0]["Available"], 100)
        call_params = mock_client.paginate.call_args.kwargs["params"]
        self.assertEqual(call_params["options"], "1")
        self.assertEqual(call_params["filter"], "all")

    def test_order_service_does_not_swallow_item_errors(self):
        mock_client = MagicMock(spec=DarazClient)
        mock_client.paginate.return_value = [{
            "page": 1,
            "offset": 0,
            "count": 1,
            "records": [{"order_id": "123"}],
            "raw_response": {"orders": [{"order_id": "123"}]},
        }]
        mock_client.fetch_order_items.side_effect = RuntimeError("safe test error")
        with self.assertRaisesRegex(RuntimeError, "safe test error"):
            list(DarazOrderService(mock_client).fetch_orders(created_after="2026-10-01"))


if __name__ == "__main__":
    unittest.main()
