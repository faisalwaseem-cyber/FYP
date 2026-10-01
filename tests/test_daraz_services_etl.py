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
        self.assertEqual(normalize_daraz_status("pending"), "confirmed")
        self.assertEqual(normalize_daraz_status("ready_to_ship"), "confirmed")
        self.assertEqual(normalize_daraz_status("shipped"), "fulfilled")
        self.assertEqual(normalize_daraz_status("toship"), "fulfilled")
        self.assertEqual(normalize_daraz_status("delivered"), "delivered")
        self.assertEqual(normalize_daraz_status("canceled"), "cancelled")
        self.assertEqual(normalize_daraz_status("cancelled"), "cancelled")
        self.assertEqual(normalize_daraz_status("failed"), "rto")
        self.assertEqual(normalize_daraz_status("failed_delivery"), "rto")
        self.assertEqual(normalize_daraz_status("returned"), "returned")
        self.assertEqual(normalize_daraz_status("returned_to_shipper"), "returned")
        self.assertEqual(normalize_daraz_status("unknown_status_xyz"), "pending")
        self.assertEqual(normalize_daraz_status(None), "pending")

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
        self.assertEqual(parse_daraz_money("1250.50"), 1250.50)
        self.assertEqual(parse_daraz_money("1,250.50"), 1250.50)
        self.assertEqual(parse_daraz_money(1250), 1250.0)
        self.assertEqual(parse_daraz_money(0), 0.0)
        self.assertEqual(parse_daraz_money(None), 0.0)
        self.assertEqual(parse_daraz_money("invalid"), 0.0)
        self.assertEqual(parse_daraz_money(-50), 0.0)

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
                "shipping_fee": "200.00",
                "customer_first_name": "Tariq",
                "customer_last_name": "Jamil",
                "customer_email": "tariq@example.com",
                "address_shipping": {
                    "first_name": "Tariq",
                    "last_name": "Jamil",
                    "phone": "+92300*****12",  # Masked phone
                    "city": "Rawalpindi",
                    "country": "Pakistan",
                },
                "items": [
                    {
                        "sku": "DRZ-PROD-001",
                        "sku_id": "112233",
                        "name": "Leather Wallet Genuine",
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
        # Masked phone must be converted into valid synthetic E.164 phone: ^\+92[0-9]{10}$
        self.assertTrue(cust_row["phone"].startswith("+923"))
        self.assertEqual(len(cust_row["phone"]), 13)
        self.assertEqual(cust_row["city"], "Rawalpindi")

        # 2. Products
        self.assertEqual(len(df_p), 1)
        prod_row = df_p.iloc[0]
        self.assertEqual(prod_row["external_product_id"], "daraz_prod_DRZ-PROD-001")
        self.assertEqual(prod_row["sku"], "DRZ-PROD-001")
        self.assertEqual(prod_row["selling_price"], 3300.00)

        # 3. Orders
        self.assertEqual(len(df_o), 1)
        ord_row = df_o.iloc[0]
        self.assertEqual(ord_row["external_order_id"], "daraz_987654321")
        self.assertEqual(ord_row["order_status"], "confirmed")
        self.assertEqual(ord_row["payment_method"], "cod")
        self.assertEqual(ord_row["total_amount"], 3500.00)
        self.assertEqual(ord_row["shipping_charged"], 200.00)
        self.assertEqual(ord_row["subtotal_amount"], 3300.00)
        self.assertEqual(ord_row["source_platform"], "daraz")

        # 4. Items
        self.assertEqual(len(df_i), 1)
        item_row = df_i.iloc[0]
        self.assertEqual(item_row["external_order_id"], "daraz_987654321")
        self.assertEqual(item_row["external_product_id"], "daraz_prod_DRZ-PROD-001")
        self.assertEqual(item_row["quantity"], 1)
        self.assertEqual(item_row["unit_price"], 3300.00)

    # ------------------------------------------------------------------
    # 4. Product Catalog Transformation
    # ------------------------------------------------------------------

    def test_product_catalog_transformation(self):
        raw_products = [
            {
                "item_id": 551122,
                "name": "Men Casual T-Shirt",
                "skus": [
                    {
                        "SellerSku": "TSHIRT-M-BLK",
                        "ShopSku": "DARAZ_TSHIRT_M",
                        "price": "999.00",
                        "Status": "active",
                    },
                    {
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
        self.assertIn(999.00, prices)
        self.assertIn(1099.00, prices)

    # ------------------------------------------------------------------
    # 5. Inventory Transformation
    # ------------------------------------------------------------------

    def test_inventory_transformation(self):
        raw_inventory_items = [
            {
                "seller_sku": "TSHIRT-M-BLK",
                "quantity": 75,
                "price": 999.00,
            },
            {
                "seller_sku": "TSHIRT-L-BLK",
                "quantity": 30,
                "price": 1099.00,
            },
        ]

        df_inv = self.transformer.transform_inventory(
            raw_inventory_items,
            location_name="Daraz Clifton Hub",
            snapshot_date=date(2026, 10, 1),
        )
        self.assertEqual(len(df_inv), 2)
        self.assertIn("daraz_prod_TSHIRT-M-BLK", list(df_inv["external_product_id"]))
        self.assertEqual(df_inv.iloc[0]["quantity_available"], 75)
        self.assertEqual(df_inv.iloc[0]["location_name"], "Daraz Clifton Hub")

    # ------------------------------------------------------------------
    # 6. Malformed and Null Edge Cases
    # ------------------------------------------------------------------

    def test_malformed_and_null_data(self):
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
        self.assertEqual(len(df_o), 1)
        self.assertEqual(df_o.iloc[0]["order_status"], "pending")
        self.assertEqual(df_o.iloc[0]["total_amount"], 0.0)
        self.assertEqual(len(df_c), 1)
        self.assertTrue(df_c.iloc[0]["phone"].startswith("+923"))
        self.assertEqual(len(df_i), 0)

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
        self.assertEqual(pages[0]["orders"][0]["items"][0]["sku"], "SKU-99")
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

    def test_daraz_inventory_service(self):
        mock_client = MagicMock(spec=DarazClient)
        mock_client.paginate.return_value = [
            {
                "page": 1,
                "offset": 0,
                "count": 1,
                "records": [
                    {
                        "item_id": 7711,
                        "skus": [{"SellerSku": "SKU-LIVE-01", "quantity": 100, "price": 450.0}],
                    }
                ],
                "raw_response": {"raw": "inv_data"},
            }
        ]

        service = DarazInventoryService(mock_client)
        pages = list(service.fetch_inventory())
        self.assertEqual(len(pages), 1)
        self.assertEqual(pages[0]["inventory_levels"][0]["seller_sku"], "SKU-LIVE-01")
        self.assertEqual(pages[0]["inventory_levels"][0]["quantity"], 100)


if __name__ == "__main__":
    unittest.main()
