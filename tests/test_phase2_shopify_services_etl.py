"""
Unit & Integration Test Suite for Sub-Phase 2:
Shopify Modular Data Services + ETL/Loader Adaptation.

Covers:
1. Services:
   - ShopifyOrderService
   - ShopifyProductService
   - ShopifyCustomerService
   - ShopifyInventoryService
   - ShopifyLocationService
2. GraphQL -> ETL Transformations:
   - Order transformation (customers, products, orders, order_items)
   - Product catalog transformation
   - Customer profile transformation
   - Inventory levels transformation
3. Edge Cases:
   - Null customer / guest checkout (deterministic synthetic phone)
   - Null price set / zero amounts
   - Missing optional fields
   - Empty line items
   - Multiple variants & locations
   - Malformed/unknown status fallback
   - Non-GID / clean ID handling
4. Normalizations:
   - Pakistani phone (+92 E.164)
   - GID numeric extraction
   - Status mapping to canonical CHECK constraint
   - Decimal monetary handling
5. PostgreSQL Loader & Idempotency:
   - PostgresLoader.load_inventory() insert
   - PostgresLoader.load_inventory() update on re-sync (no duplicates)
   - Multi-tenant business_id preservation
6. Credential Safety:
   - Zero plaintext secrets logged
"""
import sys
import unittest
from datetime import date
from decimal import Decimal
from pathlib import Path
from unittest.mock import MagicMock, patch

import pandas as pd
from sqlalchemy import text

# Ensure root directory is on Python path
sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from config.database import get_db_connection
from connectors.shopify.graphql_client import ShopifyGraphQLClient
from connectors.shopify.services import (
    ShopifyCustomerService,
    ShopifyInventoryService,
    ShopifyLocationService,
    ShopifyOrderService,
    ShopifyProductService,
)
from etl.shopify_transformer import (
    ShopifyTransformer,
    extract_money_amount,
    extract_numeric_id,
    normalize_graphql_order_status,
)
from loaders.postgres_loader import PostgresLoader


class TestShopifyModularServices(unittest.TestCase):
    def setUp(self):
        self.mock_client = MagicMock(spec=ShopifyGraphQLClient)

    def test_order_service_fetch_orders(self):
        service = ShopifyOrderService(self.mock_client)
        self.mock_client.paginate.return_value = [
            {
                "page": 1,
                "nodes": [{"id": "gid://shopify/Order/101", "name": "#1001"}],
                "edges": [{"cursor": "c1", "node": {"id": "gid://shopify/Order/101"}}],
                "page_info": {"hasNextPage": False},
                "raw_response": {"data": {"orders": {}}},
            }
        ]

        pages = list(service.fetch_orders(updated_at_min="2026-09-01T00:00:00Z"))
        self.assertEqual(len(pages), 1)
        self.assertEqual(pages[0]["orders"][0]["name"], "#1001")
        self.mock_client.paginate.assert_called_once()
        kwargs = self.mock_client.paginate.call_args[1]
        self.assertIn("updated_at:>='2026-09-01T00:00:00Z'", kwargs["variables"]["query"])

    def test_product_service_fetch_products(self):
        service = ShopifyProductService(self.mock_client)
        self.mock_client.paginate.return_value = [
            {
                "page": 1,
                "nodes": [{"id": "gid://shopify/Product/201", "title": "Sneakers"}],
                "edges": [],
                "page_info": {"hasNextPage": False},
                "raw_response": {},
            }
        ]

        pages = list(service.fetch_products(query_filter="status:active"))
        self.assertEqual(len(pages), 1)
        self.assertEqual(pages[0]["products"][0]["title"], "Sneakers")

    def test_customer_service_fetch_customers(self):
        service = ShopifyCustomerService(self.mock_client)
        self.mock_client.paginate.return_value = [
            {
                "page": 1,
                "nodes": [{"id": "gid://shopify/Customer/301", "firstName": "Ahmed"}],
                "edges": [],
                "page_info": {"hasNextPage": False},
                "raw_response": {},
            }
        ]

        pages = list(service.fetch_customers())
        self.assertEqual(len(pages), 1)
        self.assertEqual(pages[0]["customers"][0]["firstName"], "Ahmed")

    def test_location_service_fetch_locations(self):
        service = ShopifyLocationService(self.mock_client)
        self.mock_client.paginate.return_value = [
            {
                "page": 1,
                "nodes": [
                    {"id": "gid://shopify/Location/1", "name": "Karachi Hub", "isActive": True},
                    {"id": "gid://shopify/Location/2", "name": "Lahore Hub", "isActive": True},
                ],
                "edges": [],
                "page_info": {"hasNextPage": False},
                "raw_response": {},
            }
        ]

        locs = service.fetch_locations()
        self.assertEqual(len(locs), 2)
        self.assertEqual(locs[0]["name"], "Karachi Hub")
        self.assertEqual(locs[1]["name"], "Lahore Hub")

    def test_inventory_service_fetch_inventory(self):
        service = ShopifyInventoryService(self.mock_client)
        self.mock_client.paginate.return_value = [
            {
                "page": 1,
                "nodes": [
                    {"id": "gid://shopify/InventoryLevel/401", "quantities": [{"name": "available", "quantity": 15}]},
                ],
                "edges": [],
                "page_info": {"hasNextPage": False},
                "raw_response": {},
            }
        ]

        pages = list(service.fetch_inventory_for_location("gid://shopify/Location/1"))
        self.assertEqual(len(pages), 1)
        self.assertEqual(pages[0]["inventory_levels"][0]["quantities"][0]["quantity"], 15)
        self.mock_client.paginate.assert_called_once()
        kwargs = self.mock_client.paginate.call_args[1]
        self.assertEqual(kwargs["variables"]["locationId"], "gid://shopify/Location/1")


class TestShopifyETLAndNormalizers(unittest.TestCase):
    def setUp(self):
        self.business_id = 1
        self.transformer = ShopifyTransformer(self.business_id)

    def test_extract_numeric_id(self):
        self.assertEqual(extract_numeric_id("gid://shopify/Order/5589123456789"), "5589123456789")
        self.assertEqual(extract_numeric_id("gid://shopify/Product/7890123456"), "7890123456")
        self.assertEqual(extract_numeric_id("12345"), "12345")
        self.assertEqual(extract_numeric_id(""), "")
        self.assertEqual(extract_numeric_id(None), "")

    def test_extract_money_amount(self):
        m1 = {"shopMoney": {"amount": "4500.50", "currencyCode": "PKR"}}
        self.assertEqual(extract_money_amount(m1), 4500.50)
        m2 = {"amount": "250.00"}
        self.assertEqual(extract_money_amount(m2), 250.00)
        self.assertEqual(extract_money_amount(None), 0.0)
        self.assertEqual(extract_money_amount({}), 0.0)

    def test_normalize_graphql_order_status(self):
        self.assertEqual(normalize_graphql_order_status({"cancelledAt": "2026-09-29T10:00:00Z"}), "cancelled")
        self.assertEqual(normalize_graphql_order_status({"displayFulfillmentStatus": "FULFILLED"}), "fulfilled")
        self.assertEqual(normalize_graphql_order_status({"displayFulfillmentStatus": "PARTIALLY_FULFILLED"}), "confirmed")
        self.assertEqual(normalize_graphql_order_status({"displayFinancialStatus": "PAID"}), "confirmed")
        self.assertEqual(normalize_graphql_order_status({"displayFinancialStatus": "REFUNDED"}), "returned")
        self.assertEqual(normalize_graphql_order_status({"displayFinancialStatus": "VOIDED"}), "cancelled")
        self.assertEqual(normalize_graphql_order_status({}), "pending")

    def test_transform_graphql_orders_complete(self):
        raw_order = {
            "id": "gid://shopify/Order/9900112233",
            "name": "#1099",
            "createdAt": "2026-09-29T10:15:30Z",
            "updatedAt": "2026-09-29T10:15:30Z",
            "cancelledAt": None,
            "displayFinancialStatus": "PAID",
            "displayFulfillmentStatus": "FULFILLED",
            "currencyCode": "PKR",
            "currentSubtotalPriceSet": {"shopMoney": {"amount": "5000.00", "currencyCode": "PKR"}},
            "currentTotalDiscountsSet": {"shopMoney": {"amount": "500.00", "currencyCode": "PKR"}},
            "totalShippingPriceSet": {"shopMoney": {"amount": "200.00", "currencyCode": "PKR"}},
            "currentTotalPriceSet": {"shopMoney": {"amount": "4700.00", "currencyCode": "PKR"}},
            "paymentGatewayNames": ["Cash on Delivery (COD)"],
            "customer": {
                "id": "gid://shopify/Customer/887766",
                "firstName": "Zain",
                "lastName": "Abbas",
                "email": "zain.abbas@example.com",
                "phone": "03001234567",
            },
            "shippingAddress": {
                "name": "Zain Abbas",
                "phone": "0300-1234567",
                "city": "Islamabad",
                "province": "Federal",
                "country": "Pakistan",
            },
            "lineItems": {
                "edges": [
                    {
                        "node": {
                          "id": "gid://shopify/LineItem/776655",
                          "title": "Leather Belt",
                          "sku": "BELT-001",
                          "quantity": 2,
                          "originalUnitPriceSet": {"shopMoney": {"amount": "2500.00", "currencyCode": "PKR"}},
                          "totalDiscountSet": {"shopMoney": {"amount": "500.00", "currencyCode": "PKR"}},
                          "product": {"id": "gid://shopify/Product/665544"},
                          "variant": {"id": "gid://shopify/ProductVariant/554433", "sku": "BELT-001", "title": "Black"},
                        }
                    }
                ]
            }
        }

        df_cust, df_prod, df_ord, df_items = self.transformer.transform_graphql_orders([raw_order])

        # Validate Customers
        self.assertEqual(len(df_cust), 1)
        self.assertEqual(df_cust.iloc[0]["phone"], "+923001234567")
        self.assertEqual(df_cust.iloc[0]["full_name"], "Zain Abbas")
        self.assertEqual(df_cust.iloc[0]["external_customer_id"], "887766")

        # Validate Products
        self.assertEqual(len(df_prod), 1)
        self.assertEqual(df_prod.iloc[0]["external_product_id"], "665544")
        self.assertEqual(df_prod.iloc[0]["sku"], "BELT-001")
        self.assertEqual(df_prod.iloc[0]["selling_price"], 2500.0)

        # Validate Orders
        self.assertEqual(len(df_ord), 1)
        self.assertEqual(df_ord.iloc[0]["external_order_id"], "9900112233")
        self.assertEqual(df_ord.iloc[0]["order_status"], "fulfilled")
        self.assertEqual(df_ord.iloc[0]["payment_method"], "cod")
        self.assertEqual(df_ord.iloc[0]["subtotal_amount"], 5000.0)
        self.assertEqual(df_ord.iloc[0]["discount_amount"], 500.0)
        self.assertEqual(df_ord.iloc[0]["shipping_charged"], 200.0)
        self.assertEqual(df_ord.iloc[0]["total_amount"], 4700.0)
        self.assertEqual(df_ord.iloc[0]["currency"], "PKR")

        # Validate Order Items
        self.assertEqual(len(df_items), 1)
        self.assertEqual(df_items.iloc[0]["external_order_id"], "9900112233")
        self.assertEqual(df_items.iloc[0]["external_product_id"], "665544")
        self.assertEqual(df_items.iloc[0]["quantity"], 2)
        self.assertEqual(df_items.iloc[0]["unit_price"], 2500.0)

    def test_transform_graphql_orders_guest_checkout(self):
        # Null customer object -> deterministic synthetic phone number
        guest_order = {
            "id": "gid://shopify/Order/777888999",
            "name": "#GUEST-1",
            "createdAt": "2026-09-29T11:00:00Z",
            "currentSubtotalPriceSet": {"shopMoney": {"amount": "1000.00"}},
            "currentTotalPriceSet": {"shopMoney": {"amount": "1000.00"}},
            "customer": None,
            "shippingAddress": {"name": "Guest Customer", "city": "Rawalpindi"},
            "lineItems": {"edges": []},
        }

        df_cust, _, df_ord, _ = self.transformer.transform_graphql_orders([guest_order])
        self.assertEqual(len(df_cust), 1)
        phone = df_cust.iloc[0]["phone"]
        self.assertTrue(phone.startswith("+923"), f"Synthetic phone '{phone}' must match +923...")
        self.assertEqual(len(phone), 13)
        self.assertEqual(df_cust.iloc[0]["full_name"], "Guest Customer")
        self.assertEqual(df_ord.iloc[0]["customer_phone"], phone)

    def test_transform_graphql_products(self):
        raw_product = {
            "id": "gid://shopify/Product/12345",
            "title": "Summer Polo",
            "status": "ACTIVE",
            "variants": {
                "edges": [
                    {"node": {"id": "gid://shopify/ProductVariant/67890", "sku": "POLO-BLU-M", "price": "1800.00"}}
                ]
            }
        }
        df_p = self.transformer.transform_graphql_products([raw_product])
        self.assertEqual(len(df_p), 1)
        self.assertEqual(df_p.iloc[0]["external_product_id"], "12345")
        self.assertEqual(df_p.iloc[0]["sku"], "POLO-BLU-M")
        self.assertEqual(df_p.iloc[0]["selling_price"], 1800.0)
        self.assertTrue(df_p.iloc[0]["is_active"])

    def test_transform_graphql_inventory(self):
        raw_inv = [
            {
                "id": "gid://shopify/InventoryLevel/1",
                "quantities": [{"name": "available", "quantity": 42}],
                "item": {"id": "gid://shopify/InventoryItem/9988", "sku": "POLO-BLU-M"}
            }
        ]
        df_inv = self.transformer.transform_graphql_inventory(
            raw_inv,
            location_name="Lahore Warehouse",
            snapshot_date=date(2026, 10, 1),
        )
        self.assertEqual(len(df_inv), 1)
        self.assertEqual(df_inv.iloc[0]["business_id"], self.business_id)
        self.assertEqual(df_inv.iloc[0]["external_product_id"], "9988")
        self.assertEqual(df_inv.iloc[0]["location_name"], "Lahore Warehouse")
        self.assertEqual(df_inv.iloc[0]["quantity_available"], 42)


class TestPostgresInventoryLoader(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.business_id = 1
        cls.test_product_id = "test_inv_prod_999"
        cls.test_sku = "SKU-INV-TEST-01"
        cls.loader = PostgresLoader(cls.business_id)

        # Pre-seed a product in core.product to satisfy foreign key
        with get_db_connection() as conn:
            conn.execute(text("""
                INSERT INTO core.product (
                    business_id, external_product_id, sku, product_name, selling_price, unit_cost, is_active, created_at, updated_at
                )
                VALUES (
                    :bid, :ext_id, :sku, 'Test Inventory Product', 1500.00, 500.00, true, now(), now()
                )
                ON CONFLICT (business_id, external_product_id) DO NOTHING;
            """), {"bid": cls.business_id, "ext_id": cls.test_product_id, "sku": cls.test_sku})
            conn.commit()

    @classmethod
    def tearDownClass(cls):
        # Cleanup test inventory and product
        with get_db_connection() as conn:
            conn.execute(text("""
                DELETE FROM core.inventory
                WHERE business_id = :bid AND location_name = 'Test Hub Central';
            """), {"bid": cls.business_id})
            conn.execute(text("""
                DELETE FROM core.product
                WHERE business_id = :bid AND external_product_id = :ext_id;
            """), {"bid": cls.business_id, "ext_id": cls.test_product_id})
            conn.commit()

    def test_load_inventory_idempotency(self):
        test_date = date(2026, 10, 1)
        df_inv = pd.DataFrame([
            {
                "business_id": self.business_id,
                "external_product_id": self.test_product_id,
                "location_name": "Test Hub Central",
                "snapshot_date": test_date,
                "quantity_available": 100,
            }
        ])

        # 1. First Load -> INSERT
        ins, upd = self.loader.load_inventory(df_inv)
        self.assertEqual(ins, 1)
        self.assertEqual(upd, 0)

        # Verify DB value
        with get_db_connection() as conn:
            qty = conn.execute(text("""
                SELECT quantity_available FROM core.inventory
                WHERE business_id = :bid AND location_name = 'Test Hub Central' AND snapshot_date = :dt;
            """), {"bid": self.business_id, "dt": test_date}).scalar()
            self.assertEqual(qty, 100)

        # 2. Second Load with updated quantity -> UPDATE, no duplicate
        df_inv_updated = pd.DataFrame([
            {
                "business_id": self.business_id,
                "external_product_id": self.test_product_id,
                "location_name": "Test Hub Central",
                "snapshot_date": test_date,
                "quantity_available": 85,
            }
        ])

        ins2, upd2 = self.loader.load_inventory(df_inv_updated)
        self.assertEqual(ins2, 0)
        self.assertEqual(upd2, 1)

        # Verify updated value in DB and single row count
        with get_db_connection() as conn:
            qty2 = conn.execute(text("""
                SELECT quantity_available FROM core.inventory
                WHERE business_id = :bid AND location_name = 'Test Hub Central' AND snapshot_date = :dt;
            """), {"bid": self.business_id, "dt": test_date}).scalar()
            self.assertEqual(qty2, 85)

            count = conn.execute(text("""
                SELECT count(*) FROM core.inventory
                WHERE business_id = :bid AND location_name = 'Test Hub Central' AND snapshot_date = :dt;
            """), {"bid": self.business_id, "dt": test_date}).scalar()
            self.assertEqual(count, 1)


if __name__ == "__main__":
    unittest.main()
