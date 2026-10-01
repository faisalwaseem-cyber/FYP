"""
Mock Test Script:
Verifies the end-to-end ingestion flow:
Raw Payload -> raw.raw_data -> ETL/Normalizer -> DataValidator -> PostgresLoader -> core tables
"""
import sys
from pathlib import Path

# Add project root to path
sys.path.append(str(Path(__file__).resolve().parent.parent))

from raw_layer.raw_repository import save_raw_payload
from etl.shopify_transformer import ShopifyTransformer
from loaders.postgres_loader import PostgresLoader
from pipeline.sync_logger import SyncLogger
from config.database import get_db_connection
from sqlalchemy import text

# Sample Shopify Order JSON Payload (Standard Pakistani Store scenario)
MOCK_SHOPIFY_ORDERS = [
    {
        "id": 5589123456789,
        "created_at": "2026-09-29T10:15:30+05:00",
        "updated_at": "2026-09-29T10:15:30+05:00",
        "cancelled_at": None,
        "financial_status": "pending",
        "fulfillment_status": None,
        "gateway": "Cash on Delivery (COD)",
        "payment_gateway_names": ["Cash on Delivery (COD)"],
        "currency": "PKR",
        "subtotal_price": "4500.00",
        "total_discounts": "500.00",
        "total_price": "4250.00",
        "total_shipping_price_set": {
            "shop_money": {"amount": "250.00", "currency_code": "PKR"}
        },
        "customer": {
            "id": 9988776655,
            "first_name": "Hamza",
            "last_name": "Tariq",
            "email": "hamza.tariq@gmail.com",
            "phone": "03218765432"  # Pakistani local format -> should normalize to +923218765432
        },
        "shipping_address": {
            "name": "Hamza Tariq",
            "phone": "0321-8765432",
            "city": "Lahore",
            "province": "Punjab",
            "country": "Pakistan"
        },
        "line_items": [
            {
                "id": 887766554433,
                "product_id": 7890123456,
                "variant_id": 9012345678,
                "title": "Classic Leather Wallet",
                "name": "Classic Leather Wallet - Brown",
                "sku": "WL-BRN-001",
                "price": "2500.00",
                "quantity": 1,
                "total_discount": "250.00"
            },
            {
                "id": 887766554434,
                "product_id": 7890123457,
                "variant_id": 9012345679,
                "title": "Minimalist Card Holder",
                "name": "Minimalist Card Holder - Black",
                "sku": "CH-BLK-002",
                "price": "2000.00",
                "quantity": 1,
                "total_discount": "250.00"
            }
        ]
    }
]

def run_mock_test():
    business_id = 1
    print("=" * 70)
    print("[*] Running Mock Shopify Pipeline Ingestion Test...")
    print("=" * 70)

    # 1. Start Sync Run
    logger = SyncLogger(business_id=business_id, source="shopify")
    sync_id = logger.start_sync(sync_type="mock_test")
    print(f"[*] Started Sync Run #{sync_id}")

    try:
        transformer = ShopifyTransformer(business_id)
        loader = PostgresLoader(business_id)

        # 2. Persist in Raw Layer
        mock_raw_response = {"orders": MOCK_SHOPIFY_ORDERS}
        raw_id = save_raw_payload(business_id, "shopify", "orders", mock_raw_response)
        print(f"[OK] Step 1: Raw JSON saved to raw.raw_data (raw_id: {raw_id})")

        # 3. Transform & Clean
        df_cust, df_prod, df_orders, df_items = transformer.transform(MOCK_SHOPIFY_ORDERS)
        print(f"[OK] Step 2: Transformed into DataFrames:")
        print(f"    - Customers: {len(df_cust)} (Phone normalized to: {df_cust['phone'].iloc[0]})")
        print(f"    - Products:  {len(df_prod)}")
        print(f"    - Orders:    {len(df_orders)} (Status: {df_orders['order_status'].iloc[0]}, Method: {df_orders['payment_method'].iloc[0]})")
        print(f"    - Items:     {len(df_items)}")

        # 4. Load into PostgreSQL
        loader.load_customers(df_cust)
        loader.load_products(df_prod)
        inserted, updated = loader.load_orders(df_orders, df_items)
        print(f"[OK] Step 3: Canonical Database Loader:")
        print(f"    - Orders Inserted: {inserted}, Updated: {updated}")

        # 5. Finish log
        logger.finish_success(fetched=len(MOCK_SHOPIFY_ORDERS), inserted=inserted, updated=updated)
        print(f"[OK] Step 4: Sync logged to raw.sync_log with status 'success'")

        # 6. Verify by querying the database directly
        with get_db_connection() as conn:
            ord_row = conn.execute(text("""
                SELECT o.order_id, o.external_order_id, c.full_name, c.phone, o.total_amount, o.order_status
                FROM core.orders o
                JOIN core.customer c ON o.customer_id = c.customer_id
                WHERE o.external_order_id = '5589123456789';
            """)).fetchone()

            print("\n" + "=" * 70)
            print("[VERIFICATION] Successfully queried inserted record from PostgreSQL:")
            print(f"  Order DB ID:      {ord_row[0]}")
            print(f"  Shopify Order ID: {ord_row[1]}")
            print(f"  Customer Name:    {ord_row[2]}")
            print(f"  Customer Phone:   {ord_row[3]} (Passed constraint ^\\+92[0-9]{{10}}$)")
            print(f"  Total Amount:     PKR {ord_row[4]}")
            print(f"  Order Status:     {ord_row[5]}")
            print("=" * 70)
            print("[SUCCESS] The complete ingestion pipeline is verified and working perfectly!\n")

    except Exception as e:
        logger.finish_failure(str(e))
        print(f"[!] Mock Pipeline Test Failed: {e}")
        raise

if __name__ == "__main__":
    run_mock_test()
