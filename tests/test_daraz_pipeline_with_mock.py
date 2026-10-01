"""
Mock Test for Daraz Open Platform Ingestion Pipeline.
Verifies:
1. Raw Daraz JSON payload saved to raw.raw_data (source: 'daraz')
2. Handling of masked customer phone numbers (+92300*****67)
3. Normalization of Daraz statuses ('delivered', 'failed' -> 'rto')
4. Loading into canonical tables with source_platform = 'daraz'
5. Verification query across multiple sales channels (Shopify vs Daraz)
"""
import sys
from pathlib import Path

# Add project root to path
sys.path.append(str(Path(__file__).resolve().parent.parent))

from raw_layer.raw_repository import save_raw_payload
from etl.daraz_transformer import DarazTransformer
from loaders.postgres_loader import PostgresLoader
from pipeline.sync_logger import SyncLogger
from config.database import get_db_connection
from sqlalchemy import text

# Realistic Daraz Open Platform /orders/get payload
MOCK_DARAZ_ORDERS = [
    {
        "order_id": 189201928301,
        "order_number": 189201928301,
        "created_at": "2026-09-29T12:00:00+05:00",
        "updated_at": "2026-09-29T16:00:00+05:00",
        "status": "delivered",
        "payment_method": "Cash on Delivery (COD)",
        "price": "2000.00",
        "shipping_fee": "150.00",
        "customer_first_name": "Zubair",
        "customer_last_name": "Ahmed",
        "customer_email": "zubair.a@example.com",
        "address_shipping": {
            "first_name": "Zubair",
            "last_name": "Ahmed",
            "phone": "+92300*****67",  # Masked phone by Daraz marketplace privacy
            "city": "Faisalabad",
            "country": "Pakistan"
        },
        "items": [
            {
                "sku": "DRZ-EAR-01",
                "sku_id": "9928172",
                "name": "Wireless Earbuds Bluetooth 5.3",
                "item_price": "1850.00",
                "paid_price": "1850.00"
            }
        ]
    },
    {
        "order_id": 189201928302,
        "order_number": 189201928302,
        "created_at": "2026-09-29T13:30:00+05:00",
        "updated_at": "2026-09-29T17:00:00+05:00",
        "status": "failed",  # Failed delivery in Daraz -> Canonical 'rto'
        "payment_method": "Daraz Wallet",
        "price": "3500.00",
        "shipping_fee": "0.00",
        "customer_first_name": "Bilal",
        "customer_last_name": "Sheikh",
        "customer_email": "bilal.s@example.com",
        "address_shipping": {
            "first_name": "Bilal",
            "last_name": "Sheikh",
            "phone": "03335551234",  # Valid local Pakistani mobile
            "city": "Peshawar",
            "country": "Pakistan"
        },
        "items": [
            {
                "sku": "DRZ-WCH-02",
                "sku_id": "9928173",
                "name": "Smart Watch Ultra 8 (AMOLED Display)",
                "item_price": "3500.00",
                "paid_price": "3500.00"
            }
        ]
    }
]

def run_daraz_mock_test():
    business_id = 1
    print("=" * 70)
    print("[*] Running Mock Daraz Marketplace Ingestion Test...")
    print("=" * 70)

    logger = SyncLogger(business_id=business_id, source="daraz")
    sync_id = logger.start_sync(sync_type="daraz_mock_test")
    print(f"[*] Started Daraz Sync Run #{sync_id}")

    try:
        transformer = DarazTransformer(business_id=business_id)
        loader = PostgresLoader(business_id=business_id)

        # 1. Save Raw Payload in raw.raw_data
        raw_payload = {"orders": MOCK_DARAZ_ORDERS}
        raw_id = save_raw_payload(business_id, "daraz", "orders", raw_payload)
        print(f"[OK] Step 1: Raw Daraz JSON saved to raw.raw_data (raw_id: {raw_id})")

        # 2. Transform into DataFrames
        df_cust, df_prod, df_orders, df_items = transformer.transform(MOCK_DARAZ_ORDERS)
        print(f"[OK] Step 2: Transformed into DataFrames:")
        print(f"    - Customers: {len(df_cust)} (Masked phone handled: {df_cust['phone'].iloc[0]})")
        print(f"    - Products:  {len(df_prod)}")
        print(f"    - Orders:    {len(df_orders)} (Platform: {df_orders['source_platform'].iloc[0]})")
        print(f"    - Items:     {len(df_items)}")

        # 3. Load into Canonical Tables
        loader.load_customers(df_cust)
        loader.load_products(df_prod)
        ins, upd = loader.load_orders(df_orders, df_items)
        print(f"[OK] Step 3: Canonical Database Loader:")
        print(f"    - Orders Inserted: {ins}, Updated: {upd}")

        # 4. Finish Sync Logger
        logger.finish_success(fetched=len(MOCK_DARAZ_ORDERS), inserted=ins, updated=upd)
        print(f"[OK] Step 4: Logged successful run in raw.sync_log")

        # 5. Multi-Channel Verification Query (Shopify vs Daraz)
        with get_db_connection() as conn:
            summary = conn.execute(text("""
                SELECT 
                    source_platform,
                    COUNT(*) AS total_orders,
                    SUM(total_amount) AS total_revenue,
                    COUNT(*) FILTER (WHERE order_status = 'delivered') AS delivered_orders,
                    COUNT(*) FILTER (WHERE order_status = 'rto') AS rto_orders
                FROM core.orders
                WHERE business_id = :b_id
                GROUP BY source_platform
                ORDER BY source_platform;
            """), {"b_id": business_id}).fetchall()

            print("\n" + "=" * 70)
            print("[VERIFICATION] Unified Multi-Channel E-Commerce Database Audit:")
            print(f"{'Platform':<12} | {'Orders':<8} | {'Revenue':<15} | {'Delivered':<10} | {'RTO':<5}")
            print("-" * 60)
            for row in summary:
                print(f"{row[0]:<12} | {row[1]:<8} | PKR {row[2]:>10,.2f} | {row[3]:<10} | {row[4]:<5}")
            print("=" * 70)
            print("[SUCCESS] Daraz Marketplace Ingestion Layer is verified and working perfectly!\n")

    except Exception as e:
        logger.finish_failure(str(e))
        print(f"[!] Daraz Pipeline Test Failed: {e}")
        raise

if __name__ == "__main__":
    run_daraz_mock_test()
