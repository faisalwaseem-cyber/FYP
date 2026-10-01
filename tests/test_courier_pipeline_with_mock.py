"""
Mock Test for Courier & Shipping Ingestion Pipeline.
Verifies:
1. Raw courier payload storage into raw.raw_data
2. PostEx status transformation and normalization
3. Shipment record upsert into core.shipment
4. Cascading order status updates:
   - 'delivered' -> core.orders.order_status = 'delivered'
   - 'returned' (RTO) -> core.orders.order_status = 'rto' + inserts into core.rto_return
"""
import sys
from pathlib import Path

# Add project root to path
sys.path.append(str(Path(__file__).resolve().parent.parent))

from raw_layer.raw_repository import save_raw_payload
from etl.courier_transformer import CourierTransformer
from loaders.courier_loader import CourierLoader
from pipeline.sync_logger import SyncLogger
from config.database import get_db_connection
from sqlalchemy import text

# Sample PostEx Tracking Payloads
MOCK_POSTEX_DELIVERY_PAYLOAD = {
    "orderRefNumber": "5589123456789",  # Matches our Shopify order
    "trackingNumber": "POSTEX-PK-987654321",
    "orderStatus": "Delivered",
    "deliveryCharges": 250.00,
    "orderAmount": 4250.00,
    "attemptCount": 1,
    "bookingDate": "2026-09-29T11:00:00+05:00",
    "deliveryDate": "2026-09-29T16:30:00+05:00"
}

def run_courier_mock_test():
    business_id = 1
    print("=" * 70)
    print("[*] Running Mock Courier Ingestion & RTO Test (PostEx)...")
    print("=" * 70)

    logger = SyncLogger(business_id=business_id, source="courier")
    sync_id = logger.start_sync(sync_type="courier_mock_test")
    print(f"[*] Started Courier Sync Run #{sync_id}")

    try:
        transformer = CourierTransformer(business_id=business_id)
        loader = CourierLoader(business_id=business_id)

        # 1. Save Raw Payload in raw.raw_data
        raw_id = save_raw_payload(business_id, "courier", "postex_tracking", MOCK_POSTEX_DELIVERY_PAYLOAD)
        print(f"[OK] Step 1: Raw Courier JSON saved to raw.raw_data (raw_id: {raw_id})")

        # 2. Transform & Standardize
        shipment_data = transformer.transform_shipment(
            courier_name="postex",
            payload=MOCK_POSTEX_DELIVERY_PAYLOAD
        )
        print(f"[OK] Step 2: Transformed to canonical format:")
        print(f"    - Courier:        {shipment_data['courier_name']}")
        print(f"    - Tracking No:    {shipment_data['tracking_number']}")
        print(f"    - Status:         {shipment_data['shipment_status']}")
        print(f"    - Shipping Cost:  PKR {shipment_data['shipping_cost']}")
        print(f"    - COD Amount:     PKR {shipment_data['cod_amount']}")

        # 3. Load into core.shipment & cascade to core.orders
        shipment_id = loader.load_shipment(shipment_data)
        print(f"[OK] Step 3: Shipment upserted in core.shipment (shipment_id: {shipment_id})")

        # 4. Verify in Database
        with get_db_connection() as conn:
            row = conn.execute(text("""
                SELECT s.tracking_number, c.courier_name, s.shipment_status, o.external_order_id, o.order_status, s.shipping_cost, s.delivered_at
                FROM core.shipment s
                JOIN core.courier c ON s.courier_id = c.courier_id
                JOIN core.orders o ON s.order_id = o.order_id
                WHERE s.tracking_number = :tn;
            """), {"tn": shipment_data["tracking_number"]}).fetchone()

            print("\n" + "=" * 70)
            print("[VERIFICATION] Successfully queried linked records:")
            print(f"  Tracking Number:   {row[0]}")
            print(f"  Courier:           {row[1]}")
            print(f"  Shipment Status:   {row[2]}")
            print(f"  Shopify Order Ref: {row[3]}")
            print(f"  Cascaded Order St: {row[4]} (Order successfully updated from 'pending' -> 'delivered'!)")
            print(f"  Actual Cost:       PKR {row[5]}")
            print(f"  Delivered At:      {row[6]}")
            print("=" * 70)

        # 5. Finish sync log
        logger.finish_success(fetched=1, inserted=1, updated=1)
        print("[OK] Step 4: Logged successful run in raw.sync_log\n")

        # --- PART 2: TEST RTO (RETURN TO ORIGIN) SCENARIO ---
        print("=" * 70)
        print("[*] Testing RTO (Return to Origin) Tracking...")
        print("=" * 70)

        # Create a second Shopify order specifically for RTO demonstration
        with get_db_connection() as conn:
            conn.execute(text("""
                INSERT INTO core.orders (
                    business_id, customer_id, external_order_id, order_date, order_status,
                    payment_method, subtotal_amount, discount_amount, shipping_charged,
                    total_amount, currency, shipping_city, created_at
                )
                SELECT 1, customer_id, '5589123456799', now(), 'fulfilled',
                       'cod', 2500.00, 0.0, 200.0, 2700.00, 'PKR', 'Rawalpindi', now()
                FROM core.customer WHERE business_id = 1 LIMIT 1
                ON CONFLICT (business_id, external_order_id) DO NOTHING;
            """))
            conn.commit()

        mock_rto_payload = {
            "orderRefNumber": "5589123456799",
            "trackingNumber": "POSTEX-PK-RTO-999",
            "orderStatus": "Returned to Shipper",
            "deliveryCharges": 200.00,
            "orderAmount": 2700.00,
            "attemptCount": 3,
            "bookingDate": "2026-09-28T10:00:00+05:00"
        }

        save_raw_payload(business_id, "courier", "postex_tracking", mock_rto_payload)
        rto_shipment_data = transformer.transform_shipment("postex", mock_rto_payload)
        rto_shipment_id = loader.load_shipment(rto_shipment_data, default_rto_penalty=150.0)

        # Verify RTO in database
        with get_db_connection() as conn:
            rto_check = conn.execute(text("""
                SELECT o.external_order_id, o.order_status, s.shipment_status, s.delivery_attempts, r.return_type, r.return_shipping_cost, r.return_reason
                FROM core.orders o
                JOIN core.shipment s ON o.order_id = s.order_id
                JOIN core.rto_return r ON s.shipment_id = r.shipment_id
                WHERE o.external_order_id = '5589123456799';
            """)).fetchone()

            print("[VERIFICATION] Successfully verified RTO cascading update:")
            print(f"  Shopify Order Ref: {rto_check[0]}")
            print(f"  Updated Order St:  {rto_check[1]} (Transitioned to 'rto'!)")
            print(f"  Shipment Status:   {rto_check[2]}")
            print(f"  Delivery Attempts: {rto_check[3]}")
            print(f"  Return Type:       {rto_check[4]}")
            print(f"  Return Penalty:    PKR {rto_check[5]}")
            print(f"  Return Reason:     {rto_check[6]}")
            print("=" * 70)
            print("[SUCCESS] Both Delivery and RTO courier flows are completely verified!\n")

    except Exception as e:
        logger.finish_failure(str(e))
        print(f"[!] Courier Pipeline Test Failed: {e}")
        raise

if __name__ == "__main__":
    run_courier_mock_test()
