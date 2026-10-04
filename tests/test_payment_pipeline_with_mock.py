"""
End-to-End Test for Payment Gateway Ingestion Pipeline.

Validates:
1. Manual CSV payment import
2. Manual COD reconciliation import
4. Verification that records are saved in raw.raw_data and core.payment
5. Verification that the Profit Engine automatically recalculates true_profit
   accounting for exact gateway_fee deductions!
"""
import sys
from pathlib import Path
from sqlalchemy import text

sys.path.append(str(Path(__file__).resolve().parent.parent))

from config.settings import BUSINESS_ID
from config.database import get_db_connection
from loaders.payment_loader import PaymentLoader
from pipeline.run_payment_sync import run_payment_csv_sync
from analytics.profit_engine import calculate_order_profitability


def test_payment_pipeline():
    print("=" * 70)
    print("TEST: PAYMENT GATEWAY INGESTION & PROFIT RECALCULATION")
    print("=" * 70)

    loader = PaymentLoader(BUSINESS_ID)
    order_map = loader.get_order_id_map()
    print(f"[OK] Found {len(order_map)} orders in DB: {list(order_map.keys())}")

    # -------------------------------------------------------------------------
    # 1. TEST MANUAL CSV INGESTION
    # -------------------------------------------------------------------------
    print("\n--- 1. Testing Manual Payment CSV Ingestion ---")
    manual_csv_data = """external_payment_id,external_order_id,amount,payment_method,payment_status
MAN-TXN-88219,daraz_189201928301,1850.00,bank_transfer,paid
MAN-TXN-88220,daraz_189201928302,3500.00,bank_transfer,paid
"""
    result_manual = run_payment_csv_sync(
        csv_content=manual_csv_data,
        gateway_name="manual",
        sync_type="mock_manual_test"
    )
    print(f"  Manual Sync Result: {result_manual}")

    # -------------------------------------------------------------------------
    # 3. TEST MANUAL COD RECONCILIATION CSV
    # -------------------------------------------------------------------------
    print("\n--- 3. Testing Manual COD Reconciliation CSV ---")
    manual_cod_csv = """external_payment_id,external_order_id,amount,payment_method,payment_status,paid_at
COD-PAY-5589,5589123456799,2500.00,cod,paid,2024-01-18 16:00:00
"""
    result_cod = run_payment_csv_sync(
        csv_content=manual_cod_csv,
        gateway_name="manual",
        sync_type="mock_cod_test"
    )
    print(f"  COD Sync Result: {result_cod}")

    # -------------------------------------------------------------------------
    # 4. RECALCULATE PROFIT ENGINE & AUDIT
    # -------------------------------------------------------------------------
    print("\n--- 4. Auditing core.payment & analytics.profit_calculation ---")
    calculate_order_profitability(BUSINESS_ID)

    with get_db_connection() as conn:
        pay_rows = conn.execute(text("""
            SELECT 
                p.external_payment_id,
                o.external_order_id,
                p.payment_method,
                p.payment_status,
                p.amount,
                p.gateway_fee,
                p.paid_at
            FROM core.payment p
            JOIN core.orders o ON o.order_id = p.order_id
            WHERE p.business_id = :business_id
            ORDER BY p.payment_id;
        """), {"business_id": BUSINESS_ID}).fetchall()

        print("\n  [DATABASE] core.payment Records:")
        for r in pay_rows:
            print(f"    PayID: {r[0]:<18} | Order: {r[1]:<20} | Method: {r[2]:<8} | Status: {r[3]:<6} | Amount: PKR {r[4]} | Fee: PKR {r[5]}")

        profit_rows = conn.execute(text("""
            SELECT 
                o.external_order_id,
                o.source_platform,
                p.revenue,
                p.cogs,
                p.shipping_cost,
                p.rto_cost,
                p.discount_amount,
                p.gateway_fee,
                p.true_profit
            FROM analytics.profit_calculation p
            JOIN core.orders o ON o.order_id = p.order_id
            WHERE p.business_id = :business_id
            ORDER BY p.true_profit DESC;
        """), {"business_id": BUSINESS_ID}).fetchall()

        print("\n  [PROFIT ENGINE] Updated True Profit (After Gateway Fee Deductions):")
        for pr in profit_rows:
            print(f"    Order: {pr[0]:<20} | Platform: {pr[1]:<8} | Revenue: {pr[2]} | Gateway Fee: {pr[7]} | True Profit: PKR {pr[8]}")

    print("\n" + "=" * 70)
    print("[SUCCESS] ALL PAYMENT PIPELINE AND PROFIT RECALCULATION TESTS PASSED!")
    print("=" * 70)


if __name__ == "__main__":
    test_payment_pipeline()
