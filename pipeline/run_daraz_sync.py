"""
Multi-Tenant Daraz Open Platform Ingestion Pipeline (BRIDGE FLOW — Phase 3.4).

Architecture:
    Authenticated User
    → Business (business_id)
    → core.platform_connection
    → CredentialManager           (decrypts credentials, NO credential exposure)
    → DarazClient                 (HMAC-signed REST calls, retry/rate-limit)
    → DarazOrderService           (paginated order fetch + item hydration)
    → DarazProductService         (paginated product catalog)
    → DarazInventoryService       (SKU stock from /products/get)
    → raw.raw_data                (verbatim JSON persistence)
    → DarazTransformer / ETL      (canonical DataFrame production)
    → PostgresLoader              (idempotent upsert to core.*)
    → raw.sync_log                (telemetry)
    → core.platform_connection    (last_synced_at stamp)

Design invariants:
- Credentials retrieved exclusively via CredentialManager — never from env vars.
- Daraz uses auth_type='credentials': app_key + app_secret stored in
  encrypted_additional_credentials; access_token in encrypted_access_token.
- No credential values appear in logs, repr, or exceptions.
- Idempotent: ON CONFLICT upserts in loader ensure replayability.
- business_id propagated through every layer.
- Sync failure marks connection status='error' and logs to raw.sync_log.
- last_synced_at stamped on core.platform_connection after every success.
"""
import sys
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any, Dict, Optional, Tuple

sys.path.append(str(Path(__file__).resolve().parent.parent))

from sqlalchemy import text

from analytics.profit_engine import calculate_order_profitability
from auth.connection_service import (
    get_connection,
    list_connections,
    update_last_synced,
    update_status,
)
from auth.credential_manager import CredentialManager
from config.database import get_db_connection
from config.settings import BUSINESS_ID
from connectors.daraz.client import DarazClient
from connectors.daraz.services import (
    DarazInventoryService,
    DarazOrderService,
    DarazProductService,
)
from etl.daraz_transformer import DarazTransformer
from loaders.postgres_loader import PostgresLoader
from pipeline.sync_logger import SyncLogger
from raw_layer.raw_repository import save_raw_payload


# ---------------------------------------------------------------------------
# Watermark Helper
# ---------------------------------------------------------------------------

def get_daraz_last_watermark(business_id: int) -> Optional[str]:
    """
    Returns the ISO-8601 datetime of the last successful Daraz sync
    with a 10-minute safety buffer applied.

    Returns None when no previous successful sync exists (triggers a full sync).
    """
    query = text("""
        SELECT MAX(completed_at) FROM raw.sync_log
        WHERE business_id = :b_id AND source = 'daraz' AND status = 'success';
    """)
    try:
        with get_db_connection() as conn:
            last_sync = conn.execute(query, {"b_id": business_id}).scalar()
            if last_sync:
                buffer_time = last_sync - timedelta(minutes=10)
                return buffer_time.strftime("%Y-%m-%dT%H:%M:%S+05:00")
    except Exception as e:
        print(f"[!] Warning: Unable to query Daraz watermark ({e}). Running full sync.")
    return None


# ---------------------------------------------------------------------------
# Credential Resolution
# ---------------------------------------------------------------------------

def resolve_daraz_credentials(business_id: int) -> Tuple[str, str, str, str, int]:
    """
    Resolves decrypted Daraz credentials for the specified business from
    core.platform_connection via CredentialManager.

    Daraz credential storage convention:
        encrypted_access_token            → access_token (Daraz OAuth access_token)
        encrypted_additional_credentials  → {"app_key": "...", "app_secret": "..."}
        account_identifier                → Daraz seller_id or store_name

    Returns:
        (seller_id, app_key, app_secret, access_token, connection_id)

    Raises:
        ValueError — if no active Daraz connection exists for this business.
    """
    conns = list_connections(business_id)
    daraz_conn = next(
        (c for c in conns if c["platform"] == "daraz" and c["status"] in ("active", "error")),
        None,
    )

    if not daraz_conn:
        raise ValueError(
            f"No active Daraz connection found for business_id={business_id}. "
            "Please connect your Daraz seller account via the platform connection API."
        )

    seller_id = daraz_conn["account_identifier"]
    conn_id = daraz_conn["connection_id"]

    # Daraz uses auth_type='credentials' — CredentialManager returns without
    # expiry check and never triggers a refresh callback.
    cred_mgr = CredentialManager()
    creds = cred_mgr.get_valid_credentials(
        business_id=business_id,
        platform="daraz",
        account_identifier=seller_id,
        refresh_callback=None,   # Daraz credentials are static — no OAuth refresh
    )

    additional = creds.get("additional_credentials", {})
    app_key = additional.get("app_key", "")
    app_secret = additional.get("app_secret", "")
    access_token = creds.get("access_token", "")

    if not app_key or not app_secret:
        raise ValueError(
            f"Daraz connection for business_id={business_id} is missing app_key "
            "or app_secret in encrypted_additional_credentials. "
            "Please re-register the Daraz connection."
        )

    return seller_id, app_key, app_secret, access_token, conn_id


# ---------------------------------------------------------------------------
# Main Pipeline
# ---------------------------------------------------------------------------

def run_daraz_sync(business_id: int = BUSINESS_ID) -> Dict[str, Any]:
    """
    Runs the multi-tenant Daraz ingestion pipeline for a specific business.

    Pipeline order (dependency-safe):
        1. Products catalog  → core.product      (FK anchor for order items + inventory)
        2. Inventory levels  → core.inventory     (requires product FKs)
        3. Orders + items    → raw + core.*       (requires product + customer FKs)
        4. Sync log + last_synced_at stamp

    Returns a summary dict with telemetry counters.
    """
    seller_id, app_key, app_secret, access_token, conn_id = resolve_daraz_credentials(
        business_id
    )

    print("=" * 70)
    print(f"[*] Starting Multi-Tenant Daraz Sync for Business ID: {business_id}")
    print(f"[*] Daraz Seller: {seller_id}")
    print("=" * 70)

    logger = SyncLogger(business_id=business_id, source="daraz")
    watermark = get_daraz_last_watermark(business_id)
    sync_type = "incremental" if watermark else "full"

    sync_id = logger.start_sync(sync_type=sync_type)
    print(f"[*] Started Sync Run #{sync_id} (Mode: {sync_type.upper()})")
    print(f"[*] Watermark: {watermark or 'None (Full sync)'}")

    try:
        # Instantiate client with decrypted credentials
        client = DarazClient(
            app_key=app_key,
            app_secret=app_secret,
            access_token=access_token or None,
        )

        order_service = DarazOrderService(client)
        product_service = DarazProductService(client)
        inventory_service = DarazInventoryService(client)

        transformer = DarazTransformer(business_id)
        loader = PostgresLoader(business_id)

        total_fetched = 0
        total_inserted = 0
        total_updated = 0

        # ------------------------------------------------------------------
        # Step A: Products Catalog (creates FK anchors before orders/inventory)
        # ------------------------------------------------------------------
        print("[*] Ingesting Daraz Product catalog...")
        for p_batch in product_service.fetch_products(page_size=100):
            raw_products = p_batch["products"]
            if raw_products:
                save_raw_payload(
                    business_id, "daraz", "products", p_batch["raw_response"]
                )
                df_prod = transformer.transform_products(raw_products)
                loader.load_products(df_prod)
        print(f"[OK] Step A: Product catalog ingested.")

        # ------------------------------------------------------------------
        # Step B: Inventory Snapshot (derived from /products/get SKU quantities)
        # ------------------------------------------------------------------
        print("[*] Ingesting Daraz Inventory levels...")
        try:
            for inv_batch in inventory_service.fetch_inventory(page_size=100):
                inv_items = inv_batch["inventory_levels"]
                if inv_items:
                    save_raw_payload(
                        business_id, "daraz", "inventory", inv_batch["raw_response"]
                    )
                    df_inv = transformer.transform_inventory(
                        inv_items,
                        location_name="Daraz Warehouse",
                    )
                    loader.load_inventory(df_inv)
            print(f"[OK] Step B: Inventory ingested.")
        except Exception as inv_err:
            # Non-fatal: inventory sync failure should not block order ingestion
            print(f"[!] Warning: Non-fatal inventory sync error: {inv_err}")

        # ------------------------------------------------------------------
        # Step C: Orders, Customers, Line Items
        # ------------------------------------------------------------------
        print("[*] Ingesting Daraz Orders and Line Items...")
        order_params: Dict[str, Any] = {}
        if watermark:
            order_params["update_after"] = watermark

        for batch in order_service.fetch_orders(
            **order_params,
            page_size=100,
            hydrate_items=True,
        ):
            orders = batch["orders"]
            page = batch["page"]

            if not orders:
                print(f"[*] Page {page}: 0 orders found.")
                continue

            total_fetched += len(orders)
            print(f"[*] Page {page}: Processing {len(orders)} orders...")

            # 1. Raw layer persistence
            save_raw_payload(
                business_id, "daraz", "orders", batch["raw_response"]
            )

            # 2. ETL transformation
            df_cust, df_prod_orders, df_orders, df_items = transformer.transform_orders(orders)

            # 3. Canonical loading (dependency order: customers → products → orders → items)
            loader.load_customers(df_cust)
            loader.load_products(df_prod_orders)
            ins, upd = loader.load_orders(df_orders, df_items)

            total_inserted += ins
            total_updated += upd
            print(
                f"    [Canonical Loader] Batch: {ins} new orders, {upd} updated."
            )

        # ------------------------------------------------------------------
        # Step D: Telemetry + Bookkeeping
        # ------------------------------------------------------------------
        logger.finish_success(
            fetched=total_fetched,
            inserted=total_inserted,
            updated=total_updated,
        )

        # Stamp last_synced_at and reset status to 'active'
        update_last_synced(conn_id)
        update_status(conn_id, "active")

        # Recalculate profitability for all affected orders
        recalculated = calculate_order_profitability(business_id)

        print("=" * 70)
        print(
            f"[SUCCESS] Daraz Sync completed for Business ID: {business_id}!"
        )
        print(f"Total Fetched:        {total_fetched}")
        print(f"Total Inserted:       {total_inserted}")
        print(f"Total Updated:        {total_updated}")
        print(f"Recalculated Orders:  {recalculated}")
        print("=" * 70)

        return {
            "business_id": business_id,
            "seller_id": seller_id,
            "total_fetched": total_fetched,
            "total_inserted": total_inserted,
            "total_updated": total_updated,
            "recalculated_orders": recalculated,
        }

    except Exception as e:
        print(f"\n[FAILURE] Daraz Sync failed for Business ID {business_id}: {e}")
        logger.finish_failure(str(e))
        update_status(conn_id, "error", error_message=str(e))
        raise


if __name__ == "__main__":
    run_daraz_sync(BUSINESS_ID)
