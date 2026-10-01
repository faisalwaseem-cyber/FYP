"""
Multi-Tenant Shopify GraphQL Ingestion Pipeline Runner for BRIDGE FLOW.

Architecture:
1. Dynamically resolves authenticated business's Shopify connection from
   core.platform_connection via CredentialManager (with fallback to legacy store_connection).
2. Proactively refreshes expired or near-expiry offline tokens using row-level locking
   and the Shopify refresh network callback.
3. Instantiates ShopifyGraphQLClient with dynamic credentials and configured API version.
4. Executes dependency-ordered data ingestion:
   a. Product catalog -> core.product (ensures FKs exist)
   b. Locations & Inventory levels -> core.inventory
   c. Orders & line items -> raw.raw_data -> core.customer, core.orders, core.order_item
5. Persists unmodified API responses into raw.raw_data (tenant-scoped).
6. Idempotently loads into PostgreSQL canonical tables via PostgresLoader.
7. Logs telemetry in raw.sync_log and updates core.platform_connection.last_synced_at.
8. Triggers order-level True Profit calculation in analytics.profit_engine.
"""
import sys
from datetime import datetime, timedelta
from pathlib import Path
from typing import Any, Dict, Optional, Tuple

# Ensure root directory is on Python path
sys.path.append(str(Path(__file__).resolve().parent.parent))

from sqlalchemy import text

from analytics.profit_engine import calculate_order_profitability
from auth.connection_service import get_connection, list_connections, update_last_synced, update_status
from auth.credential_manager import CredentialManager
from auth.shopify_oauth import make_shopify_refresh_callback
from auth.token_manager import get_valid_shopify_access_token
from config.database import get_db_connection
from config.settings import BUSINESS_ID, SHOPIFY_API_VERSION
from connectors.shopify.graphql_client import ShopifyGraphQLClient
from connectors.shopify.services import (
    ShopifyCustomerService,
    ShopifyInventoryService,
    ShopifyLocationService,
    ShopifyOrderService,
    ShopifyProductService,
)
from etl.shopify_transformer import ShopifyTransformer
from loaders.postgres_loader import PostgresLoader
from pipeline.sync_logger import SyncLogger
from raw_layer.raw_repository import save_raw_payload


def get_last_watermark(business_id: int) -> Optional[str]:
    """
    Returns the ISO-8601 timestamp of the last successful sync with a 10-minute safety buffer.
    """
    query = text("""
        SELECT MAX(completed_at) FROM raw.sync_log 
        WHERE business_id = :b_id AND source = 'shopify' AND status = 'success';
    """)
    try:
        with get_db_connection() as conn:
            last_sync = conn.execute(query, {"b_id": business_id}).scalar()
            if last_sync:
                buffer_time = last_sync - timedelta(minutes=10)
                return buffer_time.strftime("%Y-%m-%dT%H:%M:%SZ")
    except Exception as e:
        print(f"[!] Warning: Unable to query last watermark ({e}). Running full sync.")
    return None


def resolve_shopify_credentials(business_id: int) -> Tuple[str, str, Optional[int]]:
    """
    Resolves Shopify store domain, decrypted access token, and connection_id
    for the specified business.
    Primary source: core.platform_connection via CredentialManager.
    Fallback: legacy core.store_connection via TokenManager.

    Returns:
        (store_domain, decrypted_access_token, connection_id)
    """
    # 1. Check primary universal connection registry: core.platform_connection
    conns = list_connections(business_id)
    shopify_conn = next(
        (c for c in conns if c["platform"] == "shopify" and c["status"] in ("active", "error")),
        None
    )

    if shopify_conn:
        store_domain = shopify_conn["account_identifier"]
        conn_id = shopify_conn["connection_id"]

        cred_mgr = CredentialManager()
        creds = cred_mgr.get_valid_credentials(
            business_id=business_id,
            platform="shopify",
            account_identifier=store_domain,
            refresh_callback=make_shopify_refresh_callback(store_domain),
            safety_buffer_seconds=300,
        )
        return store_domain, creds["access_token"], conn_id

    # 2. Backward-compatible fallback: legacy core.store_connection
    try:
        store_domain, access_token = get_valid_shopify_access_token(business_id)
        return store_domain, access_token, None
    except ValueError:
        pass

    raise ValueError(
        f"No active Shopify connection found for business_id={business_id}. "
        "Please connect your Shopify store via /integrations/shopify/connect."
    )


def run_shopify_sync(business_id: int = BUSINESS_ID) -> Dict[str, Any]:
    """
    Runs the multi-tenant Shopify GraphQL ingestion pipeline for a specific business.
    Credentials and access tokens are retrieved dynamically per business.
    """
    store_domain, access_token, conn_id = resolve_shopify_credentials(business_id)

    print("=" * 70)
    print(f"[*] Starting Multi-Tenant Shopify Sync for Business ID: {business_id}")
    print(f"[*] Target Store: {store_domain} (GraphQL API: {SHOPIFY_API_VERSION})")
    print("=" * 70)

    logger = SyncLogger(business_id=business_id, source="shopify")
    watermark = get_last_watermark(business_id)
    sync_type = "incremental" if watermark else "full"

    sync_id = logger.start_sync(sync_type=sync_type)
    print(f"[*] Started Sync Run #{sync_id} (Mode: {sync_type.upper()})")
    print(f"[*] Watermark: {watermark or 'None (Fetching all historical orders)'}")

    try:
        client = ShopifyGraphQLClient(
            store_domain=store_domain,
            access_token=access_token,
            api_version=SHOPIFY_API_VERSION,
        )
        order_service = ShopifyOrderService(client)
        product_service = ShopifyProductService(client)
        location_service = ShopifyLocationService(client)
        inventory_service = ShopifyInventoryService(client)

        transformer = ShopifyTransformer(business_id)
        loader = PostgresLoader(business_id)

        total_fetched = 0
        total_inserted = 0
        total_updated = 0

        # ------------------------------------------------------------------
        # Step A: Ingest Products Catalog (Ensures FKs for Inventory & Orders)
        # ------------------------------------------------------------------
        print("[*] Ingesting Shopify Products catalog...")
        for p_batch in product_service.fetch_products(page_size=50):
            products = p_batch["products"]
            raw_resp = p_batch["raw_response"]
            if products:
                save_raw_payload(business_id, "shopify", "products", raw_resp)
                df_prod = transformer.transform_graphql_products(products)
                loader.load_products(df_prod)

        # ------------------------------------------------------------------
        # Step B: Ingest Locations & Inventory Levels
        # ------------------------------------------------------------------
        print("[*] Ingesting Locations & Inventory levels...")
        try:
            locations = location_service.fetch_locations(page_size=20)
            for loc in locations:
                loc_id = loc.get("id")
                loc_name = loc.get("name") or "Default Location"
                if loc_id:
                    for inv_batch in inventory_service.fetch_inventory_for_location(loc_id, page_size=50):
                        levels = inv_batch["inventory_levels"]
                        if levels:
                            save_raw_payload(business_id, "shopify", "inventory", inv_batch["raw_response"])
                            df_inv = transformer.transform_graphql_inventory(
                                levels,
                                location_name=loc_name,
                            )
                            loader.load_inventory(df_inv)
        except Exception as inv_err:
            print(f"[!] Warning: Non-fatal error syncing inventory: {inv_err}")

        # ------------------------------------------------------------------
        # Step C: Ingest Orders, Customers, & Line Items
        # ------------------------------------------------------------------
        print("[*] Ingesting Shopify Orders & Line Items...")
        for batch in order_service.fetch_orders(updated_at_min=watermark, page_size=50):
            orders = batch["orders"]
            raw_response = batch["raw_response"]
            page = batch["page"]

            if not orders:
                print(f"[*] Page {page}: 0 orders found.")
                continue

            total_fetched += len(orders)
            print(f"[*] Page {page}: Processing {len(orders)} orders...")

            # 1. Raw layer persistence
            raw_id = save_raw_payload(business_id, "shopify", "orders", raw_response)

            # 2. ETL transformation
            df_cust, df_prod_orders, df_orders, df_items = transformer.transform_graphql_orders(orders)

            # 3. Canonical loading
            loader.load_customers(df_cust)
            loader.load_products(df_prod_orders)
            ins, upd = loader.load_orders(df_orders, df_items)

            total_inserted += ins
            total_updated += upd
            print(f"    [Canonical Loader] Loaded batch: {ins} new orders, {upd} updated orders.")

        logger.finish_success(
            fetched=total_fetched,
            inserted=total_inserted,
            updated=total_updated,
        )

        # Update last_synced timestamp on platform_connection
        if conn_id:
            update_last_synced(conn_id)
            update_status(conn_id, "active")

        # Recalculate true profitability for this business
        recalculated_orders = calculate_order_profitability(business_id)

        print("=" * 70)
        print(f"[SUCCESS] Multi-Tenant Shopify Sync completed for Business ID: {business_id}!")
        print(f"Total Fetched:        {total_fetched}")
        print(f"Total Inserted:       {total_inserted}")
        print(f"Total Updated:        {total_updated}")
        print(f"Recalculated Orders:  {recalculated_orders}")
        print("=" * 70)

        return {
            "business_id": business_id,
            "store_domain": store_domain,
            "total_fetched": total_fetched,
            "total_inserted": total_inserted,
            "total_updated": total_updated,
            "recalculated_orders": recalculated_orders,
        }

    except Exception as e:
        print(f"\n[FAILURE] Multi-Tenant Shopify Sync failed: {e}")
        logger.finish_failure(str(e))
        if conn_id:
            update_status(conn_id, "error", error_message=str(e))
        raise


if __name__ == "__main__":
    run_shopify_sync(BUSINESS_ID)
