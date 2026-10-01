"""
Multi-Tenant Courier Ingestion Pipeline (BRIDGE FLOW — Phase 5.4).

Architecture:
    Authenticated Business (business_id)
    → core.platform_connection
    → CredentialManager           (decrypts credentials safely, NO credential exposure)
    → Courier Client              (PostExClient, TraxClient, LeopardsClient)
    → raw.raw_data                (verbatim JSON persistence via save_raw_payload)
    → CourierTransformer / ETL    (canonical shipment & RTO transformation)
    → CourierLoader               (idempotent upsert to core.shipment, core.orders, core.rto_return)
    → raw.sync_log                (telemetry via SyncLogger)
    → core.platform_connection    (last_synced_at stamp & status update)

Design invariants:
    - business_id is strictly propagated through every layer (tenant isolation).
    - Credentials resolved via CredentialManager (never from env inside multi-tenant flow).
    - Client never writes to database.
    - Idempotent upserts prevent duplicate records.
    - Automatic cascading of shipment statuses to core.orders:
        * 'delivered' -> core.orders.order_status = 'delivered'
        * 'returned' / 'returning' -> core.orders.order_status = 'rto'
        * RTO recorded in core.rto_return
    - Failure behavior: error logged to raw.sync_log and connection marked 'error'.
"""
import os
import sys
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

sys.path.append(str(Path(__file__).resolve().parent.parent))

from dotenv import load_dotenv
load_dotenv()

from sqlalchemy import text

from auth.connection_service import (
    get_connection,
    list_connections,
    update_last_synced,
    update_status,
)
from auth.credential_manager import CredentialManager
from config.database import get_db_connection
from config.settings import BUSINESS_ID
from connectors.courier import (
    BaseCourierClient,
    CourierAPIError,
    CourierAuthError,
    CourierRateLimitError,
    CourierTrackingNotFoundError,
    LeopardsClient,
    PostExClient,
    TraxClient,
)
from etl.courier_transformer import CourierTransformer
from loaders.courier_loader import CourierLoader
from pipeline.sync_logger import SyncLogger
from raw_layer.raw_repository import save_raw_payload


# ---------------------------------------------------------------------------
# Client Resolution via CredentialManager
# ---------------------------------------------------------------------------

def resolve_courier_client(
    business_id: int,
    platform: str,
    account_identifier: str,
    credential_manager: Optional[CredentialManager] = None,
) -> BaseCourierClient:
    """
    Resolves and constructs a courier client using decrypted credentials
    from core.platform_connection via CredentialManager.

    Supported platforms:
      - 'postex' / 'courier': token-based auth
      - 'trax': basic auth (username + password)
      - 'leopards': api_key + api_password
    """
    cm = credential_manager or CredentialManager()
    creds = cm.get_valid_credentials(
        business_id=business_id,
        platform=platform,
        account_identifier=account_identifier,
    )

    p = platform.strip().lower()
    if p in ("postex", "courier"):
        token = creds.get("access_token") or creds.get("additional_credentials", {}).get("api_token")
        if not token:
            raise ValueError(f"PostEx connection for business {business_id} missing access_token.")
        return PostExClient(api_token=token)

    elif p == "trax":
        add_creds = creds.get("additional_credentials") or {}
        username = add_creds.get("username") or creds.get("access_token")
        password = add_creds.get("password") or creds.get("refresh_token")
        if not username or not password:
            raise ValueError(f"Trax connection for business {business_id} missing username/password.")
        return TraxClient(username=username, password=password)

    elif p == "leopards":
        add_creds = creds.get("additional_credentials") or {}
        api_key = add_creds.get("api_key") or creds.get("access_token")
        api_password = add_creds.get("api_password") or creds.get("refresh_token")
        if not api_key or not api_password:
            raise ValueError(f"Leopards connection for business {business_id} missing api_key/api_password.")
        return LeopardsClient(api_key=api_key, api_password=api_password)

    else:
        raise ValueError(f"Unsupported courier platform: '{platform}'")


# ---------------------------------------------------------------------------
# Single Connection Sync Runner
# ---------------------------------------------------------------------------

def sync_courier_connection(
    business_id: int,
    conn_info: Dict[str, Any],
    client: Optional[BaseCourierClient] = None,
    credential_manager: Optional[CredentialManager] = None,
    transformer: Optional[CourierTransformer] = None,
    loader: Optional[CourierLoader] = None,
    default_rto_penalty: float = 150.0,
) -> Dict[str, Any]:
    """
    Executes the ingestion pipeline for a single courier platform_connection row.
    """
    connection_id = conn_info["connection_id"]
    platform = conn_info["platform"].strip().lower()
    account_identifier = conn_info["account_identifier"]

    logger = SyncLogger(business_id=business_id, source=f"courier_{platform}")
    sync_id = logger.start_sync(sync_type=f"courier_{platform}_sync")
    print(f"[*] Started Courier Sync Run #{sync_id} (business_id={business_id}, platform={platform}, account={account_identifier})")

    try:
        # 1. Resolve client if not injected
        active_client = client or resolve_courier_client(
            business_id=business_id,
            platform=platform,
            account_identifier=account_identifier,
            credential_manager=credential_manager,
        )

        active_transformer = transformer or CourierTransformer(business_id=business_id)
        active_loader = loader or CourierLoader(business_id=business_id)

        # 2. Query in-flight shipments for this business and courier
        with get_db_connection() as conn:
            # If platform is a specific courier, match courier_name or null courier
            shipments_query = text("""
                SELECT s.tracking_number, o.external_order_id, c.courier_name
                FROM core.shipment s
                JOIN core.orders o ON s.order_id = o.order_id
                LEFT JOIN core.courier c ON s.courier_id = c.courier_id
                WHERE s.business_id = :b_id
                  AND s.shipment_status IN ('booked', 'in_transit', 'failed_delivery')
                  AND (c.courier_name = :p OR c.courier_name IS NULL OR :p = 'courier');
            """)
            active_shipments = conn.execute(
                shipments_query,
                {"b_id": business_id, "p": platform},
            ).fetchall()

        print(f"[*] Found {len(active_shipments)} in-flight shipments to poll for {platform}.")

        updated_count = 0
        inserted_count = 0

        for tracking_num, ext_order_id, _ in active_shipments:
            try:
                # 3. Call courier client tracking API
                raw_response = active_client.track_shipment(tracking_num)

                # 4. Save verbatim raw payload in raw.raw_data
                save_raw_payload(business_id, "courier", f"{platform}_tracking", raw_response)

                # 5. Transform raw payload via CourierTransformer
                payload = raw_response.get("dist") if isinstance(raw_response.get("dist"), dict) else raw_response
                if isinstance(payload, dict):
                    payload["external_order_id"] = ext_order_id
                    payload["tracking_number"] = tracking_num

                shipment_data = active_transformer.transform_shipment(
                    courier_name=platform,
                    payload=payload if isinstance(payload, dict) else raw_response,
                )

                # 6. Load shipment & cascade order status in core.orders / core.rto_return
                shipment_id = active_loader.load_shipment(
                    shipment_data,
                    default_rto_penalty=default_rto_penalty,
                )
                if shipment_id is not None:
                    updated_count += 1

            except CourierTrackingNotFoundError:
                print(f"[-] Tracking number '{tracking_num}' not found on {platform}; skipping.")
            except CourierAuthError:
                # Fatal auth error for connection — re-raise to fail sync and mark connection error
                raise
            except Exception as item_err:
                print(f"[!] Error tracking shipment {tracking_num}: {item_err}")

        # 7. Record sync success telemetry
        logger.finish_success(
            fetched=len(active_shipments),
            inserted=inserted_count,
            updated=updated_count,
        )

        # 8. Stamp last_synced_at & ensure connection is active
        update_last_synced(connection_id)
        update_status(connection_id, "active")

        print(f"[OK] Courier sync completed for {platform}! Updated {updated_count} shipments.")
        return {
            "status": "success",
            "connection_id": connection_id,
            "platform": platform,
            "business_id": business_id,
            "fetched": len(active_shipments),
            "updated": updated_count,
        }

    except Exception as e:
        logger.finish_failure(str(e))
        update_status(connection_id, "error", error_message=str(e))
        print(f"[!] Courier sync failed for {platform}: {e}")
        raise


# ---------------------------------------------------------------------------
# Multi-Tenant Dispatcher
# ---------------------------------------------------------------------------

def run_courier_sync(
    business_id: int = BUSINESS_ID,
    platform: Optional[str] = None,
    account_identifier: Optional[str] = None,
    client: Optional[BaseCourierClient] = None,
    credential_manager: Optional[CredentialManager] = None,
    transformer: Optional[CourierTransformer] = None,
    loader: Optional[CourierLoader] = None,
    default_rto_penalty: float = 150.0,
) -> Dict[str, Any]:
    """
    Main entry point for Courier Ingestion Pipeline.

    Discovers all active courier connections for `business_id` in
    core.platform_connection, executes sync per connection, and returns summary.
    """
    conns = list_connections(business_id)

    # Filter to courier category / platforms
    courier_conns = [
        c for c in conns
        if c.get("platform_category") == "courier"
        or c.get("platform", "").lower() in ("postex", "trax", "leopards", "courier")
    ]

    if platform:
        courier_conns = [c for c in courier_conns if c.get("platform", "").lower() == platform.lower()]

    if account_identifier:
        courier_conns = [c for c in courier_conns if c.get("account_identifier") == account_identifier]

    # Backward compatibility fallback: if no DB connection registered, check env
    if not courier_conns and not client:
        api_token = os.getenv("POSTEX_API_TOKEN", "").strip()
        if api_token and "xxxxxxxx" not in api_token:
            print("[*] Running PostEx sync using legacy POSTEX_API_TOKEN env var...")
            fake_conn = {
                "connection_id": 0,
                "platform": "postex",
                "account_identifier": "env_fallback",
            }
            # Directly instantiate client
            legacy_client = PostExClient(api_token=api_token)
            # Use a mock logger connection update guard
            return _run_legacy_sync(
                business_id=business_id,
                client=legacy_client,
                transformer=transformer,
                loader=loader,
                default_rto_penalty=default_rto_penalty,
            )

        print("=" * 70)
        print("[!] NOTICE: No active courier connections found in core.platform_connection.")
        print(f"To register a courier connection for business_id={business_id}, use:")
        print("  auth.connection_service.create_or_update_connection(...)")
        print("=" * 70)
        return {"status": "skipped", "reason": "no_courier_connections", "business_id": business_id}

    results = []
    for conn_info in courier_conns:
        if conn_info.get("status") == "disconnected":
            continue

        res = sync_courier_connection(
            business_id=business_id,
            conn_info=conn_info,
            client=client,
            credential_manager=credential_manager,
            transformer=transformer,
            loader=loader,
            default_rto_penalty=default_rto_penalty,
        )
        results.append(res)

    return {
        "status": "success",
        "business_id": business_id,
        "connections_synced": len(results),
        "results": results,
    }


def _run_legacy_sync(
    business_id: int,
    client: BaseCourierClient,
    transformer: Optional[CourierTransformer] = None,
    loader: Optional[CourierLoader] = None,
    default_rto_penalty: float = 150.0,
) -> Dict[str, Any]:
    """Executes sync when running under legacy env token without DB connection."""
    logger = SyncLogger(business_id=business_id, source="courier_postex")
    sync_id = logger.start_sync(sync_type="courier_postex_legacy_sync")

    try:
        active_transformer = transformer or CourierTransformer(business_id=business_id)
        active_loader = loader or CourierLoader(business_id=business_id)

        with get_db_connection() as conn:
            active_shipments = conn.execute(
                text("""
                    SELECT s.tracking_number, o.external_order_id, c.courier_name
                    FROM core.shipment s
                    JOIN core.orders o ON s.order_id = o.order_id
                    LEFT JOIN core.courier c ON s.courier_id = c.courier_id
                    WHERE s.business_id = :b_id
                      AND s.shipment_status IN ('booked', 'in_transit', 'failed_delivery');
                """),
                {"b_id": business_id},
            ).fetchall()

        updated_count = 0
        for tracking_num, ext_order_id, _ in active_shipments:
            try:
                raw_response = client.track_shipment(tracking_num)
                save_raw_payload(business_id, "courier", "postex_tracking", raw_response)
                payload = raw_response.get("dist") if isinstance(raw_response.get("dist"), dict) else raw_response
                if isinstance(payload, dict):
                    payload["external_order_id"] = ext_order_id
                    payload["tracking_number"] = tracking_num
                shipment_data = active_transformer.transform_shipment("postex", payload if isinstance(payload, dict) else raw_response)
                active_loader.load_shipment(shipment_data, default_rto_penalty=default_rto_penalty)
                updated_count += 1
            except Exception as e:
                print(f"[!] Error tracking shipment {tracking_num}: {e}")

        logger.finish_success(fetched=len(active_shipments), inserted=0, updated=updated_count)
        return {"status": "success", "business_id": business_id, "fetched": len(active_shipments), "updated": updated_count}
    except Exception as e:
        logger.finish_failure(str(e))
        raise


if __name__ == "__main__":
    run_courier_sync(BUSINESS_ID)
