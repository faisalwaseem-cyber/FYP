"""
Payment & Settlement Ingestion Pipeline Runner — BRIDGE FLOW Phase 6
===================================================================

Orchestrates the ingestion of payment and settlement data across providers:
- Stripe API (online credit/debit card charges & fees)
- PayFast Pakistan (portal settlement CSV/Excel exports)
- JazzCash (merchant statement exports)
- EasyPaisa (merchant statement exports)
- COD Reconciliation (courier COD remittance & settlement matching)
- Manual / Bank Transfers (IBFT / Raast / Kuickpay statement imports)

Canonical Objective:
Order → Payment → Gateway Fee → Settlement → Net Received

Pipeline Flow:
Provider Selection
→ Raw Layer (save_raw_payload)
→ PaymentTransformer (canonical normalization & net received computation)
→ PaymentLoader (core.payment idempotent upsert)
→ SyncLogger (raw.sync_log telemetry)

Invariants:
- business_id is strictly propagated through all layers.
- Duplicate prevention via unique constraint (business_id, external_payment_id).
- Final profit calculation is decoupled and deferred (Phase 7+).
"""
from __future__ import annotations

import sys
from pathlib import Path
from typing import Any, Dict, List, Optional, Union

sys.path.append(str(Path(__file__).resolve().parent.parent))

from config.settings import BUSINESS_ID
from connectors.payment.base import PaymentRecord, SettlementBatch
from connectors.payment.provider_factory import get_payment_provider
from connectors.payment.stripe_provider import StripePaymentProvider
from etl.payment_transformer import PaymentTransformer
from loaders.payment_loader import PaymentLoader
from pipeline.sync_logger import SyncLogger
from raw_layer.raw_repository import save_raw_payload


def run_payment_sync(
    provider_name: str,
    raw_payload: Any,
    sync_type: str = "statement_sync",
    business_id: int = BUSINESS_ID,
) -> Dict[str, Any]:
    """
    Ingests payments and settlements from any supported provider using the provider abstraction.

    Parameters
    ----------
    provider_name : str
        'stripe' | 'payfast' | 'jazzcash' | 'easypaisa' | 'cod' | 'manual'
    raw_payload : Any
        CSV text, JSON string, dict list, or API response.
    sync_type : str
        Identifier written to raw.sync_log.
    business_id : int
        Tenant identifier.
    """
    logger = SyncLogger(business_id, f"payment_{provider_name}")
    sync_id = logger.start_sync(sync_type=sync_type)
    loader = PaymentLoader(business_id)
    transformer = PaymentTransformer(business_id)

    try:
        # Step 1: Resolve provider & parse transactions
        provider = get_payment_provider(provider_name)
        records: List[PaymentRecord] = provider.parse_transactions(raw_payload)
        records_fetched = len(records)

        # Step 2: Save raw data to raw.raw_data
        raw_id = save_raw_payload(
            business_id=business_id,
            source="payment_gateway",
            data_type=f"{provider_name}_settlement",
            payload={"records": [r.to_dict() for r in records]} if records else {"raw": str(raw_payload)[:1000]},
        )

        # Step 3: Transform and match to orders
        order_map = loader.get_order_id_map()
        df_payments = transformer.transform_payment_records(records, order_map)

        # Step 4: Load into core.payment (idempotent upsert)
        inserted, updated = loader.load_payments(df_payments)

        # Step 5: Compute Net Received metrics
        total_gross = float(df_payments["amount"].sum()) if not df_payments.empty else 0.0
        total_fees = float(df_payments["gateway_fee"].sum()) if not df_payments.empty else 0.0
        net_received = round(total_gross - total_fees, 2)

        # Step 6: Log telemetry
        logger.finish_success(
            fetched=records_fetched,
            inserted=inserted,
            updated=updated,
        )

        print(
            f"[OK] {provider_name.upper()} payment sync completed: "
            f"{inserted} inserted, {updated} updated, "
            f"Gross: PKR {total_gross:.2f}, Fees: PKR {total_fees:.2f}, Net: PKR {net_received:.2f}"
        )

        return {
            "status": "success",
            "business_id": business_id,
            "provider": provider_name,
            "raw_id": raw_id,
            "records_fetched": records_fetched,
            "records_inserted": inserted,
            "records_updated": updated,
            "total_gross": total_gross,
            "total_fees": total_fees,
            "net_received": net_received,
        }

    except Exception as e:
        logger.finish_failure(error_msg=str(e))
        print(f"[ERROR] Payment sync failed for {provider_name}: {e}")
        raise


# ---------------------------------------------------------------------------
# Backward-compatible convenience functions
# ---------------------------------------------------------------------------

def run_payment_csv_sync(
    csv_content: str,
    gateway_name: str,
    sync_type: str = "csv_import",
    business_id: int = BUSINESS_ID,
) -> Dict[str, Any]:
    """Compatibility runner for CSV settlement uploads."""
    return run_payment_sync(
        provider_name=gateway_name,
        raw_payload=csv_content,
        sync_type=sync_type,
        business_id=business_id,
    )


def run_stripe_sync(
    secret_key: Optional[str] = None,
    created_after: Optional[int] = None,
    business_id: int = BUSINESS_ID,
) -> Dict[str, Any]:
    """Compatibility runner for live Stripe API syncing."""
    if not secret_key:
        print("[INFO] No Stripe secret key provided. Skipping live Stripe API sync.")
        return {"status": "skipped", "reason": "no_credentials"}

    from connectors.payment.stripe_client import StripeClient
    client = StripeClient(secret_key=secret_key)
    charges = client.fetch_charges(created_after=created_after)

    return run_payment_sync(
        provider_name="stripe",
        raw_payload=charges,
        sync_type="api_sync",
        business_id=business_id,
    )

