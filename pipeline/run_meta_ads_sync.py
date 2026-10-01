"""
Multi-Tenant Meta Ads Ingestion Pipeline (BRIDGE FLOW — Phase 4.4).

Architecture:
    Authenticated Business (business_id)
    → core.platform_connection
    → CredentialManager               (decrypts tokens, zero leakage)
    → MetaClient                      (Graph API v26.0)
    → MetaCampaignService / Insights  (paginated entity and metrics retrieval)
    → raw.raw_data                    (verbatim JSON persistence)
    → MetaAdsTransformer              (canonical campaign / spend normalization)
    → MetaAdsLoader                   (idempotent upsert to core.marketing_campaign + core.ad_spend)
    → raw.sync_log                    (telemetry)
    → core.platform_connection        (last_synced_at stamp)

Design invariants:
    - business_id is strictly propagated through every layer (no cross-tenant leakage).
    - Credentials resolved via CredentialManager with fallback to env vars.
    - Idempotent: safe replayability via ON CONFLICT upserts.
    - Full telemetry: logs to raw.sync_log and updates connection status.
    - Failed sync marks connection status='error' and records error in raw.sync_log.
"""
import os
import sys
from datetime import date, timedelta
from pathlib import Path
from typing import Any, Dict, Optional, Tuple

sys.path.append(str(Path(__file__).resolve().parent.parent))

from dotenv import load_dotenv
load_dotenv()

from auth.connection_service import list_connections, update_last_synced, update_status
from auth.credential_manager import CredentialManager
from config.settings import BUSINESS_ID
from connectors.meta.client import MetaClient
from connectors.meta.services import MetaCampaignService, MetaInsightsService
from etl.meta_ads_transformer import MetaAdsTransformer
from loaders.meta_ads_loader import MetaAdsLoader
from pipeline.sync_logger import SyncLogger
from raw_layer.raw_repository import save_raw_payload


def resolve_meta_credentials(business_id: int) -> Tuple[str, str, Optional[str], Optional[int]]:
    """
    Resolves decrypted Meta credentials for the specified business.
    First checks core.platform_connection (platform='meta' or 'meta_ads').
    Falls back to environment variables if no database connection is present.

    Returns:
        (access_token, ad_account_id, app_secret, connection_id)

    Raises:
        ValueError — if no connection and no environment credentials are found.
    """
    cm = CredentialManager()
    conns = list_connections(business_id)
    meta_conn = next(
        (c for c in conns if c["platform"] in ("meta", "meta_ads") and c["status"] in ("active", "error")),
        None,
    )

    if meta_conn:
        creds = cm.get_valid_credentials(
            business_id=business_id,
            platform=meta_conn["platform"],
            account_identifier=meta_conn["account_identifier"],
        )
        token = creds.get("access_token", "")
        acc_id = (
            creds.get("account_identifier")
            or creds.get("ad_account_id")
            or (creds.get("additional_credentials") or {}).get("ad_account_id", "")
        )
        secret = creds.get("app_secret") or (creds.get("additional_credentials") or {}).get("app_secret")
        if not token:
            raise ValueError(f"Meta connection for business_id={business_id} is missing access_token.")
        return token, str(acc_id), secret, meta_conn["connection_id"]

    # Fallback to environment variables
    env_token = os.getenv("META_ACCESS_TOKEN", "").strip()
    env_acc_id = os.getenv("META_AD_ACCOUNT_ID", "").strip()
    env_secret = os.getenv("META_APP_SECRET", "").strip() or None

    _PLACEHOLDERS = ("xxxxxxxx", "your_", "<", "placeholder", "changeme")
    token_is_real = env_token and not any(p in env_token.lower() for p in _PLACEHOLDERS)
    acc_id_is_real = env_acc_id and not any(p in env_acc_id.lower() for p in _PLACEHOLDERS)

    if token_is_real and acc_id_is_real:
        return env_token, env_acc_id, env_secret, None

    raise ValueError(
        f"No active Meta Ads connection or valid environment credentials found for business_id={business_id}. "
        "Set META_ACCESS_TOKEN and META_AD_ACCOUNT_ID in your .env file with real values, "
        "or connect your Meta Ad Account via the platform connection API."
    )


def run_meta_ads_sync(
    business_id: int = BUSINESS_ID,
    days_back: int = 7,
) -> Dict[str, Any]:
    """
    Fetches Meta Ads campaigns and daily insights for the given business.
    Pipes through: Services → Raw → Transformer → Loader.

    Returns a summary dict with telemetry counters.
    """
    if not business_id or business_id <= 0:
        raise ValueError(f"Invalid business_id: {business_id}")

    today = date.today()
    since_date = (today - timedelta(days=days_back)).strftime("%Y-%m-%d")
    until_date = today.strftime("%Y-%m-%d")

    logger = SyncLogger(business_id=business_id, source="meta_ads")
    logger.start_sync(sync_type=f"meta_ads_{days_back}d_sync")

    conn_id: Optional[int] = None
    try:
        access_token, ad_account_id, app_secret, conn_id = resolve_meta_credentials(business_id)

        print("=" * 70)
        print(f"[*] Starting Meta Ads Sync (business_id={business_id}, Account: {ad_account_id})")
        print(f"[*] Date Range: {since_date} to {until_date} ({days_back} days)")
        print("=" * 70)

        client = MetaClient(
            access_token=access_token,
            ad_account_id=ad_account_id,
            app_secret=app_secret,
        )
        campaign_service = MetaCampaignService(client)
        insights_service = MetaInsightsService(client)
        transformer = MetaAdsTransformer(business_id=business_id)
        loader = MetaAdsLoader(business_id=business_id)

        # ------------------------------------------------------------------
        # Step 1: Campaigns (Services → Raw → Transformer → Loader)
        # ------------------------------------------------------------------
        print("[*] Step 1: Fetching campaigns from Meta Graph API...")
        campaigns_raw_records = []
        for page in campaign_service.fetch_campaigns():
            raw_resp = page.get("raw_response", {})
            save_raw_payload(business_id, "meta_ads", "campaigns", raw_resp)
            campaigns_raw_records.extend(page.get("campaigns", []))

        df_campaigns = transformer.transform_campaigns(campaigns_raw_records)
        campaigns_loaded = loader.load_campaigns(df_campaigns)
        print(f"[OK] Campaigns Synced: {campaigns_loaded} loaded/updated.")

        # ------------------------------------------------------------------
        # Step 2: Daily Insights (Services → Raw → Transformer → Loader)
        # ------------------------------------------------------------------
        print(f"[*] Step 2: Fetching daily insights for {since_date} to {until_date}...")
        insights_raw_records = []
        for page in insights_service.fetch_insights(
            level="campaign",
            since=since_date,
            until=until_date,
            time_increment=1,
        ):
            raw_resp = page.get("raw_response", {})
            save_raw_payload(business_id, "meta_ads", "daily_insights", raw_resp)
            insights_raw_records.extend(page.get("insights", []))

        df_insight_campaigns, df_spend = transformer.transform_daily_insights(insights_raw_records)
        if not df_insight_campaigns.empty:
            loader.load_campaigns(df_insight_campaigns)

        inserted, updated = loader.load_ad_spend(df_spend)
        print(f"[OK] Daily Ad Spend loaded: {inserted} inserted, {updated} updated.")

        # ------------------------------------------------------------------
        # Step 3: Telemetry & State Updates
        # ------------------------------------------------------------------
        total_fetched = len(campaigns_raw_records) + len(insights_raw_records)
        logger.finish_success(
            fetched=total_fetched,
            inserted=inserted + campaigns_loaded,
            updated=updated,
        )

        if conn_id is not None:
            update_last_synced(conn_id)
            update_status(conn_id, "active")

        print("=" * 70)
        print("[SUCCESS] Meta Ads sync completed successfully!")
        print("=" * 70)
        return {
            "status": "success",
            "business_id": business_id,
            "ad_account_id": ad_account_id,
            "date_range": f"{since_date} to {until_date}",
            "campaigns_synced": len(df_campaigns),
            "insights_fetched": len(insights_raw_records),
            "spend_inserted": inserted,
            "spend_updated": updated,
        }

    except Exception as e:
        logger.finish_failure(str(e))
        if conn_id is not None:
            update_status(conn_id, "error", error_message=str(e))
        print(f"[!] Meta Ads sync failed: {e}")
        raise


if __name__ == "__main__":
    run_meta_ads_sync(BUSINESS_ID)

