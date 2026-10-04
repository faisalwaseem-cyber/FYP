"""Tenant-scoped Meta Ads ingestion through services, raw storage, ETL, and loader."""

from datetime import date, timedelta
from typing import Any, Dict, List, Optional, Tuple

from auth.connection_service import list_connections, update_last_synced, update_status
from auth.credential_manager import CredentialManager
from config.settings import BUSINESS_ID, META_GRAPH_API_BASE_URL
from connectors.meta_ads.client import MetaAdsClient, MetaAuthenticationError
from connectors.meta_ads.services import (
    MetaAdAccountService,
    MetaAdService,
    MetaAdSetService,
    MetaCampaignService,
    MetaInsightsService,
)
from etl.meta_ads_transformer import MetaAdsTransformer
from loaders.meta_ads_loader import MetaAdsLoader
from pipeline.sync_logger import SyncLogger
from raw_layer.raw_repository import save_raw_payload


def resolve_meta_credentials(
    business_id: int,
    account_identifier: Optional[str] = None,
) -> Tuple[str, str, None, int]:
    """Resolve one active Meta token only through the encrypted connection registry."""
    connections = [
        connection
        for connection in list_connections(business_id)
        if connection["platform"] == "meta_ads" and connection["status"] == "active"
    ]
    if account_identifier is not None:
        connections = [
            connection for connection in connections
            if connection["account_identifier"] == account_identifier
        ]
    if len(connections) != 1:
        raise ValueError("Select one active Meta ad account connection for this business.")

    connection = connections[0]
    credentials = CredentialManager().get_valid_credentials(
        business_id=business_id,
        platform="meta_ads",
        account_identifier=connection["account_identifier"],
        refresh_callback=None,
    )
    access_token = credentials.get("access_token")
    if not access_token:
        raise ValueError("Meta access token is unavailable for this connection.")
    return (
        access_token,
        connection["account_identifier"],
        None,
        connection["connection_id"],
    )


def run_meta_ads_sync(
    business_id: int = BUSINESS_ID,
    days_back: int = 7,
    account_identifier: Optional[str] = None,
) -> Dict[str, Any]:
    """Sync active Meta account connections for one server-resolved business."""
    if isinstance(business_id, bool) or not isinstance(business_id, int) or business_id <= 0:
        raise ValueError("business_id must be a positive server-resolved integer.")
    if isinstance(days_back, bool) or not isinstance(days_back, int) or not 1 <= days_back <= 90:
        raise ValueError("days_back must be between 1 and 90.")

    connections = [
        connection
        for connection in list_connections(business_id)
        if connection["platform"] == "meta_ads" and connection["status"] == "active"
    ]
    if account_identifier is not None:
        connections = [
            connection for connection in connections
            if connection["account_identifier"] == account_identifier
        ]
    if not connections:
        raise ValueError("No active Meta ad account connection was found for this business.")

    today = date.today()
    since_date = (today - timedelta(days=days_back)).isoformat()
    until_date = today.isoformat()
    logger = SyncLogger(business_id=business_id, source="meta_ads")
    logger.start_sync(sync_type=f"meta_ads_{days_back}d_sync")

    fetched_count = 0
    inserted_count = 0
    updated_count = 0
    campaigns_count = 0
    insights_count = 0
    current_connection_id: Optional[int] = None

    try:
        transformer = MetaAdsTransformer(business_id=business_id)
        loader = MetaAdsLoader(business_id=business_id)
        for connection in connections:
            connection_id = connection.get("connection_id")
            connected_account_identifier = connection.get("account_identifier")
            if not isinstance(connection_id, int) or isinstance(connection_id, bool):
                raise ValueError("Meta connection is missing a valid connection_id.")
            if not isinstance(connected_account_identifier, str) or not connected_account_identifier:
                raise ValueError("Meta connection is missing an account identifier.")
            current_connection_id = connection_id
            client = MetaAdsClient(
                business_id=business_id,
                account_identifier=connected_account_identifier,
                ad_account_id=connected_account_identifier,
                base_url=META_GRAPH_API_BASE_URL,
            )
            account_context = {"account_identifier": connected_account_identifier}

            raw_by_type: Dict[str, List[Dict[str, Any]]] = {}

            def fetch_and_preserve(data_type: str, records: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
                nonlocal fetched_count
                raw_records = [dict(record) for record in records]
                raw_by_type[data_type] = raw_records
                fetched_count += len(raw_records)
                save_raw_payload(
                    business_id,
                    "meta_ads",
                    data_type,
                    {**account_context, "data": raw_records},
                )
                return raw_records

            account_service = MetaAdAccountService(client)
            campaign_service = MetaCampaignService(client)
            ad_set_service = MetaAdSetService(client)
            ad_service = MetaAdService(client)
            insights_service = MetaInsightsService(client)

            fetch_and_preserve("ad_accounts", account_service.fetch_accounts())
            raw_by_type["campaigns"] = fetch_and_preserve(
                "campaigns", campaign_service.fetch_campaigns()
            )
            fetch_and_preserve("ad_sets", ad_set_service.fetch_ad_sets())
            fetch_and_preserve("ads", ad_service.fetch_ads())
            fetch_and_preserve("ad_creatives", ad_service.fetch_creatives())
            raw_by_type["insights"] = fetch_and_preserve(
                "insights",
                insights_service.fetch_insights(
                    level="campaign",
                    since=since_date,
                    until=until_date,
                    time_increment=1,
                ),
            )

            campaign_frame = transformer.transform_campaigns(raw_by_type["campaigns"])
            campaigns_count += len(campaign_frame)
            inserted_count += loader.load_campaigns(campaign_frame)

            _, spend_frame = transformer.transform_insights(
                raw_by_type["insights"], level="campaign"
            )
            insights_count += len(raw_by_type["insights"])
            spend_inserted, spend_updated = loader.load_ad_spend(spend_frame)
            inserted_count += spend_inserted
            updated_count += spend_updated
            update_last_synced(connection_id)

        logger.finish_success(
            fetched=fetched_count,
            inserted=inserted_count,
            updated=updated_count,
        )
        return {
            "status": "success",
            "business_id": business_id,
            "accounts_synced": len(connections),
            "campaigns_synced": campaigns_count,
            "insights_fetched": insights_count,
            "spend_inserted": inserted_count - campaigns_count,
            "spend_updated": updated_count,
            "date_range": {"since": since_date, "until": until_date},
        }
    except Exception as exc:
        logger.finish_failure(str(exc))
        if current_connection_id is not None and not isinstance(exc, MetaAuthenticationError):
            try:
                update_status(current_connection_id, "error", error_message=str(exc))
            except Exception:
                pass
        raise


if __name__ == "__main__":
    run_meta_ads_sync(BUSINESS_ID)

