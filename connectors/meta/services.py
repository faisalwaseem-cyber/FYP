"""
Modular Meta Marketing Services for BRIDGE FLOW.

Provides domain services for Meta Marketing API v26.0:
- MetaCampaignService: Campaign discovery, status inspection, and pagination
- MetaAdSetService: Ad set retrieval, optimization goals, targeting metadata, and pagination
- MetaAdService: Individual ad configurations, creative attachments, and pagination
- MetaInsightsService: Ad spend, impressions, clicks, actions (conversions), and attribution windows

Services DO NOT write directly to the database or compute analytics/ML models;
they strictly handle API queries and pagination.
"""
import json
from typing import Any, Dict, Generator, List, Optional

from connectors.meta.client import MetaClient

# Standard field sets aligned with official Meta Marketing API specifications
DEFAULT_CAMPAIGN_FIELDS = [
    "id",
    "name",
    "status",
    "effective_status",
    "objective",
    "buying_type",
    "start_time",
    "stop_time",
    "daily_budget",
    "lifetime_budget",
    "created_time",
    "updated_time",
]

DEFAULT_ADSET_FIELDS = [
    "id",
    "name",
    "status",
    "effective_status",
    "campaign_id",
    "daily_budget",
    "lifetime_budget",
    "optimization_goal",
    "bid_amount",
    "targeting",
    "created_time",
    "updated_time",
]

DEFAULT_AD_FIELDS = [
    "id",
    "name",
    "status",
    "effective_status",
    "adset_id",
    "campaign_id",
    "creative{id,name,title,body}",
    "created_time",
    "updated_time",
]

DEFAULT_INSIGHTS_FIELDS = [
    "campaign_id",
    "campaign_name",
    "adset_id",
    "adset_name",
    "ad_id",
    "ad_name",
    "spend",
    "impressions",
    "clicks",
    "inline_link_clicks",
    "actions",
    "action_values",
    "cost_per_action_type",
    "date_start",
    "date_stop",
    "cpc",
    "cpm",
    "ctr",
]


class MetaCampaignService:
    """Retrieves campaign configurations and metadata from Meta Graph API."""

    def __init__(self, client: MetaClient):
        self.client = client

    def fetch_campaigns(
        self,
        status: Optional[str] = None,
        effective_status: Optional[List[str]] = None,
        fields: Optional[List[str]] = None,
        page_size: int = 100,
        max_pages: Optional[int] = None,
    ) -> Generator[Dict[str, Any], None, None]:
        """
        Yields pages of campaigns for the configured ad account.
        Endpoint: GET /v26.0/act_{account_id}/campaigns
        """
        requested_fields = fields or DEFAULT_CAMPAIGN_FIELDS
        params: Dict[str, Any] = {
            "fields": ",".join(requested_fields),
        }

        filtering = []
        if status:
            filtering.append({"field": "status", "operator": "EQUAL", "value": status})
        if effective_status:
            filtering.append({"field": "effective_status", "operator": "IN", "value": effective_status})
        if filtering:
            params["filtering"] = json.dumps(filtering)

        endpoint = f"{self.client.ad_account_id}/campaigns"
        for page in self.client.paginate(endpoint, params=params, limit=page_size, max_pages=max_pages):
            yield {
                "page": page["page"],
                "count": page["count"],
                "campaigns": page["data"],
                "paging": page["paging"],
                "raw_response": page["raw_response"],
            }

    def get_campaign(
        self,
        campaign_id: str,
        fields: Optional[List[str]] = None,
    ) -> Dict[str, Any]:
        """Fetches a single campaign by ID."""
        requested_fields = fields or DEFAULT_CAMPAIGN_FIELDS
        params = {"fields": ",".join(requested_fields)}
        return self.client.get(campaign_id, params=params)


class MetaAdSetService:
    """Retrieves ad set configurations, budgets, and targeting from Meta Graph API."""

    def __init__(self, client: MetaClient):
        self.client = client

    def fetch_adsets(
        self,
        campaign_id: Optional[str] = None,
        status: Optional[str] = None,
        fields: Optional[List[str]] = None,
        page_size: int = 100,
        max_pages: Optional[int] = None,
    ) -> Generator[Dict[str, Any], None, None]:
        """
        Yields pages of ad sets for an ad account or filtered by a parent campaign.
        Endpoint: GET /v26.0/act_{account_id}/adsets or GET /v26.0/{campaign_id}/adsets
        """
        requested_fields = fields or DEFAULT_ADSET_FIELDS
        params: Dict[str, Any] = {
            "fields": ",".join(requested_fields),
        }

        if status:
            params["filtering"] = json.dumps([{"field": "status", "operator": "EQUAL", "value": status}])

        endpoint = f"{campaign_id}/adsets" if campaign_id else f"{self.client.ad_account_id}/adsets"
        for page in self.client.paginate(endpoint, params=params, limit=page_size, max_pages=max_pages):
            yield {
                "page": page["page"],
                "count": page["count"],
                "adsets": page["data"],
                "paging": page["paging"],
                "raw_response": page["raw_response"],
            }

    def get_adset(
        self,
        adset_id: str,
        fields: Optional[List[str]] = None,
    ) -> Dict[str, Any]:
        """Fetches a single ad set by ID."""
        requested_fields = fields or DEFAULT_ADSET_FIELDS
        params = {"fields": ",".join(requested_fields)}
        return self.client.get(adset_id, params=params)


class MetaAdService:
    """Retrieves ad creatives and ad definitions from Meta Graph API."""

    def __init__(self, client: MetaClient):
        self.client = client

    def fetch_ads(
        self,
        adset_id: Optional[str] = None,
        campaign_id: Optional[str] = None,
        status: Optional[str] = None,
        fields: Optional[List[str]] = None,
        page_size: int = 100,
        max_pages: Optional[int] = None,
    ) -> Generator[Dict[str, Any], None, None]:
        """
        Yields pages of ads.
        Endpoint: GET /v26.0/act_{account_id}/ads, GET /v26.0/{adset_id}/ads, or GET /v26.0/{campaign_id}/ads
        """
        requested_fields = fields or DEFAULT_AD_FIELDS
        params: Dict[str, Any] = {
            "fields": ",".join(requested_fields),
        }

        if status:
            params["filtering"] = json.dumps([{"field": "status", "operator": "EQUAL", "value": status}])

        if adset_id:
            endpoint = f"{adset_id}/ads"
        elif campaign_id:
            endpoint = f"{campaign_id}/ads"
        else:
            endpoint = f"{self.client.ad_account_id}/ads"

        for page in self.client.paginate(endpoint, params=params, limit=page_size, max_pages=max_pages):
            yield {
                "page": page["page"],
                "count": page["count"],
                "ads": page["data"],
                "paging": page["paging"],
                "raw_response": page["raw_response"],
            }

    def get_ad(
        self,
        ad_id: str,
        fields: Optional[List[str]] = None,
    ) -> Dict[str, Any]:
        """Fetches a single ad by ID."""
        requested_fields = fields or DEFAULT_AD_FIELDS
        params = {"fields": ",".join(requested_fields)}
        return self.client.get(ad_id, params=params)


class MetaInsightsService:
    """Retrieves ad performance, spend, impressions, clicks, and conversions from Meta Graph API."""

    def __init__(self, client: MetaClient):
        self.client = client

    def fetch_insights(
        self,
        level: str = "campaign",
        since: Optional[str] = None,
        until: Optional[str] = None,
        date_preset: Optional[str] = None,
        time_increment: Optional[Any] = 1,
        fields: Optional[List[str]] = None,
        action_attribution_windows: Optional[List[str]] = None,
        breakdowns: Optional[List[str]] = None,
        object_id: Optional[str] = None,
        page_size: int = 100,
        max_pages: Optional[int] = None,
    ) -> Generator[Dict[str, Any], None, None]:
        """
        Yields pages of insights for an ad account or specific object node.
        Endpoint: GET /v26.0/{object_id or ad_account_id}/insights

        Parameters
        ----------
        level : str
            Granularity: 'account', 'campaign', 'adset', or 'ad'.
        since : str, optional
            Start date (YYYY-MM-DD).
        until : str, optional
            End date (YYYY-MM-DD).
        date_preset : str, optional
            Preset range, e.g. 'today', 'yesterday', 'last_7d', 'last_30d', 'maximum'.
        time_increment : int or str, optional
            '1' for daily breakdown, 'all_days' or None for cumulative. Default: 1.
        fields : List[str], optional
            Metric fields to request.
        action_attribution_windows : List[str], optional
            Attribution windows, e.g. ['1d_click', '7d_click', '1d_view'].
        breakdowns : List[str], optional
            Breakdown fields, e.g. ['age', 'gender', 'device_platform'].
        object_id : str, optional
            Explicit node ID (e.g. campaign ID). Defaults to client ad_account_id.
        """
        requested_fields = fields or DEFAULT_INSIGHTS_FIELDS
        params: Dict[str, Any] = {
            "level": level,
            "fields": ",".join(requested_fields),
        }

        if since and until:
            params["time_range"] = json.dumps({"since": str(since), "until": str(until)})
        elif date_preset:
            params["date_preset"] = date_preset

        if time_increment is not None:
            params["time_increment"] = str(time_increment)

        if action_attribution_windows:
            params["action_attribution_windows"] = json.dumps(action_attribution_windows)

        if breakdowns:
            params["breakdowns"] = ",".join(breakdowns)

        endpoint = f"{object_id or self.client.ad_account_id}/insights"
        for page in self.client.paginate(endpoint, params=params, limit=page_size, max_pages=max_pages):
            yield {
                "page": page["page"],
                "count": page["count"],
                "insights": page["data"],
                "paging": page["paging"],
                "raw_response": page["raw_response"],
            }

