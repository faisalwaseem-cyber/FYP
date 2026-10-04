"""Focused, read-only Meta Marketing API retrieval services."""

from typing import Any, Dict, List, Optional

from connectors.meta_ads.client import MetaAdsClient


class MetaAdAccountService:
    def __init__(self, client: MetaAdsClient):
        self.client = client

    def fetch_accounts(self, max_pages: Optional[int] = None) -> List[Dict[str, Any]]:
        return self.client.fetch_accessible_ad_accounts(max_pages=max_pages)


class MetaCampaignService:
    def __init__(self, client: MetaAdsClient):
        self.client = client

    def fetch_campaigns(self, max_pages: Optional[int] = None) -> List[Dict[str, Any]]:
        return self.client.fetch_campaigns(max_pages=max_pages)


class MetaAdSetService:
    def __init__(self, client: MetaAdsClient):
        self.client = client

    def fetch_ad_sets(
        self,
        campaign_id: Optional[str] = None,
        max_pages: Optional[int] = None,
    ) -> List[Dict[str, Any]]:
        return self.client.fetch_ad_sets(
            campaign_id=campaign_id, max_pages=max_pages
        )


class MetaAdService:
    def __init__(self, client: MetaAdsClient):
        self.client = client

    def fetch_ads(
        self,
        ad_set_id: Optional[str] = None,
        max_pages: Optional[int] = None,
    ) -> List[Dict[str, Any]]:
        return self.client.fetch_ads(ad_set_id=ad_set_id, max_pages=max_pages)

    def fetch_creatives(self, max_pages: Optional[int] = None) -> List[Dict[str, Any]]:
        return self.client.fetch_ad_creatives(max_pages=max_pages)


class MetaInsightsService:
    def __init__(self, client: MetaAdsClient):
        self.client = client

    def fetch_insights(
        self,
        *,
        level: str,
        since: str,
        until: str,
        time_increment: Optional[Any] = None,
        max_pages: Optional[int] = None,
    ) -> List[Dict[str, Any]]:
        """Return raw Meta metrics; action values and ROAS remain Meta-attributed."""
        return self.client.fetch_insights(
            level=level,
            since=since,
            until=until,
            time_increment=time_increment,
            max_pages=max_pages,
        )