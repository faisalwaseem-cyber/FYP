"""
Modular Shopify Data Services for BRIDGE FLOW.

Each service class receives a ShopifyGraphQLClient and manages data retrieval
for a specific domain (Orders, Products, Customers, Inventory, Locations).
Services DO NOT interact directly with PostgreSQL or ETL layers; they focus
strictly on Shopify API retrieval and pagination.
"""
from typing import Any, Dict, Generator, List, Optional

from connectors.shopify.graphql_client import ShopifyGraphQLClient
from connectors.shopify.queries import (
    ORDERS_GRAPHQL_QUERY,
    PRODUCTS_GRAPHQL_QUERY,
    CUSTOMERS_GRAPHQL_QUERY,
    LOCATIONS_GRAPHQL_QUERY,
    INVENTORY_LEVELS_GRAPHQL_QUERY,
)


class ShopifyOrderService:
    """Retrieves order records and line items via GraphQL cursor pagination."""

    def __init__(self, client: ShopifyGraphQLClient):
        self.client = client

    def fetch_orders(
        self,
        updated_at_min: Optional[str] = None,
        query_filter: Optional[str] = None,
        page_size: int = 50,
        max_pages: Optional[int] = None,
    ) -> Generator[Dict[str, Any], None, None]:
        """
        Yields pages of orders from Shopify GraphQL Admin API.

        Parameters
        ----------
        updated_at_min : str, optional
            ISO timestamp for incremental sync watermark filter.
        query_filter : str, optional
            Additional search query syntax for Shopify GraphQL.
        page_size : int
            Records requested per page (default: 50).
        max_pages : int, optional
            Maximum pages to fetch.
        """
        filters: List[str] = []
        if updated_at_min:
            filters.append(f"updated_at:>='{updated_at_min}'")
        if query_filter:
            filters.append(query_filter)

        search_query = " AND ".join(filters) if filters else None
        variables: Dict[str, Any] = {}
        if search_query:
            variables["query"] = search_query

        for page in self.client.paginate(
            query=ORDERS_GRAPHQL_QUERY,
            variables=variables,
            connection_path=("orders",),
            page_size=page_size,
            max_pages=max_pages,
        ):
            yield {
                "page": page["page"],
                "orders": page["nodes"],
                "edges": page["edges"],
                "page_info": page["page_info"],
                "raw_response": page["raw_response"],
            }


class ShopifyProductService:
    """Retrieves product catalog and variant records via GraphQL cursor pagination."""

    def __init__(self, client: ShopifyGraphQLClient):
        self.client = client

    def fetch_products(
        self,
        query_filter: Optional[str] = None,
        page_size: int = 50,
        max_pages: Optional[int] = None,
    ) -> Generator[Dict[str, Any], None, None]:
        variables: Dict[str, Any] = {}
        if query_filter:
            variables["query"] = query_filter

        for page in self.client.paginate(
            query=PRODUCTS_GRAPHQL_QUERY,
            variables=variables,
            connection_path=("products",),
            page_size=page_size,
            max_pages=max_pages,
        ):
            yield {
                "page": page["page"],
                "products": page["nodes"],
                "edges": page["edges"],
                "page_info": page["page_info"],
                "raw_response": page["raw_response"],
            }


class ShopifyCustomerService:
    """Retrieves customer profiles respecting protected customer data requirements."""

    def __init__(self, client: ShopifyGraphQLClient):
        self.client = client

    def fetch_customers(
        self,
        query_filter: Optional[str] = None,
        page_size: int = 50,
        max_pages: Optional[int] = None,
    ) -> Generator[Dict[str, Any], None, None]:
        variables: Dict[str, Any] = {}
        if query_filter:
            variables["query"] = query_filter

        for page in self.client.paginate(
            query=CUSTOMERS_GRAPHQL_QUERY,
            variables=variables,
            connection_path=("customers",),
            page_size=page_size,
            max_pages=max_pages,
        ):
            yield {
                "page": page["page"],
                "customers": page["nodes"],
                "edges": page["edges"],
                "page_info": page["page_info"],
                "raw_response": page["raw_response"],
            }


class ShopifyLocationService:
    """Retrieves store locations for inventory association."""

    def __init__(self, client: ShopifyGraphQLClient):
        self.client = client

    def fetch_locations(self, page_size: int = 50) -> List[Dict[str, Any]]:
        """Returns all store locations."""
        locations: List[Dict[str, Any]] = []
        for page in self.client.paginate(
            query=LOCATIONS_GRAPHQL_QUERY,
            connection_path=("locations",),
            page_size=page_size,
        ):
            locations.extend(page["nodes"])
        return locations


class ShopifyInventoryService:
    """Retrieves inventory levels per location using GraphQL."""

    def __init__(self, client: ShopifyGraphQLClient):
        self.client = client

    def fetch_inventory_for_location(
        self,
        location_id: str,
        page_size: int = 50,
        max_pages: Optional[int] = None,
    ) -> Generator[Dict[str, Any], None, None]:
        variables = {"locationId": location_id}

        for page in self.client.paginate(
            query=INVENTORY_LEVELS_GRAPHQL_QUERY,
            variables=variables,
            connection_path=("location", "inventoryLevels"),
            page_size=page_size,
            max_pages=max_pages,
        ):
            yield {
                "page": page["page"],
                "location_id": location_id,
                "inventory_levels": page["nodes"],
                "edges": page["edges"],
                "page_info": page["page_info"],
                "raw_response": page["raw_response"],
            }
