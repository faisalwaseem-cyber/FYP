"""Retrieval-only services for Daraz Open Platform data."""
from collections.abc import Mapping
from typing import Any, Dict, Generator, List, Optional

from connectors.daraz.client import DarazClient


class DarazOrderService:
    """Retrieves orders and optional item detail without transforming provider data."""

    def __init__(self, client: DarazClient):
        self.client = client

    def fetch_orders(
        self,
        created_after: Optional[str] = None,
        created_before: Optional[str] = None,
        update_after: Optional[str] = None,
        update_before: Optional[str] = None,
        status: Optional[str] = None,
        page_size: int = 100,
        max_pages: Optional[int] = None,
        hydrate_items: bool = True,
    ) -> Generator[Dict[str, Any], None, None]:
        """Yield provider order pages, optionally attaching separately fetched raw items."""
        params: Dict[str, Any] = {}
        if created_after:
            params["created_after"] = created_after
        if created_before:
            params["created_before"] = created_before
        if update_after:
            params["update_after"] = update_after
        if update_before:
            params["update_before"] = update_before
        if status:
            params["status"] = status

        for page in self.client.paginate(
            api_path="/orders/get",
            params=params,
            data_key="orders",
            page_size=page_size,
            max_pages=max_pages,
        ):
            raw_orders = page.get("records", [])
            orders = []
            for raw_order in raw_orders:
                if not isinstance(raw_order, Mapping):
                    orders.append(raw_order)
                    continue
                order = dict(raw_order)
                if hydrate_items and not any(key in order for key in ("items", "order_items", "fetched_order_items")):
                    order_id = order.get("order_id")
                    if order_id is not None and str(order_id).strip():
                        order["fetched_order_items"] = self.client.fetch_order_items(str(order_id))
                orders.append(order)

            yield {
                "page": page["page"],
                "offset": page["offset"],
                "count": page["count"],
                "orders": orders,
                "raw_response": page["raw_response"],
            }

    def fetch_order_items(self, order_id: str) -> List[Dict[str, Any]]:
        """Return the raw item response for a seller-assigned order ID."""
        return self.client.fetch_order_items(order_id)


class DarazProductService:
    """Retrieves raw product/SKU catalog pages and individual item details."""

    def __init__(self, client: DarazClient):
        self.client = client

    def fetch_products(
        self,
        filter_status: str = "all",
        create_after: Optional[str] = None,
        create_before: Optional[str] = None,
        update_after: Optional[str] = None,
        update_before: Optional[str] = None,
        page_size: int = 50,
        max_pages: Optional[int] = None,
    ) -> Generator[Dict[str, Any], None, None]:
        """Yield raw product pages using documented filters and page limits."""
        params: Dict[str, Any] = {}
        params["filter"] = filter_status
        if create_after:
            params["create_after"] = create_after
        if create_before:
            params["create_before"] = create_before
        if update_after:
            params["update_after"] = update_after
        if update_before:
            params["update_before"] = update_before

        for page in self.client.paginate(
            api_path="/products/get",
            params=params,
            data_key="products",
            page_size=page_size,
            max_pages=max_pages,
        ):
            yield {
                "page": page["page"],
                "offset": page["offset"],
                "count": page["count"],
                "products": page.get("records", []),
                "raw_response": page["raw_response"],
            }

    def fetch_product_item(
        self,
        item_id: str,
        seller_sku: Optional[str] = None,
    ) -> Any:
        """Fetch raw `/product/item/get` detail for a Daraz item."""
        if not item_id:
            raise ValueError("A Daraz item_id is required.")
        params: Dict[str, Any] = {"item_id": item_id}
        if seller_sku:
            params["seller_sku"] = seller_sku
        return self.client.execute("/product/item/get", params=params, method="GET")


class DarazInventoryService:
    """Retrieves raw Daraz stock-bearing product records without assigning semantics."""

    def __init__(self, client: DarazClient):
        self.client = client

    def fetch_inventory(
        self,
        page_size: int = 50,
        max_pages: Optional[int] = None,
        update_after: Optional[str] = None,
        create_after: Optional[str] = None,
    ) -> Generator[Dict[str, Any], None, None]:
        """Yield raw product stock fields; quantity/location mapping is not verified."""
        params: Dict[str, Any] = {"filter": "all", "options": "1"}
        if update_after:
            params["update_after"] = update_after
        if create_after:
            params["create_after"] = create_after

        for page in self.client.paginate(
            api_path="/products/get",
            params=params,
            data_key="products",
            page_size=page_size,
            max_pages=max_pages,
        ):
            yield {
                "page": page["page"],
                "offset": page["offset"],
                "count": page["count"],
                "products": page.get("records", []),
                "raw_response": page["raw_response"],
            }
