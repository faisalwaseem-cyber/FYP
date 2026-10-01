"""
Modular Daraz Data Services for BRIDGE FLOW (Phase 3.3).

Separates API querying and pagination from database storage and business logic.
Provides:
- DarazOrderService: Order fetching, optional item hydration, and offset pagination
- DarazProductService: Product catalog retrieval with pagination
- DarazInventoryService: Product SKU stock level extraction

Services DO NOT write directly to the database.
"""
from typing import Any, Dict, Generator, List, Optional

from connectors.daraz.client import DarazClient


class DarazOrderService:
    """Retrieves order records and order items from Daraz Open Platform."""

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
        """
        Yields pages of orders from Daraz Open Platform (/orders/get).
        If hydrate_items=True, queries /order/items/get for each order
        if items are not already present in the payload.
        """
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
            orders = page.get("records", [])

            # Hydrate items for each order if needed
            if hydrate_items and orders:
                for ord_obj in orders:
                    if "items" not in ord_obj and "order_items" not in ord_obj:
                        o_id = str(ord_obj.get("order_id") or ord_obj.get("order_number") or "")
                        if o_id:
                            try:
                                items = self.client.fetch_order_items(o_id)
                                ord_obj["items"] = items
                            except Exception:
                                ord_obj["items"] = []

            yield {
                "page": page["page"],
                "offset": page["offset"],
                "count": page["count"],
                "orders": orders,
                "raw_response": page["raw_response"],
            }


class DarazProductService:
    """Retrieves product catalog and SKU details from Daraz Open Platform."""

    def __init__(self, client: DarazClient):
        self.client = client

    def fetch_products(
        self,
        filter_status: Optional[str] = None,
        create_after: Optional[str] = None,
        update_after: Optional[str] = None,
        search: Optional[str] = None,
        page_size: int = 100,
        max_pages: Optional[int] = None,
    ) -> Generator[Dict[str, Any], None, None]:
        """
        Yields pages of products from Daraz Open Platform (/products/get).
        """
        params: Dict[str, Any] = {}
        if filter_status:
            params["filter"] = filter_status
        if create_after:
            params["create_after"] = create_after
        if update_after:
            params["update_after"] = update_after
        if search:
            params["search"] = search

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


class DarazInventoryService:
    """Extracts SKU inventory levels from Daraz Open Platform."""

    def __init__(self, client: DarazClient):
        self.client = client

    def fetch_inventory(
        self,
        page_size: int = 100,
        max_pages: Optional[int] = None,
    ) -> Generator[Dict[str, Any], None, None]:
        """
        Yields inventory levels by querying products and extracting SKU stock.
        """
        for page in self.client.paginate(
            api_path="/products/get",
            data_key="products",
            page_size=page_size,
            max_pages=max_pages,
        ):
            products = page.get("records", [])
            inventory_items = []

            for prod in products:
                # Skus can be in 'skus' or 'Skus'
                skus = prod.get("skus") or prod.get("Skus") or []
                for sku in skus:
                    seller_sku = sku.get("SellerSku") or sku.get("seller_sku") or sku.get("sku") or ""
                    qty = int(sku.get("quantity") or sku.get("Available") or 0)
                    price = float(sku.get("price") or sku.get("special_price") or 0.0)

                    inventory_items.append({
                        "seller_sku": seller_sku,
                        "sku_id": str(sku.get("ShopSku") or sku.get("sku_id") or seller_sku),
                        "quantity": max(0, qty),
                        "price": max(0.0, price),
                        "item_id": str(prod.get("item_id") or prod.get("id") or ""),
                    })

            yield {
                "page": page["page"],
                "offset": page["offset"],
                "count": len(inventory_items),
                "inventory_levels": inventory_items,
                "raw_response": page["raw_response"],
            }
