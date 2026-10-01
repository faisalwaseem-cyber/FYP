"""
Unit Test Suite for Sub-Phase 1: Shopify GraphQL Foundation.

Tests cover:
    1. Successful GraphQL request execution
    2. GraphQL error response handling (ShopifyGraphQLError)
    3. HTTP transient error handling & retry (500/502/503/504)
    4. Cost-based throttle handling (HTTP 429 & GraphQL THROTTLED code)
    5. Cursor pagination across multiple pages
    6. Single-page response pagination
    7. Empty response pagination
    8. Credential & logging safety (token masking in repr and errors)
"""
import sys
import unittest
from pathlib import Path
from unittest.mock import MagicMock, patch

# Ensure root directory is on Python path
sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from connectors.shopify.graphql_client import (
    ShopifyGraphQLClient,
    ShopifyGraphQLError,
    ShopifyRateLimitError,
)
from connectors.shopify.queries import (
    ORDERS_GRAPHQL_QUERY,
    PRODUCTS_GRAPHQL_QUERY,
    CUSTOMERS_GRAPHQL_QUERY,
    LOCATIONS_GRAPHQL_QUERY,
    INVENTORY_LEVELS_GRAPHQL_QUERY,
)


class TestShopifyGraphQLClient(unittest.TestCase):
    def setUp(self):
        self.store_domain = "test-store.myshopify.com"
        self.secret_token = "shpat_ultra_secret_token_12345"
        self.client = ShopifyGraphQLClient(
            store_domain=self.store_domain,
            access_token=self.secret_token,
            api_version="2026-07",
            timeout=10.0,
            max_retries=3,
        )

    # ------------------------------------------------------------------
    # 1. Successful GraphQL request
    # ------------------------------------------------------------------
    @patch("requests.Session.post")
    def test_successful_graphql_request(self, mock_post):
        mock_response = MagicMock()
        mock_response.status_code = 200
        mock_response.json.return_value = {
            "data": {
                "shop": {"name": "Test Store"}
            },
            "extensions": {
                "cost": {
                    "requestedQueryCost": 5,
                    "actualQueryCost": 2,
                    "throttleStatus": {
                        "maximumAvailable": 2000.0,
                        "currentlyAvailable": 1998.0,
                        "restoreRate": 100.0,
                    }
                }
            }
        }
        mock_post.return_value = mock_response

        res = self.client.execute("query { shop { name } }")
        self.assertIn("data", res)
        self.assertEqual(res["data"]["shop"]["name"], "Test Store")
        
        # Verify endpoint and headers
        mock_post.assert_called_once()
        args, kwargs = mock_post.call_args
        self.assertEqual(args[0], "https://test-store.myshopify.com/admin/api/2026-07/graphql.json")
        self.assertEqual(kwargs["json"]["query"], "query { shop { name } }")
        self.assertEqual(self.client._session.headers["X-Shopify-Access-Token"], self.secret_token)

    # ------------------------------------------------------------------
    # 2. GraphQL error response
    # ------------------------------------------------------------------
    @patch("requests.Session.post")
    def test_graphql_error_response(self, mock_post):
        mock_response = MagicMock()
        mock_response.status_code = 200
        mock_response.json.return_value = {
            "errors": [
                {
                    "message": "Field 'invalidField' doesn't exist on type 'QueryRoot'",
                    "locations": [{"line": 1, "column": 9}],
                }
            ]
        }
        mock_post.return_value = mock_response

        with self.assertRaises(ShopifyGraphQLError) as ctx:
            self.client.execute("query { invalidField }")

        self.assertIn("invalidField", str(ctx.exception))
        self.assertNotIn(self.secret_token, str(ctx.exception))

    # ------------------------------------------------------------------
    # 3. HTTP transient error retry
    # ------------------------------------------------------------------
    @patch("time.sleep", return_value=None)
    @patch("requests.Session.post")
    def test_http_transient_error_retry(self, mock_post, mock_sleep):
        # 502 Bad Gateway followed by 200 OK
        resp_502 = MagicMock()
        resp_502.status_code = 502
        resp_502.reason = "Bad Gateway"

        resp_200 = MagicMock()
        resp_200.status_code = 200
        resp_200.json.return_value = {"data": {"status": "recovered"}}

        mock_post.side_effect = [resp_502, resp_200]

        res = self.client.execute("query { test }")
        self.assertEqual(res["data"]["status"], "recovered")
        self.assertEqual(mock_post.call_count, 2)
        mock_sleep.assert_called_once()

    # ------------------------------------------------------------------
    # 4. Rate-limit / throttle behavior
    # ------------------------------------------------------------------
    @patch("time.sleep", return_value=None)
    @patch("requests.Session.post")
    def test_http_429_throttle_handling(self, mock_post, mock_sleep):
        # 429 with Retry-After header, then 200 OK
        resp_429 = MagicMock()
        resp_429.status_code = 429
        resp_429.headers = {"Retry-After": "2.5"}
        resp_429.json.return_value = {}

        resp_200 = MagicMock()
        resp_200.status_code = 200
        resp_200.json.return_value = {"data": {"ok": True}}

        mock_post.side_effect = [resp_429, resp_200]

        res = self.client.execute("query { test }")
        self.assertTrue(res["data"]["ok"])
        self.assertEqual(mock_post.call_count, 2)
        mock_sleep.assert_called_with(2.5)

    @patch("time.sleep", return_value=None)
    @patch("requests.Session.post")
    def test_graphql_throttled_code_handling(self, mock_post, mock_sleep):
        # GraphQL 200 with THROTTLED code, then 200 OK
        resp_throttled = MagicMock()
        resp_throttled.status_code = 200
        resp_throttled.headers = {}
        resp_throttled.json.return_value = {
            "errors": [
                {
                    "message": "Throttled",
                    "extensions": {
                        "code": "THROTTLED",
                        "cost": {
                            "requestedQueryCost": 100,
                            "throttleStatus": {
                                "maximumAvailable": 2000.0,
                                "currentlyAvailable": 10.0,
                                "restoreRate": 50.0,
                            }
                        }
                    }
                }
            ],
            "extensions": {
                "cost": {
                    "requestedQueryCost": 100,
                    "throttleStatus": {
                        "maximumAvailable": 2000.0,
                        "currentlyAvailable": 10.0,
                        "restoreRate": 50.0,
                    }
                }
            }
        }

        resp_ok = MagicMock()
        resp_ok.status_code = 200
        resp_ok.json.return_value = {"data": {"success": True}}

        mock_post.side_effect = [resp_throttled, resp_ok]

        res = self.client.execute("query { test }")
        self.assertTrue(res["data"]["success"])
        self.assertEqual(mock_post.call_count, 2)
        # deficit = 100 - 10 = 90; 90 / 50 = 1.8 + 0.2 buffer = 2.0s
        mock_sleep.assert_called_with(2.0)

    # ------------------------------------------------------------------
    # 5. Cursor pagination across multiple pages
    # ------------------------------------------------------------------
    @patch("connectors.shopify.graphql_client.ShopifyGraphQLClient.execute")
    def test_cursor_pagination_multiple_pages(self, mock_execute):
        page1_data = {
            "data": {
                "orders": {
                    "edges": [
                        {"cursor": "c1", "node": {"id": "gid://shopify/Order/1", "name": "#1001"}},
                        {"cursor": "c2", "node": {"id": "gid://shopify/Order/2", "name": "#1002"}},
                    ],
                    "pageInfo": {
                        "hasNextPage": True,
                        "endCursor": "c2",
                    }
                }
            }
        }
        page2_data = {
            "data": {
                "orders": {
                    "edges": [
                        {"cursor": "c3", "node": {"id": "gid://shopify/Order/3", "name": "#1003"}},
                    ],
                    "pageInfo": {
                        "hasNextPage": False,
                        "endCursor": "c3",
                    }
                }
            }
        }
        mock_execute.side_effect = [page1_data, page2_data]

        pages = list(self.client.paginate(
            query=ORDERS_GRAPHQL_QUERY,
            variables={"query": "status:open"},
            connection_path=("orders",),
            page_size=2,
        ))

        self.assertEqual(len(pages), 2)
        self.assertEqual(pages[0]["page"], 1)
        self.assertEqual(len(pages[0]["nodes"]), 2)
        self.assertEqual(pages[0]["nodes"][0]["name"], "#1001")
        self.assertEqual(pages[1]["page"], 2)
        self.assertEqual(len(pages[1]["nodes"]), 1)
        self.assertEqual(pages[1]["nodes"][0]["name"], "#1003")

        # Verify variables passed to execute
        self.assertEqual(mock_execute.call_count, 2)
        first_call_vars = mock_execute.call_args_list[0][0][1]
        second_call_vars = mock_execute.call_args_list[1][0][1]
        self.assertNotIn("after", first_call_vars)
        self.assertEqual(second_call_vars["after"], "c2")

    # ------------------------------------------------------------------
    # 6. Single-page response
    # ------------------------------------------------------------------
    @patch("connectors.shopify.graphql_client.ShopifyGraphQLClient.execute")
    def test_cursor_pagination_single_page(self, mock_execute):
        page_data = {
            "data": {
                "products": {
                    "edges": [
                        {"cursor": "p1", "node": {"id": "gid://shopify/Product/1", "title": "Shirt"}},
                    ],
                    "pageInfo": {
                        "hasNextPage": False,
                        "endCursor": "p1",
                    }
                }
            }
        }
        mock_execute.return_value = page_data

        pages = list(self.client.paginate(
            query=PRODUCTS_GRAPHQL_QUERY,
            connection_path=("products",),
            page_size=10,
        ))

        self.assertEqual(len(pages), 1)
        self.assertEqual(len(pages[0]["nodes"]), 1)
        self.assertEqual(pages[0]["nodes"][0]["title"], "Shirt")
        self.assertEqual(mock_execute.call_count, 1)

    # ------------------------------------------------------------------
    # 7. Empty response
    # ------------------------------------------------------------------
    @patch("connectors.shopify.graphql_client.ShopifyGraphQLClient.execute")
    def test_cursor_pagination_empty_response(self, mock_execute):
        page_data = {
            "data": {
                "customers": {
                    "edges": [],
                    "pageInfo": {
                        "hasNextPage": False,
                        "endCursor": None,
                    }
                }
            }
        }
        mock_execute.return_value = page_data

        pages = list(self.client.paginate(
            query=CUSTOMERS_GRAPHQL_QUERY,
            connection_path=("customers",),
            page_size=50,
        ))

        self.assertEqual(len(pages), 1)
        self.assertEqual(len(pages[0]["nodes"]), 0)

    # ------------------------------------------------------------------
    # 8. Credential & logging safety
    # ------------------------------------------------------------------
    def test_credential_safety(self):
        # __repr__ must mask token
        repr_str = repr(self.client)
        self.assertIn("test-store.myshopify.com", repr_str)
        self.assertNotIn(self.secret_token, repr_str)
        self.assertIn("token='***'", repr_str)

        # Exception representations must not leak token
        err = ShopifyGraphQLError([{"message": "Access denied"}])
        self.assertNotIn(self.secret_token, str(err))

    # ------------------------------------------------------------------
    # 9. Queries sanity check
    # ------------------------------------------------------------------
    def test_queries_contain_expected_fields(self):
        self.assertIn("orders", ORDERS_GRAPHQL_QUERY)
        self.assertIn("products", PRODUCTS_GRAPHQL_QUERY)
        self.assertIn("customers", CUSTOMERS_GRAPHQL_QUERY)
        self.assertIn("locations", LOCATIONS_GRAPHQL_QUERY)
        self.assertIn("inventoryLevels", INVENTORY_LEVELS_GRAPHQL_QUERY)


if __name__ == "__main__":
    unittest.main()
