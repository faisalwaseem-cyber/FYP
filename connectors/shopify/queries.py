"""
Modular, parameterized Shopify GraphQL Admin API queries for BRIDGE FLOW.

Designed for API version 2026-07+.
Requests ONLY fields required by the BRIDGE FLOW canonical models and
profitability engine, respecting Shopify protected customer data constraints.
"""

# ---------------------------------------------------------------------------
# 1. Orders Query
# ---------------------------------------------------------------------------
ORDERS_GRAPHQL_QUERY = """
query GetOrders($first: Int!, $after: String, $query: String) {
  orders(first: $first, after: $after, query: $query, sortKey: CREATED_AT, reverse: false) {
    edges {
      cursor
      node {
        id
        name
        createdAt
        updatedAt
        cancelledAt
        displayFinancialStatus
        displayFulfillmentStatus
        currencyCode
        currentSubtotalPriceSet {
          shopMoney {
            amount
            currencyCode
          }
        }
        currentTotalDiscountsSet {
          shopMoney {
            amount
            currencyCode
          }
        }
        totalShippingPriceSet {
          shopMoney {
            amount
            currencyCode
          }
        }
        currentTotalPriceSet {
          shopMoney {
            amount
            currencyCode
          }
        }
        paymentGatewayNames
        customer {
          id
          firstName
          lastName
          email
          phone
        }
        shippingAddress {
          name
          phone
          city
          province
          country
        }
        lineItems(first: 100) {
          edges {
            node {
              id
              title
              sku
              quantity
              originalUnitPriceSet {
                shopMoney {
                  amount
                  currencyCode
                }
              }
              totalDiscountSet {
                shopMoney {
                  amount
                  currencyCode
                }
              }
              product {
                id
              }
              variant {
                id
                sku
                title
              }
            }
          }
        }
      }
    }
    pageInfo {
      hasNextPage
      endCursor
    }
  }
}
"""

# ---------------------------------------------------------------------------
# 2. Products Query
# ---------------------------------------------------------------------------
PRODUCTS_GRAPHQL_QUERY = """
query GetProducts($first: Int!, $after: String, $query: String) {
  products(first: $first, after: $after, query: $query) {
    edges {
      cursor
      node {
        id
        title
        status
        variants(first: 100) {
          edges {
            node {
              id
              sku
              title
              price
              inventoryItem {
                id
              }
            }
          }
        }
      }
    }
    pageInfo {
      hasNextPage
      endCursor
    }
  }
}
"""

# ---------------------------------------------------------------------------
# 3. Customers Query
# ---------------------------------------------------------------------------
CUSTOMERS_GRAPHQL_QUERY = """
query GetCustomers($first: Int!, $after: String, $query: String) {
  customers(first: $first, after: $after, query: $query) {
    edges {
      cursor
      node {
        id
        firstName
        lastName
        email
        phone
        defaultAddress {
          city
          province
          country
        }
      }
    }
    pageInfo {
      hasNextPage
      endCursor
    }
  }
}
"""

# ---------------------------------------------------------------------------
# 4. Locations Query
# ---------------------------------------------------------------------------
LOCATIONS_GRAPHQL_QUERY = """
query GetLocations($first: Int!, $after: String) {
  locations(first: $first, after: $after) {
    edges {
      cursor
      node {
        id
        name
        isActive
      }
    }
    pageInfo {
      hasNextPage
      endCursor
    }
  }
}
"""

# ---------------------------------------------------------------------------
# 5. Inventory Levels by Location Query
# ---------------------------------------------------------------------------
INVENTORY_LEVELS_GRAPHQL_QUERY = """
query GetInventoryLevels($first: Int!, $after: String, $locationId: ID!) {
  location(id: $locationId) {
    id
    name
    inventoryLevels(first: $first, after: $after) {
      edges {
        cursor
        node {
          id
          quantities(names: ["available"]) {
            name
            quantity
          }
          item {
            id
            sku
          }
        }
      }
      pageInfo {
        hasNextPage
        endCursor
      }
    }
  }
}
"""
