"""Calculate and persist order-level profitability."""
from sqlalchemy import text

from config.database import get_db_connection


_UPSERT_PROFITABILITY = text("""
    WITH target_orders AS (
        SELECT
            order_id,
            business_id,
            order_date,
            campaign_id,
            subtotal_amount,
            discount_amount
        FROM core.orders
        WHERE business_id = :business_id
    ),
    item_costs AS (
        SELECT
            oi.order_id,
            SUM(
                oi.quantity * COALESCE(NULLIF(oi.unit_cost_at_sale, 0), p.unit_cost, 0)
            ) AS cogs
        FROM core.order_item oi
        JOIN target_orders o ON o.order_id = oi.order_id
        LEFT JOIN core.product p ON p.product_id = oi.product_id
        GROUP BY oi.order_id
    ),
    shipment_costs AS (
        SELECT s.order_id, SUM(s.shipping_cost) AS shipping_cost
        FROM core.shipment s
        JOIN target_orders o ON o.order_id = s.order_id
        GROUP BY s.order_id
    ),
    rto_costs AS (
        SELECT r.order_id, SUM(r.return_shipping_cost) AS rto_cost
        FROM core.rto_return r
        JOIN target_orders o ON o.order_id = r.order_id
        GROUP BY r.order_id
    ),
    payment_fees AS (
        SELECT p.order_id, SUM(p.gateway_fee) AS gateway_fee
        FROM core.payment p
        JOIN target_orders o ON o.order_id = p.order_id
        GROUP BY p.order_id
    ),
    ad_allocation_basis AS (
        SELECT
            o.order_id,
            spend.ad_spend_id,
            spend.spend_amount,
            o.subtotal_amount,
            SUM(o.subtotal_amount) OVER (PARTITION BY spend.ad_spend_id) AS attributed_revenue,
            COUNT(*) OVER (PARTITION BY spend.ad_spend_id) AS attributed_orders,
            ROW_NUMBER() OVER (
                PARTITION BY spend.ad_spend_id
                ORDER BY o.order_date, o.order_id
            ) AS allocation_rank
        FROM target_orders o
        JOIN core.ad_spend spend
          ON spend.business_id = o.business_id
         AND spend.campaign_id = o.campaign_id
         AND spend.spend_date = o.order_date::date
        WHERE o.campaign_id IS NOT NULL
    ),
    ad_allocation_rounded AS (
        SELECT
            *,
            ROUND(
                CASE
                    WHEN attributed_revenue > 0
                        THEN spend_amount * subtotal_amount / attributed_revenue
                    ELSE spend_amount / attributed_orders
                END,
                2
            ) AS rounded_allocation
        FROM ad_allocation_basis
    ),
    ad_allocation_totals AS (
        SELECT
            *,
            SUM(rounded_allocation) OVER (PARTITION BY ad_spend_id) AS rounded_total
        FROM ad_allocation_rounded
    ),
    ad_costs AS (
        SELECT
            order_id,
            SUM(
                CASE
                    WHEN allocation_rank = 1
                        THEN spend_amount - rounded_total + rounded_allocation
                    ELSE rounded_allocation
                END
            ) AS ad_cost_allocated
        FROM ad_allocation_totals
        GROUP BY order_id
    ),
    linked_expenses AS (
        SELECT
            txn.order_id,
            SUM(txn.amount) FILTER (
                WHERE expense.expense_category = 'platform_fee'
            ) AS platform_fee,
            SUM(txn.amount) FILTER (
                WHERE expense.expense_category IS DISTINCT FROM 'platform_fee'
            ) AS other_expense_allocated
        FROM core.financial_transaction txn
        JOIN target_orders o ON o.order_id = txn.order_id
        LEFT JOIN core.expense expense ON expense.expense_id = txn.expense_id
        WHERE txn.direction = 'out'
          AND txn.transaction_type = 'expense'
        GROUP BY txn.order_id
    ),
    components AS (
        SELECT
            o.business_id,
            o.order_id,
            o.subtotal_amount::numeric(12, 2) AS revenue,
            COALESCE(items.cogs, 0)::numeric(12, 2) AS cogs,
            COALESCE(ads.ad_cost_allocated, 0)::numeric(12, 2) AS ad_cost_allocated,
            COALESCE(shipments.shipping_cost, 0)::numeric(12, 2) AS shipping_cost,
            COALESCE(returns.rto_cost, 0)::numeric(12, 2) AS rto_cost,
            o.discount_amount::numeric(12, 2) AS discount_amount,
            COALESCE(payments.gateway_fee, 0)::numeric(12, 2) AS gateway_fee,
            COALESCE(expenses.platform_fee, 0)::numeric(12, 2) AS platform_fee,
            COALESCE(expenses.other_expense_allocated, 0)::numeric(12, 2)
                AS other_expense_allocated
        FROM target_orders o
        LEFT JOIN item_costs items ON items.order_id = o.order_id
        LEFT JOIN ad_costs ads ON ads.order_id = o.order_id
        LEFT JOIN shipment_costs shipments ON shipments.order_id = o.order_id
        LEFT JOIN rto_costs returns ON returns.order_id = o.order_id
        LEFT JOIN payment_fees payments ON payments.order_id = o.order_id
        LEFT JOIN linked_expenses expenses ON expenses.order_id = o.order_id
    )
    INSERT INTO analytics.profit_calculation (
        business_id,
        order_id,
        revenue,
        cogs,
        ad_cost_allocated,
        shipping_cost,
        rto_cost,
        discount_amount,
        gateway_fee,
        platform_fee,
        other_expense_allocated,
        true_profit,
        calculated_at
    )
    SELECT
        business_id,
        order_id,
        revenue,
        cogs,
        ad_cost_allocated,
        shipping_cost,
        rto_cost,
        discount_amount,
        gateway_fee,
        platform_fee,
        other_expense_allocated,
        revenue - cogs - ad_cost_allocated - shipping_cost - rto_cost
            - discount_amount - gateway_fee - platform_fee - other_expense_allocated,
        now()
    FROM components
    ON CONFLICT (order_id) DO UPDATE SET
        business_id = EXCLUDED.business_id,
        revenue = EXCLUDED.revenue,
        cogs = EXCLUDED.cogs,
        ad_cost_allocated = EXCLUDED.ad_cost_allocated,
        shipping_cost = EXCLUDED.shipping_cost,
        rto_cost = EXCLUDED.rto_cost,
        discount_amount = EXCLUDED.discount_amount,
        gateway_fee = EXCLUDED.gateway_fee,
        platform_fee = EXCLUDED.platform_fee,
        other_expense_allocated = EXCLUDED.other_expense_allocated,
        true_profit = EXCLUDED.true_profit,
        calculated_at = now()
    RETURNING profit_id;
""")


def calculate_order_profitability(business_id: int) -> int:
    """Upsert profitability for every order belonging to a business.

    Returns the number of inserted or updated profit rows.
    """
    with get_db_connection() as connection:
        result = connection.execute(
            _UPSERT_PROFITABILITY,
            {"business_id": business_id},
        )
        affected_rows = len(result.fetchall())
        connection.commit()
        return affected_rows