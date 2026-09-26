-- Test: Assert that no order in fact_orders has a non-positive or null total_amount.
-- Returns failing rows. If 0 rows returned, test passes.

SELECT
    order_id,
    total_amount
FROM {{ ref('fact_orders') }}
WHERE total_amount <= 0 OR total_amount IS NULL
