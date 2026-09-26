/*
  Fact Orders - Grain: One record per order.
  Connects directly to dim_customers using point-in-time SCD Type 2 dimension matching.
*/

WITH orders AS (
    SELECT * FROM {{ ref('stg_orders') }}
),

dim_customers AS (
    SELECT * FROM {{ ref('dim_customers') }}
),

final AS (
    SELECT
        o.order_id,
        o.customer_id,
        COALESCE(c.customer_sk, MD5('UNKNOWN')) AS customer_sk,
        o.product_id,
        MD5(o.product_id) AS product_sk,
        o.quantity,
        o.unit_price,
        o.total_amount,
        o.status,
        o.is_high_value_order,
        o.order_timestamp,
        o.order_date
    FROM orders o
    LEFT JOIN dim_customers c
        ON o.customer_id = c.customer_id
        AND o.order_timestamp >= c.valid_from
        AND o.order_timestamp < c.valid_to
)

SELECT * FROM final
