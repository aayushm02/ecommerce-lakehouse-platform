/*
  Daily Performance Metrics Mart
  Demonstrating Advanced Analytical SQL:
  - CTE modularization
  - Window functions (rolling 7-day windows, cumulative sums, lag)
  - Cohort / AOV / Repeat buyer calculations
*/

WITH daily_orders AS (
    SELECT
        order_date,
        COUNT(DISTINCT order_id) AS total_orders,
        COUNT(DISTINCT customer_id) AS active_customers,
        SUM(total_amount) AS daily_gmv,
        AVG(total_amount) AS average_order_value,
        SUM(CASE WHEN is_high_value_order THEN 1 ELSE 0 END) AS high_value_orders_count,
        SUM(CASE WHEN status = 'COMPLETED' THEN total_amount ELSE 0 END) AS completed_revenue
    FROM {{ ref('fact_orders') }}
    GROUP BY order_date
),

metrics_with_windows AS (
    SELECT
        order_date,
        total_orders,
        active_customers,
        daily_gmv,
        ROUND(average_order_value, 2) AS average_order_value,
        high_value_orders_count,
        completed_revenue,
        -- Rolling 7-day window revenue
        ROUND(
            SUM(daily_gmv) OVER (
                ORDER BY order_date 
                ROWS BETWEEN 6 PRECEDING AND CURRENT ROW
            ), 2
        ) AS rolling_7d_gmv,
        -- Cumulative all-time revenue up to this date
        ROUND(
            SUM(daily_gmv) OVER (
                ORDER BY order_date 
                ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
            ), 2
        ) AS cumulative_gmv,
        -- Previous day GMV comparison
        LAG(daily_gmv, 1) OVER (
            ORDER BY order_date
        ) AS prev_day_gmv,
        -- Day over day GMV growth rate (%)
        ROUND(
            (daily_gmv - LAG(daily_gmv, 1) OVER (ORDER BY order_date))
            / NULLIF(LAG(daily_gmv, 1) OVER (ORDER BY order_date), 0) * 100, 2
        ) AS dod_growth_rate_pct
    FROM daily_orders
)

SELECT * FROM metrics_with_windows
ORDER BY order_date DESC
