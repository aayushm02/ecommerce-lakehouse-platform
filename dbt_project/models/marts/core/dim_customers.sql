/*
  Customer Dimension - Slowly Changing Dimension (SCD Type 2)
  Maintains complete historical records of customer tier and residency movements.
*/

WITH customer_snapshots AS (
    SELECT
        customer_id,
        first_name,
        last_name,
        email,
        customer_tier,
        city,
        updated_at,
        ROW_NUMBER() OVER (
            PARTITION BY customer_id, updated_at 
            ORDER BY updated_at
        ) AS dedup_rank
    FROM {{ ref('stg_customers') }}
),

unique_snapshots AS (
    SELECT * FROM customer_snapshots WHERE dedup_rank = 1
),

scd2_windows AS (
    SELECT
        customer_id,
        first_name,
        last_name,
        email,
        customer_tier,
        city,
        updated_at AS valid_from,
        LEAD(updated_at) OVER (
            PARTITION BY customer_id 
            ORDER BY updated_at
        ) AS next_updated_at
    FROM unique_snapshots
),

final AS (
    SELECT
        MD5(CONCAT(customer_id, '_', CAST(valid_from AS TEXT))) AS customer_sk,
        customer_id,
        first_name,
        last_name,
        email,
        customer_tier,
        city,
        valid_from,
        COALESCE(next_updated_at, CAST('9999-12-31 23:59:59' AS TIMESTAMP)) AS valid_to,
        CASE 
            WHEN next_updated_at IS NULL THEN TRUE 
            ELSE FALSE 
        END AS is_current
    FROM scd2_windows
)

SELECT * FROM final
