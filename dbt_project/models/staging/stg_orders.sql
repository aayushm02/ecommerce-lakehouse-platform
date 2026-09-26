WITH source AS (
    SELECT * FROM {{ source('raw_stage', 'stg_raw_orders') }}
),

renamed AS (
    SELECT
        CAST(order_id AS VARCHAR(64)) AS order_id,
        CAST(customer_id AS VARCHAR(64)) AS customer_id,
        CAST(product_id AS VARCHAR(64)) AS product_id,
        CAST(product_name AS VARCHAR(255)) AS product_name,
        CAST(product_category AS VARCHAR(100)) AS product_category,
        CAST(quantity AS INTEGER) AS quantity,
        CAST(unit_price AS NUMERIC(10, 2)) AS unit_price,
        CAST(total_amount AS NUMERIC(10, 2)) AS total_amount,
        UPPER(TRIM(status)) AS status,
        CAST(is_high_value_order AS BOOLEAN) AS is_high_value_order,
        CAST(order_timestamp AS TIMESTAMP) AS order_timestamp,
        CAST(order_date AS DATE) AS order_date
    FROM source
    WHERE order_id IS NOT NULL
)

SELECT * FROM renamed
