WITH distinct_products AS (
    SELECT DISTINCT
        product_id,
        product_name,
        product_category,
        unit_price AS current_unit_price
    FROM {{ ref('stg_orders') }}
)

SELECT
    MD5(product_id) AS product_sk,
    product_id,
    product_name,
    product_category,
    current_unit_price
FROM distinct_products
