WITH source AS (
    SELECT * FROM {{ source('raw_stage', 'stg_raw_customers') }}
),

renamed AS (
    SELECT
        CAST(customer_id AS VARCHAR(64)) AS customer_id,
        TRIM(first_name) AS first_name,
        TRIM(last_name) AS last_name,
        LOWER(TRIM(email)) AS email,
        TRIM(tier) AS customer_tier,
        TRIM(city) AS city,
        CAST(updated_at AS TIMESTAMP) AS updated_at
    FROM source
    WHERE customer_id IS NOT NULL
)

SELECT * FROM renamed
