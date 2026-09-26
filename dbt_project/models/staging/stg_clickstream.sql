WITH source AS (
    SELECT * FROM {{ source('raw_stage', 'stg_raw_clickstream') }}
),

renamed AS (
    SELECT
        CAST(event_id AS VARCHAR(64)) AS event_id,
        CAST(session_id AS VARCHAR(64)) AS session_id,
        CAST(customer_id AS VARCHAR(64)) AS customer_id,
        CAST(url AS VARCHAR(255)) AS page_url,
        CAST(device AS VARCHAR(50)) AS device_type,
        CAST(referral_source AS VARCHAR(50)) AS referral_source,
        CAST(page_type AS VARCHAR(50)) AS page_type,
        CAST(event_timestamp AS TIMESTAMP) AS event_timestamp,
        CAST(event_date AS DATE) AS event_date
    FROM source
    WHERE event_id IS NOT NULL
)

SELECT * FROM renamed
