WITH source AS (
    SELECT *
    FROM {{ ref('stg_events') }}
),

--  compute previous event timestamp per user
lagged AS (
    SELECT
        *,
        LAG(event_at) OVER (PARTITION BY user_pseudo_id ORDER BY event_at) AS prev_event_at
    FROM source
),

--  compute session_number based on inactivity gap
sessions AS (
    SELECT
        *,
        SUM(
            CASE 
                WHEN prev_event_at IS NULL THEN 1
                WHEN TIMESTAMP_DIFF(event_at, prev_event_at, MINUTE) > 30 THEN 1
                ELSE 0
            END
        ) OVER (PARTITION BY user_pseudo_id ORDER BY event_at) AS session_number
    FROM lagged
),

touchpoints AS (
    SELECT DISTINCT
        user_pseudo_id,
        session_number,
        event_at,
        CONCAT(source, ' / ', medium) AS channel
    FROM sessions
    WHERE source IS NOT NULL
),

conversions AS (
    SELECT
        event_id AS conversion_id,
        user_pseudo_id,
        event_at AS conversion_at,
        purchase_value
    FROM sessions
    WHERE event_name = 'purchase'
      AND purchase_value IS NOT NULL
)

SELECT
    tp.user_pseudo_id,
    tp.session_number AS session_id,
    tp.channel,
    tp.event_at AS touchpoint_at,
    conv.conversion_at,
    conv.purchase_value,
    conv.conversion_id
FROM touchpoints tp
JOIN conversions conv
    ON tp.user_pseudo_id = conv.user_pseudo_id
   AND tp.event_at <= conv.conversion_at
