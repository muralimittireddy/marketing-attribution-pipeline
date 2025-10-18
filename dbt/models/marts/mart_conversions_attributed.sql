{{ config(
    materialized='incremental',
    unique_key='conversion_id'
) }}

WITH touchpoints_and_conversions AS (
    SELECT *
    FROM {{ ref('int_sessions') }}
    WHERE
        TIMESTAMP_DIFF(conversion_at, touchpoint_at, DAY) <= 30
        {% if is_incremental() %}
        AND conversion_at > (SELECT MAX(conversion_at) FROM {{ this }})
        {% endif %}
),


attributed AS (
    SELECT
        conversion_id,
        user_pseudo_id,
        conversion_at,
        purchase_value,

        FIRST_VALUE(channel) OVER (
            PARTITION BY conversion_id
            ORDER BY touchpoint_at ASC, channel ASC
            ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING
        ) AS first_click_channel,


        LAST_VALUE(channel IGNORE NULLS) OVER (
            PARTITION BY conversion_id
            ORDER BY touchpoint_at ASC, channel ASC
            ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING
        ) AS last_click_channel

    FROM touchpoints_and_conversions
)

SELECT DISTINCT
    conversion_id,
    user_pseudo_id,
    conversion_at,
    purchase_value,
    first_click_channel,
    last_click_channel
FROM attributed
