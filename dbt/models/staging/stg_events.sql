SELECT
    user_pseudo_id,
    TIMESTAMP_MICROS(event_timestamp) AS event_at,
    DATE(TIMESTAMP_MICROS(event_timestamp)) AS event_date,  
    event_name,
    MAX(IF(param.key = 'source', param.value.string_value, NULL)) AS source,
    MAX(IF(param.key = 'medium', param.value.string_value, NULL)) AS medium,
    MAX(IF(param.key = 'campaign', param.value.string_value, NULL)) AS campaign,
    MAX(IF(param.key = 'value', CAST(param.value.double_value AS FLOAT64), NULL)) AS purchase_value,
    MAX(IF(param.key = 'currency', param.value.string_value, NULL)) AS currency,
    CASE WHEN event_name = 'purchase' THEN TRUE ELSE FALSE END AS is_conversion,
    CONCAT(user_pseudo_id, '_', event_timestamp, '_', event_name) AS event_id
FROM
    `bigquery-public-data.ga4_obfuscated_sample_ecommerce.events_*`
CROSS JOIN
    UNNEST(event_params) AS param

GROUP BY
    user_pseudo_id,
    event_timestamp,
    event_name