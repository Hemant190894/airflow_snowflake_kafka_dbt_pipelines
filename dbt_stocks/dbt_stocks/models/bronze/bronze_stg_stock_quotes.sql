SELECT
    v:current_price::float AS current_price,
    v:change::float AS change_amount,
    v:percent_change::float AS change_percent,
    v:high::float AS day_high,
    v:low::float AS day_low,
    v:open::float AS day_open,
    v:previous_close_price::float AS prev_close,
    v:symbol::string AS symbol,
    v:timestamp::timestamp AS market_timestamp,
    v:fetched_at::timestamp AS fetched_at
FROM {{ source('raw', 'bronze_stock_quotes_raw') }}
