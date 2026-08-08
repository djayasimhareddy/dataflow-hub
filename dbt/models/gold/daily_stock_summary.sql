{{ config(materialized='table') }}

-- Gold: daily return % and 7-day moving average close, per ticker.

with prices as (
    select * from {{ ref('clean_stock_prices') }}
),

with_lag as (
    select
        *,
        lag(close) over (partition by ticker order by date) as prev_close
    from prices
)

select
    ticker,
    date,
    close,
    round(((close - prev_close) / nullif(prev_close, 0)) * 100, 2) as daily_return_pct,
    round(
        avg(close) over (
            partition by ticker
            order by date
            rows between 6 preceding and current row
        ), 2
    ) as moving_avg_7d
from with_lag
order by ticker, date
