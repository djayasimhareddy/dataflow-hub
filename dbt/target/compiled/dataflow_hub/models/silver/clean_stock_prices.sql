

-- Silver: one row per (ticker, date). Bronze already blocks exact
-- duplicate (ticker, date) rows via ON CONFLICT DO NOTHING, but this
-- guards the case where a row gets reloaded — keeps the most
-- recently loaded row per ticker/date, nothing else.

with ranked as (

    select
        ticker,
        date,
        open,
        high,
        low,
        close,
        volume,
        row_number() over (
            partition by ticker, date
            order by loaded_at desc
        ) as rn
    from "dataflow_hub"."public"."raw_stock_prices"

)

select ticker, date, open, high, low, close, volume
from ranked
where rn = 1