-- Revenue in the daily mart must add up to revenue in fct_trips.
-- Returns a row (= test fails) if the totals differ by more than 1 dollar.

with mart as (

    select sum(total_revenue) as revenue from {{ ref('agg_daily_zone_revenue') }}

),

fact as (

    select sum(total_amount) as revenue from {{ ref('fct_trips') }}

)

select
    mart.revenue as mart_revenue,
    fact.revenue as fact_revenue
from mart, fact
where abs(mart.revenue - fact.revenue) > 1