-- One row per pickup date x pickup borough (grain).
-- Revenue totals include reversals so refunds net out.
-- Averages use only trips that pass every quality flag.

with trips as (

    select * from {{ ref('fct_trips') }}

),

zones as (

    select * from {{ ref('dim_zones') }}

),

trips_with_borough as (

    select
        trips.*,
        zones.borough as pickup_borough
    from trips
    left join zones
        on trips.pickup_location_id = zones.location_id

)

select
    pickup_date,
    pickup_borough,

    -- volume
    count(*) filter (where not is_reversal)                         as trip_count,
    count(*) filter (where is_reversal)                             as reversal_count,

    -- revenue (net of reversals)
    round(sum(fare_amount), 2)                                      as fare_revenue,
    round(sum(tip_amount), 2)                                       as tip_revenue,
    round(sum(total_amount), 2)                                     as total_revenue,
    round(sum(total_amount) filter (where is_reversal), 2)          as reversed_amount,

    -- averages (clean trips only)
    round(avg(fare_amount)       filter (where is_valid_for_analysis), 2) as avg_fare,
    round(avg(trip_distance)     filter (where is_valid_for_analysis), 2) as avg_distance_mi,
    round(avg(trip_duration_min) filter (where is_valid_for_analysis), 1) as avg_duration_min,
    round(avg(tip_pct)           filter (where is_valid_for_analysis and payment_type = 1), 3) as avg_card_tip_pct

from trips_with_borough
group by pickup_date, pickup_borough