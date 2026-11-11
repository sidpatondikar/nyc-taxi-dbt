-- One row per trip (grain). Holds IDs + measures + quality flags.
-- Incremental: each run only loads TLC file months not already in the table.

{{
    config(
        materialized='incremental',
        unique_key='trip_id',
        incremental_strategy='delete+insert'
    )
}}

with trips as (

    select * from {{ ref('int_trips_enriched') }}

    {% if is_incremental() %}
    -- only months newer than the latest month already loaded
    where file_month > (select max(file_month) from {{ this }})
    {% endif %}

),

keyed as (

    select
        {{ dbt_utils.generate_surrogate_key([
            'vendor_id', 'pickup_at', 'dropoff_at',
            'pickup_location_id', 'dropoff_location_id',
            'payment_type', 'trip_distance', 'fare_amount', 'total_amount'
        ]) }}                                   as trip_id,
        *
    from trips

),

deduplicated as (

    select * from keyed
    qualify row_number() over (partition by trip_id order by source_file) = 1

)

select
    trip_id,

    -- foreign keys / codes
    vendor_id,
    payment_type,
    rate_code_id,
    pickup_location_id,
    dropoff_location_id,

    -- time
    pickup_at,
    dropoff_at,
    pickup_date,
    pickup_hour,
    is_weekend,
    file_month,

    -- measures
    passenger_count,
    trip_distance,
    trip_duration_min,
    avg_speed_mph,
    fare_amount,
    extra,
    mta_tax,
    tip_amount,
    tolls_amount,
    improvement_surcharge,
    congestion_surcharge,
    airport_fee,
    total_amount,
    tip_pct,

    -- quality flags
    is_reversal,
    is_in_file_month,
    has_valid_duration,
    has_valid_distance,
    has_valid_fare_cap,
    is_valid_for_analysis

from deduplicated