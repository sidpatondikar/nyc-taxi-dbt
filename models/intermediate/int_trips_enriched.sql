-- One row per trip, enriched with zone names, derived metrics, and data-quality flags.

with trips as (

    select * from {{ ref('stg_yellow_trips') }}

),

zones as (

    select * from {{ ref('stg_taxi_zones') }}

),

-- 1. join: look up pickup and dropoff zone names
joined as (

    select
        trips.*,
        pickup_zone.borough       as pickup_borough,
        pickup_zone.zone_name     as pickup_zone,
        dropoff_zone.borough      as dropoff_borough,
        dropoff_zone.zone_name    as dropoff_zone

    from trips
    left join zones as pickup_zone
        on trips.pickup_location_id = pickup_zone.location_id
    left join zones as dropoff_zone
        on trips.dropoff_location_id = dropoff_zone.location_id

),

-- 2. derive: new columns calculated from existing ones
derived as (

    select
        *,
        regexp_extract(source_file, '(\d{4}-\d{2})\.parquet', 1)          as file_month,
        cast(regexp_extract(source_file, '(\d{4}-\d{2})\.parquet', 1) || '-01' as date) as file_month_start,
        cast(pickup_at as date)                                            as pickup_date,
        hour(pickup_at)                                                    as pickup_hour,
        isodow(pickup_at) in (6, 7)                                        as is_weekend,
        date_diff('second', pickup_at, dropoff_at) / 60.0                  as trip_duration_min,
        trip_distance
            / nullif(date_diff('second', pickup_at, dropoff_at) / 3600.0, 0) as avg_speed_mph,
        tip_amount / nullif(fare_amount, 0)                                as tip_pct

    from joined

),

-- 3. flag: marking problems instead of deleting rows
flagged as (

    select
        *,
        fare_amount < 0                                as is_reversal,
        strftime(pickup_at, '%Y-%m') = file_month      as is_in_file_month,
        trip_duration_min between 1 and 300            as has_valid_duration,
        trip_distance > 0 and trip_distance <= 200     as has_valid_distance,
        fare_amount <= 500                             as has_valid_fare_cap

    from derived

),

final as (

    select
        *,
        not is_reversal
            and is_in_file_month
            and has_valid_duration
            and has_valid_distance
            and has_valid_fare_cap                     as is_valid_for_analysis

    from flagged

)

-- 4. remove only physically impossible rows
select * from final
where dropoff_at >= pickup_at
  and pickup_at >= file_month_start - interval 1 day
  and pickup_at <  file_month_start + interval 1 month + interval 1 day