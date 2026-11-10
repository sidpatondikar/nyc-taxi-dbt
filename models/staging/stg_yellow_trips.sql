-- Staging model for yellow taxi trips.
-- One row per raw row

with source as (

    select * from {{ source('raw', 'yellow_trips') }}

),

renamed as (

    select
        -- ids
        cast(VendorID as integer)              as vendor_id,
        cast(RatecodeID as integer)            as rate_code_id,
        cast(PULocationID as integer)          as pickup_location_id,
        cast(DOLocationID as integer)          as dropoff_location_id,
        cast(payment_type as integer)          as payment_type,

        -- timestamps
        tpep_pickup_datetime                   as pickup_at,
        tpep_dropoff_datetime                  as dropoff_at,

        -- trip info
        cast(passenger_count as integer)       as passenger_count,
        trip_distance,
        store_and_fwd_flag,

        -- amounts
        fare_amount,
        extra,
        mta_tax,
        tip_amount,
        tolls_amount,
        improvement_surcharge,
        congestion_surcharge,
        Airport_fee                            as airport_fee,
        total_amount,

        -- lineage: which monthly file this row came from
        filename                               as source_file

    from source

)

select * from renamed
