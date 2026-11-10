-- Staging model for the TLC taxi zone lookup.
-- One row per zone
with source as (

    select * from {{ source('raw', 'taxi_zones') }}

),

renamed as (

    select
        cast(LocationID as integer)  as location_id,
        Borough                      as borough,
        Zone                         as zone_name,
        service_zone

    from source

)

select * from renamed
