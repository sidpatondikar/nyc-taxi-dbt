-- One row per TLC taxi zone.

with zones as (

    select * from {{ ref('stg_taxi_zones') }}

)

select
    location_id,
    zone_name,
    borough,
    service_zone,
    zone_name ilike '%airport%'     as is_airport,
    location_id in (264, 265)       as is_unknown_location

from zones