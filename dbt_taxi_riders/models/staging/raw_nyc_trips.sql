{{ config(Materialized='view') }}

WITH source AS (
  select * 
from {{ source('nyc', 'raw_nyc_trips') }}
), renamed AS (
  SELECT
    cast(VendorID AS integer) as vendor_id,
    {{ get_vendo_name('VendorID') }} as vendor_name,
    cast(ratecodeid as integer) as rate_code_id,
    cast(pulocationid as integer) as pickup_location_id,
    cast(dolocationid as integer) as dropoff_location_id,
    cast(pickup_datetime as timestamp) as pickup_datetime,
    cast(dropoff_datetime as timestamp) as dropoff_datetime,
    {{ get_trip_minutes('pickup_datetime', 'dropoff_datetime') }} as trip_duration,
    cast(passenger_count as integer) as passenger_count,
    cast(trip_distance as double) as trip_distance,
    store_and_fwd_flag,
    payment_type,
    {{ get_credit_type('payment_type') }} as payment_method,
    cast(fare_amount as double) as fare_amount,
    cast(extra as double) as extra,
    cast(mta_tax as double) as mta_tax,
    cast(tip_amount as double) as tip_amount,
    cast(improvement_surcharge as double) as improvement_surcharge,
    cast(total_amount as double) as total_amount,
    cast(congestion_surcharge as double) as congestion_surcharge,
    trip_type,
    taxi_type,
    year,
    month

  FROM source
),
deduped AS (
    SELECT
        *,
        row_number() OVER (
            PARTITION BY vendor_id, pickup_datetime, pickup_location_id, taxi_type
            ORDER BY dropoff_datetime
        ) AS rn
    FROM renamed
)

SELECT *
FROM deduped
WHERE rn = 1;