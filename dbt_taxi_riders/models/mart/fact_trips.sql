{{ config(
   materialized='incremental',
    unique_key='trip_id',
    incremental_strategy='merge',
    on_schema_change='append_new_columns'
) }}


SELECT
  {{ dbt.generate_surrogate_key(['vendor_id', 'taxi_type', 'pickup_datetime', 'dropoff_datetime']) }} as trip_id,
  trips.vendor_id,
  trips.taxi_type,
  trips.rate_code_id,
  trips.pickup_datetime,
  trips.dropoff_datetime,

  trips.pickup_location_id,
  pz.borough as pickup_borough,
  pz.zone as pickup_zone,
  trips.dropoff_location_id,
  dz.borough as dropoff_borough,
  dz.zone as dropoff_zone,

  trips.trip_duration,
  trips.passenger_count,
  trips.trip_distance,
  trips.payment_method,
  trips.fare_amount,
  trips.extra,
  trips.mta_tax,
  trips.tip_amount,
  trips.improvement_surcharge,
  trips.total_amount,
  trips.congestion_surcharge,
  trips.year,
  trips.month
  
FROM {{ ref('raw_nyc_trips') }} as trips
LEFT JOIN {{ ref('dim_zones') }} as pz
ON trips.pickup_location_id = pz.location_id
LEFT JOIN {{ ref('dim_zones') }} as dz
ON trips.pickup_location_id = dz.location_id
{% if is_incremental() %}
  -- Only process new trips based on pickup datetime
  where trips.pickup_datetime > (select max(pickup_datetime) from {{ this }})
{% endif %}
