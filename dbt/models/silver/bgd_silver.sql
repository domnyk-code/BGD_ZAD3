{{ config(
    materialized='incremental',
    on_schema_change='sync_all_columns'
) }}

WITH source AS (
    SELECT * FROM {{ source('bronze', 'nyc_taxi_bronze') }}
    {% if is_incremental() %}
        WHERE _loaded_at > (SELECT MAX(_loaded_at) FROM {{ this }})
    {% endif %}
),

nyc_taxi_silver AS (
    SELECT
        vendor_id::int AS vendor_id,
        tpep_pickup_datetime::timestamp AS pickup_time,
        tpep_dropoff_datetime::timestamp AS dropoff_time,
        passenger_count::int AS passenger_count,
        trip_distance::numeric AS trip_distance,
        "RatecodeID"::int AS ratecode_id,
        store_and_fwd::char AS store_and_fwd,
        "PULocationID"::int AS pickup_location,
        "DOLocationID"::int AS dropoff_location,
        payment_type::int  AS payment_type,
        fare_amount::numeric AS fare_amount,
        extra::numeric AS extra,
        mta_tax::numeric AS mta_tax,
        tip_amount::numeric  AS tip_amount,
        tolls_amount::numeric AS tolls_amount,
        total_amount::numeric AS total_amount,
        congestion_surcharge::numeric AS congestion_surcharge,
        loaded_at AS bronze_load_time,
        source_file AS source_file
    FROM source
    WHERE tpep_dropoff_datetime > tpep_pickup_datetime
)

SELECT * FROM nyc_taxi_silver