SELECT
    pickup_at::date AS trip_date,
    COUNT(*) AS total_trips,
    SUM(passenger_count) AS total_passengers,
    ROUND(AVG(trip_distance_miles), 2) AS avg_distance_miles,
    ROUND(AVG(trip_duration_minutes), 2) AS avg_duration_minutes,
    ROUND(SUM(total_amount), 2) AS total_revenue,
    ROUND(AVG(total_amount), 2) AS avg_fare
FROM {{ ref('bgd_silver') }}
GROUP BY trip_date
ORDER BY trip_date