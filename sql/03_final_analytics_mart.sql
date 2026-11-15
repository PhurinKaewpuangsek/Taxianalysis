-- =============================================================================
-- 03_final_analytics_mart.sql
-- Purpose:
--   Step 7: Answer the core analytical question:
--   "Which taxi type was the most used in the first half of 2024?"
--   Produces the H1 2024 monthly pivot table, total valid trips, normalized
--   daily average across the 182 days of H1 2024, and share of total trips (%).
-- =============================================================================

SELECT
    RANK() OVER (ORDER BY SUM(valid_trip_count) DESC) AS usage_rank,
    taxi_type,
    SUM(CASE WHEN year_month = '2024-01' THEN valid_trip_count ELSE 0 END) AS jan_2024,
    SUM(CASE WHEN year_month = '2024-02' THEN valid_trip_count ELSE 0 END) AS feb_2024,
    SUM(CASE WHEN year_month = '2024-03' THEN valid_trip_count ELSE 0 END) AS mar_2024,
    SUM(CASE WHEN year_month = '2024-04' THEN valid_trip_count ELSE 0 END) AS apr_2024,
    SUM(CASE WHEN year_month = '2024-05' THEN valid_trip_count ELSE 0 END) AS may_2024,
    SUM(CASE WHEN year_month = '2024-06' THEN valid_trip_count ELSE 0 END) AS jun_2024,
    SUM(valid_trip_count) AS total_h1_2024_valid_trips,
    CAST(
        ROUND(
            CAST(SUM(valid_trip_count) AS DOUBLE) / NULLIF(COUNT(DISTINCT pickup_date), 0),
            0
        ) AS BIGINT
    ) AS avg_daily_trips_h1,
    ROUND(
        CAST(SUM(valid_trip_count) AS DOUBLE) * 100.0
        / SUM(SUM(valid_trip_count)) OVER (),
        2
    ) AS share_of_total_trips_pct
FROM nyc_taxi_db.curated_daily_trips
WHERE year_month BETWEEN '2024-01' AND '2024-06'
GROUP BY taxi_type
ORDER BY total_h1_2024_valid_trips DESC;
