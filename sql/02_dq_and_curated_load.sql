-- =============================================================================
-- 02_dq_and_curated_load.sql
-- Purpose:
--   1. Idempotent DELETE of existing target window (2024-01 to 2024-06) in both
--      Apache Iceberg tables so pipeline re-runs produce zero duplicates.
--   2. Harmonize 4 heterogeneous raw schemas (yellow, green, fhv, fhvhv),
--      apply the 3 cleaning rules, and log batch-level DQ metrics into dq_audit_log.
--   3. Evaluate the Data Quality Gate (all 24 batches present and 0 failed batches).
--   4. Populate the curated_daily_trips Iceberg table (182 days x 4 types = 728 rows).
-- =============================================================================

-- -----------------------------------------------------------------------------
-- Step 3: Idempotent Target Window Cleanup (ACID DELETE on Apache Iceberg)
-- -----------------------------------------------------------------------------
DELETE FROM nyc_taxi_db.dq_audit_log
WHERE year_month BETWEEN '2024-01' AND '2024-06';

DELETE FROM nyc_taxi_db.curated_daily_trips
WHERE year_month BETWEEN '2024-01' AND '2024-06';

-- -----------------------------------------------------------------------------
-- Step 4: Harmonize Raw Schemas, Apply 3 Cleaning Rules & Log DQ Metrics
--   Rule 1: pickup_dt IS NOT NULL
--   Rule 2: pickup_dt >= month_start AND pickup_dt < next_month_start
--   Rule 3: Deduplicate on 5 keys (provider_id, pickup_dt, dropoff_dt, pu_id, do_id)
--           within each (taxi_type, year_month) partition.
-- -----------------------------------------------------------------------------
INSERT INTO nyc_taxi_db.dq_audit_log
WITH harmonized AS (
    SELECT
        'yellow' AS taxi_type,
        year_month,
        CAST(vendorid AS VARCHAR) AS provider_id,
        tpep_pickup_datetime AS pickup_dt,
        tpep_dropoff_datetime AS dropoff_dt,
        CAST(pulocationid AS BIGINT) AS pu_id,
        CAST(dolocationid AS BIGINT) AS do_id
    FROM nyc_taxi_db.raw_yellow
    WHERE year_month BETWEEN '2024-01' AND '2024-06'

    UNION ALL

    SELECT
        'green' AS taxi_type,
        year_month,
        CAST(vendorid AS VARCHAR) AS provider_id,
        lpep_pickup_datetime AS pickup_dt,
        lpep_dropoff_datetime AS dropoff_dt,
        CAST(pulocationid AS BIGINT) AS pu_id,
        CAST(dolocationid AS BIGINT) AS do_id
    FROM nyc_taxi_db.raw_green
    WHERE year_month BETWEEN '2024-01' AND '2024-06'

    UNION ALL

    SELECT
        'fhv' AS taxi_type,
        year_month,
        CAST(dispatching_base_num AS VARCHAR) AS provider_id,
        pickup_datetime AS pickup_dt,
        dropoff_datetime AS dropoff_dt,
        CAST(pulocationid AS BIGINT) AS pu_id,
        CAST(dolocationid AS BIGINT) AS do_id
    FROM nyc_taxi_db.raw_fhv
    WHERE year_month BETWEEN '2024-01' AND '2024-06'

    UNION ALL

    SELECT
        'fhvhv' AS taxi_type,
        year_month,
        CAST(hvfhs_license_num AS VARCHAR) AS provider_id,
        pickup_datetime AS pickup_dt,
        dropoff_datetime AS dropoff_dt,
        CAST(pulocationid AS BIGINT) AS pu_id,
        CAST(dolocationid AS BIGINT) AS do_id
    FROM nyc_taxi_db.raw_fhvhv
    WHERE year_month BETWEEN '2024-01' AND '2024-06'
),
with_bounds AS (
    SELECT
        taxi_type,
        year_month,
        provider_id,
        pickup_dt,
        dropoff_dt,
        pu_id,
        do_id,
        CAST(date_parse(year_month || '-01', '%Y-%m-%d') AS TIMESTAMP) AS month_start,
        CAST(date_add('month', 1, date_parse(year_month || '-01', '%Y-%m-%d')) AS TIMESTAMP) AS next_month_start
    FROM harmonized
),
raw_metrics AS (
    SELECT
        taxi_type,
        year_month,
        COUNT(*) AS raw_count,
        COUNT_IF(pickup_dt IS NULL) AS null_pickup_count,
        COUNT_IF(pickup_dt IS NOT NULL AND (pickup_dt < month_start OR pickup_dt >= next_month_start)) AS out_of_month_count,
        COUNT_IF(pickup_dt IS NOT NULL AND dropoff_dt IS NOT NULL AND dropoff_dt < pickup_dt) AS bad_order_count
    FROM with_bounds
    GROUP BY taxi_type, year_month
),
deduped_valid AS (
    SELECT
        taxi_type,
        year_month,
        provider_id,
        pickup_dt,
        dropoff_dt,
        pu_id,
        do_id
    FROM with_bounds
    WHERE pickup_dt IS NOT NULL
      AND pickup_dt >= month_start
      AND pickup_dt < next_month_start
    GROUP BY
        taxi_type,
        year_month,
        provider_id,
        pickup_dt,
        dropoff_dt,
        pu_id,
        do_id
),
valid_metrics AS (
    SELECT
        taxi_type,
        year_month,
        COUNT(*) AS valid_deduped_count
    FROM deduped_valid
    GROUP BY taxi_type, year_month
)
SELECT
    r.taxi_type,
    r.year_month,
    r.raw_count,
    r.null_pickup_count,
    r.out_of_month_count,
    r.bad_order_count,
    COALESCE(v.valid_deduped_count, 0) AS valid_deduped_count,
    (r.raw_count - COALESCE(v.valid_deduped_count, 0)) AS removed_count,
    ROUND(
        CAST(r.raw_count - COALESCE(v.valid_deduped_count, 0) AS DOUBLE) * 100.0
        / NULLIF(CAST(r.raw_count AS DOUBLE), 0),
        4
    ) AS removed_pct,
    CASE
        WHEN COALESCE(v.valid_deduped_count, 0) > 0
         AND (
             CAST(r.raw_count - COALESCE(v.valid_deduped_count, 0) AS DOUBLE) * 100.0
             / NULLIF(CAST(r.raw_count AS DOUBLE), 0)
         ) < 5.0
        THEN 'PASS'
        ELSE 'FAIL'
    END AS dq_status,
    CAST(current_timestamp AS TIMESTAMP) AS processed_at
FROM raw_metrics r
LEFT JOIN valid_metrics v
    ON r.taxi_type = v.taxi_type
   AND r.year_month = v.year_month;

-- -----------------------------------------------------------------------------
-- Step 5: Data Quality Gate Query (Evaluated by Step Functions Choice State)
--   Expects: total_batches = 24 AND failed_batches = 0
-- -----------------------------------------------------------------------------
SELECT
    COUNT(*) AS total_batches,
    COUNT_IF(dq_status <> 'PASS') AS failed_batches
FROM nyc_taxi_db.dq_audit_log
WHERE year_month BETWEEN '2024-01' AND '2024-06';

-- -----------------------------------------------------------------------------
-- Step 6: Populate Curated Daily Trips Table (728 rows = 182 days x 4 types)
-- -----------------------------------------------------------------------------
INSERT INTO nyc_taxi_db.curated_daily_trips
WITH harmonized AS (
    SELECT
        'yellow' AS taxi_type,
        year_month,
        CAST(vendorid AS VARCHAR) AS provider_id,
        tpep_pickup_datetime AS pickup_dt,
        tpep_dropoff_datetime AS dropoff_dt,
        CAST(pulocationid AS BIGINT) AS pu_id,
        CAST(dolocationid AS BIGINT) AS do_id
    FROM nyc_taxi_db.raw_yellow
    WHERE year_month BETWEEN '2024-01' AND '2024-06'

    UNION ALL

    SELECT
        'green' AS taxi_type,
        year_month,
        CAST(vendorid AS VARCHAR) AS provider_id,
        lpep_pickup_datetime AS pickup_dt,
        lpep_dropoff_datetime AS dropoff_dt,
        CAST(pulocationid AS BIGINT) AS pu_id,
        CAST(dolocationid AS BIGINT) AS do_id
    FROM nyc_taxi_db.raw_green
    WHERE year_month BETWEEN '2024-01' AND '2024-06'

    UNION ALL

    SELECT
        'fhv' AS taxi_type,
        year_month,
        CAST(dispatching_base_num AS VARCHAR) AS provider_id,
        pickup_datetime AS pickup_dt,
        dropoff_datetime AS dropoff_dt,
        CAST(pulocationid AS BIGINT) AS pu_id,
        CAST(dolocationid AS BIGINT) AS do_id
    FROM nyc_taxi_db.raw_fhv
    WHERE year_month BETWEEN '2024-01' AND '2024-06'

    UNION ALL

    SELECT
        'fhvhv' AS taxi_type,
        year_month,
        CAST(hvfhs_license_num AS VARCHAR) AS provider_id,
        pickup_datetime AS pickup_dt,
        dropoff_datetime AS dropoff_dt,
        CAST(pulocationid AS BIGINT) AS pu_id,
        CAST(dolocationid AS BIGINT) AS do_id
    FROM nyc_taxi_db.raw_fhvhv
    WHERE year_month BETWEEN '2024-01' AND '2024-06'
),
deduped_valid AS (
    SELECT
        taxi_type,
        year_month,
        provider_id,
        pickup_dt,
        dropoff_dt,
        pu_id,
        do_id
    FROM harmonized
    WHERE pickup_dt IS NOT NULL
      AND pickup_dt >= CAST(date_parse(year_month || '-01', '%Y-%m-%d') AS TIMESTAMP)
      AND pickup_dt < CAST(date_add('month', 1, date_parse(year_month || '-01', '%Y-%m-%d')) AS TIMESTAMP)
    GROUP BY
        taxi_type,
        year_month,
        provider_id,
        pickup_dt,
        dropoff_dt,
        pu_id,
        do_id
)
SELECT
    taxi_type,
    year_month,
    CAST(pickup_dt AS DATE) AS pickup_date,
    COUNT(*) AS valid_trip_count,
    COUNT(DISTINCT pu_id) AS distinct_pu_zones,
    COUNT(DISTINCT do_id) AS distinct_do_zones,
    CAST(current_timestamp AS TIMESTAMP) AS updated_at
FROM deduped_valid
GROUP BY
    taxi_type,
    year_month,
    CAST(pickup_dt AS DATE);
