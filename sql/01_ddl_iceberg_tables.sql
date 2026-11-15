-- =============================================================================
-- 01_ddl_iceberg_tables.sql
-- Purpose:
--   1. Register partitioned raw external Parquet tables across all 4 NYC taxi types.
--   2. Create Apache Iceberg tables for Data Quality auditing (dq_audit_log)
--      and Curated Daily Trips (curated_daily_trips).
-- Note:
--   Replace ${BUCKET_NAME} with your S3 bucket name (e.g., nyc-taxi-pipeline-<ACCOUNT_ID>)
--   when running manually in the Athena Query Editor.
-- =============================================================================

-- Step 1: Discover and register Hive-style S3 partitions (year_month=YYYY-MM)
MSCK REPAIR TABLE nyc_taxi_db.raw_yellow;
MSCK REPAIR TABLE nyc_taxi_db.raw_green;
MSCK REPAIR TABLE nyc_taxi_db.raw_fhv;
MSCK REPAIR TABLE nyc_taxi_db.raw_fhvhv;

-- Step 2A: Create Apache Iceberg Data Quality Audit Log Table
CREATE TABLE IF NOT EXISTS nyc_taxi_db.dq_audit_log (
    taxi_type string,
    year_month string,
    raw_count bigint,
    null_pickup_count bigint,
    out_of_month_count bigint,
    bad_order_count bigint,
    valid_deduped_count bigint,
    removed_count bigint,
    removed_pct double,
    dq_status string,
    processed_at timestamp
)
PARTITIONED BY (year_month)
LOCATION 's3://${BUCKET_NAME}/dq_audit/'
TBLPROPERTIES (
    'table_type' = 'ICEBERG',
    'format' = 'parquet',
    'write_compression' = 'snappy'
);

-- Step 2B: Create Apache Iceberg Curated Daily Trips Table
CREATE TABLE IF NOT EXISTS nyc_taxi_db.curated_daily_trips (
    taxi_type string,
    year_month string,
    pickup_date date,
    valid_trip_count bigint,
    distinct_pu_zones bigint,
    distinct_do_zones bigint,
    updated_at timestamp
)
PARTITIONED BY (taxi_type, year_month)
LOCATION 's3://${BUCKET_NAME}/curated/daily_trips/'
TBLPROPERTIES (
    'table_type' = 'ICEBERG',
    'format' = 'parquet',
    'write_compression' = 'snappy'
);
