#!/usr/bin/env bash
# =============================================================================
# scripts/ingest_to_s3.sh
# Purpose:
#   Streams NYC TLC Trip Record Parquet files directly from CloudFront to S3
#   without writing to local disk (respecting the 1 GB AWS CloudShell limit).
#   Supports idempotent execution (skips existing S3 objects unless FORCE=true)
#   and parameterized YEAR / MONTHS for incremental monthly runs.
#
# Usage:
#   ./scripts/ingest_to_s3.sh [BUCKET_NAME] [YEAR] [MONTHS]
#
# Examples:
#   # Default H1 2024 load (Months 01..06):
#   ./scripts/ingest_to_s3.sh nyc-taxi-pipeline-123456789012 2024 "01 02 03 04 05 06"
#
#   # Incremental load for July 2024:
#   ./scripts/ingest_to_s3.sh nyc-taxi-pipeline-123456789012 2024 "07"
#
#   # Force overwrite existing objects:
#   FORCE=true ./scripts/ingest_to_s3.sh nyc-taxi-pipeline-123456789012 2024 "01 02 03 04 05 06"
# =============================================================================

set -euo pipefail

AWS_REGION="${AWS_REGION:-us-east-1}"
ACCOUNT_ID="$(aws sts get-caller-identity --query Account --output text)"
BUCKET_NAME="${1:-${BUCKET_NAME:-nyc-taxi-pipeline-${ACCOUNT_ID}}}"
YEAR="${2:-${YEAR:-2024}}"
MONTHS="${3:-${MONTHS:-01 02 03 04 05 06}}"
FORCE="${FORCE:-false}"

BASE_URL="https://d37ci6vzurychx.cloudfront.net/trip-data"
TAXI_TYPES=("yellow" "green" "fhv" "fhvhv")

echo "=================================================================="
echo " NYC Taxi CloudFront -> S3 Streaming Ingestion"
echo " Target Bucket : s3://${BUCKET_NAME}"
echo " Target Year   : ${YEAR}"
echo " Target Months : ${MONTHS}"
echo " Force Reload  : ${FORCE}"
echo "=================================================================="

uploaded_count=0
skipped_count=0

for taxi_type in "${TAXI_TYPES[@]}"; do
  for month in ${MONTHS}; do
    year_month="${YEAR}-${month}"
    file_name="${taxi_type}_tripdata_${year_month}.parquet"
    source_url="${BASE_URL}/${file_name}"
    s3_key="raw/${taxi_type}/year_month=${year_month}/${file_name}"
    s3_uri="s3://${BUCKET_NAME}/${s3_key}"

    if [[ "${FORCE}" != "true" ]] && aws s3api head-object --bucket "${BUCKET_NAME}" --key "${s3_key}" --region "${AWS_REGION}" >/dev/null 2>&1; then
      echo "[SKIP] Exists in S3: ${s3_uri}"
      skipped_count=$((skipped_count + 1))
      continue
    fi

    echo "[STREAM] ${source_url} -> ${s3_uri}"
    curl --retry 3 --retry-delay 2 -sSL --fail "${source_url}" \
      | aws s3 cp - "${s3_uri}" --region "${AWS_REGION}"
    uploaded_count=$((uploaded_count + 1))
  done
done

echo "=================================================================="
echo " Ingestion Complete: ${uploaded_count} streamed, ${skipped_count} skipped."
echo "=================================================================="
