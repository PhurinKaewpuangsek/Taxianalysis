#!/usr/bin/env bash
# =============================================================================
# scripts/export_evidence.sh
# Purpose:
#   Exports all required Submission 2 grading evidence into the evidence/ folder:
#     1. evidence/stepfunctions_execution.json
#     2. evidence/dq_audit_results.csv (full 24-row audit log)
#     3. evidence/curated_sample_data.csv (sample of curated_daily_trips)
#     4. evidence/final_most_used_2024h1.csv (final H1 2024 ranking & monthly pivot)
#
# Usage:
#   ./scripts/export_evidence.sh [EXECUTION_ARN] [BUCKET_NAME]
# =============================================================================

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "${SCRIPT_DIR}/.." && pwd)"
EVIDENCE_DIR="${REPO_ROOT}/evidence"
mkdir -p "${EVIDENCE_DIR}"

export AWS_REGION="${AWS_REGION:-us-east-1}"
ACCOUNT_ID="$(aws sts get-caller-identity --query Account --output text)"
EXECUTION_ARN="${1:-}"
BUCKET_NAME="${2:-${BUCKET_NAME:-nyc-taxi-pipeline-${ACCOUNT_ID}}}"
DATABASE_NAME="${DATABASE_NAME:-nyc_taxi_db}"
STACK_NAME="${STACK_NAME:-nyc-taxi-pipeline-stack}"

# Auto-discover latest execution ARN if not explicitly provided
if [[ -z "${EXECUTION_ARN}" ]]; then
  STATE_MACHINE_ARN="$(aws cloudformation describe-stacks \
    --stack-name "${STACK_NAME}" \
    --region "${AWS_REGION}" \
    --query "Stacks[0].Outputs[?OutputKey=='StateMachineArn'].OutputValue" \
    --output text)"

  EXECUTION_ARN="$(aws stepfunctions list-executions \
    --state-machine-arn "${STATE_MACHINE_ARN}" \
    --max-results 1 \
    --region "${AWS_REGION}" \
    --query "executions[0].executionArn" \
    --output text)"
fi

echo "=================================================================="
echo " Exporting Submission 2 Evidence Artifacts"
echo " Execution ARN : ${EXECUTION_ARN}"
echo " S3 Bucket     : s3://${BUCKET_NAME}"
echo " Output Folder : ${EVIDENCE_DIR}"
echo "=================================================================="

# Helper function to run an Athena query and download its CSV result
run_athena_to_csv() {
  local query_sql="$1"
  local local_dest="$2"
  local s3_Output_prefix="s3://${BUCKET_NAME}/athena-results/evidence-export/"

  local qid
  qid="$(aws athena start-query-execution \
    --query-string "${query_sql}" \
    --query-execution-context "Database=${DATABASE_NAME}" \
    --result-configuration "OutputLocation=${s3_Output_prefix}" \
    --region "${AWS_REGION}" \
    --query "QueryExecutionId" \
    --output text)"

  while true; do
    local qstate
    qstate="$(aws athena get-query-execution \
      --query-execution-id "${qid}" \
      --region "${AWS_REGION}" \
      --query "QueryExecution.Status.State" \
      --output text)"

    if [[ "${qstate}" == "SUCCEEDED" ]]; then
      break
    elif [[ "${qstate}" == "FAILED" || "${qstate}" == "CANCELLED" ]]; then
      echo "ERROR: Athena query ${qid} ended with state ${qstate}" >&2
      aws athena get-query-execution --query-execution-id "${qid}" --region "${AWS_REGION}" >&2
      exit 1
    fi
    sleep 2
  done

  aws s3 cp "${s3_Output_prefix}${qid}.csv" "${local_dest}" --region "${AWS_REGION}" >/dev/null
}

# 1. Export Step Functions execution metadata
echo "[1/4] Exporting evidence/stepfunctions_execution.json..."
aws stepfunctions describe-execution \
  --execution-arn "${EXECUTION_ARN}" \
  --region "${AWS_REGION}" \
  --output json > "${EVIDENCE_DIR}/stepfunctions_execution.json"

# 2. Export full 24-row Data Quality Audit Log
echo "[2/4] Exporting evidence/dq_audit_results.csv..."
run_athena_to_csv \
  "SELECT taxi_type, year_month, raw_count, null_pickup_count, out_of_month_count, bad_order_count, valid_deduped_count, removed_count, removed_pct, dq_status, processed_at FROM ${DATABASE_NAME}.dq_audit_log WHERE year_month BETWEEN '2024-01' AND '2024-06' ORDER BY taxi_type, year_month" \
  "${EVIDENCE_DIR}/dq_audit_results.csv"

# 3. Export sample rows from Curated Daily Trips (728 total rows, exporting top 120 sample rows across months/types)
echo "[3/4] Exporting evidence/curated_sample_data.csv..."
run_athena_to_csv \
  "SELECT taxi_type, year_month, pickup_date, valid_trip_count, distinct_pu_zones, distinct_do_zones, updated_at FROM ${DATABASE_NAME}.curated_daily_trips WHERE year_month BETWEEN '2024-01' AND '2024-06' ORDER BY pickup_date ASC, valid_trip_count DESC LIMIT 120" \
  "${EVIDENCE_DIR}/curated_sample_data.csv"

# 4. Export Final H1 2024 Analytical Answer Table
echo "[4/4] Exporting evidence/final_most_used_2024h1.csv..."
run_athena_to_csv \
  "SELECT RANK() OVER (ORDER BY SUM(valid_trip_count) DESC) AS usage_rank, taxi_type, SUM(CASE WHEN year_month = '2024-01' THEN valid_trip_count ELSE 0 END) AS jan_2024, SUM(CASE WHEN year_month = '2024-02' THEN valid_trip_count ELSE 0 END) AS feb_2024, SUM(CASE WHEN year_month = '2024-03' THEN valid_trip_count ELSE 0 END) AS mar_2024, SUM(CASE WHEN year_month = '2024-04' THEN valid_trip_count ELSE 0 END) AS apr_2024, SUM(CASE WHEN year_month = '2024-05' THEN valid_trip_count ELSE 0 END) AS may_2024, SUM(CASE WHEN year_month = '2024-06' THEN valid_trip_count ELSE 0 END) AS jun_2024, SUM(valid_trip_count) AS total_h1_2024_valid_trips, CAST(ROUND(CAST(SUM(valid_trip_count) AS DOUBLE) / NULLIF(COUNT(DISTINCT pickup_date), 0), 0) AS BIGINT) AS avg_daily_trips_h1, ROUND(CAST(SUM(valid_trip_count) AS DOUBLE) * 100.0 / SUM(SUM(valid_trip_count)) OVER (), 2) AS share_of_total_trips_pct FROM ${DATABASE_NAME}.curated_daily_trips WHERE year_month BETWEEN '2024-01' AND '2024-06' GROUP BY taxi_type ORDER BY total_h1_2024_valid_trips DESC" \
  "${EVIDENCE_DIR}/final_most_used_2024h1.csv"

echo "=================================================================="
echo " Evidence Export Complete!"
echo "------------------------------------------------------------------"
echo " Final Analytical Result (evidence/final_most_used_2024h1.csv):"
cat "${EVIDENCE_DIR}/final_most_used_2024h1.csv"
echo "=================================================================="
