#!/usr/bin/env bash
# =============================================================================
# scripts/deploy_and_run.sh
# Purpose:
#   End-to-end 1-command deployment and execution script for AWS CloudShell
#   in AWS Academy Learner Lab (us-east-1).
#   1. Detects AWS Account ID and resolves LabRole ARN.
#   2. Creates S3 bucket nyc-taxi-pipeline-<ACCOUNT_ID> if not present.
#   3. Streams all 24 Parquet files from CloudFront to S3 via ingest_to_s3.sh.
#   4. Deploys template.yaml (Glue Catalog + Step Functions) using LabRole.
#   5. Starts a Step Functions execution and polls until SUCCEEDED or FAILED.
#   6. Automatically runs export_evidence.sh to populate evidence/ artifacts.
#
# Usage:
#   ./scripts/deploy_and_run.sh [YEAR] [MONTHS]
# =============================================================================

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "${SCRIPT_DIR}/.." && pwd)"

export AWS_REGION="${AWS_REGION:-us-east-1}"
ACCOUNT_ID="$(aws sts get-caller-identity --query Account --output text)"
BUCKET_NAME="${BUCKET_NAME:-nyc-taxi-pipeline-${ACCOUNT_ID}}"
LAB_ROLE_ARN="${LAB_ROLE_ARN:-arn:aws:iam::${ACCOUNT_ID}:role/LabRole}"
STACK_NAME="${STACK_NAME:-nyc-taxi-pipeline-stack}"
DATABASE_NAME="${DATABASE_NAME:-nyc_taxi_db}"
YEAR="${1:-${YEAR:-2024}}"
MONTHS="${2:-${MONTHS:-01 02 03 04 05 06}}"

echo "=================================================================="
echo " NYC Taxi Reproducible Pipeline - Deploy & Run"
echo " AWS Account ID : ${ACCOUNT_ID}"
echo " AWS Region     : ${AWS_REGION}"
echo " LabRole ARN    : ${LAB_ROLE_ARN}"
echo " S3 Bucket      : s3://${BUCKET_NAME}"
echo " CFN Stack Name : ${STACK_NAME}"
echo "=================================================================="

# 1. Ensure S3 bucket exists
if aws s3api head-bucket --bucket "${BUCKET_NAME}" --region "${AWS_REGION}" >/dev/null 2>&1; then
  echo "[1/5] S3 bucket s3://${BUCKET_NAME} already exists."
else
  echo "[1/5] Creating S3 bucket s3://${BUCKET_NAME} in ${AWS_REGION}..."
  aws s3 mb "s3://${BUCKET_NAME}" --region "${AWS_REGION}"
fi

# 2. Stream Parquet files from CloudFront to S3
echo "[2/5] Running CloudFront -> S3 ingestion..."
bash "${SCRIPT_DIR}/ingest_to_s3.sh" "${BUCKET_NAME}" "${YEAR}" "${MONTHS}"

# 3. Deploy CloudFormation stack using existing LabRole
echo "[3/5] Deploying CloudFormation template (${STACK_NAME})..."
aws cloudformation deploy \
  --template-file "${REPO_ROOT}/template.yaml" \
  --stack-name "${STACK_NAME}" \
  --parameter-overrides \
    BucketName="${BUCKET_NAME}" \
    LabRoleArn="${LAB_ROLE_ARN}" \
    DatabaseName="${DATABASE_NAME}" \
  --region "${AWS_REGION}" \
  --no-fail-on-empty-changeset

STATE_MACHINE_ARN="$(aws cloudformation describe-stacks \
  --stack-name "${STACK_NAME}" \
  --region "${AWS_REGION}" \
  --query "Stacks[0].Outputs[?OutputKey=='StateMachineArn'].OutputValue" \
  --output text)"

echo "      State Machine ARN: ${STATE_MACHINE_ARN}"

# 4. Start Step Functions pipeline execution
EXECUTION_NAME="nyc-taxi-h1-${YEAR}-$(date +%Y%m%d-%H%M%S)"
echo "[4/5] Starting Step Functions execution: ${EXECUTION_NAME}..."
EXECUTION_ARN="$(aws stepfunctions start-execution \
  --state-machine-arn "${STATE_MACHINE_ARN}" \
  --name "${EXECUTION_NAME}" \
  --input "{\"year\":\"${YEAR}\",\"target_window\":\"2024-01..2024-06\"}" \
  --region "${AWS_REGION}" \
  --query "executionArn" \
  --output text)"

echo "      Execution ARN: ${EXECUTION_ARN}"

# 5. Poll execution status until terminal state
echo "[5/5] Polling execution status..."
while true; do
  STATUS="$(aws stepfunctions describe-execution \
    --execution-arn "${EXECUTION_ARN}" \
    --region "${AWS_REGION}" \
    --query "status" \
    --output text)"

  echo "      [$(date +%H:%M:%S)] Execution Status: ${STATUS}"

  if [[ "${STATUS}" == "SUCCEEDED" ]]; then
    echo "=================================================================="
    echo " Pipeline Execution SUCCEEDED!"
    echo "=================================================================="
    break
  elif [[ "${STATUS}" == "FAILED" || "${STATUS}" == "TIMED_OUT" || "${STATUS}" == "ABORTED" ]]; then
    echo "=================================================================="
    echo " ERROR: Pipeline Execution ended with status: ${STATUS}"
    echo " Details:"
    aws stepfunctions describe-execution \
      --execution-arn "${EXECUTION_ARN}" \
      --region "${AWS_REGION}" \
      --output json
    exit 1
  fi

  sleep 10
done

# 6. Export grading evidence into evidence/
echo "Exporting pipeline execution evidence into evidence/ directory..."
bash "${SCRIPT_DIR}/export_evidence.sh" "${EXECUTION_ARN}" "${BUCKET_NAME}"
