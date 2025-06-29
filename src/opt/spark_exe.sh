#!/bin/bash
#
# IgniteFlow Spark Job Executor
#
# Modern execution wrapper for Spark jobs in the IgniteFlow framework
# Usage: spark_exe.sh --job JOB_NAME --env ENVIRONMENT [OPTIONS]
#

set -euo pipefail  # Exit on error, undefined variables, and pipe failures

# Script configuration
readonly SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
readonly PROJECT_ROOT="$(cd "${SCRIPT_DIR}/../.." && pwd)"
readonly DEFAULT_CONFIG_PATH="${PROJECT_ROOT}/src/config"

# Default values
JOB_NAME=""
ENVIRONMENT="local"
CONFIG_PATH="${DEFAULT_CONFIG_PATH}"
SPARK_MASTER="local[*]"
VERBOSE=false
EXTRA_ARGS=""

# Logging functions
log_info() {
    echo "[$(date +'%Y-%m-%d %H:%M:%S')] INFO: $*" >&2
}

log_error() {
    echo "[$(date +'%Y-%m-%d %H:%M:%S')] ERROR: $*" >&2
}

log_debug() {
    if [[ "${VERBOSE}" == "true" ]]; then
        echo "[$(date +'%Y-%m-%d %H:%M:%S')] DEBUG: $*" >&2
    fi
}

# Usage information
usage() {
    cat << EOF
Usage: $0 --job JOB_NAME --env ENVIRONMENT [OPTIONS] [-- [EXTRA_ARGS]]

Required Arguments:
  --job JOB_NAME        Name of the job/pipeline to execute
  --env ENVIRONMENT     Target environment (local, dev, staging, prod)

Optional Arguments:
  --config CONFIG_PATH  Path to configuration files directory (default: src/config)
  --master SPARK_MASTER Spark master URL (default: local[*])
  --verbose             Enable verbose logging
  --help                Show this help message

Extra Arguments:
  Any arguments after '--' will be passed directly to the main application.

Examples:
  $0 --job wordcount --env local
  $0 --job data_pipeline --env dev -- --custom_arg value

EOF
}

# Parse command line arguments
parse_args() {
    while [[ $# -gt 0 ]]; do
        case $1 in
            --job)
                JOB_NAME="$2"
                shift 2
                ;;
            --env)
                ENVIRONMENT="$2"
                shift 2
                ;;
            --config)
                CONFIG_PATH="$2"
                shift 2
                ;;
            --master)
                SPARK_MASTER="$2"
                shift 2
                ;;
            --verbose)
                VERBOSE=true
                shift
                ;;
            --help)
                usage
                exit 0
                ;;
            --)
                shift
                EXTRA_ARGS="$@"
                break
                ;;
            *)
                log_error "Unknown option: $1"
                usage
                exit 1
                ;;
        esac
    done

    # Validate required arguments
    if [[ -z "${JOB_NAME}" ]]; then
        log_error "Job name is required"
        usage
        exit 1
    fi
}

# ... (rest of the script is the same)

# Build Spark submit command with proper configurations
build_spark_command() {
    local spark_cmd=(
        "${SPARK_HOME:-/opt/spark}/bin/spark-submit"
        "--master" "${SPARK_MASTER}"
        "--name" "IgniteFlow-${JOB_NAME}-${ENVIRONMENT}"
    )

    # Cloud-specific configurations
    case "${CLOUD_PROVIDER}" in
        aws)
            spark_cmd+=(
                "--conf" "spark.hadoop.fs.s3a.impl=org.apache.hadoop.fs.s3a.S3AFileSystem"
                "--packages" "org.apache.hadoop:hadoop-aws:3.3.4"
            )
            ;;
        gcp)
            spark_cmd+=(
                "--conf" "spark.hadoop.fs.gs.impl=com.google.cloud.hadoop.fs.gcs.GoogleHadoopFileSystem"
                "--packages" "com.google.cloud.spark:spark-bigquery-with-dependencies_2.12:0.23.2,com.google.cloud.bigdataoss:gcs-connector:hadoop3-2.2.5"
            )
            ;;
        azure)
            spark_cmd+=(
                "--conf" "spark.hadoop.fs.azure.account.key.<storage_account_name>.dfs.core.windows.net=<storage_account_key>"
                "--packages" "org.apache.hadoop:hadoop-azure:3.3.4,com.microsoft.azure:azure-storage:8.6.6"
            )
            ;;
    esac

    # Kubernetes-specific configurations
    if [[ "${SPARK_MASTER}" == k8s* ]]; then
        spark_cmd+=(
            "--conf" "spark.kubernetes.container.image=${SPARK_KUBERNETES_EXECUTOR_CONTAINER_IMAGE}"
            "--conf" "spark.kubernetes.driver.pod.templateFile=${PROJECT_ROOT}/k8s/driver-pod-template.yaml"
            "--conf" "spark.kubernetes.executor.pod.templateFile=${PROJECT_ROOT}/k8s/executor-pod-template.yaml"
        )
    fi

    # Add the main Python script and arguments
    spark_cmd+=(
        "${PROJECT_ROOT}/src/bin/main.py"
        "--job" "${JOB_NAME}"
        "--environment" "${ENVIRONMENT}"
    )

    # Add extra arguments
    if [[ -n "${EXTRA_ARGS}" ]]; then
        spark_cmd+=(${EXTRA_ARGS})
    fi

    echo "${spark_cmd[@]}"
}

# ... (rest of the script is the same)
