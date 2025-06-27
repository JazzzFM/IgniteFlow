#!/bin/bash
#
# IgniteFlow Spark Job Executor
#
# Modern execution wrapper for Spark jobs in the IgniteFlow framework
# Usage: spark_exe.sh --job JOB_NAME --env ENVIRONMENT [--config CONFIG_PATH]
#
# Examples:
#   ./spark_exe.sh --job wordcount --env local
#   ./spark_exe.sh --job etl_pipeline --env dev --config config/custom.json
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
Usage: $0 --job JOB_NAME --env ENVIRONMENT [OPTIONS]

Required Arguments:
  --job JOB_NAME        Name of the job/pipeline to execute
  --env ENVIRONMENT     Target environment (local, dev, staging, prod)

Optional Arguments:
  --config CONFIG_PATH  Path to configuration files directory (default: src/config)
  --master SPARK_MASTER Spark master URL (default: local[*])
  --verbose            Enable verbose logging
  --help               Show this help message

Examples:
  $0 --job wordcount --env local
  $0 --job data_pipeline --env dev --config /custom/config
  $0 --job ml_training --env staging --master spark://cluster:7077

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

    if [[ -z "${ENVIRONMENT}" ]]; then
        log_error "Environment is required"
        usage
        exit 1
    fi
}

# Validate environment and paths
validate_environment() {
    log_debug "Validating environment and paths"
    
    # Check if config directory exists
    if [[ ! -d "${CONFIG_PATH}" ]]; then
        log_error "Configuration directory not found: ${CONFIG_PATH}"
        exit 1
    fi

    # Check if main.py exists
    local main_script="${PROJECT_ROOT}/src/bin/main.py"
    if [[ ! -f "${main_script}" ]]; then
        log_error "Main script not found: ${main_script}"
        exit 1
    fi

    # Validate environment
    case "${ENVIRONMENT}" in
        local|dev|staging|prod)
            log_debug "Environment '${ENVIRONMENT}' is valid"
            ;;
        *)
            log_error "Invalid environment: ${ENVIRONMENT}. Must be one of: local, dev, staging, prod"
            exit 1
            ;;
    esac
}

# Set up environment variables for different deployment targets
setup_environment() {
    log_info "Setting up environment for: ${ENVIRONMENT}"
    
    # Common environment variables
    export IGNITEFLOW_ENV="${ENVIRONMENT}"
    export IGNITEFLOW_CONFIG_PATH="${CONFIG_PATH}"
    export IGNITEFLOW_PROJECT_ROOT="${PROJECT_ROOT}"
    export PYTHONPATH="${PROJECT_ROOT}/src:${PYTHONPATH:-}"
    
    case "${ENVIRONMENT}" in
        local)
            export SPARK_HOME="${SPARK_HOME:-/opt/spark}"
            export JAVA_HOME="${JAVA_HOME:-$(readlink -f /usr/bin/java | sed 's/bin\/java$//')}"
            ;;
        dev|staging|prod)
            # Cloud/cluster specific configurations
            export SPARK_CONF_DIR="${CONFIG_PATH}/spark"
            export HADOOP_CONF_DIR="${HADOOP_CONF_DIR:-/etc/hadoop/conf}"
            ;;
    esac
    
    log_debug "Environment variables set successfully"
}

# Build Spark submit command with proper configurations
build_spark_command() {
    local spark_cmd=(
        "${SPARK_HOME:-/opt/spark}/bin/spark-submit"
        "--master" "${SPARK_MASTER}"
        "--name" "IgniteFlow-${JOB_NAME}-${ENVIRONMENT}"
        "--conf" "spark.app.name=IgniteFlow-${JOB_NAME}"
        "--conf" "spark.sql.adaptive.enabled=true"
        "--conf" "spark.sql.adaptive.coalescePartitions.enabled=true"
    )

    # Add environment-specific configurations
    case "${ENVIRONMENT}" in
        local)
            spark_cmd+=(
                "--conf" "spark.sql.warehouse.dir=/tmp/spark-warehouse"
                "--conf" "spark.driver.memory=2g"
                "--conf" "spark.executor.memory=2g"
            )
            ;;
        dev|staging|prod)
            spark_cmd+=(
                "--conf" "spark.kubernetes.authenticate.driver.serviceAccountName=spark"
                "--conf" "spark.kubernetes.namespace=igniteflow"
                "--conf" "spark.dynamicAllocation.enabled=true"
                "--conf" "spark.dynamicAllocation.minExecutors=1"
                "--conf" "spark.dynamicAllocation.maxExecutors=10"
            )
            ;;
    esac

    # Add the main Python script and arguments
    spark_cmd+=(
        "${PROJECT_ROOT}/src/bin/main.py"
        "--job" "${JOB_NAME}"
        "--environment" "${ENVIRONMENT}"
        "--config-path" "${CONFIG_PATH}"
    )

    echo "${spark_cmd[@]}"
}

# Execute the Spark job
execute_job() {
    log_info "Starting IgniteFlow job: ${JOB_NAME} in environment: ${ENVIRONMENT}"
    
    local spark_command
    spark_command=$(build_spark_command)
    
    log_debug "Executing command: ${spark_command}"
    
    # Create logs directory if it doesn't exist
    mkdir -p "${PROJECT_ROOT}/logs"
    
    # Execute the job with proper error handling
    if eval "${spark_command}"; then
        log_info "Job completed successfully: ${JOB_NAME}"
        exit 0
    else
        local exit_code=$?
        log_error "Job failed with exit code: ${exit_code}"
        exit ${exit_code}
    fi
}

# Main execution flow
main() {
    log_info "IgniteFlow Spark Executor starting..."
    
    parse_args "$@"
    validate_environment
    setup_environment
    execute_job
}

# Execute main function with all arguments
main "$@"
