#!/bin/bash
#
# IgniteFlow Environment Profile
#
# Cloud-native environment configuration for IgniteFlow framework
# Supports local development and Kubernetes deployment
#

# Get the current directory for path resolution
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_ROOT="$(cd "${SCRIPT_DIR}/../.." && pwd)"

# Core environment variables
export IGNITEFLOW_HOME="${PROJECT_ROOT}"
export IGNITEFLOW_VERSION="${IGNITEFLOW_VERSION:-1.0.0}"
export IGNITEFLOW_ENV="${IGNITEFLOW_ENV:-local}"

# Python environment
export PYTHON_VERSION="${PYTHON_VERSION:-3.12}"
export PYSPARK_PYTHON="${PYSPARK_PYTHON:-python3}"
export PYSPARK_DRIVER_PYTHON="${PYSPARK_DRIVER_PYTHON:-python3}"

# Spark configuration based on environment
case "${IGNITEFLOW_ENV}" in
    local)
        export SPARK_HOME="${SPARK_HOME:-/opt/spark}"
        export JAVA_HOME="${JAVA_HOME:-$(readlink -f $(which java) 2>/dev/null | sed 's/bin\/java$//' || echo '/usr/lib/jvm/default-java')}"
        export SPARK_MASTER="${SPARK_MASTER:-local[*]}"
        export SPARK_DRIVER_MEMORY="${SPARK_DRIVER_MEMORY:-2g}"
        export SPARK_EXECUTOR_MEMORY="${SPARK_EXECUTOR_MEMORY:-2g}"
        ;;
    dev|staging|prod)
        export SPARK_HOME="${SPARK_HOME:-/opt/spark}"
        export JAVA_HOME="${JAVA_HOME:-/usr/lib/jvm/java-11-openjdk}"
        export SPARK_MASTER="${SPARK_MASTER:-k8s://https://kubernetes.default.svc:443}"
        export SPARK_DRIVER_MEMORY="${SPARK_DRIVER_MEMORY:-4g}"
        export SPARK_EXECUTOR_MEMORY="${SPARK_EXECUTOR_MEMORY:-4g}"
        ;;
esac

# Path configuration
export SRC_PATH="${PROJECT_ROOT}/src"
export CONFIG_PATH="${PROJECT_ROOT}/src/config"
export BIN_PATH="${PROJECT_ROOT}/src/bin"
export OPT_PATH="${PROJECT_ROOT}/src/opt"
export LOGS_PATH="${PROJECT_ROOT}/logs"
export DATA_PATH="${PROJECT_ROOT}/data"

# Data storage paths (cloud-native)
case "${IGNITEFLOW_ENV}" in
    local)
        export DATA_RAW_PATH="${DATA_PATH}/raw"
        export DATA_PROCESSED_PATH="${DATA_PATH}/processed"
        export DATA_MODELS_PATH="${DATA_PATH}/models"
        export DATA_CHECKPOINTS_PATH="${DATA_PATH}/checkpoints"
        ;;
    dev)
        export DATA_RAW_PATH="${DATA_BUCKET_PREFIX:-s3a://igniteflow-dev}/raw"
        export DATA_PROCESSED_PATH="${DATA_BUCKET_PREFIX:-s3a://igniteflow-dev}/processed"
        export DATA_MODELS_PATH="${DATA_BUCKET_PREFIX:-s3a://igniteflow-dev}/models"
        export DATA_CHECKPOINTS_PATH="${DATA_BUCKET_PREFIX:-s3a://igniteflow-dev}/checkpoints"
        ;;
    staging)
        export DATA_RAW_PATH="${DATA_BUCKET_PREFIX:-s3a://igniteflow-staging}/raw"
        export DATA_PROCESSED_PATH="${DATA_BUCKET_PREFIX:-s3a://igniteflow-staging}/processed"
        export DATA_MODELS_PATH="${DATA_BUCKET_PREFIX:-s3a://igniteflow-staging}/models"
        export DATA_CHECKPOINTS_PATH="${DATA_BUCKET_PREFIX:-s3a://igniteflow-staging}/checkpoints"
        ;;
    prod)
        export DATA_RAW_PATH="${DATA_BUCKET_PREFIX:-s3a://igniteflow-prod}/raw"
        export DATA_PROCESSED_PATH="${DATA_BUCKET_PREFIX:-s3a://igniteflow-prod}/processed"
        export DATA_MODELS_PATH="${DATA_BUCKET_PREFIX:-s3a://igniteflow-prod}/models"
        export DATA_CHECKPOINTS_PATH="${DATA_BUCKET_PREFIX:-s3a://igniteflow-prod}/checkpoints"
        ;;
esac

# Logging configuration
export LOG_LEVEL="${LOG_LEVEL:-INFO}"
export LOG_FORMAT="${LOG_FORMAT:-json}"
export LOG_FILE_PREFIX="${LOG_FILE_PREFIX:-igniteflow}"

# Observability
export METRICS_ENABLED="${METRICS_ENABLED:-true}"
export TRACING_ENABLED="${TRACING_ENABLED:-false}"
export PROMETHEUS_PUSHGATEWAY_URL="${PROMETHEUS_PUSHGATEWAY_URL:-}"

# Kubernetes-specific configurations
if [[ "${IGNITEFLOW_ENV}" != "local" ]]; then
    export SPARK_KUBERNETES_NAMESPACE="${SPARK_KUBERNETES_NAMESPACE:-igniteflow}"
    export SPARK_KUBERNETES_DRIVER_SERVICE_ACCOUNT="${SPARK_KUBERNETES_DRIVER_SERVICE_ACCOUNT:-spark}"
    export SPARK_KUBERNETES_EXECUTOR_SERVICE_ACCOUNT="${SPARK_KUBERNETES_EXECUTOR_SERVICE_ACCOUNT:-spark}"
    export SPARK_KUBERNETES_DRIVER_CONTAINER_IMAGE="${SPARK_DRIVER_IMAGE:-igniteflow/spark-driver:latest}"
    export SPARK_KUBERNETES_EXECUTOR_CONTAINER_IMAGE="${SPARK_EXECUTOR_IMAGE:-igniteflow/spark-executor:latest}"
fi

# Database connections (from environment variables or secrets)
export DATABASE_URL="${DATABASE_URL:-}"
export METASTORE_URI="${METASTORE_URI:-}"

# Security configurations
export TLS_ENABLED="${TLS_ENABLED:-false}"
export AUTH_ENABLED="${AUTH_ENABLED:-false}"

# Function to create necessary directories
create_directories() {
    local dirs=(
        "${LOGS_PATH}"
        "${DATA_PATH}"
        "${DATA_RAW_PATH}"
        "${DATA_PROCESSED_PATH}"
        "${DATA_MODELS_PATH}"
        "${DATA_CHECKPOINTS_PATH}"
    )
    
    for dir in "${dirs[@]}"; do
        # Only create local directories, not cloud storage paths
        if [[ "${dir}" != s3a://* ]] && [[ "${dir}" != gs://* ]] && [[ "${dir}" != abfs://* ]]; then
            mkdir -p "${dir}" 2>/dev/null || true
        fi
    done
}

# Function to validate environment
validate_environment() {
    local errors=()
    
    # Check required paths
    if [[ ! -d "${SRC_PATH}" ]]; then
        errors+=("Source directory not found: ${SRC_PATH}")
    fi
    
    if [[ ! -d "${CONFIG_PATH}" ]]; then
        errors+=("Configuration directory not found: ${CONFIG_PATH}")
    fi
    
    # Check Python availability
    if ! command -v "${PYSPARK_PYTHON}" &> /dev/null; then
        errors+=("Python not found: ${PYSPARK_PYTHON}")
    fi
    
    # Check Java availability
    if [[ -n "${JAVA_HOME}" ]] && [[ ! -d "${JAVA_HOME}" ]]; then
        errors+=("Java home not found: ${JAVA_HOME}")
    fi
    
    if [[ ${#errors[@]} -gt 0 ]]; then
        echo "Environment validation errors:" >&2
        printf '%s\n' "${errors[@]}" >&2
        return 1
    fi
    
    return 0
}

# Function to print environment summary
print_environment_summary() {
    cat << EOF
IgniteFlow Environment Configuration
===================================
Environment: ${IGNITEFLOW_ENV}
Project Root: ${PROJECT_ROOT}
Python: ${PYSPARK_PYTHON}
Java Home: ${JAVA_HOME}
Spark Home: ${SPARK_HOME}
Spark Master: ${SPARK_MASTER}
Data Raw Path: ${DATA_RAW_PATH}
Data Processed Path: ${DATA_PROCESSED_PATH}
Logs Path: ${LOGS_PATH}
EOF
}

# Initialize environment
initialize_environment() {
    create_directories
    
    if ! validate_environment; then
        echo "Environment validation failed" >&2
        return 1
    fi
    
    # Add Python paths
    export PYTHONPATH="${SRC_PATH}:${PYTHONPATH:-}"
    
    # Set up logging
    export LOG_FILE="${LOGS_PATH}/${LOG_FILE_PREFIX}_$(date '+%Y-%m-%d_%H-%M-%S').log"
    
    # Create log file if running locally
    if [[ "${IGNITEFLOW_ENV}" == "local" ]]; then
        touch "${LOG_FILE}" 2>/dev/null || true
    fi
    
    return 0
}

# Auto-initialize if script is sourced
if [[ "${BASH_SOURCE[0]}" == "${0}" ]]; then
    echo "This script should be sourced, not executed directly" >&2
    exit 1
fi

# Initialize environment when sourced
if initialize_environment; then
    if [[ "${IGNITEFLOW_DEBUG:-false}" == "true" ]]; then
        print_environment_summary
    fi
else
    echo "Failed to initialize IgniteFlow environment" >&2
    return 1
fi
