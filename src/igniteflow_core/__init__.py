"""
IgniteFlow Core Framework.

This package provides the core functionality for the IgniteFlow framework,
including configuration management, Spark session handling, logging,
metrics collection, and exception handling.
"""

__version__ = "1.0.0"
__author__ = "IgniteFlow Team"

# Core imports (always available)
from .exceptions import *
from .config import ConfigurationManager
from .base import BasePipeline
from .logging import setup_logging, get_logger
from .metrics import MetricsCollector, get_metrics_collector

# Optional imports with graceful fallbacks
try:
    from .spark import SparkSessionManager
    SPARK_AVAILABLE = True
except ImportError:
    SparkSessionManager = None
    SPARK_AVAILABLE = False

try:
    from .data_quality import DataQualityValidator, create_validator
    DATA_QUALITY_AVAILABLE = True
except ImportError:
    DataQualityValidator = None
    create_validator = None
    DATA_QUALITY_AVAILABLE = False

try:
    from .mlops import MLflowTracker, SageMakerDeployer
    MLOPS_AVAILABLE = True
except ImportError:
    MLflowTracker = None
    SageMakerDeployer = None
    MLOPS_AVAILABLE = False

__all__ = [
    # Exceptions
    "IgniteFlowError",
    "ConfigurationError",
    "SparkError", 
    "DataQualityError",
    "MLOpsError",
    # Core components
    "ConfigurationManager",
    "BasePipeline", 
    "setup_logging",
    "get_logger", 
    "MetricsCollector",
    "get_metrics_collector",
    # Optional components
    "SparkSessionManager",
    "DataQualityValidator",
    "create_validator", 
    "MLflowTracker",
    "SageMakerDeployer",
    # Availability flags
    "SPARK_AVAILABLE",
    "DATA_QUALITY_AVAILABLE", 
    "MLOPS_AVAILABLE"
]