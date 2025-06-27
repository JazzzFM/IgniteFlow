"""
IgniteFlow Core Framework.

This package provides the core functionality for the IgniteFlow framework,
including configuration management, Spark session handling, logging,
metrics collection, and exception handling.
"""

__version__ = "1.0.0"
__author__ = "IgniteFlow Team"

from .exceptions import IgniteFlowError
from .config import ConfigurationManager
from .spark import SparkSessionManager
from .logging import setup_logging
from .metrics import MetricsCollector

__all__ = [
    "IgniteFlowError",
    "ConfigurationManager", 
    "SparkSessionManager",
    "setup_logging",
    "MetricsCollector"
]