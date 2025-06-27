"""
PyTest configuration and fixtures for IgniteFlow tests.

This module provides common test fixtures and configuration for all tests.
"""

import os
import pytest
import tempfile
import shutil
from pathlib import Path
from typing import Dict, Any, Generator
import json

from pyspark.sql import SparkSession
from pyspark.sql.types import *

# Add src to Python path for imports
import sys
sys.path.insert(0, str(Path(__file__).parent.parent))

from igniteflow_core.config import ConfigurationManager
from igniteflow_core.spark import SparkSessionManager
from igniteflow_core.logging import setup_logging
from igniteflow_core.metrics import MetricsCollector


@pytest.fixture(scope="session")
def spark_session() -> Generator[SparkSession, None, None]:
    """
    Create a test Spark session.
    
    This fixture creates a Spark session optimized for testing with
    minimal resource usage and fast startup.
    """
    spark = SparkSession.builder \
        .appName("IgniteFlowTest") \
        .master("local[2]") \
        .config("spark.sql.shuffle.partitions", "2") \
        .config("spark.sql.adaptive.enabled", "false") \
        .config("spark.ui.enabled", "false") \
        .config("spark.sql.warehouse.dir", tempfile.mkdtemp()) \
        .getOrCreate()
    
    # Set log level to reduce noise during testing
    spark.sparkContext.setLogLevel("ERROR")
    
    yield spark
    
    spark.stop()


@pytest.fixture(scope="function")
def temp_dir() -> Generator[str, None, None]:
    """Create a temporary directory for test files."""
    temp_path = tempfile.mkdtemp()
    yield temp_path
    shutil.rmtree(temp_path, ignore_errors=True)


@pytest.fixture(scope="function")
def test_config(temp_dir: str) -> Dict[str, Any]:
    """Create a test configuration."""
    return {
        "spark_config": {
            "app_name": "IgniteFlowTest",
            "master": "local[2]",
            "driver_memory": "1g",
            "executor_memory": "1g",
            "log_level": "ERROR"
        },
        "data": {
            "input_path": f"{temp_dir}/input",
            "output_path": f"{temp_dir}/output"
        },
        "mlflow": {
            "enabled": False,
            "tracking_uri": "file:///tmp/mlruns"
        },
        "data_quality": {
            "enabled": True,
            "fail_on_error": False,
            "rules": []
        },
        "metrics": {
            "enabled": False
        }
    }


@pytest.fixture(scope="function")
def config_manager(test_config: Dict[str, Any], temp_dir: str) -> ConfigurationManager:
    """Create a test configuration manager."""
    # Create a temporary config file
    config_file = Path(temp_dir) / "test_config.json"
    with open(config_file, 'w') as f:
        json.dump(test_config, f, indent=2)
    
    return ConfigurationManager(temp_dir, "test")


@pytest.fixture(scope="function")
def spark_manager(test_config: Dict[str, Any]) -> Generator[SparkSessionManager, None, None]:
    """Create a test Spark session manager."""
    manager = SparkSessionManager(test_config)
    yield manager
    manager.stop()


@pytest.fixture(scope="function")
def metrics_collector() -> MetricsCollector:
    """Create a test metrics collector."""
    config = {
        "enabled": True,
        "port": None,  # Don't start HTTP server in tests
        "pushgateway_url": None
    }
    return MetricsCollector(config)


@pytest.fixture(scope="function")
def sample_transactions_data(spark_session: SparkSession):
    """Create sample transaction data for testing."""
    from datetime import datetime, timedelta
    import random
    
    # Generate sample data
    data = []
    base_time = datetime.now() - timedelta(days=30)
    
    for i in range(1000):
        data.append({
            "transaction_id": f"txn_{i:06d}",
            "customer_id": f"cust_{i % 100:04d}",
            "merchant_id": f"merchant_{i % 50:03d}",
            "amount": round(random.uniform(10, 1000), 2),
            "timestamp": base_time + timedelta(hours=random.randint(0, 720)),
            "merchant_category": random.choice(["grocery", "gas", "restaurant", "retail"]),
            "payment_method": random.choice(["credit", "debit", "cash"]),
            "is_fraud": 1 if random.random() < 0.02 else 0  # 2% fraud rate
        })
    
    schema = StructType([
        StructField("transaction_id", StringType(), False),
        StructField("customer_id", StringType(), False),
        StructField("merchant_id", StringType(), False),
        StructField("amount", DoubleType(), False),
        StructField("timestamp", TimestampType(), False),
        StructField("merchant_category", StringType(), False),
        StructField("payment_method", StringType(), False),
        StructField("is_fraud", IntegerType(), False)
    ])
    
    return spark_session.createDataFrame(data, schema)


@pytest.fixture(scope="function")
def sample_customers_data(spark_session: SparkSession):
    """Create sample customer data for testing."""
    import random
    
    data = []
    for i in range(100):
        data.append({
            "customer_id": f"cust_{i:04d}",
            "age": random.randint(18, 80),
            "income": random.randint(25000, 150000),
            "credit_score": random.randint(300, 850),
            "account_age_days": random.randint(30, 3650)
        })
    
    schema = StructType([
        StructField("customer_id", StringType(), False),
        StructField("age", IntegerType(), False),
        StructField("income", IntegerType(), False),
        StructField("credit_score", IntegerType(), False),
        StructField("account_age_days", IntegerType(), False)
    ])
    
    return spark_session.createDataFrame(data, schema)


@pytest.fixture(scope="function")
def sample_ratings_data(spark_session: SparkSession):
    """Create sample ratings data for recommendation system testing."""
    import random
    
    data = []
    for i in range(5000):
        data.append({
            "user_id": f"user_{i % 500:04d}",
            "item_id": f"item_{i % 200:04d}",
            "rating": random.randint(1, 5),
            "timestamp": "2023-01-01"
        })
    
    schema = StructType([
        StructField("user_id", StringType(), False),
        StructField("item_id", StringType(), False),
        StructField("rating", IntegerType(), False),
        StructField("timestamp", StringType(), False)
    ])
    
    return spark_session.createDataFrame(data, schema)


@pytest.fixture(scope="function")
def mock_mlflow_config() -> Dict[str, Any]:
    """Create mock MLflow configuration for testing."""
    return {
        "enabled": False,
        "tracking_uri": "file:///tmp/test_mlruns",
        "experiment_name": "test_experiment",
        "registry_uri": None
    }


# Test data schemas for validation
@pytest.fixture
def fraud_detection_schemas() -> Dict[str, Dict[str, Any]]:
    """Get schemas for fraud detection testing."""
    return {
        "transactions_schema": {
            "columns": [
                "transaction_id", "customer_id", "merchant_id",
                "amount", "timestamp", "merchant_category",
                "payment_method", "is_fraud"
            ]
        },
        "customers_schema": {
            "columns": [
                "customer_id", "age", "income",
                "credit_score", "account_age_days"
            ]
        }
    }


@pytest.fixture
def recommendation_schemas() -> Dict[str, Dict[str, Any]]:
    """Get schemas for recommendation system testing."""
    return {
        "ratings_schema": {
            "columns": ["user_id", "item_id", "rating", "timestamp"]
        },
        "users_schema": {
            "columns": ["user_id", "age", "gender", "occupation"]
        },
        "items_schema": {
            "columns": ["item_id", "title", "genre", "year"]
        }
    }


# Performance testing fixtures
@pytest.fixture
def performance_test_data(spark_session: SparkSession):
    """Create larger dataset for performance testing."""
    # Generate 100k records for performance testing
    data = []
    for i in range(100000):
        data.append({
            "id": i,
            "value": i * 2,
            "category": f"cat_{i % 10}",
            "score": i * 0.001
        })
    
    schema = StructType([
        StructField("id", IntegerType(), False),
        StructField("value", IntegerType(), False),
        StructField("category", StringType(), False),
        StructField("score", DoubleType(), False)
    ])
    
    return spark_session.createDataFrame(data, schema)


# Setup logging for tests
def pytest_configure(config):
    """Configure pytest with logging setup."""
    setup_logging(level="ERROR", format_type="text", environment="test")


def pytest_unconfigure(config):
    """Cleanup after pytest run."""
    # Clean up any temporary files or resources
    pass


# Custom markers for test categories
def pytest_configure(config):
    """Register custom pytest markers."""
    config.addinivalue_line(
        "markers", "unit: mark test as a unit test"
    )
    config.addinivalue_line(
        "markers", "integration: mark test as an integration test"
    )
    config.addinivalue_line(
        "markers", "performance: mark test as a performance test"
    )
    config.addinivalue_line(
        "markers", "slow: mark test as slow running"
    )


# Skip marks for conditional testing
skip_if_no_spark = pytest.mark.skipif(
    not os.getenv("SPARK_HOME"), 
    reason="Spark not available"
)

skip_if_no_mlflow = pytest.mark.skipif(
    not os.getenv("MLFLOW_TRACKING_URI"), 
    reason="MLflow not configured"
)

skip_performance = pytest.mark.skipif(
    os.getenv("SKIP_PERFORMANCE_TESTS", "false").lower() == "true",
    reason="Performance tests skipped"
)