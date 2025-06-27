#!/usr/bin/env python3
"""
Test runner for IgniteFlow without external dependencies.

This script runs core functionality tests that don't require
PySpark, MLflow, or other optional dependencies.
"""

import sys
import json
import tempfile
from pathlib import Path

# Add src to path
sys.path.insert(0, 'src')


def test_imports():
    """Test that all core modules can be imported."""
    print("Testing imports...")
    
    from igniteflow_core import (
        IgniteFlowError, ConfigurationError, 
        ConfigurationManager, get_logger, MetricsCollector,
        BasePipeline,
        SPARK_AVAILABLE, DATA_QUALITY_AVAILABLE, MLOPS_AVAILABLE
    )
    
    print(f"✓ Core imports successful")
    print(f"  - Spark available: {SPARK_AVAILABLE}")
    print(f"  - Data Quality available: {DATA_QUALITY_AVAILABLE}")
    print(f"  - MLOps available: {MLOPS_AVAILABLE}")
    
    return True


def test_exceptions():
    """Test exception handling."""
    print("\nTesting exceptions...")
    
    from igniteflow_core import IgniteFlowError, ConfigurationError
    
    # Test base exception
    try:
        raise IgniteFlowError("Test base exception")
    except IgniteFlowError as e:
        print(f"✓ Base exception works: {e}")
    
    # Test specific exception
    try:
        raise ConfigurationError("Test configuration error")
    except IgniteFlowError as e:
        print(f"✓ Configuration exception works: {e}")
    
    return True


def test_logging():
    """Test logging functionality."""
    print("\nTesting logging...")
    
    from igniteflow_core import setup_logging, get_logger
    
    # Setup logging
    setup_logging(level="INFO", format_type="json", environment="test")
    
    # Get logger
    logger = get_logger("test_runner")
    
    # Test different log levels
    logger.debug("Debug message")
    logger.info("Info message", test_id="123")
    logger.warning("Warning message")
    logger.error("Error message", error_code="E001")
    
    # Test performance timing
    logger.start_timer("test_operation")
    import time
    time.sleep(0.01)
    duration = logger.stop_timer("test_operation")
    
    print(f"✓ Logging works (timer duration: {duration:.3f}s)")
    
    return True


def test_metrics():
    """Test metrics collection."""
    print("\nTesting metrics...")
    
    import time
    from igniteflow_core import MetricsCollector
    
    # Create metrics collector
    config = {
        "enabled": True,
        "job_name": "test_job"
    }
    
    metrics = MetricsCollector(config)
    
    # Test different metric types
    metrics.increment("test_counter", 1.0, {"type": "test"})
    metrics.gauge("test_gauge", 42.5)
    metrics.histogram("test_histogram", 1.23)
    
    # Test timer context manager
    with metrics.timer("test_timer"):
        time.sleep(0.01)
    
    # Test job metrics
    metrics.record_job_metrics(
        job_name="test_job",
        status="success", 
        duration=1.5,
        records_processed=100,
        environment="test"
    )
    
    # Get snapshot
    snapshot = metrics.get_metrics_snapshot()
    metric_count = len(snapshot["metrics"])
    
    print(f"✓ Metrics collection works ({metric_count} metrics recorded)")
    
    return True


def test_configuration():
    """Test configuration management."""
    print("\nTesting configuration...")
    
    from igniteflow_core import ConfigurationManager
    
    # Create temporary config
    with tempfile.TemporaryDirectory() as temp_dir:
        config_data = {
            "app_config": {
                "name": "test_app",
                "version": "1.0.0",
                "debug": True
            },
            "database": {
                "host": "localhost",
                "port": 5432
            }
        }
        
        # Write config file
        config_file = Path(temp_dir) / "test.json"
        with open(config_file, 'w') as f:
            json.dump(config_data, f)
        
        # Test configuration manager
        config_manager = ConfigurationManager(temp_dir, "test")
        
        # Test getting values
        app_name = config_manager.get("app_config.name")
        db_port = config_manager.get("database.port")
        missing_value = config_manager.get("missing.key", "default")
        
        assert app_name == "test_app"
        assert db_port == 5432
        assert missing_value == "default"
        
        print(f"✓ Configuration management works")
        print(f"  - App name: {app_name}")
        print(f"  - DB port: {db_port}")
        print(f"  - Default value: {missing_value}")
    
    return True


def test_base_pipeline():
    """Test base pipeline functionality."""
    print("\nTesting base pipeline...")
    
    from igniteflow_core import BasePipeline
    
    # Create a simple test pipeline
    class TestPipeline(BasePipeline):
        def extract(self):
            return {"data": "test"}
        
        def transform(self, data):
            return {"transformed": data["data"].upper()}
        
        def load(self, data):
            return f"Loaded: {data['transformed']}"
        
        def run(self):
            data = self.extract()
            transformed = self.transform(data)
            result = self.load(transformed)
            return result
    
    # Test without Spark (should work)
    config = {"test": True}
    pipeline = TestPipeline(None, config)
    
    result = pipeline.run()
    assert result == "Loaded: TEST"
    
    print(f"✓ Base pipeline works: {result}")
    
    return True


def run_all_tests():
    """Run all tests and report results."""
    print("=" * 60)
    print("IgniteFlow Core Tests (No External Dependencies)")
    print("=" * 60)
    
    tests = [
        test_imports,
        test_exceptions, 
        test_logging,
        test_metrics,
        test_configuration,
        test_base_pipeline
    ]
    
    passed = 0
    failed = 0
    
    for test in tests:
        try:
            if test():
                passed += 1
        except Exception as e:
            print(f"✗ Test {test.__name__} failed: {e}")
            failed += 1
    
    print("\n" + "=" * 60)
    print(f"Test Results: {passed} passed, {failed} failed")
    print("=" * 60)
    
    if failed > 0:
        print("\n⚠️  Some tests failed. Check dependencies and implementation.")
        return False
    else:
        print("\n🎉 All core tests passed! IgniteFlow is working correctly.")
        print("\n📝 Note: Some features require additional dependencies:")
        print("   - Install PySpark for Spark functionality")
        print("   - Install MLflow for experiment tracking")
        print("   - Install boto3 for AWS SageMaker deployment")
        return True


if __name__ == "__main__":
    success = run_all_tests()
    sys.exit(0 if success else 1)
