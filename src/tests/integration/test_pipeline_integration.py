"""
Integration tests for IgniteFlow pipelines.
"""

import pytest
import tempfile
import os
from pathlib import Path
from typing import Dict, Any

# Add src to Python path for imports
import sys
sys.path.insert(0, str(Path(__file__).parent.parent.parent))

from igniteflow_core.config import ConfigurationManager
from igniteflow_core.spark import SparkSessionManager
from igniteflow_core.logging import setup_logging, get_logger
from igniteflow_core.metrics import MetricsCollector
from igniteflow_core.data_quality import DataQualityValidator, CompletenessRule, UniquenessRule
from igniteflow_core.mlops import MLflowTracker


@pytest.mark.integration
class TestPipelineIntegration:
    """Integration tests for complete pipeline workflows."""
    
    def test_complete_data_pipeline(self, temp_dir, spark_session):
        """Test a complete data processing pipeline with all components."""
        # Setup configuration
        config_data = {
            "app_config": {
                "name": "integration_test",
                "version": "1.0.0"
            },
            "spark_config": {
                "app_name": "IntegrationTest",
                "master": "local[2]",
                "driver_memory": "1g",
                "executor_memory": "1g"
            },
            "data_quality": {
                "enabled": True,
                "fail_on_error": False
            },
            "metrics": {
                "enabled": True
            },
            "mlflow": {
                "enabled": False  # Disable MLflow for integration test
            }
        }
        
        # Save configuration
        config_file = Path(temp_dir) / "integration_config.json"
        import json
        with open(config_file, 'w') as f:
            json.dump(config_data, f, indent=2)
        
        # Initialize components
        config_manager = ConfigurationManager(temp_dir, "integration")
        spark_manager = SparkSessionManager(config_data)
        logger = get_logger("integration_test")
        metrics_collector = MetricsCollector(config_data["metrics"])
        
        # Setup logging
        setup_logging(level="INFO", environment="test")
        
        try:
            # Start timing the pipeline
            metrics_collector.start_timer("pipeline_execution")
            
            # Get Spark session
            spark = spark_manager.get_session()
            logger.info("Spark session initialized")
            
            # Create test data
            test_data = [
                ("001", "John Doe", 25, "john@example.com", 50000),
                ("002", "Jane Smith", 30, "jane@example.com", 60000),
                ("003", "Bob Johnson", 35, "bob@example.com", 70000),
                ("004", "Alice Brown", 28, "alice@example.com", 55000),
                ("005", "Charlie Wilson", 42, "charlie@example.com", 80000)
            ]
            
            from pyspark.sql.types import StructType, StructField, StringType, IntegerType
            schema = StructType([
                StructField("id", StringType(), False),
                StructField("name", StringType(), False),
                StructField("age", IntegerType(), False),
                StructField("email", StringType(), False),
                StructField("salary", IntegerType(), False)
            ])
            
            df = spark.createDataFrame(test_data, schema)
            logger.info(f"Created DataFrame with {df.count()} records")
            
            # Data quality validation
            rules = [
                CompletenessRule("id_completeness", "id", 0.95),
                CompletenessRule("name_completeness", "name", 0.95),
                UniquenessRule("id_uniqueness", "id")
            ]
            
            validator = DataQualityValidator(rules)
            quality_results = validator.validate(df)
            
            logger.info(f"Data quality validation completed: {len(quality_results)} rules checked")
            
            # Log quality metrics
            for result in quality_results:
                metrics_collector.record_data_quality_metrics(
                    dataset="test_data",
                    rule=result.rule_name,
                    score=result.score,
                    environment="test"
                )
            
            # Perform data transformation
            df_transformed = df.withColumn("salary_category", 
                spark.sql("CASE WHEN salary < 60000 THEN 'Low' WHEN salary < 75000 THEN 'Medium' ELSE 'High' END").expr
            )
            
            logger.info("Data transformation completed")
            
            # Write results to temporary location
            output_path = Path(temp_dir) / "output"
            df_transformed.coalesce(1).write.mode("overwrite").parquet(str(output_path))
            
            logger.info(f"Results written to {output_path}")
            
            # Record pipeline metrics
            duration = metrics_collector.stop_timer("pipeline_execution")
            metrics_collector.record_job_metrics(
                job_name="integration_test_pipeline",
                status="success",
                duration=duration,
                records_processed=df.count(),
                environment="test"
            )
            
            # Verify output exists
            assert output_path.exists()
            
            # Read back and verify
            df_result = spark.read.parquet(str(output_path))
            assert df_result.count() == 5
            assert "salary_category" in df_result.columns
            
            logger.info("Integration test completed successfully")
            
        finally:
            # Cleanup
            spark_manager.stop()
    
    def test_spark_session_lifecycle(self, temp_dir):
        """Test Spark session lifecycle management."""
        config = {
            "spark_config": {
                "app_name": "LifecycleTest",
                "master": "local[2]",
                "driver_memory": "1g"
            }
        }
        
        spark_manager = SparkSessionManager(config)
        
        # Test session creation
        session1 = spark_manager.get_session()
        assert session1 is not None
        
        # Test session reuse
        session2 = spark_manager.get_session()
        assert session1 is session2
        
        # Test restart
        session3 = spark_manager.restart_session()
        assert session3 is not session1
        
        # Test metrics
        metrics = spark_manager.get_session_metrics()
        assert metrics["status"] == "active"
        assert "app_id" in metrics
        
        # Test health check
        health = spark_manager.health_check()
        assert "status" in health
        assert "checks" in health
        
        # Cleanup
        spark_manager.stop()
    
    def test_configuration_integration(self, temp_dir):
        """Test configuration management integration."""
        # Create base configuration
        base_config = {
            "app_config": {
                "name": "test_app",
                "debug": False
            },
            "spark_config": {
                "driver_memory": "1g",
                "executor_memory": "1g"
            }
        }
        
        # Create environment-specific override
        env_config = {
            "app_config": {
                "debug": True,
                "log_level": "DEBUG"
            },
            "spark_config": {
                "driver_memory": "2g"
            }
        }
        
        # Write configurations
        import json
        with open(Path(temp_dir) / "base.json", 'w') as f:
            json.dump(base_config, f)
        
        with open(Path(temp_dir) / "test.json", 'w') as f:
            json.dump(env_config, f)
        
        # Test configuration manager
        config_manager = ConfigurationManager(temp_dir, "test")
        
        # Verify merged configuration
        assert config_manager.get("app_config.name") == "test_app"
        assert config_manager.get("app_config.debug") is True  # Overridden
        assert config_manager.get("app_config.log_level") == "DEBUG"  # Added
        assert config_manager.get("spark_config.driver_memory") == "2g"  # Overridden
        assert config_manager.get("spark_config.executor_memory") == "1g"  # Preserved
        
        # Test Spark configuration extraction
        spark_config = config_manager.get_spark_config()
        assert spark_config["driver_memory"] == "2g"
        assert spark_config["executor_memory"] == "1g"
    
    def test_metrics_integration(self):
        """Test metrics collection integration."""
        config = {
            "enabled": True,
            "job_name": "integration_test"
        }
        
        metrics_collector = MetricsCollector(config)
        
        # Test various metric types
        metrics_collector.increment("test_counter", 1.0, {"type": "integration"})
        metrics_collector.gauge("test_gauge", 42.5)
        metrics_collector.histogram("test_histogram", 1.23)
        
        # Test timer
        with metrics_collector.timer("test_operation"):
            import time
            time.sleep(0.01)
        
        # Test job metrics
        metrics_collector.record_job_metrics(
            job_name="integration_test",
            status="success",
            duration=1.5,
            records_processed=100,
            environment="test"
        )
        
        # Test model metrics
        metrics_collector.record_model_metrics(
            model_name="test_model",
            version="v1.0",
            accuracy=0.95,
            precision=0.92,
            recall=0.89
        )
        
        # Verify metrics were collected
        snapshot = metrics_collector.get_metrics_snapshot()
        assert "metrics" in snapshot
        assert len(snapshot["metrics"]) > 0
    
    @pytest.mark.slow
    def test_performance_integration(self, temp_dir, performance_test_data):
        """Test performance with larger datasets."""
        config = {
            "spark_config": {
                "app_name": "PerformanceTest",
                "master": "local[*]",
                "driver_memory": "2g",
                "executor_memory": "2g"
            },
            "metrics": {
                "enabled": True
            }
        }
        
        spark_manager = SparkSessionManager(config)
        metrics_collector = MetricsCollector(config["metrics"])
        
        try:
            # Start performance timer
            metrics_collector.start_timer("performance_test")
            
            spark = spark_manager.get_session()
            
            # Perform operations on large dataset
            df = performance_test_data
            
            # Test transformations
            df_filtered = df.filter(df.score > 50)
            df_grouped = df_filtered.groupBy("category").count()
            
            # Force execution and measure
            with metrics_collector.timer("data_processing"):
                result_count = df_grouped.count()
                df_grouped.collect()  # Force execution
            
            duration = metrics_collector.stop_timer("performance_test")
            
            # Record performance metrics
            metrics_collector.record_job_metrics(
                job_name="performance_test",
                status="success",
                duration=duration,
                records_processed=df.count(),
                environment="test"
            )
            
            # Verify results
            assert result_count > 0
            assert duration > 0
            
        finally:
            spark_manager.stop()


@pytest.mark.integration
class TestComponentIntegration:
    """Test integration between different IgniteFlow components."""
    
    def test_logging_metrics_integration(self):
        """Test integration between logging and metrics."""
        logger = get_logger("integration_test")
        metrics_collector = MetricsCollector({"enabled": True})
        
        # Test logging with metrics
        logger.start_timer("operation")
        
        # Simulate work
        logger.info("Starting operation", operation_id="op123")
        
        # Log some metrics
        logger.log_metric("records_processed", 1000, "count")
        
        duration = logger.stop_timer("operation")
        logger.info("Operation completed", duration=duration)
        
        # Verify timer was recorded
        assert duration > 0
    
    def test_spark_data_quality_integration(self, spark_session, sample_transactions_data):
        """Test integration between Spark and data quality."""
        # Create data quality rules
        rules = [
            CompletenessRule("transaction_id_complete", "transaction_id", 0.99),
            CompletenessRule("customer_id_complete", "customer_id", 0.99),
            UniquenessRule("transaction_id_unique", "transaction_id")
        ]
        
        validator = DataQualityValidator(rules)
        
        # Validate sample data
        results = validator.validate(sample_transactions_data)
        
        # Should have results for all rules
        assert len(results) == 3
        
        # Get summary
        summary = validator.get_summary_report(results)
        assert "overall_score" in summary
        assert summary["total_rules"] == 3
    
    def test_config_spark_integration(self, temp_dir):
        """Test integration between configuration and Spark."""
        # Create configuration with Spark settings
        config_data = {
            "spark_config": {
                "app_name": "ConfigIntegrationTest",
                "master": "local[2]",
                "driver_memory": "1g",
                "executor_memory": "1g",
                "configs": {
                    "spark.sql.adaptive.enabled": "true",
                    "spark.sql.adaptive.coalescePartitions.enabled": "true"
                }
            }
        }
        
        # Save configuration
        import json
        config_file = Path(temp_dir) / "config.json"
        with open(config_file, 'w') as f:
            json.dump(config_data, f)
        
        # Load configuration and create Spark manager
        config_manager = ConfigurationManager(temp_dir, "test")
        spark_config = config_manager.get_spark_config()
        
        spark_manager = SparkSessionManager({"spark_config": spark_config})
        
        try:
            session = spark_manager.get_session()
            
            # Verify Spark session was created with correct configuration
            assert session.sparkContext.appName == "ConfigIntegrationTest"
            
            # Verify custom configurations were applied
            # Note: In a real test, you'd check session.conf.get() values
            
        finally:
            spark_manager.stop()
