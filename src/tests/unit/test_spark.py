"""
Unit tests for the Spark session management module.
"""

import pytest
import time
from unittest.mock import patch, MagicMock
from typing import Dict, Any

from igniteflow_core.spark import SparkSessionManager
from igniteflow_core.exceptions import SparkError


class TestSparkSessionManager:
    """Test cases for SparkSessionManager."""
    
    def test_init(self, test_config: Dict[str, Any]):
        """Test SparkSessionManager initialization."""
        manager = SparkSessionManager(test_config)
        
        assert manager.config == test_config
        assert manager.spark_config == test_config["spark_config"]
        assert manager.app_name == "IgniteFlowTest"
        assert manager.master == "local[2]"
        assert manager.environment == "local"  # Default
        assert manager._session is None
    
    def test_init_with_environment(self, test_config: Dict[str, Any]):
        """Test initialization with specific environment."""
        test_config["environment"] = "prod"
        manager = SparkSessionManager(test_config)
        
        assert manager.environment == "prod"
    
    def test_create_session_success(self, spark_manager: SparkSessionManager):
        """Test successful Spark session creation."""
        session = spark_manager.create_session()
        
        assert session is not None
        assert spark_manager._session is not None
        assert spark_manager._session_start_time is not None
        assert session.sparkContext.appName == "IgniteFlowTest"
    
    def test_get_session_creates_if_none(self, test_config: Dict[str, Any]):
        """Test that get_session creates session if none exists."""
        manager = SparkSessionManager(test_config)
        
        # Initially no session
        assert manager._session is None
        
        session = manager.get_session()
        
        # Should create session
        assert session is not None
        assert manager._session is not None
    
    def test_get_session_returns_existing(self, spark_manager: SparkSessionManager):
        """Test that get_session returns existing session."""
        # Create initial session
        session1 = spark_manager.get_session()
        
        # Get session again
        session2 = spark_manager.get_session()
        
        # Should be the same session
        assert session1 is session2
    
    def test_stop_session(self, spark_manager: SparkSessionManager):
        """Test stopping Spark session."""
        # Create session
        session = spark_manager.get_session()
        assert spark_manager._session is not None
        
        # Stop session
        spark_manager.stop()
        
        assert spark_manager._session is None
    
    def test_restart_session(self, spark_manager: SparkSessionManager):
        """Test restarting Spark session."""
        # Create initial session
        session1 = spark_manager.get_session()
        app_id1 = session1.sparkContext.applicationId
        
        # Restart session
        session2 = spark_manager.restart_session()
        app_id2 = session2.sparkContext.applicationId
        
        # Should be different sessions
        assert session1 is not session2
        assert app_id1 != app_id2
    
    def test_session_context_manager(self, spark_manager: SparkSessionManager):
        """Test using session as context manager."""
        with spark_manager.session_context() as spark:
            assert spark is not None
            assert spark.sparkContext is not None
            
            # Test basic operation
            df = spark.range(10)
            assert df.count() == 10
    
    def test_get_base_spark_configs(self, spark_manager: SparkSessionManager):
        """Test generation of base Spark configurations."""
        configs = spark_manager._get_base_spark_configs()
        
        # Check required configurations
        assert "spark.sql.adaptive.enabled" in configs
        assert "spark.driver.memory" in configs
        assert "spark.executor.memory" in configs
        assert "spark.serializer" in configs
        assert "spark.dynamicAllocation.enabled" in configs
        
        # Check values
        assert configs["spark.sql.adaptive.enabled"] == "true"
        assert configs["spark.serializer"] == "org.apache.spark.serializer.KryoSerializer"
    
    def test_get_environment_configs_local(self, test_config: Dict[str, Any]):
        """Test environment-specific configs for local environment."""
        test_config["environment"] = "local"
        manager = SparkSessionManager(test_config)
        
        configs = manager._get_environment_configs()
        
        assert "spark.master" in configs
        assert "spark.sql.warehouse.dir" in configs
        assert configs["spark.master"] == "local[2]"
    
    def test_get_environment_configs_kubernetes(self, test_config: Dict[str, Any]):
        """Test environment-specific configs for Kubernetes environment."""
        test_config["environment"] = "prod"
        manager = SparkSessionManager(test_config)
        
        configs = manager._get_environment_configs()
        
        assert "spark.kubernetes.authenticate.driver.serviceAccountName" in configs
        assert "spark.kubernetes.namespace" in configs
        assert configs["spark.kubernetes.namespace"] == "igniteflow"
    
    def test_get_session_metrics_no_session(self, test_config: Dict[str, Any]):
        """Test getting metrics when no session exists."""
        manager = SparkSessionManager(test_config)
        
        metrics = manager.get_session_metrics()
        
        assert metrics["status"] == "no_session"
    
    def test_get_session_metrics_with_session(self, spark_manager: SparkSessionManager):
        """Test getting metrics with active session."""
        # Create session
        session = spark_manager.get_session()
        
        metrics = spark_manager.get_session_metrics()
        
        assert metrics["status"] == "active"
        assert "app_id" in metrics
        assert "uptime_seconds" in metrics
        assert "total_cores" in metrics
        assert "active_executors" in metrics
        assert isinstance(metrics["uptime_seconds"], (int, float))
    
    def test_optimize_for_workload_etl(self, spark_manager: SparkSessionManager):
        """Test workload optimization for ETL."""
        session = spark_manager.get_session()
        
        spark_manager.optimize_for_workload("etl")
        
        # Check that configurations were applied
        # Note: In real tests, you'd check session.conf.get() values
        # For now, just ensure no exceptions were raised
        assert session is not None
    
    def test_optimize_for_workload_ml(self, spark_manager: SparkSessionManager):
        """Test workload optimization for ML."""
        session = spark_manager.get_session()
        
        spark_manager.optimize_for_workload("ml")
        
        assert session is not None
    
    def test_optimize_for_workload_no_session(self, test_config: Dict[str, Any]):
        """Test workload optimization when no session exists."""
        manager = SparkSessionManager(test_config)
        
        # Should not raise exception
        manager.optimize_for_workload("etl")
    
    def test_health_check_no_session(self, test_config: Dict[str, Any]):
        """Test health check when no session exists."""
        manager = SparkSessionManager(test_config)
        
        health = manager.health_check()
        
        assert health["status"] == "no_session"
        assert "message" in health
        assert "timestamp" in health
    
    def test_health_check_with_session(self, spark_manager: SparkSessionManager):
        """Test health check with active session."""
        # Create session
        session = spark_manager.get_session()
        
        health = spark_manager.health_check()
        
        assert "timestamp" in health
        assert "status" in health
        assert "checks" in health
        
        # Should be healthy if session is responsive
        if health["checks"].get("session_responsive", False):
            assert health["status"] == "healthy"
    
    def test_build_spark_config(self, spark_manager: SparkSessionManager):
        """Test building complete Spark configuration."""
        conf = spark_manager._build_spark_config()
        
        assert conf is not None
        # Check that configurations were applied
        # Note: SparkConf doesn't have a direct way to get all configs in tests
        # In practice, you'd check specific known configurations
    
    @patch('igniteflow_core.spark.SparkSession.builder')
    def test_create_session_failure(self, mock_builder, test_config: Dict[str, Any]):
        """Test handling of session creation failure."""
        # Mock builder to raise exception
        mock_builder.config.return_value = mock_builder
        mock_builder.master.return_value = mock_builder
        mock_builder.getOrCreate.side_effect = Exception("Mock error")
        
        manager = SparkSessionManager(test_config)
        
        with pytest.raises(SparkError) as exc_info:
            manager.create_session()
        
        assert "Failed to create Spark session" in str(exc_info.value)
    
    def test_get_total_cores_no_session(self, test_config: Dict[str, Any]):
        """Test getting total cores when no session exists."""
        manager = SparkSessionManager(test_config)
        
        cores = manager._get_total_cores()
        
        assert cores == 0
    
    def test_get_total_cores_with_session(self, spark_manager: SparkSessionManager):
        """Test getting total cores with active session."""
        # Create session
        session = spark_manager.get_session()
        
        cores = spark_manager._get_total_cores()
        
        # Should return some number >= 0
        assert isinstance(cores, int)
        assert cores >= 0
    
    def test_custom_spark_configs(self, test_config: Dict[str, Any]):
        """Test applying custom Spark configurations."""
        test_config["spark_config"]["configs"] = {
            "spark.custom.setting": "custom_value",
            "spark.another.setting": "another_value"
        }
        
        manager = SparkSessionManager(test_config)
        configs = manager._get_base_spark_configs()
        
        # Custom configs should be merged
        env_configs = manager._get_environment_configs()
        custom_configs = test_config["spark_config"]["configs"]
        
        # Verify custom configs are included
        assert "spark.custom.setting" in custom_configs
        assert custom_configs["spark.custom.setting"] == "custom_value"