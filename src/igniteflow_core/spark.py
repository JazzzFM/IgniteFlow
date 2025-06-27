"""
Spark Session Management for IgniteFlow.

This module provides comprehensive Spark session management including:
- Session lifecycle management
- Configuration optimization
- Resource management
- Performance monitoring
- Error handling and recovery
"""

import logging
import os
import time
from typing import Dict, Any, Optional, List
from contextlib import contextmanager

from pyspark.sql import SparkSession
from pyspark.conf import SparkConf
from pyspark import SparkContext

from .exceptions import SparkError, ConfigurationError


class SparkSessionManager:
    """
    Comprehensive Spark session manager for IgniteFlow.
    
    This class handles all aspects of Spark session management:
    - Session creation with optimized configurations
    - Resource management and cleanup
    - Performance monitoring
    - Error handling and recovery
    - Environment-specific optimizations
    """
    
    def __init__(self, config: Dict[str, Any]) -> None:
        """
        Initialize Spark session manager.
        
        Args:
            config: Configuration dictionary containing Spark settings
        """
        self.config = config
        self.logger = logging.getLogger(__name__)
        self._session: Optional[SparkSession] = None
        self._session_start_time: Optional[float] = None
        
        # Extract Spark configuration
        self.spark_config = config.get("spark_config", {})
        self.app_name = self.spark_config.get("app_name", "IgniteFlow")
        self.master = self.spark_config.get("master", "local[*]")
        self.environment = config.get("environment", "local")
        
        self.logger.info(f"SparkSessionManager initialized for environment: {self.environment}")
    
    def create_session(self) -> SparkSession:
        """
        Create and configure a new Spark session.
        
        Returns:
            Configured SparkSession instance
            
        Raises:
            SparkError: If session creation fails
        """
        try:
            self.logger.info("Creating Spark session...")
            self._session_start_time = time.time()
            
            # Build Spark configuration
            spark_conf = self._build_spark_config()
            
            # Create Spark session
            builder = SparkSession.builder.config(conf=spark_conf)
            
            # Environment-specific configurations
            if self.environment == "local":
                builder = builder.master(self.master)
            
            # Enable Hive support if configured
            if self.spark_config.get("enable_hive_support", False):
                builder = builder.enableHiveSupport()
            
            self._session = builder.getOrCreate()
            
            # Set log level
            log_level = self.spark_config.get("log_level", "WARN")
            self._session.sparkContext.setLogLevel(log_level)
            
            # Apply additional configurations
            self._apply_session_configurations()
            
            # Log session details
            self._log_session_info()
            
            self.logger.info("Spark session created successfully")
            return self._session
            
        except Exception as e:
            raise SparkError(f"Failed to create Spark session: {str(e)}") from e
    
    def get_session(self) -> SparkSession:
        """
        Get the current Spark session, creating one if it doesn't exist.
        
        Returns:
            SparkSession instance
        """
        if self._session is None or self._session._jvm is None:
            return self.create_session()
        
        return self._session
    
    def stop(self) -> None:
        """Stop the Spark session and clean up resources."""
        if self._session is not None:
            try:
                session_duration = time.time() - (self._session_start_time or 0)
                self.logger.info(f"Stopping Spark session after {session_duration:.2f} seconds")
                
                self._session.stop()
                self._session = None
                
                self.logger.info("Spark session stopped successfully")
                
            except Exception as e:
                self.logger.error(f"Error stopping Spark session: {str(e)}")
    
    def restart_session(self) -> SparkSession:
        """
        Restart the Spark session.
        
        Returns:
            New SparkSession instance
        """
        self.logger.info("Restarting Spark session...")
        self.stop()
        return self.create_session()
    
    @contextmanager
    def session_context(self):
        """
        Context manager for Spark session lifecycle.
        
        Usage:
            with spark_manager.session_context() as spark:
                # Use spark session
                df = spark.read.parquet("path/to/data")
        """
        session = self.get_session()
        try:
            yield session
        finally:
            # Session cleanup handled by stop() method
            pass
    
    def _build_spark_config(self) -> SparkConf:
        """
        Build optimized Spark configuration.
        
        Returns:
            SparkConf object with all configurations
        """
        conf = SparkConf()
        
        # Set application name
        conf.setAppName(self.app_name)
        
        # Core Spark configurations
        spark_configs = self._get_base_spark_configs()
        
        # Environment-specific optimizations
        env_configs = self._get_environment_configs()
        spark_configs.update(env_configs)
        
        # Custom configurations from config
        custom_configs = self.spark_config.get("configs", {})
        spark_configs.update(custom_configs)
        
        # Apply all configurations
        for key, value in spark_configs.items():
            conf.set(key, str(value))
        
        self.logger.debug(f"Applied {len(spark_configs)} Spark configurations")
        
        return conf
    
    def _get_base_spark_configs(self) -> Dict[str, Any]:
        """Get base Spark configurations for all environments."""
        return {
            # SQL Optimizations
            "spark.sql.adaptive.enabled": "true",
            "spark.sql.adaptive.coalescePartitions.enabled": "true",
            "spark.sql.adaptive.localShuffleReader.enabled": "true",
            "spark.sql.adaptive.skewJoin.enabled": "true",
            
            # Memory Management
            "spark.driver.memory": self.spark_config.get("driver_memory", "2g"),
            "spark.executor.memory": self.spark_config.get("executor_memory", "2g"),
            "spark.executor.memoryFraction": "0.8",
            "spark.storage.memoryFraction": "0.5",
            
            # Serialization
            "spark.serializer": "org.apache.spark.serializer.KryoSerializer",
            "spark.sql.execution.arrow.pyspark.enabled": "true",
            
            # Network and I/O
            "spark.network.timeout": "300s",
            "spark.sql.files.maxPartitionBytes": "134217728",  # 128MB
            
            # Dynamic Allocation
            "spark.dynamicAllocation.enabled": "true",
            "spark.dynamicAllocation.minExecutors": "1",
            "spark.dynamicAllocation.maxExecutors": "10",
            "spark.dynamicAllocation.initialExecutors": "2",
            
            # Checkpointing
            "spark.sql.streaming.checkpointLocation": "/tmp/spark-checkpoints",
            
            # UI and History
            "spark.ui.enabled": "true",
            "spark.eventLog.enabled": "true",
            "spark.eventLog.dir": "/tmp/spark-events"
        }
    
    def _get_environment_configs(self) -> Dict[str, Any]:
        """Get environment-specific configurations."""
        configs = {}
        
        if self.environment == "local":
            configs.update({
                "spark.master": self.master,
                "spark.sql.warehouse.dir": "/tmp/spark-warehouse",
                "spark.driver.bindAddress": "localhost",
                "spark.driver.host": "localhost"
            })
        
        elif self.environment in ["dev", "staging", "prod"]:
            # Kubernetes configurations
            configs.update({
                "spark.kubernetes.authenticate.driver.serviceAccountName": "spark",
                "spark.kubernetes.namespace": "igniteflow",
                "spark.kubernetes.driver.container.image": "igniteflow/spark-driver:latest",
                "spark.kubernetes.executor.container.image": "igniteflow/spark-executor:latest",
                "spark.kubernetes.driver.podTemplateFile": "/opt/spark/conf/driver-pod-template.yaml",
                "spark.kubernetes.executor.podTemplateFile": "/opt/spark/conf/executor-pod-template.yaml"
            })
            
            # Production-specific optimizations
            if self.environment == "prod":
                configs.update({
                    "spark.dynamicAllocation.maxExecutors": "50",
                    "spark.sql.adaptive.advisoryPartitionSizeInBytes": "256MB",
                    "spark.sql.adaptive.coalescePartitions.minPartitionSize": "64MB"
                })
        
        return configs
    
    def _apply_session_configurations(self) -> None:
        """Apply additional session-level configurations."""
        if self._session is None:
            return
        
        # Set SQL configurations
        sql_configs = self.spark_config.get("sql", {})
        for key, value in sql_configs.items():
            if isinstance(value, dict):
                for sub_key, sub_value in value.items():
                    config_key = f"spark.sql.{key}.{sub_key}"
                    self._session.conf.set(config_key, str(sub_value))
            else:
                config_key = f"spark.sql.{key}"
                self._session.conf.set(config_key, str(value))
        
        # Set checkpoint directory if specified
        checkpoint_dir = self.spark_config.get("checkpoint_location")
        if checkpoint_dir:
            self._session.sparkContext.setCheckpointDir(checkpoint_dir)
    
    def _log_session_info(self) -> None:
        """Log detailed session information."""
        if self._session is None:
            return
        
        spark_context = self._session.sparkContext
        
        session_info = {
            "app_name": spark_context.appName,
            "app_id": spark_context.applicationId,
            "master": spark_context.master,
            "spark_version": spark_context.version,
            "python_version": spark_context.pythonVer,
            "default_parallelism": spark_context.defaultParallelism,
            "total_cores": self._get_total_cores()
        }
        
        self.logger.info(f"Spark session details: {session_info}")
    
    def _get_total_cores(self) -> int:
        """Get total number of cores available to Spark."""
        try:
            if self._session:
                status = self._session.sparkContext.statusTracker()
                executor_infos = status.getExecutorInfos()
                return sum(info.totalCores for info in executor_infos)
        except Exception:
            pass
        return 0
    
    def get_session_metrics(self) -> Dict[str, Any]:
        """
        Get current session metrics and statistics.
        
        Returns:
            Dictionary containing session metrics
        """
        if self._session is None:
            return {"status": "no_session"}
        
        try:
            spark_context = self._session.sparkContext
            status_tracker = spark_context.statusTracker()
            
            # Get executor information
            executor_infos = status_tracker.getExecutorInfos()
            
            # Calculate metrics
            total_cores = sum(info.totalCores for info in executor_infos)
            total_memory = sum(info.maxMemory for info in executor_infos)
            active_executors = len([info for info in executor_infos if info.isActive])
            
            # Get job information
            active_jobs = len(status_tracker.getActiveJobIds())
            active_stages = len(status_tracker.getActiveStageIds())
            
            # Session uptime
            uptime = time.time() - (self._session_start_time or 0)
            
            metrics = {
                "status": "active",
                "app_id": spark_context.applicationId,
                "uptime_seconds": round(uptime, 2),
                "total_cores": total_cores,
                "total_memory_mb": total_memory // (1024 * 1024),
                "active_executors": active_executors,
                "total_executors": len(executor_infos),
                "active_jobs": active_jobs,
                "active_stages": active_stages,
                "default_parallelism": spark_context.defaultParallelism
            }
            
            return metrics
            
        except Exception as e:
            self.logger.error(f"Failed to get session metrics: {str(e)}")
            return {"status": "error", "error": str(e)}
    
    def optimize_for_workload(self, workload_type: str) -> None:
        """
        Apply workload-specific optimizations.
        
        Args:
            workload_type: Type of workload (etl, ml, streaming, analytics)
        """
        if self._session is None:
            self.logger.warning("No active session to optimize")
            return
        
        optimizations = {
            "etl": {
                "spark.sql.adaptive.coalescePartitions.enabled": "true",
                "spark.sql.adaptive.skewJoin.enabled": "true",
                "spark.sql.files.maxPartitionBytes": "268435456",  # 256MB
            },
            "ml": {
                "spark.serializer": "org.apache.spark.serializer.KryoSerializer",
                "spark.sql.execution.arrow.pyspark.enabled": "true",
                "spark.sql.execution.arrow.maxRecordsPerBatch": "10000",
            },
            "streaming": {
                "spark.sql.streaming.continuous.executorQueueSize": "1024",
                "spark.sql.streaming.continuous.executorPollIntervalMs": "10",
                "spark.sql.adaptive.enabled": "false",  # Not recommended for streaming
            },
            "analytics": {
                "spark.sql.adaptive.enabled": "true",
                "spark.sql.adaptive.localShuffleReader.enabled": "true",
                "spark.sql.adaptive.advisoryPartitionSizeInBytes": "134217728",  # 128MB
            }
        }
        
        workload_configs = optimizations.get(workload_type, {})
        
        for key, value in workload_configs.items():
            self._session.conf.set(key, value)
        
        self.logger.info(f"Applied {len(workload_configs)} optimizations for {workload_type} workload")
    
    def health_check(self) -> Dict[str, Any]:
        """
        Perform a health check on the Spark session.
        
        Returns:
            Dictionary containing health check results
        """
        health_status = {
            "timestamp": time.time(),
            "status": "unknown",
            "checks": {}
        }
        
        try:
            if self._session is None:
                health_status.update({
                    "status": "no_session",
                    "message": "No active Spark session"
                })
                return health_status
            
            # Check if session is responsive
            try:
                test_df = self._session.range(1).count()
                health_status["checks"]["session_responsive"] = True
            except Exception as e:
                health_status["checks"]["session_responsive"] = False
                health_status["checks"]["session_error"] = str(e)
            
            # Check executor status
            try:
                metrics = self.get_session_metrics()
                health_status["checks"]["active_executors"] = metrics.get("active_executors", 0)
                health_status["checks"]["total_cores"] = metrics.get("total_cores", 0)
            except Exception as e:
                health_status["checks"]["executor_error"] = str(e)
            
            # Overall health determination
            if health_status["checks"].get("session_responsive", False):
                health_status["status"] = "healthy"
            else:
                health_status["status"] = "unhealthy"
            
        except Exception as e:
            health_status.update({
                "status": "error",
                "error": str(e)
            })
        
        return health_status