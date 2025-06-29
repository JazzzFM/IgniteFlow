"""
Metrics Collection and Monitoring for IgniteFlow.

This module provides comprehensive metrics collection including:
- Prometheus metrics integration
- Custom business metrics
- Performance monitoring
- System health metrics
- Real-time dashboards support
"""

import logging
import time
import threading
import os
from typing import Dict, Any, List, Optional, Union, Callable
from datetime import datetime
from dataclasses import dataclass, field
from collections import defaultdict, deque
import json

try:
    from prometheus_client import (
        Counter, Histogram, Gauge, Summary, Info,
        CollectorRegistry, generate_latest, CONTENT_TYPE_LATEST,
        start_http_server, push_to_gateway
    )
    PROMETHEUS_AVAILABLE = True
except ImportError:
    PROMETHEUS_AVAILABLE = False
    logging.warning("Prometheus client not available. Metrics will be limited.")


@dataclass
class MetricDefinition:
    """Definition of a metric with metadata."""
    name: str
    metric_type: str  # counter, gauge, histogram, summary
    description: str
    labels: List[str] = field(default_factory=list)
    buckets: Optional[List[float]] = None  # For histograms


@dataclass
class MetricValue:
    """A metric value with timestamp and labels."""
    name: str
    value: Union[int, float]
    labels: Dict[str, str] = field(default_factory=dict)
    timestamp: float = field(default_factory=time.time)


class MetricsRegistry:
    """
    Registry for managing metric definitions and instances.
    """
    
    def __init__(self):
        self.metrics: Dict[str, Any] = {}
        self.definitions: Dict[str, MetricDefinition] = {}
        self.registry = CollectorRegistry() if PROMETHEUS_AVAILABLE else None
        self.logger = logging.getLogger(__name__)
    
    def register_metric(self, definition: MetricDefinition) -> None:
        """Register a metric definition."""
        if not PROMETHEUS_AVAILABLE:
            self.logger.warning("Prometheus not available, metric registration skipped")
            return
        
        self.definitions[definition.name] = definition
        
        # Create Prometheus metric based on type
        if definition.metric_type == "counter":
            metric = Counter(
                definition.name,
                definition.description,
                definition.labels,
                registry=self.registry
            )
        elif definition.metric_type == "gauge":
            metric = Gauge(
                definition.name,
                definition.description,
                definition.labels,
                registry=self.registry
            )
        elif definition.metric_type == "histogram":
            metric = Histogram(
                definition.name,
                definition.description,
                definition.labels,
                buckets=definition.buckets,
                registry=self.registry
            )
        elif definition.metric_type == "summary":
            metric = Summary(
                definition.name,
                definition.description,
                definition.labels,
                registry=self.registry
            )
        else:
            raise ValueError(f"Unknown metric type: {definition.metric_type}")
        
        self.metrics[definition.name] = metric
        self.logger.debug(f"Registered metric: {definition.name}")
    
    def get_metric(self, name: str) -> Optional[Any]:
        """Get a registered metric by name."""
        return self.metrics.get(name)


class MetricsCollector:
    """
    Main metrics collector for IgniteFlow applications.
    
    This class provides comprehensive metrics collection including:
    - Application performance metrics
    - Business metrics
    - System health metrics
    - Custom metrics
    """
    
    def __init__(self, config: Optional[Dict[str, Any]] = None):
        self.config = config or {}
        self.logger = logging.getLogger(__name__)
        
        # Initialize registries
        self.registry = MetricsRegistry()
        self.enabled = self.config.get("enabled", True)
        
        # Metrics storage for non-Prometheus environments
        self.metric_values: Dict[str, deque] = defaultdict(lambda: deque(maxlen=1000))
        self.timers: Dict[str, float] = {}
        
        # Configuration
        self.pushgateway_url = self.config.get("pushgateway_url")
        self.job_name = self.config.get("job_name", "igniteflow")
        self.instance_id = os.getenv("HOSTNAME", "unknown")
        
        # Thread safety
        self._lock = threading.Lock()
        
        # Initialize default metrics
        self._register_default_metrics()
        
        # Start metrics server if configured
        metrics_port = self.config.get("port")
        if metrics_port and PROMETHEUS_AVAILABLE:
            self._start_metrics_server(metrics_port)
        
        self.logger.info("MetricsCollector initialized")
    
    def _register_default_metrics(self) -> None:
        """Register default system and application metrics."""
        default_metrics = [
            MetricDefinition(
                name="igniteflow_job_duration_seconds",
                metric_type="histogram",
                description="Duration of IgniteFlow jobs in seconds",
                labels=["job_name", "status", "environment"],
                buckets=[0.1, 0.5, 1, 5, 10, 30, 60, 300, 600, 1800, 3600]
            ),
            MetricDefinition(
                name="igniteflow_job_total",
                metric_type="counter",
                description="Total number of IgniteFlow jobs executed",
                labels=["job_name", "status", "environment"]
            ),
            MetricDefinition(
                name="igniteflow_records_processed_total",
                metric_type="counter",
                description="Total number of records processed",
                labels=["job_name", "stage", "environment"]
            ),
            MetricDefinition(
                name="igniteflow_data_quality_score",
                metric_type="gauge",
                description="Data quality score (0-1)",
                labels=["dataset", "rule", "environment"]
            ),
            MetricDefinition(
                name="igniteflow_model_accuracy",
                metric_type="gauge",
                description="Model accuracy score",
                labels=["model_name", "version", "environment"]
            ),
            MetricDefinition(
                name="igniteflow_spark_executors_active",
                metric_type="gauge",
                description="Number of active Spark executors",
                labels=["app_id", "environment"]
            ),
            MetricDefinition(
                name="igniteflow_memory_usage_bytes",
                metric_type="gauge",
                description="Memory usage in bytes",
                labels=["component", "environment"]
            ),
            MetricDefinition(
                name="igniteflow_api_requests_total",
                metric_type="counter",
                description="Total API requests",
                labels=["endpoint", "method", "status", "environment"]
            ),
            MetricDefinition(
                name="igniteflow_api_request_duration_seconds",
                metric_type="histogram",
                description="API request duration in seconds",
                labels=["endpoint", "method", "environment"],
                buckets=[0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1, 2.5, 5, 10]
            )
        ]
        
        for metric_def in default_metrics:
            self.registry.register_metric(metric_def)
    
    def _start_metrics_server(self, port: int) -> None:
        """Start Prometheus metrics HTTP server."""
        try:
            start_http_server(port, registry=self.registry.registry)
            self.logger.info(f"Metrics server started on port {port}")
        except Exception as e:
            self.logger.error(f"Failed to start metrics server: {str(e)}")
    
    def increment(self, name: str, value: float = 1, tags: Optional[Dict[str, str]] = None) -> None:
        """
        Increment a counter metric.
        
        Args:
            name: Metric name
            value: Value to increment by
            tags: Optional tags/labels
        """
        if not self.enabled:
            return
        
        tags = tags or {}
        
        with self._lock:
            # Prometheus metric
            metric = self.registry.get_metric(name)
            if metric:
                if tags:
                    metric.labels(**tags).inc(value)
                else:
                    metric.inc(value)
            
            # Store for non-Prometheus environments
            self.metric_values[name].append(MetricValue(name, value, tags))
        
        self.logger.debug(f"Incremented metric: {name} by {value}")
    
    def gauge(self, name: str, value: float, tags: Optional[Dict[str, str]] = None) -> None:
        """
        Set a gauge metric value.
        
        Args:
            name: Metric name
            value: Gauge value
            tags: Optional tags/labels
        """
        if not self.enabled:
            return
        
        tags = tags or {}
        
        with self._lock:
            # Prometheus metric
            metric = self.registry.get_metric(name)
            if metric:
                if tags:
                    metric.labels(**tags).set(value)
                else:
                    metric.set(value)
            
            # Store for non-Prometheus environments
            self.metric_values[name].append(MetricValue(name, value, tags))
        
        self.logger.debug(f"Set gauge metric: {name} to {value}")
    
    def histogram(self, name: str, value: float, tags: Optional[Dict[str, str]] = None) -> None:
        """
        Observe a histogram metric.
        
        Args:
            name: Metric name
            value: Value to observe
            tags: Optional tags/labels
        """
        if not self.enabled:
            return
        
        tags = tags or {}
        
        with self._lock:
            # Prometheus metric
            metric = self.registry.get_metric(name)
            if metric:
                if tags:
                    metric.labels(**tags).observe(value)
                else:
                    metric.observe(value)
            
            # Store for non-Prometheus environments
            self.metric_values[name].append(MetricValue(name, value, tags))
        
        self.logger.debug(f"Observed histogram metric: {name} with value {value}")
    
    def timer(self, name: str, tags: Optional[Dict[str, str]] = None):
        """
        Context manager for timing operations.
        
        Usage:
            with metrics.timer("operation_duration"):
                # timed operation
                pass
        """
        return MetricTimer(self, name, tags)
    
    def start_timer(self, name: str) -> None:
        """Start a named timer."""
        self.timers[name] = time.time()
        self.logger.debug(f"Started timer: {name}")
    
    def stop_timer(self, name: str, tags: Optional[Dict[str, str]] = None) -> float:
        """
        Stop a named timer and record the duration.
        
        Returns:
            Duration in seconds
        """
        if name not in self.timers:
            self.logger.warning(f"Timer '{name}' was not started")
            return 0.0
        
        duration = time.time() - self.timers[name]
        del self.timers[name]
        
        # Record as histogram
        histogram_name = f"{name}_duration_seconds"
        self.histogram(histogram_name, duration, tags)
        
        self.logger.debug(f"Stopped timer: {name}, duration: {duration:.3f}s")
        return duration
    
    def record_job_metrics(self, job_name: str, status: str, duration: float, 
                          records_processed: int = 0, environment: str = "unknown") -> None:
        """
        Record comprehensive job execution metrics.
        
        Args:
            job_name: Name of the job
            status: Job status (success, failure, timeout)
            duration: Job duration in seconds
            records_processed: Number of records processed
            environment: Environment name
        """
        tags = {
            "job_name": job_name,
            "status": status,
            "environment": environment
        }
        
        # Job completion counter
        self.increment("igniteflow_job_total", tags=tags)
        
        # Job duration histogram
        self.histogram("igniteflow_job_duration_seconds", duration, tags=tags)
        
        # Records processed
        if records_processed > 0:
            processing_tags = {
                "job_name": job_name,
                "stage": "processing",
                "environment": environment
            }
            self.increment("igniteflow_records_processed_total", records_processed, processing_tags)
        
        self.logger.info(f"Recorded job metrics for {job_name}: {status}, {duration:.2f}s, {records_processed} records")
    
    def record_model_metrics(self, model_name: str, version: str, accuracy: float,
                           precision: float = None, recall: float = None, 
                           f1_score: float = None, environment: str = "unknown") -> None:
        """
        Record ML model performance metrics.
        
        Args:
            model_name: Name of the model
            version: Model version
            accuracy: Model accuracy
            precision: Optional precision score
            recall: Optional recall score
            f1_score: Optional F1 score
            environment: Environment name
        """
        base_tags = {
            "model_name": model_name,
            "version": version,
            "environment": environment
        }
        
        # Record all available metrics
        metrics_to_record = {
            "igniteflow_model_accuracy": accuracy,
            "igniteflow_model_precision": precision,
            "igniteflow_model_recall": recall,
            "igniteflow_model_f1_score": f1_score
        }
        
        for metric_name, value in metrics_to_record.items():
            if value is not None:
                self.gauge(metric_name, value, base_tags)
        
        self.logger.info(f"Recorded model metrics for {model_name} v{version}")
    
    def record_data_quality_metrics(self, dataset: str, rule: str, score: float,
                                   environment: str = "unknown") -> None:
        """
        Record data quality metrics.
        
        Args:
            dataset: Dataset name
            rule: Quality rule name
            score: Quality score (0-1)
            environment: Environment name
        """
        tags = {
            "dataset": dataset,
            "rule": rule,
            "environment": environment
        }
        
        self.gauge("igniteflow_data_quality_score", score, tags)
        self.logger.debug(f"Recorded data quality metric: {dataset}.{rule} = {score}")
    
    def record_spark_metrics(self, app_id: str, active_executors: int, total_cores: int,
                           memory_usage: int, environment: str = "unknown") -> None:
        """
        Record Spark cluster metrics.
        
        Args:
            app_id: Spark application ID
            active_executors: Number of active executors
            total_cores: Total number of cores
            memory_usage: Memory usage in bytes
            environment: Environment name
        """
        tags = {"app_id": app_id, "environment": environment}
        
        self.gauge("igniteflow_spark_executors_active", active_executors, tags)
        self.gauge("igniteflow_spark_cores_total", total_cores, tags)
        
        memory_tags = {"component": "spark", "environment": environment}
        self.gauge("igniteflow_memory_usage_bytes", memory_usage, memory_tags)
        
        self.logger.debug(f"Recorded Spark metrics for {app_id}")
    
    def get_metrics_snapshot(self) -> Dict[str, Any]:
        """
        Get a snapshot of current metrics.
        
        Returns:
            Dictionary containing current metric values
        """
        snapshot = {
            "timestamp": datetime.now().isoformat(),
            "metrics": {}
        }
        
        with self._lock:
            for name, values in self.metric_values.items():
                if values:
                    latest_value = values[-1]
                    snapshot["metrics"][name] = {
                        "value": latest_value.value,
                        "labels": latest_value.labels,
                        "timestamp": latest_value.timestamp
                    }
        
        return snapshot
    
    def export_prometheus_metrics(self) -> str:
        """
        Export metrics in Prometheus format.
        
        Returns:
            Prometheus formatted metrics string
        """
        if not PROMETHEUS_AVAILABLE or not self.registry.registry:
            return "# Prometheus not available\n"
        
        return generate_latest(self.registry.registry).decode('utf-8')
    
    def push_to_gateway(self, gateway_url: Optional[str] = None) -> None:
        """
        Push metrics to Prometheus Pushgateway.
        
        Args:
            gateway_url: Optional gateway URL (uses config if not provided)
        """
        if not PROMETHEUS_AVAILABLE:
            self.logger.warning("Prometheus not available, cannot push metrics")
            return
        
        gateway_url = gateway_url or self.pushgateway_url
        if not gateway_url:
            self.logger.warning("No Pushgateway URL configured")
            return
        
        try:
            push_to_gateway(
                gateway_url,
                job=self.job_name,
                registry=self.registry.registry,
                grouping_key={"instance": self.instance_id}
            )
            self.logger.debug(f"Pushed metrics to gateway: {gateway_url}")
        except Exception as e:
            self.logger.error(f"Failed to push metrics to gateway: {str(e)}")
    
    def publish(self) -> None:
        """
        Publish metrics to configured backends.
        
        This method handles publishing metrics to various backends like
        Prometheus Pushgateway, CloudWatch, etc.
        """
        try:
            # Push to Prometheus Pushgateway if configured
            if self.pushgateway_url:
                self.push_to_gateway()
            
            # Add other publishing backends here
            # e.g., CloudWatch, DataDog, etc.
            
            self.logger.debug("Metrics published successfully")
            
        except Exception as e:
            self.logger.error(f"Failed to publish metrics: {str(e)}")


class MetricTimer:
    """Context manager for timing operations with metrics."""
    
    def __init__(self, collector: MetricsCollector, name: str, tags: Optional[Dict[str, str]] = None):
        self.collector = collector
        self.name = name
        self.tags = tags or {}
        self.start_time = None
    
    def __enter__(self):
        self.start_time = time.time()
        return self
    
    def __exit__(self, exc_type, exc_val, exc_tb):
        if self.start_time:
            duration = time.time() - self.start_time
            
            # Add status tag based on exception
            tags = self.tags.copy()
            tags["status"] = "error" if exc_type else "success"
            
            self.collector.histogram(f"{self.name}_duration_seconds", duration, tags)


# Global metrics collector instance
_global_collector: Optional[MetricsCollector] = None


def get_metrics_collector(config: Optional[Dict[str, Any]] = None) -> MetricsCollector:
    """
    Get the global metrics collector instance.
    
    Args:
        config: Optional configuration for the collector
        
    Returns:
        MetricsCollector instance
    """
    global _global_collector
    
    if _global_collector is None:
        _global_collector = MetricsCollector(config)
    
    return _global_collector


def configure_metrics(config: Dict[str, Any]) -> None:
    """
    Configure the global metrics collector.
    
    Args:
        config: Metrics configuration dictionary
    """
    global _global_collector
    _global_collector = MetricsCollector(config)