"""
Unit tests for the metrics module.
"""

import pytest
import time
import threading
from unittest.mock import patch, MagicMock, call
from typing import Dict, Any
from collections import deque

from igniteflow_core.metrics import (
    MetricDefinition, MetricValue, MetricsRegistry, MetricsCollector,
    MetricTimer, get_metrics_collector, configure_metrics
)


class TestMetricDefinition:
    """Test cases for MetricDefinition."""
    
    def test_init_basic(self):
        """Test basic MetricDefinition initialization."""
        definition = MetricDefinition(
            name="test_metric",
            metric_type="counter",
            description="Test metric description"
        )
        
        assert definition.name == "test_metric"
        assert definition.metric_type == "counter"
        assert definition.description == "Test metric description"
        assert definition.labels == []
        assert definition.buckets is None
    
    def test_init_with_labels_and_buckets(self):
        """Test MetricDefinition with labels and buckets."""
        definition = MetricDefinition(
            name="test_histogram",
            metric_type="histogram",
            description="Test histogram",
            labels=["method", "status"],
            buckets=[0.1, 0.5, 1.0, 5.0]
        )
        
        assert definition.labels == ["method", "status"]
        assert definition.buckets == [0.1, 0.5, 1.0, 5.0]


class TestMetricValue:
    """Test cases for MetricValue."""
    
    def test_init_basic(self):
        """Test basic MetricValue initialization."""
        value = MetricValue(name="test_metric", value=42.5)
        
        assert value.name == "test_metric"
        assert value.value == 42.5
        assert value.labels == {}
        assert isinstance(value.timestamp, float)
    
    def test_init_with_labels(self):
        """Test MetricValue with labels."""
        labels = {"method": "GET", "status": "200"}
        value = MetricValue(
            name="http_requests",
            value=1,
            labels=labels
        )
        
        assert value.labels == labels


class TestMetricsRegistry:
    """Test cases for MetricsRegistry."""
    
    def test_init(self):
        """Test MetricsRegistry initialization."""
        registry = MetricsRegistry()
        
        assert registry.metrics == {}
        assert registry.definitions == {}
        assert registry.logger is not None
    
    @patch('igniteflow_core.metrics.PROMETHEUS_AVAILABLE', True)
    @patch('igniteflow_core.metrics.Counter')
    def test_register_counter_metric(self, mock_counter):
        """Test registering a counter metric."""
        registry = MetricsRegistry()
        definition = MetricDefinition(
            name="test_counter",
            metric_type="counter",
            description="Test counter",
            labels=["method"]
        )
        
        registry.register_metric(definition)
        
        assert "test_counter" in registry.definitions
        mock_counter.assert_called_once()
    
    @patch('igniteflow_core.metrics.PROMETHEUS_AVAILABLE', True)
    @patch('igniteflow_core.metrics.Gauge')
    def test_register_gauge_metric(self, mock_gauge):
        """Test registering a gauge metric."""
        registry = MetricsRegistry()
        definition = MetricDefinition(
            name="test_gauge",
            metric_type="gauge",
            description="Test gauge"
        )
        
        registry.register_metric(definition)
        
        mock_gauge.assert_called_once()
    
    @patch('igniteflow_core.metrics.PROMETHEUS_AVAILABLE', True)
    @patch('igniteflow_core.metrics.Histogram')
    def test_register_histogram_metric(self, mock_histogram):
        """Test registering a histogram metric."""
        registry = MetricsRegistry()
        definition = MetricDefinition(
            name="test_histogram",
            metric_type="histogram",
            description="Test histogram",
            buckets=[0.1, 0.5, 1.0]
        )
        
        registry.register_metric(definition)
        
        mock_histogram.assert_called_once()
    
    @patch('igniteflow_core.metrics.PROMETHEUS_AVAILABLE', True)
    @patch('igniteflow_core.metrics.Summary')
    def test_register_summary_metric(self, mock_summary):
        """Test registering a summary metric."""
        registry = MetricsRegistry()
        definition = MetricDefinition(
            name="test_summary",
            metric_type="summary",
            description="Test summary"
        )
        
        registry.register_metric(definition)
        
        mock_summary.assert_called_once()
    
    @patch('igniteflow_core.metrics.PROMETHEUS_AVAILABLE', True)
    def test_register_invalid_metric_type(self):
        """Test registering metric with invalid type."""
        registry = MetricsRegistry()
        definition = MetricDefinition(
            name="test_invalid",
            metric_type="invalid_type",
            description="Invalid metric"
        )
        
        with pytest.raises(ValueError, match="Unknown metric type"):
            registry.register_metric(definition)
    
    @patch('igniteflow_core.metrics.PROMETHEUS_AVAILABLE', False)
    def test_register_metric_no_prometheus(self):
        """Test registering metric when Prometheus is not available."""
        registry = MetricsRegistry()
        definition = MetricDefinition(
            name="test_metric",
            metric_type="counter",
            description="Test metric"
        )
        
        # Should not raise exception
        registry.register_metric(definition)
        assert "test_metric" not in registry.metrics
    
    def test_get_metric_existing(self):
        """Test getting an existing metric."""
        registry = MetricsRegistry()
        mock_metric = MagicMock()
        registry.metrics["test_metric"] = mock_metric
        
        result = registry.get_metric("test_metric")
        assert result == mock_metric
    
    def test_get_metric_nonexistent(self):
        """Test getting a non-existent metric."""
        registry = MetricsRegistry()
        
        result = registry.get_metric("nonexistent")
        assert result is None


class TestMetricsCollector:
    """Test cases for MetricsCollector."""
    
    def test_init_default_config(self):
        """Test MetricsCollector initialization with default config."""
        collector = MetricsCollector()
        
        assert collector.config == {}
        assert collector.enabled is True
        assert collector.registry is not None
        assert isinstance(collector.metric_values, dict)
        assert isinstance(collector.timers, dict)
    
    def test_init_custom_config(self):
        """Test MetricsCollector initialization with custom config."""
        config = {
            "enabled": False,
            "pushgateway_url": "http://pushgateway:9091",
            "job_name": "test_job"
        }
        
        collector = MetricsCollector(config)
        
        assert collector.config == config
        assert collector.enabled is False
        assert collector.pushgateway_url == "http://pushgateway:9091"
        assert collector.job_name == "test_job"
    
    @patch('igniteflow_core.metrics.start_http_server')
    def test_init_with_metrics_port(self, mock_start_server):
        """Test initialization with metrics server port."""
        config = {"port": 8080}
        
        with patch('igniteflow_core.metrics.PROMETHEUS_AVAILABLE', True):
            collector = MetricsCollector(config)
        
        mock_start_server.assert_called_once_with(8080, registry=collector.registry.registry)
    
    def test_increment_enabled(self):
        """Test increment method when enabled."""
        collector = MetricsCollector({"enabled": True})
        
        collector.increment("test_counter", 2.0, {"method": "GET"})
        
        # Check that metric value was stored
        assert "test_counter" in collector.metric_values
        assert len(collector.metric_values["test_counter"]) == 1
        
        stored_value = collector.metric_values["test_counter"][0]
        assert stored_value.value == 2.0
        assert stored_value.labels == {"method": "GET"}
    
    def test_increment_disabled(self):
        """Test increment method when disabled."""
        collector = MetricsCollector({"enabled": False})
        
        collector.increment("test_counter", 1.0)
        
        # No metrics should be stored
        assert len(collector.metric_values) == 0
    
    @patch('igniteflow_core.metrics.PROMETHEUS_AVAILABLE', True)
    def test_increment_with_prometheus_metric(self):
        """Test increment with Prometheus metric available."""
        collector = MetricsCollector()
        mock_metric = MagicMock()
        collector.registry.metrics["test_counter"] = mock_metric
        
        collector.increment("test_counter", 1.0, {"method": "GET"})
        
        mock_metric.labels.assert_called_once_with(method="GET")
        mock_metric.labels().inc.assert_called_once_with(1.0)
    
    def test_gauge(self):
        """Test gauge method."""
        collector = MetricsCollector()
        
        collector.gauge("test_gauge", 42.5, {"instance": "server1"})
        
        assert "test_gauge" in collector.metric_values
        stored_value = collector.metric_values["test_gauge"][0]
        assert stored_value.value == 42.5
        assert stored_value.labels == {"instance": "server1"}
    
    def test_histogram(self):
        """Test histogram method."""
        collector = MetricsCollector()
        
        collector.histogram("test_histogram", 1.23)
        
        assert "test_histogram" in collector.metric_values
        stored_value = collector.metric_values["test_histogram"][0]
        assert stored_value.value == 1.23
    
    def test_timer_context_manager(self):
        """Test timer context manager."""
        collector = MetricsCollector()
        
        with collector.timer("test_timer") as timer:
            assert isinstance(timer, MetricTimer)
            time.sleep(0.01)  # Small delay
        
        # Check that histogram metric was recorded
        assert "test_timer_duration_seconds" in collector.metric_values
    
    def test_start_stop_timer(self):
        """Test manual timer start/stop."""
        collector = MetricsCollector()
        
        collector.start_timer("manual_timer")
        assert "manual_timer" in collector.timers
        
        time.sleep(0.01)  # Small delay
        duration = collector.stop_timer("manual_timer")
        
        assert isinstance(duration, float)
        assert duration > 0
        assert "manual_timer" not in collector.timers
        assert "manual_timer_duration_seconds" in collector.metric_values
    
    def test_stop_timer_not_started(self):
        """Test stopping a timer that wasn't started."""
        collector = MetricsCollector()
        
        duration = collector.stop_timer("nonexistent_timer")
        assert duration == 0.0
    
    def test_record_job_metrics(self):
        """Test recording job metrics."""
        collector = MetricsCollector()
        
        collector.record_job_metrics(
            job_name="test_job",
            status="success",
            duration=123.45,
            records_processed=1000,
            environment="test"
        )
        
        # Check that metrics were recorded
        assert "igniteflow_job_total" in collector.metric_values
        assert "igniteflow_job_duration_seconds" in collector.metric_values
        assert "igniteflow_records_processed_total" in collector.metric_values
    
    def test_record_model_metrics(self):
        """Test recording model metrics."""
        collector = MetricsCollector()
        
        collector.record_model_metrics(
            model_name="fraud_detection",
            version="v1.0",
            accuracy=0.95,
            precision=0.92,
            recall=0.89,
            f1_score=0.905
        )
        
        # Check that all metrics were recorded
        assert "igniteflow_model_accuracy" in collector.metric_values
        assert "igniteflow_model_precision" in collector.metric_values
        assert "igniteflow_model_recall" in collector.metric_values
        assert "igniteflow_model_f1_score" in collector.metric_values
    
    def test_record_data_quality_metrics(self):
        """Test recording data quality metrics."""
        collector = MetricsCollector()
        
        collector.record_data_quality_metrics(
            dataset="transactions",
            rule="completeness",
            score=0.98
        )
        
        assert "igniteflow_data_quality_score" in collector.metric_values
    
    def test_record_spark_metrics(self):
        """Test recording Spark metrics."""
        collector = MetricsCollector()
        
        collector.record_spark_metrics(
            app_id="app-123",
            active_executors=4,
            total_cores=16,
            memory_usage=8589934592  # 8GB in bytes
        )
        
        assert "igniteflow_spark_executors_active" in collector.metric_values
        assert "igniteflow_spark_cores_total" in collector.metric_values
        assert "igniteflow_memory_usage_bytes" in collector.metric_values
    
    def test_get_metrics_snapshot(self):
        """Test getting metrics snapshot."""
        collector = MetricsCollector()
        
        # Add some metrics
        collector.gauge("test_gauge", 42.0)
        collector.increment("test_counter", 5.0)
        
        snapshot = collector.get_metrics_snapshot()
        
        assert "timestamp" in snapshot
        assert "metrics" in snapshot
        assert "test_gauge" in snapshot["metrics"]
        assert "test_counter" in snapshot["metrics"]
        
        # Check metric structure
        gauge_metric = snapshot["metrics"]["test_gauge"]
        assert gauge_metric["value"] == 42.0
        assert "labels" in gauge_metric
        assert "timestamp" in gauge_metric
    
    @patch('igniteflow_core.metrics.PROMETHEUS_AVAILABLE', True)
    @patch('igniteflow_core.metrics.generate_latest')
    def test_export_prometheus_metrics(self, mock_generate_latest):
        """Test exporting Prometheus metrics."""
        mock_generate_latest.return_value = b"# Prometheus metrics\n"
        
        collector = MetricsCollector()
        result = collector.export_prometheus_metrics()
        
        assert result == "# Prometheus metrics\n"
        mock_generate_latest.assert_called_once()
    
    @patch('igniteflow_core.metrics.PROMETHEUS_AVAILABLE', False)
    def test_export_prometheus_metrics_not_available(self):
        """Test exporting metrics when Prometheus not available."""
        collector = MetricsCollector()
        result = collector.export_prometheus_metrics()
        
        assert "Prometheus not available" in result
    
    @patch('igniteflow_core.metrics.PROMETHEUS_AVAILABLE', True)
    @patch('igniteflow_core.metrics.push_to_gateway')
    def test_push_to_gateway(self, mock_push):
        """Test pushing metrics to Pushgateway."""
        config = {"pushgateway_url": "http://pushgateway:9091"}
        collector = MetricsCollector(config)
        
        collector.push_to_gateway()
        
        mock_push.assert_called_once()
    
    @patch('igniteflow_core.metrics.PROMETHEUS_AVAILABLE', False)
    def test_push_to_gateway_not_available(self):
        """Test pushing when Prometheus not available."""
        collector = MetricsCollector()
        
        # Should not raise exception
        collector.push_to_gateway()
    
    def test_push_to_gateway_no_url(self):
        """Test pushing when no gateway URL is configured."""
        collector = MetricsCollector()
        
        # Should not raise exception
        collector.push_to_gateway()
    
    @patch('igniteflow_core.metrics.MetricsCollector.push_to_gateway')
    def test_publish(self, mock_push):
        """Test publishing metrics."""
        config = {"pushgateway_url": "http://pushgateway:9091"}
        collector = MetricsCollector(config)
        
        collector.publish()
        
        mock_push.assert_called_once()
    
    def test_thread_safety(self):
        """Test thread safety of metrics collection."""
        collector = MetricsCollector()
        
        def worker():
            for i in range(100):
                collector.increment("thread_test", 1.0)
                collector.gauge("thread_gauge", float(i))
        
        # Run multiple threads
        threads = [threading.Thread(target=worker) for _ in range(5)]
        for thread in threads:
            thread.start()
        for thread in threads:
            thread.join()
        
        # Check that all increments were recorded
        assert len(collector.metric_values["thread_test"]) == 500  # 5 threads * 100 increments


class TestMetricTimer:
    """Test cases for MetricTimer."""
    
    def test_context_manager_success(self):
        """Test MetricTimer context manager on success."""
        collector = MetricsCollector()
        
        with MetricTimer(collector, "test_operation"):
            time.sleep(0.01)
        
        # Check that metric was recorded with success status
        assert "test_operation_duration_seconds" in collector.metric_values
        
        stored_value = collector.metric_values["test_operation_duration_seconds"][0]
        assert stored_value.labels["status"] == "success"
    
    def test_context_manager_with_exception(self):
        """Test MetricTimer context manager with exception."""
        collector = MetricsCollector()
        
        try:
            with MetricTimer(collector, "test_operation"):
                raise ValueError("Test exception")
        except ValueError:
            pass
        
        # Check that metric was recorded with error status
        assert "test_operation_duration_seconds" in collector.metric_values
        
        stored_value = collector.metric_values["test_operation_duration_seconds"][0]
        assert stored_value.labels["status"] == "error"


class TestGlobalFunctions:
    """Test cases for global functions."""
    
    def test_get_metrics_collector_default(self):
        """Test getting global metrics collector with default config."""
        # Reset global collector
        import igniteflow_core.metrics
        igniteflow_core.metrics._global_collector = None
        
        collector = get_metrics_collector()
        
        assert isinstance(collector, MetricsCollector)
        assert collector.config == {}
    
    def test_get_metrics_collector_with_config(self):
        """Test getting global metrics collector with config."""
        # Reset global collector
        import igniteflow_core.metrics
        igniteflow_core.metrics._global_collector = None
        
        config = {"enabled": False}
        collector = get_metrics_collector(config)
        
        assert collector.enabled is False
    
    def test_get_metrics_collector_singleton(self):
        """Test that get_metrics_collector returns singleton."""
        collector1 = get_metrics_collector()
        collector2 = get_metrics_collector()
        
        assert collector1 is collector2
    
    def test_configure_metrics(self):
        """Test configuring global metrics collector."""
        config = {"job_name": "test_job"}
        configure_metrics(config)
        
        collector = get_metrics_collector()
        assert collector.job_name == "test_job"
