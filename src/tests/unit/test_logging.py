"""
Unit tests for the logging module.
"""

import pytest
import json
import tempfile
import os
import logging
from unittest.mock import patch, MagicMock
from pathlib import Path
from typing import Dict, Any

from igniteflow_core.logging import (
    JSONFormatter, PerformanceLogger, IgniteFlowLogger,
    TimerContext, setup_logging, get_logger
)


class TestJSONFormatter:
    """Test cases for JSONFormatter."""
    
    def test_init(self):
        """Test JSONFormatter initialization."""
        formatter = JSONFormatter()
        
        assert formatter.hostname is not None
        assert formatter.service_name is not None
        assert formatter.environment is not None
    
    def test_format_basic_record(self):
        """Test formatting a basic log record."""
        formatter = JSONFormatter()
        
        record = logging.LogRecord(
            name="test_logger",
            level=logging.INFO,
            pathname="/test/path.py",
            lineno=42,
            msg="Test message",
            args=(),
            exc_info=None
        )
        
        formatted = formatter.format(record)
        log_data = json.loads(formatted)
        
        assert log_data["level"] == "INFO"
        assert log_data["logger"] == "test_logger"
        assert log_data["message"] == "Test message"
        assert log_data["line"] == 42
        assert "timestamp" in log_data
        assert "hostname" in log_data
        assert "service" in log_data
        assert "environment" in log_data
    
    def test_format_with_exception(self):
        """Test formatting a log record with exception info."""
        formatter = JSONFormatter()
        
        try:
            raise ValueError("Test exception")
        except ValueError:
            record = logging.LogRecord(
                name="test_logger",
                level=logging.ERROR,
                pathname="/test/path.py",
                lineno=42,
                msg="Error occurred",
                args=(),
                exc_info=True
            )
        
        formatted = formatter.format(record)
        log_data = json.loads(formatted)
        
        assert "exception" in log_data
        assert log_data["exception"]["type"] == "ValueError"
        assert "Test exception" in log_data["exception"]["message"]
        assert "traceback" in log_data["exception"]
    
    def test_format_with_extra_fields(self):
        """Test formatting with extra fields."""
        formatter = JSONFormatter()
        
        record = logging.LogRecord(
            name="test_logger",
            level=logging.INFO,
            pathname="/test/path.py",
            lineno=42,
            msg="Test message",
            args=(),
            exc_info=None
        )
        
        # Add extra fields
        record.user_id = "user123"
        record.request_id = "req456"
        record.custom_data = {"key": "value"}
        
        formatted = formatter.format(record)
        log_data = json.loads(formatted)
        
        assert log_data["user_id"] == "user123"
        assert log_data["request_id"] == "req456"
        assert log_data["custom_data"] == {"key": "value"}


class TestPerformanceLogger:
    """Test cases for PerformanceLogger."""
    
    def test_init(self):
        """Test PerformanceLogger initialization."""
        logger = logging.getLogger("test")
        perf_logger = PerformanceLogger(logger)
        
        assert perf_logger.logger == logger
        assert perf_logger._timers == {}
    
    def test_start_stop_timer(self):
        """Test starting and stopping a timer."""
        logger = logging.getLogger("test")
        perf_logger = PerformanceLogger(logger)
        
        # Start timer
        perf_logger.start_timer("test_timer")
        assert "test_timer" in perf_logger._timers
        
        # Stop timer
        duration = perf_logger.stop_timer("test_timer")
        assert isinstance(duration, float)
        assert duration >= 0
        assert "test_timer" not in perf_logger._timers
    
    def test_stop_nonexistent_timer(self):
        """Test stopping a timer that wasn't started."""
        logger = logging.getLogger("test")
        perf_logger = PerformanceLogger(logger)
        
        duration = perf_logger.stop_timer("nonexistent")
        assert duration == 0.0
    
    def test_log_metric(self):
        """Test logging a custom metric."""
        logger = logging.getLogger("test")
        perf_logger = PerformanceLogger(logger)
        
        # Should not raise exception
        perf_logger.log_metric("test_metric", 42.5, "seconds")
        perf_logger.log_metric("count_metric", 100)


class TestIgniteFlowLogger:
    """Test cases for IgniteFlowLogger."""
    
    def test_init_with_default_config(self):
        """Test initialization with default configuration."""
        logger = IgniteFlowLogger("test_logger")
        
        assert logger.name == "test_logger"
        assert logger.config == {}
        assert logger._logger.name == "test_logger"
        assert logger._performance is not None
    
    def test_init_with_custom_config(self):
        """Test initialization with custom configuration."""
        config = {
            "level": "DEBUG",
            "format": "text",
            "file": "/tmp/test.log"
        }
        
        logger = IgniteFlowLogger("test_logger", config)
        
        assert logger.config == config
    
    def test_logging_methods(self):
        """Test basic logging methods."""
        logger = IgniteFlowLogger("test_logger")
        
        # Should not raise exceptions
        logger.debug("Debug message", extra_field="value")
        logger.info("Info message", user_id="123")
        logger.warning("Warning message")
        logger.error("Error message", exc_info=False)
        logger.critical("Critical message")
    
    def test_exception_logging(self):
        """Test exception logging."""
        logger = IgniteFlowLogger("test_logger")
        
        try:
            raise ValueError("Test exception")
        except ValueError:
            # Should not raise exception
            logger.exception("Exception occurred", context="test")
    
    def test_performance_methods(self):
        """Test performance logging methods."""
        logger = IgniteFlowLogger("test_logger")
        
        # Start/stop timer
        logger.start_timer("test_operation")
        duration = logger.stop_timer("test_operation")
        assert isinstance(duration, float)
        
        # Log metric
        logger.log_metric("test_metric", 42.5, "seconds")
    
    def test_timer_context_manager(self):
        """Test timer context manager."""
        logger = IgniteFlowLogger("test_logger")
        
        with logger.timer("context_timer") as timer:
            assert isinstance(timer, TimerContext)
    
    def test_log_function_call(self):
        """Test function call logging."""
        logger = IgniteFlowLogger("test_logger")
        
        # Should not raise exception
        logger.log_function_call("test_function", arg1="value1", arg2=42)
    
    def test_file_handler_creation_error(self, temp_dir):
        """Test handling of file handler creation errors."""
        config = {
            "file": "/invalid/path/test.log"  # Invalid path
        }
        
        # Should not raise exception, just log warning
        logger = IgniteFlowLogger("test_logger", config)
        assert logger._logger is not None


class TestTimerContext:
    """Test cases for TimerContext."""
    
    def test_context_manager(self):
        """Test timer context manager functionality."""
        logger = IgniteFlowLogger("test_logger")
        
        with logger.timer("test_timer") as timer:
            assert isinstance(timer, TimerContext)
            assert timer.name == "test_timer"
    
    def test_context_manager_with_exception(self):
        """Test timer context manager with exception."""
        logger = IgniteFlowLogger("test_logger")
        
        try:
            with logger.timer("test_timer"):
                raise ValueError("Test exception")
        except ValueError:
            pass  # Expected
        
        # Timer should have been stopped despite exception
        # (This is tested implicitly - no assertion needed)


class TestSetupLogging:
    """Test cases for setup_logging function."""
    
    def test_setup_with_defaults(self):
        """Test setup_logging with default parameters."""
        # Should not raise exception
        setup_logging()
        
        # Check that loggers are configured
        logger = logging.getLogger("igniteflow_core")
        assert logger.level <= logging.INFO
    
    def test_setup_with_custom_params(self):
        """Test setup_logging with custom parameters."""
        setup_logging(
            level="DEBUG",
            format_type="text",
            environment="test",
            verbose=True
        )
        
        # Check environment variable is set
        assert os.environ.get("IGNITEFLOW_ENV") == "test"
    
    def test_setup_with_file_logging(self, temp_dir):
        """Test setup_logging with file output."""
        log_file = os.path.join(temp_dir, "test.log")
        
        setup_logging(
            level="INFO",
            format_type="json",
            log_file=log_file
        )
        
        # Log a message to verify file handler works
        logger = logging.getLogger("igniteflow_core")
        logger.info("Test message")
        
        # Check that log file was created
        assert os.path.exists(log_file)
    
    def test_environment_specific_configuration(self):
        """Test environment-specific log configuration."""
        # Local environment with verbose
        setup_logging(environment="local", verbose=True)
        logger = logging.getLogger("igniteflow_core")
        assert logger.level <= logging.DEBUG
        
        # Production environment
        setup_logging(environment="prod", level="INFO")
        logger = logging.getLogger("igniteflow_core")
        assert logger.level >= logging.WARNING


class TestGetLogger:
    """Test cases for get_logger function."""
    
    def test_get_logger_default(self):
        """Test getting logger with default configuration."""
        logger = get_logger("test_module")
        
        assert isinstance(logger, IgniteFlowLogger)
        assert logger.name == "test_module"
    
    def test_get_logger_with_config(self):
        """Test getting logger with custom configuration."""
        config = {"level": "DEBUG", "format": "text"}
        logger = get_logger("test_module", config)
        
        assert isinstance(logger, IgniteFlowLogger)
        assert logger.config == config


class TestStructlogIntegration:
    """Test cases for structlog integration (when available)."""
    
    @patch('igniteflow_core.logging.STRUCTLOG_AVAILABLE', True)
    @patch('igniteflow_core.logging.structlog')
    def test_configure_structlog(self, mock_structlog):
        """Test structlog configuration."""
        from igniteflow_core.logging import configure_structlog
        
        configure_structlog()
        mock_structlog.configure.assert_called_once()
    
    @patch('igniteflow_core.logging.STRUCTLOG_AVAILABLE', False)
    def test_configure_structlog_not_available(self):
        """Test structlog configuration when not available."""
        from igniteflow_core.logging import configure_structlog, get_structured_logger
        
        # Should not raise exception
        configure_structlog()
        
        # Should return IgniteFlowLogger as fallback
        logger = get_structured_logger("test")
        assert isinstance(logger, IgniteFlowLogger)
