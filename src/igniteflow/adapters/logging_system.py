"""
Structured Logging System for IgniteFlow.

This module provides comprehensive logging capabilities including:
- Structured logging with JSON formatting
- Environment-specific log levels
- Performance logging
- Error tracking and alerting
- Integration with observability systems
"""

import logging
import logging.config
import os
import sys
import json
import time
from typing import Dict, Any, Optional, Union
from datetime import datetime
from pathlib import Path
import traceback

try:
    import structlog
    STRUCTLOG_AVAILABLE = True
except ImportError:
    STRUCTLOG_AVAILABLE = False


class JSONFormatter(logging.Formatter):
    """
    Custom JSON formatter for structured logging.
    
    This formatter outputs log records as JSON objects, making them
    easily parseable by log aggregation systems like ELK, Splunk, etc.
    """
    
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.hostname = os.getenv('HOSTNAME', 'unknown')
        self.service_name = os.getenv('SERVICE_NAME', 'igniteflow')
        self.environment = os.getenv('IGNITEFLOW_ENV', 'local')
    
    def format(self, record: logging.LogRecord) -> str:
        """
        Format log record as JSON.
        
        Args:
            record: LogRecord to format
            
        Returns:
            JSON string representation of the log record
        """
        # Base log entry
        log_entry = {
            'timestamp': datetime.fromtimestamp(record.created).isoformat(),
            'level': record.levelname,
            'logger': record.name,
            'message': record.getMessage(),
            'module': record.module,
            'function': record.funcName,
            'line': record.lineno,
            'thread': record.thread,
            'thread_name': record.threadName,
            'process': record.process,
            'hostname': self.hostname,
            'service': self.service_name,
            'environment': self.environment
        }
        
        # Add exception information if present
        if record.exc_info:
            log_entry['exception'] = {
                'type': record.exc_info[0].__name__ if record.exc_info[0] else None,
                'message': str(record.exc_info[1]) if record.exc_info[1] else None,
                'traceback': traceback.format_exception(*record.exc_info)
            }
        
        # Add extra fields from record
        for key, value in record.__dict__.items():
            if key not in log_entry and not key.startswith('_'):
                # Convert non-serializable objects to strings
                try:
                    json.dumps(value)
                    log_entry[key] = value
                except (TypeError, ValueError):
                    log_entry[key] = str(value)
        
        return json.dumps(log_entry, default=str)


class PerformanceLogger:
    """
    Performance logging utility for tracking execution times and metrics.
    """
    
    def __init__(self, logger: logging.Logger):
        self.logger = logger
        self._timers: Dict[str, float] = {}
    
    def start_timer(self, name: str) -> None:
        """Start a named timer."""
        self._timers[name] = time.time()
        self.logger.debug(f"Started timer: {name}")
    
    def stop_timer(self, name: str) -> float:
        """
        Stop a named timer and log the duration.
        
        Returns:
            Duration in seconds
        """
        if name not in self._timers:
            self.logger.warning(f"Timer '{name}' was not started")
            return 0.0
        
        duration = time.time() - self._timers[name]
        del self._timers[name]
        
        self.logger.info(
            f"Timer completed: {name}",
            extra={
                'timer_name': name,
                'duration_seconds': duration,
                'performance_metric': True
            }
        )
        
        return duration
    
    def log_metric(self, name: str, value: Union[int, float], unit: str = None) -> None:
        """
        Log a custom metric.
        
        Args:
            name: Metric name
            value: Metric value
            unit: Optional unit (e.g., 'seconds', 'bytes', 'count')
        """
        extra = {
            'metric_name': name,
            'metric_value': value,
            'performance_metric': True
        }
        
        if unit:
            extra['metric_unit'] = unit
        
        self.logger.info(f"Metric: {name} = {value} {unit or ''}", extra=extra)


class IgniteFlowLogger:
    """
    Main logger class for IgniteFlow applications.
    
    This class provides a centralized logging interface with:
    - Structured logging
    - Performance tracking
    - Error handling
    - Environment-specific configuration
    """
    
    def __init__(self, name: str, config: Optional[Dict[str, Any]] = None):
        self.name = name
        self.config = config or {}
        self._logger = logging.getLogger(name)
        self._performance = PerformanceLogger(self._logger)
        
        # Configure logger if not already configured
        if not self._logger.handlers:
            self._configure_logger()
    
    def _configure_logger(self) -> None:
        """Configure the logger with appropriate handlers and formatters."""
        log_level = self.config.get('level', 'INFO').upper()
        log_format = self.config.get('format', 'json')
        
        # Set log level
        self._logger.setLevel(getattr(logging, log_level))
        
        # Create console handler
        console_handler = logging.StreamHandler(sys.stdout)
        console_handler.setLevel(getattr(logging, log_level))
        
        # Set formatter
        if log_format == 'json':
            formatter = JSONFormatter()
        else:
            formatter = logging.Formatter(
                '%(asctime)s - %(name)s - %(levelname)s - %(message)s'
            )
        
        console_handler.setFormatter(formatter)
        self._logger.addHandler(console_handler)
        
        # Add file handler if specified
        log_file = self.config.get('file')
        if log_file:
            self._add_file_handler(log_file, formatter)
    
    def _add_file_handler(self, log_file: str, formatter: logging.Formatter) -> None:
        """Add file handler for logging to file."""
        try:
            # Ensure log directory exists
            log_path = Path(log_file)
            log_path.parent.mkdir(parents=True, exist_ok=True)
            
            file_handler = logging.FileHandler(log_file)
            file_handler.setFormatter(formatter)
            self._logger.addHandler(file_handler)
            
        except Exception as e:
            self._logger.warning(f"Failed to create file handler: {str(e)}")
    
    def debug(self, message: str, **kwargs) -> None:
        """Log debug message."""
        self._logger.debug(message, extra=kwargs)
    
    def info(self, message: str, **kwargs) -> None:
        """Log info message."""
        self._logger.info(message, extra=kwargs)
    
    def warning(self, message: str, **kwargs) -> None:
        """Log warning message."""
        self._logger.warning(message, extra=kwargs)
    
    def error(self, message: str, exc_info: bool = False, **kwargs) -> None:
        """Log error message."""
        self._logger.error(message, exc_info=exc_info, extra=kwargs)
    
    def critical(self, message: str, exc_info: bool = False, **kwargs) -> None:
        """Log critical message."""
        self._logger.critical(message, exc_info=exc_info, extra=kwargs)
    
    def exception(self, message: str, **kwargs) -> None:
        """Log exception with traceback."""
        self._logger.exception(message, extra=kwargs)
    
    # Performance logging methods
    def start_timer(self, name: str) -> None:
        """Start a performance timer."""
        self._performance.start_timer(name)
    
    def stop_timer(self, name: str) -> float:
        """Stop a performance timer and return duration."""
        return self._performance.stop_timer(name)
    
    def log_metric(self, name: str, value: Union[int, float], unit: str = None) -> None:
        """Log a custom metric."""
        self._performance.log_metric(name, value, unit)
    
    # Context managers for performance tracking
    def timer(self, name: str):
        """Context manager for timing code blocks."""
        return TimerContext(self, name)
    
    def log_function_call(self, func_name: str, **kwargs) -> None:
        """Log function call with parameters."""
        self.debug(
            f"Function called: {func_name}",
            extra={
                'function_name': func_name,
                'function_args': kwargs,
                'event_type': 'function_call'
            }
        )


class TimerContext:
    """Context manager for performance timing."""
    
    def __init__(self, logger: IgniteFlowLogger, name: str):
        self.logger = logger
        self.name = name
    
    def __enter__(self):
        self.logger.start_timer(self.name)
        return self
    
    def __exit__(self, exc_type, exc_val, exc_tb):
        duration = self.logger.stop_timer(self.name)
        
        if exc_type is not None:
            self.logger.error(
                f"Timer '{self.name}' completed with exception",
                exc_info=True,
                extra={
                    'timer_name': self.name,
                    'duration_seconds': duration,
                    'exception_occurred': True
                }
            )


def setup_logging(
    level: str = "INFO",
    format_type: str = "json",
    environment: str = "local",
    verbose: bool = False,
    log_file: Optional[str] = None
) -> None:
    """
    Set up logging configuration for IgniteFlow applications.
    
    Args:
        level: Log level (DEBUG, INFO, WARNING, ERROR, CRITICAL)
        format_type: Log format ('json' or 'text')
        environment: Environment name (local, dev, staging, prod)
        verbose: Enable verbose logging
        log_file: Optional file path for logging
    """
    # Environment-specific log level adjustment
    if environment == "local" and verbose:
        level = "DEBUG"
    elif environment == "prod":
        level = level or "WARNING"
    
    # Set environment variables for formatters
    os.environ['IGNITEFLOW_ENV'] = environment
    os.environ['SERVICE_NAME'] = os.environ.get('SERVICE_NAME', 'igniteflow')
    
    # Configure root logger
    logging_config = {
        'version': 1,
        'disable_existing_loggers': False,
        'formatters': {
            'json': {
                '()': JSONFormatter,
            },
            'text': {
                'format': '%(asctime)s - %(name)s - %(levelname)s - %(message)s',
                'datefmt': '%Y-%m-%d %H:%M:%S'
            }
        },
        'handlers': {
            'console': {
                'class': 'logging.StreamHandler',
                'level': level,
                'formatter': format_type,
                'stream': 'ext://sys.stdout'
            }
        },
        'loggers': {
            'igniteflow.adapters': {
                'level': level,
                'handlers': ['console'],
                'propagate': False
            },
            'igniteflow.application': {
                'level': level,
                'handlers': ['console'],
                'propagate': False
            }
        },
        'root': {
            'level': level,
            'handlers': ['console']
        }
    }
    
    # Add file handler if specified
    if log_file:
        logging_config['handlers']['file'] = {
            'class': 'logging.FileHandler',
            'level': level,
            'formatter': format_type,
            'filename': log_file,
            'mode': 'a'
        }
        
        # Add file handler to all loggers
        for logger_config in logging_config['loggers'].values():
            logger_config['handlers'].append('file')
        logging_config['root']['handlers'].append('file')
    
    # Apply configuration
    logging.config.dictConfig(logging_config)
    
    # Configure third-party loggers
    logging.getLogger('py4j').setLevel(logging.WARNING)
    logging.getLogger('pyspark').setLevel(logging.WARNING)
    logging.getLogger('boto3').setLevel(logging.WARNING)
    logging.getLogger('botocore').setLevel(logging.WARNING)
    logging.getLogger('urllib3').setLevel(logging.WARNING)
    
    # Log setup completion
    logger = logging.getLogger(__name__)
    logger.info(
        "Logging configured",
        extra={
            'log_level': level,
            'log_format': format_type,
            'environment': environment,
            'log_file': log_file
        }
    )


def get_logger(name: str, config: Optional[Dict[str, Any]] = None) -> IgniteFlowLogger:
    """
    Get a configured IgniteFlow logger instance.
    
    Args:
        name: Logger name (usually module name)
        config: Optional logging configuration
        
    Returns:
        Configured IgniteFlowLogger instance
    """
    return IgniteFlowLogger(name, config)


# Structured logging integration (if structlog is available)
if STRUCTLOG_AVAILABLE:
    def configure_structlog() -> None:
        """Configure structlog for enhanced structured logging."""
        structlog.configure(
            processors=[
                structlog.stdlib.filter_by_level,
                structlog.stdlib.add_logger_name,
                structlog.stdlib.add_log_level,
                structlog.stdlib.PositionalArgumentsFormatter(),
                structlog.processors.TimeStamper(fmt="iso"),
                structlog.processors.StackInfoRenderer(),
                structlog.processors.format_exc_info,
                structlog.processors.UnicodeDecoder(),
                structlog.processors.JSONRenderer()
            ],
            context_class=dict,
            logger_factory=structlog.stdlib.LoggerFactory(),
            wrapper_class=structlog.stdlib.BoundLogger,
            cache_logger_on_first_use=True,
        )
    
    def get_structured_logger(name: str) -> Any:
        """Get a structlog logger instance."""
        configure_structlog()
        return structlog.get_logger(name)

else:
    def configure_structlog() -> None:
        """Structlog not available - no-op."""
        pass
    
    def get_structured_logger(name: str) -> IgniteFlowLogger:
        """Fallback to IgniteFlow logger when structlog not available."""
        return get_logger(name)