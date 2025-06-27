"""
IgniteFlow Exception Hierarchy.

This module defines the exception hierarchy for the IgniteFlow framework,
providing structured error handling with proper context and traceability.
"""

from typing import Optional, Dict, Any


class IgniteFlowError(Exception):
    """
    Base exception class for all IgniteFlow errors.
    
    This exception provides structured error handling with optional
    error codes, context information, and proper inheritance.
    
    Attributes:
        message: Human-readable error message
        error_code: Optional error code for programmatic handling
        context: Optional dictionary with additional error context
    """
    
    def __init__(
        self, 
        message: str, 
        error_code: Optional[str] = None,
        context: Optional[Dict[str, Any]] = None
    ) -> None:
        """
        Initialize IgniteFlow error.
        
        Args:
            message: Human-readable error message
            error_code: Optional error code for programmatic handling
            context: Optional dictionary with additional error context
        """
        super().__init__(message)
        self.message = message
        self.error_code = error_code
        self.context = context or {}
    
    def __str__(self) -> str:
        """Return string representation of the error."""
        error_str = self.message
        if self.error_code:
            error_str = f"[{self.error_code}] {error_str}"
        if self.context:
            context_str = ", ".join(f"{k}={v}" for k, v in self.context.items())
            error_str = f"{error_str} (Context: {context_str})"
        return error_str


class ConfigurationError(IgniteFlowError):
    """
    Exception raised for configuration-related errors.
    
    This includes missing configuration files, invalid configuration
    values, or configuration parsing errors.
    """
    pass


class SparkError(IgniteFlowError):
    """
    Exception raised for Spark-related errors.
    
    This includes Spark session creation failures, job execution
    errors, or resource management issues.
    """
    pass


class JobError(IgniteFlowError):
    """
    Exception raised for job execution errors.
    
    This includes job loading failures, runtime errors during
    job execution, or job validation failures.
    """
    pass


class DataError(IgniteFlowError):
    """
    Exception raised for data-related errors.
    
    This includes data validation failures, data format errors,
    or data pipeline processing errors.
    """
    pass


class ValidationError(IgniteFlowError):
    """
    Exception raised for validation failures.
    
    This includes schema validation errors, data quality check
    failures, or business rule validation errors.
    """
    pass