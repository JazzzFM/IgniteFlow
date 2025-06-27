"""
Data Quality Validation Module for IgniteFlow.

This module provides comprehensive data quality validation capabilities:
- Schema validation
- Data completeness checks
- Data freshness validation
- Statistical outlier detection
- Custom validation rules
"""

import logging
from typing import Dict, Any, List, Optional, Callable
from datetime import datetime, timedelta
from dataclasses import dataclass

from pyspark.sql import DataFrame
from pyspark.sql import functions as F
from pyspark.sql.types import StructType, StructField

from .exceptions import ValidationError


@dataclass
class ValidationRule:
    """
    Data class representing a validation rule.
    
    Attributes:
        name: Name of the validation rule
        rule_type: Type of validation (completeness, uniqueness, etc.)
        columns: Columns to validate
        threshold: Threshold value for the rule
        condition: Optional custom condition function
    """
    name: str
    rule_type: str
    columns: List[str]
    threshold: float
    condition: Optional[Callable] = None


@dataclass
class ValidationResult:
    """
    Data class representing validation results.
    
    Attributes:
        rule_name: Name of the validation rule
        passed: Whether the validation passed
        actual_value: Actual measured value
        expected_threshold: Expected threshold
        message: Descriptive message about the result
    """
    rule_name: str
    passed: bool
    actual_value: float
    expected_threshold: float
    message: str


class DataQualityValidator:
    """
    Comprehensive data quality validation framework.
    
    This class provides various data quality checks including:
    - Schema validation against expected schemas
    - Completeness checks (null value detection)
    - Uniqueness validation
    - Data freshness checks
    - Statistical outlier detection
    - Custom validation rules
    """
    
    def __init__(self, config: Dict[str, Any]) -> None:
        """
        Initialize the data quality validator.
        
        Args:
            config: Data quality configuration dictionary
        """
        self.config = config
        self.logger = logging.getLogger(__name__)
        
        self.enabled = config.get("enabled", True)
        self.fail_on_error = config.get("fail_on_error", True)
        
        # Load validation rules from config
        self.rules = self._load_validation_rules(config.get("rules", []))
        
        self.logger.info(f"DataQualityValidator initialized with {len(self.rules)} rules")
    
    def _load_validation_rules(self, rules_config: List[Dict[str, Any]]) -> List[ValidationRule]:
        """
        Load validation rules from configuration.
        
        Args:
            rules_config: List of rule configurations
            
        Returns:
            List of ValidationRule objects
        """
        rules = []
        
        for rule_config in rules_config:
            rule = ValidationRule(
                name=rule_config["name"],
                rule_type=rule_config["type"],
                columns=rule_config.get("columns", []),
                threshold=rule_config.get("threshold", 0.0)
            )
            rules.append(rule)
        
        return rules
    
    def validate_schema(self, df: DataFrame, expected_schema_name: str) -> ValidationResult:
        """
        Validate DataFrame schema against expected schema.
        
        Args:
            df: DataFrame to validate
            expected_schema_name: Name of the expected schema configuration
            
        Returns:
            ValidationResult indicating if schema validation passed
        """
        try:
            expected_schema_config = self.config.get("schemas", {}).get(expected_schema_name)
            
            if not expected_schema_config:
                return ValidationResult(
                    rule_name=f"schema_{expected_schema_name}",
                    passed=False,
                    actual_value=0.0,
                    expected_threshold=1.0,
                    message=f"Expected schema configuration not found: {expected_schema_name}"
                )
            
            # Check column presence
            expected_columns = set(expected_schema_config.get("columns", []))
            actual_columns = set(df.columns)
            
            missing_columns = expected_columns - actual_columns
            extra_columns = actual_columns - expected_columns
            
            if missing_columns or extra_columns:
                message_parts = []
                if missing_columns:
                    message_parts.append(f"Missing columns: {missing_columns}")
                if extra_columns:
                    message_parts.append(f"Extra columns: {extra_columns}")
                
                return ValidationResult(
                    rule_name=f"schema_{expected_schema_name}",
                    passed=False,
                    actual_value=len(actual_columns & expected_columns) / len(expected_columns),
                    expected_threshold=1.0,
                    message="; ".join(message_parts)
                )
            
            # TODO: Add data type validation
            
            return ValidationResult(
                rule_name=f"schema_{expected_schema_name}",
                passed=True,
                actual_value=1.0,
                expected_threshold=1.0,
                message="Schema validation passed"
            )
            
        except Exception as e:
            return ValidationResult(
                rule_name=f"schema_{expected_schema_name}",
                passed=False,
                actual_value=0.0,
                expected_threshold=1.0,
                message=f"Schema validation error: {str(e)}"
            )
    
    def validate_completeness(self, df: DataFrame, rule: ValidationRule) -> ValidationResult:
        """
        Validate data completeness (non-null values).
        
        Args:
            df: DataFrame to validate
            rule: Validation rule for completeness
            
        Returns:
            ValidationResult indicating completeness status
        """
        try:
            total_rows = df.count()
            
            if total_rows == 0:
                return ValidationResult(
                    rule_name=rule.name,
                    passed=False,
                    actual_value=0.0,
                    expected_threshold=rule.threshold,
                    message="Dataset is empty"
                )
            
            completeness_scores = {}
            overall_completeness = 0.0
            
            for column in rule.columns:
                if column in df.columns:
                    non_null_count = df.filter(F.col(column).isNotNull()).count()
                    completeness = non_null_count / total_rows
                    completeness_scores[column] = completeness
                else:
                    completeness_scores[column] = 0.0
            
            overall_completeness = sum(completeness_scores.values()) / len(rule.columns)
            passed = overall_completeness >= rule.threshold
            
            message = f"Overall completeness: {overall_completeness:.3f}"
            if not passed:
                low_quality_columns = [
                    col for col, score in completeness_scores.items() 
                    if score < rule.threshold
                ]
                message += f"; Low quality columns: {low_quality_columns}"
            
            return ValidationResult(
                rule_name=rule.name,
                passed=passed,
                actual_value=overall_completeness,
                expected_threshold=rule.threshold,
                message=message
            )
            
        except Exception as e:
            return ValidationResult(
                rule_name=rule.name,
                passed=False,
                actual_value=0.0,
                expected_threshold=rule.threshold,
                message=f"Completeness validation error: {str(e)}"
            )
    
    def validate_uniqueness(self, df: DataFrame, rule: ValidationRule) -> ValidationResult:
        """
        Validate data uniqueness.
        
        Args:
            df: DataFrame to validate
            rule: Validation rule for uniqueness
            
        Returns:
            ValidationResult indicating uniqueness status
        """
        try:
            total_rows = df.count()
            
            if total_rows == 0:
                return ValidationResult(
                    rule_name=rule.name,
                    passed=False,
                    actual_value=0.0,
                    expected_threshold=rule.threshold,
                    message="Dataset is empty"
                )
            
            # Check uniqueness for the combination of specified columns
            unique_rows = df.select(*rule.columns).distinct().count()
            uniqueness_ratio = unique_rows / total_rows
            
            passed = uniqueness_ratio >= rule.threshold
            
            message = f"Uniqueness ratio: {uniqueness_ratio:.3f}"
            if not passed:
                duplicate_count = total_rows - unique_rows
                message += f"; Duplicates found: {duplicate_count}"
            
            return ValidationResult(
                rule_name=rule.name,
                passed=passed,
                actual_value=uniqueness_ratio,
                expected_threshold=rule.threshold,
                message=message
            )
            
        except Exception as e:
            return ValidationResult(
                rule_name=rule.name,
                passed=False,
                actual_value=0.0,
                expected_threshold=rule.threshold,
                message=f"Uniqueness validation error: {str(e)}"
            )
    
    def validate_freshness(self, df: DataFrame, rule: ValidationRule) -> ValidationResult:
        """
        Validate data freshness based on timestamp columns.
        
        Args:
            df: DataFrame to validate
            rule: Validation rule for freshness
            
        Returns:
            ValidationResult indicating freshness status
        """
        try:
            # Assume the first column in the rule is the timestamp column
            timestamp_column = rule.columns[0] if rule.columns else None
            
            if not timestamp_column or timestamp_column not in df.columns:
                return ValidationResult(
                    rule_name=rule.name,
                    passed=False,
                    actual_value=0.0,
                    expected_threshold=rule.threshold,
                    message=f"Timestamp column not found: {timestamp_column}"
                )
            
            # Get the maximum timestamp in the dataset
            max_timestamp = df.agg(F.max(timestamp_column)).collect()[0][0]
            
            if max_timestamp is None:
                return ValidationResult(
                    rule_name=rule.name,
                    passed=False,
                    actual_value=0.0,
                    expected_threshold=rule.threshold,
                    message="No valid timestamps found"
                )
            
            # Calculate hours since the latest data
            current_time = datetime.now()
            if isinstance(max_timestamp, str):
                max_timestamp = datetime.fromisoformat(max_timestamp.replace('Z', '+00:00'))
            
            hours_since_latest = (current_time - max_timestamp).total_seconds() / 3600
            
            # Threshold is expected to be in hours
            passed = hours_since_latest <= rule.threshold
            
            message = f"Hours since latest data: {hours_since_latest:.1f}"
            if not passed:
                message += f"; Data is stale (threshold: {rule.threshold} hours)"
            
            return ValidationResult(
                rule_name=rule.name,
                passed=passed,
                actual_value=hours_since_latest,
                expected_threshold=rule.threshold,
                message=message
            )
            
        except Exception as e:
            return ValidationResult(
                rule_name=rule.name,
                passed=False,
                actual_value=0.0,
                expected_threshold=rule.threshold,
                message=f"Freshness validation error: {str(e)}"
            )
    
    def validate_range(self, df: DataFrame, rule: ValidationRule) -> ValidationResult:
        """
        Validate that numeric values are within expected ranges.
        
        Args:
            df: DataFrame to validate
            rule: Validation rule for range validation
            
        Returns:
            ValidationResult indicating range validation status
        """
        try:
            total_rows = df.count()
            
            if total_rows == 0:
                return ValidationResult(
                    rule_name=rule.name,
                    passed=False,
                    actual_value=0.0,
                    expected_threshold=rule.threshold,
                    message="Dataset is empty"
                )
            
            # Get range configuration from rule (assuming it's in the config)
            range_config = self.config.get("ranges", {}).get(rule.name, {})
            min_value = range_config.get("min")
            max_value = range_config.get("max")
            
            if min_value is None and max_value is None:
                return ValidationResult(
                    rule_name=rule.name,
                    passed=False,
                    actual_value=0.0,
                    expected_threshold=rule.threshold,
                    message="No range configuration found"
                )
            
            valid_rows = total_rows
            invalid_columns = []
            
            for column in rule.columns:
                if column in df.columns:
                    column_filter = F.col(column).isNotNull()
                    
                    if min_value is not None:
                        column_filter = column_filter & (F.col(column) >= min_value)
                    
                    if max_value is not None:
                        column_filter = column_filter & (F.col(column) <= max_value)
                    
                    column_valid_rows = df.filter(column_filter).count()
                    
                    if column_valid_rows < total_rows:
                        invalid_columns.append(column)
                        valid_rows = min(valid_rows, column_valid_rows)
            
            validity_ratio = valid_rows / total_rows
            passed = validity_ratio >= rule.threshold
            
            message = f"Range validity ratio: {validity_ratio:.3f}"
            if not passed and invalid_columns:
                message += f"; Invalid columns: {invalid_columns}"
            
            return ValidationResult(
                rule_name=rule.name,
                passed=passed,
                actual_value=validity_ratio,
                expected_threshold=rule.threshold,
                message=message
            )
            
        except Exception as e:
            return ValidationResult(
                rule_name=rule.name,
                passed=False,
                actual_value=0.0,
                expected_threshold=rule.threshold,
                message=f"Range validation error: {str(e)}"
            )
    
    def validate_all(self, df: DataFrame, schema_name: Optional[str] = None) -> List[ValidationResult]:
        """
        Run all configured validation rules on the DataFrame.
        
        Args:
            df: DataFrame to validate
            schema_name: Optional schema name for schema validation
            
        Returns:
            List of ValidationResult objects
        """
        if not self.enabled:
            self.logger.info("Data quality validation is disabled")
            return []
        
        self.logger.info("Starting comprehensive data quality validation")
        
        results = []
        
        # Schema validation
        if schema_name:
            schema_result = self.validate_schema(df, schema_name)
            results.append(schema_result)
        
        # Rule-based validation
        for rule in self.rules:
            if rule.rule_type == "completeness":
                result = self.validate_completeness(df, rule)
            elif rule.rule_type == "uniqueness":
                result = self.validate_uniqueness(df, rule)
            elif rule.rule_type == "freshness":
                result = self.validate_freshness(df, rule)
            elif rule.rule_type == "range":
                result = self.validate_range(df, rule)
            else:
                result = ValidationResult(
                    rule_name=rule.name,
                    passed=False,
                    actual_value=0.0,
                    expected_threshold=rule.threshold,
                    message=f"Unknown validation rule type: {rule.rule_type}"
                )
            
            results.append(result)
        
        # Log results
        passed_count = sum(1 for r in results if r.passed)
        failed_count = len(results) - passed_count
        
        self.logger.info(f"Validation completed: {passed_count} passed, {failed_count} failed")
        
        # Handle failures
        if failed_count > 0:
            failed_rules = [r.rule_name for r in results if not r.passed]
            error_message = f"Data quality validation failed for rules: {failed_rules}"
            
            if self.fail_on_error:
                raise ValidationError(error_message)
            else:
                self.logger.warning(error_message)
        
        return results
    
    def get_data_profile(self, df: DataFrame) -> Dict[str, Any]:
        """
        Generate a comprehensive data profile for the DataFrame.
        
        Args:
            df: DataFrame to profile
            
        Returns:
            Dictionary containing data profile information
        """
        try:
            total_rows = df.count()
            total_columns = len(df.columns)
            
            # Column-level statistics
            column_stats = {}
            
            for column in df.columns:
                col_type = str(df.schema[column].dataType)
                null_count = df.filter(F.col(column).isNull()).count()
                non_null_count = total_rows - null_count
                
                stats = {
                    "data_type": col_type,
                    "null_count": null_count,
                    "non_null_count": non_null_count,
                    "completeness": non_null_count / total_rows if total_rows > 0 else 0.0
                }
                
                # Add numeric statistics for numeric columns
                if "int" in col_type.lower() or "double" in col_type.lower() or "float" in col_type.lower():
                    numeric_stats = df.agg(
                        F.min(column).alias("min"),
                        F.max(column).alias("max"),
                        F.mean(column).alias("mean"),
                        F.stddev(column).alias("stddev")
                    ).collect()[0]
                    
                    stats.update({
                        "min": numeric_stats["min"],
                        "max": numeric_stats["max"],
                        "mean": numeric_stats["mean"],
                        "stddev": numeric_stats["stddev"]
                    })
                
                # Add distinct count for all columns (sample for large datasets)
                if total_rows > 1000000:  # Sample for large datasets
                    sample_fraction = min(0.1, 100000 / total_rows)
                    distinct_count = df.sample(sample_fraction).select(column).distinct().count()
                    distinct_count = int(distinct_count / sample_fraction)  # Estimate
                else:
                    distinct_count = df.select(column).distinct().count()
                
                stats["distinct_count"] = distinct_count
                stats["cardinality"] = distinct_count / total_rows if total_rows > 0 else 0.0
                
                column_stats[column] = stats
            
            profile = {
                "total_rows": total_rows,
                "total_columns": total_columns,
                "column_statistics": column_stats,
                "generated_at": datetime.now().isoformat()
            }
            
            self.logger.info(f"Data profile generated for {total_columns} columns, {total_rows} rows")
            
            return profile
            
        except Exception as e:
            self.logger.error(f"Failed to generate data profile: {str(e)}")
            return {
                "error": str(e),
                "generated_at": datetime.now().isoformat()
            }