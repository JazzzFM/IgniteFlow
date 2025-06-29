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
from typing import Dict, Any, List, Optional
from datetime import datetime

from pyspark.sql import DataFrame
from pyspark.sql import functions as F
from pyspark.sql.types import StructType, StructField

from igniteflow.domain.exceptions import ValidationError
from igniteflow.adapters.data_quality_strategies import ValidationStrategy, CompletenessStrategy, UniquenessStrategy, RangeStrategy, RegexStrategy


class ValidationStrategyFactory:
    """
    Factory for creating data quality validation strategies.
    """
    _strategies = {
        "completeness": CompletenessStrategy,
        "uniqueness": UniquenessStrategy,
        "range": RangeStrategy,
        "regex": RegexStrategy,
    }

    @classmethod
    def create_strategy(cls, rule_config: Dict[str, Any]) -> ValidationStrategy:
        rule_type = rule_config.get("type")
        strategy_class = cls._strategies.get(rule_type)
        if not strategy_class:
            raise ValueError(f"Unknown validation rule type: {rule_type}")
        return strategy_class(rule_config)


class DataQualityValidator:
    """
    Comprehensive data quality validation framework.
    """
    
    def __init__(self, config: Dict[str, Any]) -> None:
        self.config = config
        self.logger = logging.getLogger(__name__)
        
        self.enabled = config.get("enabled", True)
        self.fail_on_error = config.get("fail_on_error", True)
        
        self.strategies: List[ValidationStrategy] = []
        for rule_config in config.get("rules", []):
            self.strategies.append(ValidationStrategyFactory.create_strategy(rule_config))
        
        self.logger.info(f"DataQualityValidator initialized with {len(self.strategies)} rules")
    
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
        for strategy in self.strategies:
            validation_output = strategy.validate(df)
            result = ValidationResult(
                rule_name=strategy.rule_config["name"],
                passed=validation_output["passed"],
                actual_value=validation_output.get(strategy.rule_config["type"], 0.0), # Use specific metric from output
                expected_threshold=strategy.rule_config.get("threshold", 0.0),
                message=validation_output.get("message", "")
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