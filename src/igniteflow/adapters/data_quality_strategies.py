"""
Data Quality Validation Strategies for IgniteFlow.

This module defines the Strategy pattern for data quality validation,
allowing for extensible and decoupled validation rules.
"""

from abc import ABC, abstractmethod
from typing import Dict, Any

from pyspark.sql import DataFrame
from pyspark.sql import functions as F


class ValidationStrategy(ABC):
    """
    Abstract base class for all data quality validation strategies.
    """

    def __init__(self, rule_config: Dict[str, Any]):
        self.rule_config = rule_config

    @abstractmethod
    def validate(self, df: DataFrame) -> Dict[str, Any]:
        """Execute the validation strategy."""
        pass


class CompletenessStrategy(ValidationStrategy):
    """
    Validates data completeness (non-null values).
    """

    def validate(self, df: DataFrame) -> Dict[str, Any]:
        column = self.rule_config["column"]
        threshold = self.rule_config["threshold"]

        total_rows = df.count()
        if total_rows == 0:
            return {"passed": False, "message": "Dataset is empty"}

        non_null_count = df.filter(df[column].isNotNull()).count()
        completeness = non_null_count / total_rows

        return {
            "passed": completeness >= threshold,
            "completeness": completeness,
        }


class UniquenessStrategy(ValidationStrategy):
    """
    Validates data uniqueness.
    """

    def validate(self, df: DataFrame) -> Dict[str, Any]:
        column = self.rule_config["column"]
        threshold = self.rule_config["threshold"]

        total_rows = df.count()
        if total_rows == 0:
            return {"passed": False, "message": "Dataset is empty"}

        distinct_count = df.select(column).distinct().count()
        uniqueness = distinct_count / total_rows

        return {
            "passed": uniqueness >= threshold,
            "uniqueness": uniqueness,
        }


class RangeStrategy(ValidationStrategy):
    """
    Validates that numeric values are within expected ranges.
    """

    def validate(self, df: DataFrame) -> Dict[str, Any]:
        column = self.rule_config["column"]
        min_value = self.rule_config.get("min")
        max_value = self.rule_config.get("max")

        total_rows = df.count()
        if total_rows == 0:
            return {"passed": False, "message": "Dataset is empty"}

        condition = df[column].isNotNull()
        if min_value is not None:
            condition = condition & (df[column] >= min_value)
        if max_value is not None:
            condition = condition & (df[column] <= max_value)

        valid_rows = df.filter(condition).count()
        validity_ratio = valid_rows / total_rows

        return {
            "passed": validity_ratio >= self.rule_config.get("threshold", 1.0),
            "validity_ratio": validity_ratio,
        }


class RegexStrategy(ValidationStrategy):
    """
    Validates that string values match a regular expression.
    """

    def validate(self, df: DataFrame) -> Dict[str, Any]:
        column = self.rule_config["column"]
        pattern = self.rule_config["pattern"]

        total_rows = df.count()
        if total_rows == 0:
            return {"passed": False, "message": "Dataset is empty"}

        matching_rows = df.filter(df[column].rlike(pattern)).count()
        match_ratio = matching_rows / total_rows

        return {
            "passed": match_ratio >= self.rule_config.get("threshold", 1.0),
            "match_ratio": match_ratio,
        }