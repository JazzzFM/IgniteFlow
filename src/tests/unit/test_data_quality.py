"""
Unit tests for the data quality module.
"""

import pytest
from unittest.mock import patch, MagicMock
from typing import Dict, Any, List
from pyspark.sql import DataFrame
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DoubleType

from igniteflow_core.data_quality import (
    DataQualityRule, DataQualityResult, DataQualityValidator,
    CompletenessRule, UniquenessRule, RangeRule, PatternRule,
    create_validator
)
from igniteflow_core.exceptions import DataQualityError


class TestDataQualityRule:
    """Test cases for DataQualityRule base class."""
    
    def test_init(self):
        """Test DataQualityRule initialization."""
        rule = DataQualityRule(
            name="test_rule",
            description="Test rule description",
            severity="critical"
        )
        
        assert rule.name == "test_rule"
        assert rule.description == "Test rule description"
        assert rule.severity == "critical"
        assert rule.enabled is True
    
    def test_validate_not_implemented(self):
        """Test that validate method raises NotImplementedError."""
        rule = DataQualityRule("test", "test desc")
        
        with pytest.raises(NotImplementedError):
            rule.validate(None)


class TestDataQualityResult:
    """Test cases for DataQualityResult."""
    
    def test_init(self):
        """Test DataQualityResult initialization."""
        result = DataQualityResult(
            rule_name="test_rule",
            passed=True,
            score=0.95,
            total_records=1000,
            failed_records=50,
            message="Test passed"
        )
        
        assert result.rule_name == "test_rule"
        assert result.passed is True
        assert result.score == 0.95
        assert result.total_records == 1000
        assert result.failed_records == 50
        assert result.message == "Test passed"
        assert result.details == {}
    
    def test_init_with_details(self):
        """Test DataQualityResult with details."""
        details = {"column": "test_col", "threshold": 0.9}
        result = DataQualityResult(
            rule_name="test_rule",
            passed=False,
            score=0.85,
            details=details
        )
        
        assert result.details == details


class TestCompletenessRule:
    """Test cases for CompletenessRule."""
    
    def test_init(self):
        """Test CompletenessRule initialization."""
        rule = CompletenessRule(
            name="completeness_test",
            column="test_column",
            threshold=0.95
        )
        
        assert rule.column == "test_column"
        assert rule.threshold == 0.95
        assert "completeness" in rule.description.lower()
    
    def test_validate_complete_data(self, spark_session, sample_customers_data):
        """Test validation with complete data."""
        rule = CompletenessRule(
            name="completeness_test",
            column="customer_id",
            threshold=0.95
        )
        
        result = rule.validate(sample_customers_data)
        
        assert isinstance(result, DataQualityResult)
        assert result.rule_name == "completeness_test"
        assert result.passed is True
        assert result.score == 1.0  # All customer_ids should be complete
    
    def test_validate_incomplete_data(self, spark_session):
        """Test validation with incomplete data."""
        # Create data with nulls
        data = [
            ("1", "John", 25),
            ("2", None, 30),  # Missing name
            ("3", "Jane", 28),
            ("4", None, 35),  # Missing name
        ]
        
        schema = StructType([
            StructField("id", StringType(), False),
            StructField("name", StringType(), True),
            StructField("age", IntegerType(), False)
        ])
        
        df = spark_session.createDataFrame(data, schema)
        
        rule = CompletenessRule(
            name="name_completeness",
            column="name",
            threshold=0.8
        )
        
        result = rule.validate(df)
        
        assert result.passed is False  # 50% completeness < 80% threshold
        assert result.score == 0.5
        assert result.failed_records == 2
    
    def test_validate_nonexistent_column(self, spark_session, sample_customers_data):
        """Test validation with non-existent column."""
        rule = CompletenessRule(
            name="nonexistent_test",
            column="nonexistent_column",
            threshold=0.95
        )
        
        with pytest.raises(DataQualityError, match="Column not found"):
            rule.validate(sample_customers_data)


class TestUniquenessRule:
    """Test cases for UniquenessRule."""
    
    def test_init(self):
        """Test UniquenessRule initialization."""
        rule = UniquenessRule(
            name="uniqueness_test",
            column="id_column"
        )
        
        assert rule.column == "id_column"
        assert "uniqueness" in rule.description.lower()
    
    def test_validate_unique_data(self, spark_session, sample_customers_data):
        """Test validation with unique data."""
        rule = UniquenessRule(
            name="id_uniqueness",
            column="customer_id"
        )
        
        result = rule.validate(sample_customers_data)
        
        assert result.passed is True
        assert result.score == 1.0
    
    def test_validate_duplicate_data(self, spark_session):
        """Test validation with duplicate data."""
        data = [
            ("1", "John"),
            ("2", "Jane"),
            ("1", "Bob"),  # Duplicate ID
            ("3", "Alice"),
            ("2", "Carol")  # Duplicate ID
        ]
        
        schema = StructType([
            StructField("id", StringType(), False),
            StructField("name", StringType(), False)
        ])
        
        df = spark_session.createDataFrame(data, schema)
        
        rule = UniquenessRule(
            name="id_uniqueness",
            column="id"
        )
        
        result = rule.validate(df)
        
        assert result.passed is False
        assert result.score == 0.6  # 3 unique out of 5 total = 60%
        assert result.failed_records == 2  # 2 duplicates


class TestRangeRule:
    """Test cases for RangeRule."""
    
    def test_init_with_bounds(self):
        """Test RangeRule initialization with bounds."""
        rule = RangeRule(
            name="age_range",
            column="age",
            min_value=18,
            max_value=65
        )
        
        assert rule.column == "age"
        assert rule.min_value == 18
        assert rule.max_value == 65
        assert "range" in rule.description.lower()
    
    def test_init_min_only(self):
        """Test RangeRule with only minimum value."""
        rule = RangeRule(
            name="positive_values",
            column="amount",
            min_value=0
        )
        
        assert rule.min_value == 0
        assert rule.max_value is None
    
    def test_validate_values_in_range(self, spark_session, sample_customers_data):
        """Test validation with values in range."""
        rule = RangeRule(
            name="age_range",
            column="age",
            min_value=0,
            max_value=120
        )
        
        result = rule.validate(sample_customers_data)
        
        assert result.passed is True
        assert result.score == 1.0
    
    def test_validate_values_out_of_range(self, spark_session):
        """Test validation with values out of range."""
        data = [
            ("1", 25, 50000.0),
            ("2", 30, -1000.0),  # Negative amount
            ("3", 28, 75000.0),
            ("4", 35, 0.0),  # Zero amount (valid)
        ]
        
        schema = StructType([
            StructField("id", StringType(), False),
            StructField("age", IntegerType(), False),
            StructField("amount", DoubleType(), False)
        ])
        
        df = spark_session.createDataFrame(data, schema)
        
        rule = RangeRule(
            name="positive_amount",
            column="amount",
            min_value=0
        )
        
        result = rule.validate(df)
        
        assert result.passed is False
        assert result.score == 0.75  # 3 out of 4 valid
        assert result.failed_records == 1


class TestPatternRule:
    """Test cases for PatternRule."""
    
    def test_init(self):
        """Test PatternRule initialization."""
        rule = PatternRule(
            name="email_pattern",
            column="email",
            pattern=r"^[\w\.-]+@[\w\.-]+\.[a-zA-Z]{2,}$"
        )
        
        assert rule.column == "email"
        assert rule.pattern == r"^[\w\.-]+@[\w\.-]+\.[a-zA-Z]{2,}$"
        assert "pattern" in rule.description.lower()
    
    def test_validate_matching_pattern(self, spark_session):
        """Test validation with matching patterns."""
        data = [
            ("1", "john@example.com"),
            ("2", "jane.doe@company.org"),
            ("3", "alice@test.co.uk"),
        ]
        
        schema = StructType([
            StructField("id", StringType(), False),
            StructField("email", StringType(), False)
        ])
        
        df = spark_session.createDataFrame(data, schema)
        
        rule = PatternRule(
            name="email_validation",
            column="email",
            pattern=r"^[\w\.-]+@[\w\.-]+\.[a-zA-Z]{2,}$"
        )
        
        result = rule.validate(df)
        
        assert result.passed is True
        assert result.score == 1.0
    
    def test_validate_non_matching_pattern(self, spark_session):
        """Test validation with non-matching patterns."""
        data = [
            ("1", "john@example.com"),
            ("2", "invalid-email"),  # Invalid
            ("3", "jane@"),  # Invalid
            ("4", "alice@test.co.uk"),
        ]
        
        schema = StructType([
            StructField("id", StringType(), False),
            StructField("email", StringType(), False)
        ])
        
        df = spark_session.createDataFrame(data, schema)
        
        rule = PatternRule(
            name="email_validation",
            column="email",
            pattern=r"^[\w\.-]+@[\w\.-]+\.[a-zA-Z]{2,}$"
        )
        
        result = rule.validate(df)
        
        assert result.passed is False
        assert result.score == 0.5  # 2 out of 4 valid
        assert result.failed_records == 2


class TestDataQualityValidator:
    """Test cases for DataQualityValidator."""
    
    def test_init(self):
        """Test DataQualityValidator initialization."""
        rules = [
            CompletenessRule("completeness", "id", 0.95),
            UniquenessRule("uniqueness", "id")
        ]
        
        validator = DataQualityValidator(rules)
        
        assert len(validator.rules) == 2
        assert validator.fail_fast is False
    
    def test_init_with_config(self):
        """Test DataQualityValidator with configuration."""
        config = {
            "fail_fast": True,
            "enable_caching": True
        }
        
        validator = DataQualityValidator([], config)
        
        assert validator.fail_fast is True
        assert validator.config["enable_caching"] is True
    
    def test_add_rule(self):
        """Test adding a rule to validator."""
        validator = DataQualityValidator([])
        rule = CompletenessRule("test", "column", 0.9)
        
        validator.add_rule(rule)
        
        assert len(validator.rules) == 1
        assert validator.rules[0] == rule
    
    def test_validate_all_pass(self, spark_session, sample_customers_data):
        """Test validation when all rules pass."""
        rules = [
            CompletenessRule("id_complete", "customer_id", 0.9),
            UniquenessRule("id_unique", "customer_id"),
            RangeRule("age_range", "age", 0, 120)
        ]
        
        validator = DataQualityValidator(rules)
        results = validator.validate(sample_customers_data)
        
        assert len(results) == 3
        assert all(result.passed for result in results)
    
    def test_validate_some_fail(self, spark_session):
        """Test validation when some rules fail."""
        # Create data with quality issues
        data = [
            ("1", "John", 25),
            ("1", "Jane", 30),  # Duplicate ID
            ("3", None, 28),   # Missing name
            ("4", "Alice", 150) # Invalid age
        ]
        
        schema = StructType([
            StructField("id", StringType(), False),
            StructField("name", StringType(), True),
            StructField("age", IntegerType(), False)
        ])
        
        df = spark_session.createDataFrame(data, schema)
        
        rules = [
            CompletenessRule("name_complete", "name", 0.9),  # Will fail
            UniquenessRule("id_unique", "id"),  # Will fail
            RangeRule("age_range", "age", 0, 120)  # Will fail
        ]
        
        validator = DataQualityValidator(rules)
        results = validator.validate(df)
        
        assert len(results) == 3
        assert all(not result.passed for result in results)
    
    def test_validate_fail_fast(self, spark_session):
        """Test validation with fail_fast enabled."""
        data = [("1", None, 25)]  # Missing name
        
        schema = StructType([
            StructField("id", StringType(), False),
            StructField("name", StringType(), True),
            StructField("age", IntegerType(), False)
        ])
        
        df = spark_session.createDataFrame(data, schema)
        
        rules = [
            CompletenessRule("name_complete", "name", 0.9),  # Will fail
            UniquenessRule("id_unique", "id"),  # Would pass but shouldn't run
        ]
        
        validator = DataQualityValidator(rules, {"fail_fast": True})
        
        with pytest.raises(DataQualityError, match="Data quality validation failed"):
            validator.validate(df)
    
    def test_validate_disabled_rule(self, spark_session, sample_customers_data):
        """Test validation with disabled rule."""
        rule = CompletenessRule("test", "customer_id", 0.9)
        rule.enabled = False
        
        validator = DataQualityValidator([rule])
        results = validator.validate(sample_customers_data)
        
        assert len(results) == 0  # Disabled rule should not run
    
    def test_get_overall_score(self, spark_session, sample_customers_data):
        """Test getting overall quality score."""
        rules = [
            CompletenessRule("complete", "customer_id", 0.9),
            UniquenessRule("unique", "customer_id")
        ]
        
        validator = DataQualityValidator(rules)
        results = validator.validate(sample_customers_data)
        
        overall_score = validator.get_overall_score(results)
        
        assert isinstance(overall_score, float)
        assert 0 <= overall_score <= 1
        assert overall_score == 1.0  # All rules should pass
    
    def test_get_summary_report(self, spark_session, sample_customers_data):
        """Test getting summary report."""
        rules = [
            CompletenessRule("complete", "customer_id", 0.9),
            UniquenessRule("unique", "customer_id")
        ]
        
        validator = DataQualityValidator(rules)
        results = validator.validate(sample_customers_data)
        
        summary = validator.get_summary_report(results)
        
        assert "overall_score" in summary
        assert "total_rules" in summary
        assert "passed_rules" in summary
        assert "failed_rules" in summary
        assert "rule_results" in summary
        
        assert summary["total_rules"] == 2
        assert summary["passed_rules"] == 2
        assert summary["failed_rules"] == 0


class TestCreateValidator:
    """Test cases for create_validator function."""
    
    def test_create_validator_from_config(self):
        """Test creating validator from configuration."""
        config = {
            "rules": [
                {
                    "type": "completeness",
                    "name": "id_complete",
                    "column": "id",
                    "threshold": 0.95
                },
                {
                    "type": "uniqueness",
                    "name": "id_unique",
                    "column": "id"
                },
                {
                    "type": "range",
                    "name": "age_range",
                    "column": "age",
                    "min_value": 0,
                    "max_value": 120
                },
                {
                    "type": "pattern",
                    "name": "email_pattern",
                    "column": "email",
                    "pattern": r"^[\w\.-]+@[\w\.-]+\.[a-zA-Z]{2,}$"
                }
            ],
            "fail_fast": True
        }
        
        validator = create_validator(config)
        
        assert isinstance(validator, DataQualityValidator)
        assert len(validator.rules) == 4
        assert validator.fail_fast is True
        
        # Check rule types
        rule_types = [type(rule).__name__ for rule in validator.rules]
        assert "CompletenessRule" in rule_types
        assert "UniquenessRule" in rule_types
        assert "RangeRule" in rule_types
        assert "PatternRule" in rule_types
    
    def test_create_validator_unknown_rule_type(self):
        """Test creating validator with unknown rule type."""
        config = {
            "rules": [
                {
                    "type": "unknown_rule",
                    "name": "test",
                    "column": "test_col"
                }
            ]
        }
        
        with pytest.raises(ValueError, match="Unknown rule type"):
            create_validator(config)
    
    def test_create_validator_empty_config(self):
        """Test creating validator with empty configuration."""
        validator = create_validator({})
        
        assert isinstance(validator, DataQualityValidator)
        assert len(validator.rules) == 0
        assert validator.fail_fast is False
