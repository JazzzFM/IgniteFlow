"""
Unit tests for the configuration management module.
"""

import pytest
import json
import tempfile
import os
from pathlib import Path
from typing import Dict, Any

from igniteflow_core.config import ConfigurationManager
from igniteflow_core.exceptions import ConfigurationError


class TestConfigurationManager:
    """Test cases for ConfigurationManager."""
    
    def test_init_with_valid_config(self, temp_dir: str):
        """Test initialization with valid configuration."""
        # Create test config file
        config_data = {
            "app_config": {
                "name": "test_app",
                "version": "1.0.0"
            },
            "spark_config": {
                "driver_memory": "2g",
                "executor_memory": "2g"
            }
        }
        
        config_file = Path(temp_dir) / "test_config.json"
        with open(config_file, 'w') as f:
            json.dump(config_data, f)
        
        config_manager = ConfigurationManager(temp_dir, "test")
        
        assert config_manager.config_path == Path(temp_dir)
        assert config_manager.environment == "test"
        assert "app_config" in config_manager.config_data
        assert "spark_config" in config_manager.config_data
    
    def test_init_with_invalid_path(self):
        """Test initialization with invalid config path."""
        with pytest.raises(ConfigurationError) as exc_info:
            ConfigurationManager("/nonexistent/path", "test")
        
        assert "Configuration directory not found" in str(exc_info.value)
    
    def test_get_existing_key(self, config_manager: ConfigurationManager):
        """Test getting an existing configuration key."""
        # Add test data to config
        config_manager.config_data = {
            "database": {
                "host": "localhost",
                "port": 5432
            }
        }
        
        assert config_manager.get("database.host") == "localhost"
        assert config_manager.get("database.port") == 5432
    
    def test_get_nonexistent_key(self, config_manager: ConfigurationManager):
        """Test getting a non-existent configuration key."""
        assert config_manager.get("nonexistent.key") is None
        assert config_manager.get("nonexistent.key", "default") == "default"
    
    def test_get_section_existing(self, config_manager: ConfigurationManager):
        """Test getting an existing configuration section."""
        config_manager.config_data = {
            "spark_config": {
                "driver_memory": "2g",
                "executor_memory": "2g"
            }
        }
        
        section = config_manager.get_section("spark_config")
        assert section["driver_memory"] == "2g"
        assert section["executor_memory"] == "2g"
    
    def test_get_section_nonexistent(self, config_manager: ConfigurationManager):
        """Test getting a non-existent configuration section."""
        with pytest.raises(ConfigurationError) as exc_info:
            config_manager.get_section("nonexistent_section")
        
        assert "Configuration section not found" in str(exc_info.value)
    
    def test_environment_specific_overrides(self, temp_dir: str):
        """Test environment-specific configuration overrides."""
        # Base config
        base_config = {
            "app_config": {
                "name": "test_app",
                "debug": False
            }
        }
        
        # Environment-specific config
        env_config = {
            "app_config": {
                "debug": True,
                "log_level": "DEBUG"
            }
        }
        
        # Write configs
        with open(Path(temp_dir) / "base.json", 'w') as f:
            json.dump(base_config, f)
        
        with open(Path(temp_dir) / "test.json", 'w') as f:
            json.dump(env_config, f)
        
        config_manager = ConfigurationManager(temp_dir, "test")
        
        # Should have merged configuration
        assert config_manager.get("app_config.name") == "test_app"
        assert config_manager.get("app_config.debug") is True
        assert config_manager.get("app_config.log_level") == "DEBUG"
    
    def test_environment_variable_overrides(self, temp_dir: str, monkeypatch):
        """Test environment variable configuration overrides."""
        # Base config
        config_data = {
            "database": {
                "host": "localhost",
                "port": 5432
            }
        }
        
        with open(Path(temp_dir) / "config.json", 'w') as f:
            json.dump(config_data, f)
        
        # Set environment variables
        monkeypatch.setenv("IGNITEFLOW_DATABASE_HOST", "prod-db.example.com")
        monkeypatch.setenv("IGNITEFLOW_DATABASE_PORT", "3306")
        
        config_manager = ConfigurationManager(temp_dir, "test")
        
        # Should be overridden by environment variables
        assert config_manager.get("database.host") == "prod-db.example.com"
        assert config_manager.get("database.port") == 3306
    
    def test_validate_required_configs_success(self, config_manager: ConfigurationManager):
        """Test successful validation of required configurations."""
        config_manager.config_data = {
            "app": {"name": "test"},
            "database": {"host": "localhost"}
        }
        
        required_keys = ["app.name", "database.host"]
        
        # Should not raise exception
        config_manager.validate_required_configs(required_keys)
    
    def test_validate_required_configs_failure(self, config_manager: ConfigurationManager):
        """Test failed validation of required configurations."""
        config_manager.config_data = {
            "app": {"name": "test"}
        }
        
        required_keys = ["app.name", "database.host", "missing.key"]
        
        with pytest.raises(ConfigurationError) as exc_info:
            config_manager.validate_required_configs(required_keys)
        
        error_message = str(exc_info.value)
        assert "Missing required configuration keys" in error_message
        assert "database.host" in error_message
        assert "missing.key" in error_message
    
    def test_get_spark_config(self, config_manager: ConfigurationManager):
        """Test getting Spark configuration."""
        config_manager.config_data = {
            "spark_config": {
                "driver_memory": "2g",
                "executor_memory": "2g"
            },
            "spark_config_test": {
                "driver_memory": "4g"
            }
        }
        
        spark_config = config_manager.get_spark_config()
        
        # Should merge base and environment-specific configs
        assert spark_config["driver_memory"] == "4g"  # Overridden
        assert spark_config["executor_memory"] == "2g"  # From base
    
    def test_json_parsing_error(self, temp_dir: str):
        """Test handling of JSON parsing errors."""
        # Create invalid JSON file
        invalid_json_file = Path(temp_dir) / "invalid.json"
        with open(invalid_json_file, 'w') as f:
            f.write('{"invalid": json}')  # Invalid JSON
        
        with pytest.raises(ConfigurationError) as exc_info:
            ConfigurationManager(temp_dir, "test")
        
        assert "Invalid JSON" in str(exc_info.value)
    
    def test_deep_merge(self, config_manager: ConfigurationManager):
        """Test deep merging of configuration dictionaries."""
        base_dict = {
            "level1": {
                "level2": {
                    "key1": "value1",
                    "key2": "value2"
                },
                "other_key": "other_value"
            }
        }
        
        override_dict = {
            "level1": {
                "level2": {
                    "key2": "overridden_value2",
                    "key3": "value3"
                }
            }
        }
        
        config_manager._deep_merge(base_dict, override_dict)
        
        # Check that merge worked correctly
        assert base_dict["level1"]["level2"]["key1"] == "value1"  # Preserved
        assert base_dict["level1"]["level2"]["key2"] == "overridden_value2"  # Overridden
        assert base_dict["level1"]["level2"]["key3"] == "value3"  # Added
        assert base_dict["level1"]["other_key"] == "other_value"  # Preserved
    
    def test_parse_env_value_types(self, config_manager: ConfigurationManager):
        """Test parsing of environment variable values to correct types."""
        # Test boolean values
        assert config_manager._parse_env_value("true") is True
        assert config_manager._parse_env_value("false") is False
        assert config_manager._parse_env_value("yes") is True
        assert config_manager._parse_env_value("no") is False
        assert config_manager._parse_env_value("1") is True
        assert config_manager._parse_env_value("0") is False
        
        # Test null values
        assert config_manager._parse_env_value("null") is None
        assert config_manager._parse_env_value("none") is None
        assert config_manager._parse_env_value("") is None
        
        # Test numeric values
        assert config_manager._parse_env_value("123") == 123
        assert config_manager._parse_env_value("123.45") == 123.45
        
        # Test string values
        assert config_manager._parse_env_value("hello") == "hello"
    
    def test_to_dict(self, config_manager: ConfigurationManager):
        """Test converting configuration to dictionary."""
        test_config = {
            "app": {"name": "test"},
            "database": {"host": "localhost"}
        }
        config_manager.config_data = test_config
        
        result = config_manager.to_dict()
        
        # Should return a copy
        assert result == test_config
        assert result is not test_config  # Different object
    
    def test_repr(self, config_manager: ConfigurationManager):
        """Test string representation of ConfigurationManager."""
        repr_str = repr(config_manager)
        
        assert "ConfigurationManager" in repr_str
        assert config_manager.environment in repr_str
        assert str(config_manager.config_path) in repr_str