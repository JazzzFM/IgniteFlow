
"""
Configuration Management for IgniteFlow.

This module provides a modern, type-safe configuration management system
that supports multiple environments, configuration sources, and validation.
"""

import json
import os
from pathlib import Path
from typing import Dict, Any, Optional, Union, List
import logging
import yaml

from igniteflow.domain.exceptions import ConfigurationError


class ConfigurationManager:
    """
    Modern configuration manager for IgniteFlow.
    
    This class implements the Dependency Inversion Principle by providing
    a clean abstraction for configuration management that can work with
    multiple configuration sources and formats.
    
    Attributes:
        config_path: Path to configuration directory
        environment: Target environment
        config_data: Loaded configuration data
    """
    
    def __init__(self, config_path: str, environment: str) -> None:
        """
        Initialize configuration manager.
        
        Args:
            config_path: Path to configuration directory
            environment: Target environment (local, dev, staging, prod)
            
        Raises:
            ConfigurationError: If configuration loading fails
        """
        self.config_path = Path(config_path)
        self.environment = environment
        self.config_data: Dict[str, Any] = {}
        self.logger = logging.getLogger(__name__)
        
        self._validate_paths()
        self._load_configurations()
    
    def _validate_paths(self) -> None:
        """
        Validate that configuration paths exist.
        
        Raises:
            ConfigurationError: If paths are invalid
        """
        if not self.config_path.exists():
            raise ConfigurationError(
                f"Configuration directory not found: {self.config_path}",
                error_code="CONFIG_PATH_NOT_FOUND",
                context={"config_path": str(self.config_path)}
            )
        
        if not self.config_path.is_dir():
            raise ConfigurationError(
                f"Configuration path is not a directory: {self.config_path}",
                error_code="CONFIG_PATH_INVALID"
            )
    
    def _load_configurations(self) -> None:
        """
        Load all configuration files from the configuration directory.
        
        This method loads configurations in the following order:
        1. Base configurations (*.json, *.yaml, *.yml files)
        2. Environment-specific overrides
        3. Environment variables
        """
        try:
            # Load base configurations
            self._load_base_configs()
            
            # Apply environment-specific overrides
            self._apply_environment_overrides()
            
            # Apply environment variable overrides
            self._apply_env_var_overrides()

            # Apply cloud secrets overrides
            self._apply_cloud_secrets_overrides()
            
            self.logger.info(f"Configuration loaded successfully for environment: {self.environment}")
            
        except Exception as e:
            raise ConfigurationError(
                f"Failed to load configurations: {str(e)}",
                error_code="CONFIG_LOAD_FAILED"
            ) from e
    
    def _load_base_configs(self) -> None:
        """Load base configuration files."""
        config_files = list(self.config_path.glob("*.json")) + \
                       list(self.config_path.glob("*.yaml")) + \
                       list(self.config_path.glob("*.yml"))
        
        for config_file in sorted(config_files):
            try:
                with open(config_file, 'r', encoding='utf-8') as f:
                    if config_file.suffix in (".yaml", ".yml"):
                        config_data = yaml.safe_load(f)
                    else:
                        config_data = json.load(f)
                
                config_name = config_file.stem
                self.config_data[config_name] = config_data
                
                self.logger.debug(f"Loaded configuration: {config_name}")
                
            except (json.JSONDecodeError, yaml.YAMLError) as e:
                raise ConfigurationError(
                    f"Invalid format in configuration file: {config_file}",
                    error_code="INVALID_CONFIG_FORMAT",
                    context={"file": str(config_file), "error": str(e)}
                ) from e
            except Exception as e:
                raise ConfigurationError(
                    f"Failed to load configuration file: {config_file}",
                    error_code="FILE_LOAD_FAILED",
                    context={"file": str(config_file)}
                ) from e
    
    def _apply_environment_overrides(self) -> None:
        """Apply environment-specific configuration overrides."""
        for ext in ["json", "yaml", "yml"]:
            env_config_file = self.config_path / f"{self.environment}.{ext}"
            if env_config_file.exists():
                try:
                    with open(env_config_file, 'r', encoding='utf-8') as f:
                        if ext in ("yaml", "yml"):
                            env_config = yaml.safe_load(f)
                        else:
                            env_config = json.load(f)
                    
                    # Deep merge environment overrides
                    self._deep_merge(self.config_data, env_config)
                    
                    self.logger.debug(f"Applied environment overrides from: {env_config_file}")
                    
                except Exception as e:
                    self.logger.warning(
                        f"Failed to load environment overrides: {env_config_file}, error: {str(e)}"
                    )
                break

    
    def _apply_env_var_overrides(self) -> None:
        """Apply environment variable overrides."""
        env_prefix = "IGNITEFLOW_"
        
        for key, value in os.environ.items():
            if key.startswith(env_prefix):
                config_key = key[len(env_prefix):].lower().replace('_', '.')
                self._set_nested_value(self.config_data, config_key, self._parse_env_value(value))
                self.logger.debug(f"Applied environment variable override: {config_key}")

    def _apply_cloud_secrets_overrides(self) -> None:
        """Apply overrides from cloud secrets managers."""
        cloud_provider = self.get("cloud_provider", "local")
        if cloud_provider == "aws":
            self._load_aws_secrets()

    def _load_aws_secrets(self) -> None:
        """Load secrets from AWS Secrets Manager."""
        secrets_config = self.get("secrets.aws", [])
        if not secrets_config:
            return

        try:
            import boto3
            secretsmanager_client = boto3.client("secretsmanager")

            for secret_conf in secrets_config:
                secret_name = secret_conf.get("name")
                mapping = secret_conf.get("mapping", {})

                if not secret_name or not mapping:
                    continue

                try:
                    response = secretsmanager_client.get_secret_value(SecretId=secret_name)
                    secret_data = json.loads(response["SecretString"])

                    for secret_key, config_key in mapping.items():
                        if secret_key in secret_data:
                            self._set_nested_value(self.config_data, config_key, secret_data[secret_key])
                            self.logger.debug(f"Loaded secret '{secret_key}' from AWS Secrets Manager")

                except Exception as e:
                    self.logger.warning(f"Failed to load secret '{secret_name}' from AWS: {e}")

        except ImportError:
            self.logger.warning("boto3 is not installed. Cannot load secrets from AWS.")
        except Exception as e:
            self.logger.error(f"An error occurred while loading secrets from AWS: {e}")
    
    def _deep_merge(self, base: Dict[str, Any], override: Dict[str, Any]) -> None:
        """
        Recursively merge override dictionary into base dictionary.
        
        Args:
            base: Base dictionary to merge into
            override: Override dictionary to merge from
        """
        for key, value in override.items():
            if key in base and isinstance(base[key], dict) and isinstance(value, dict):
                self._deep_merge(base[key], value)
            else:
                base[key] = value
    
    def _set_nested_value(self, data: Dict[str, Any], key_path: str, value: Any) -> None:
        """
        Set a nested value in the configuration using dot notation.
        
        Args:
            data: Dictionary to set value in
            key_path: Dot-separated key path (e.g., "spark.driver.memory")
            value: Value to set
        """
        keys = key_path.split('.')
        current = data
        
        for key in keys[:-1]:
            if key not in current:
                current[key] = {}
            current = current[key]
        
        current[keys[-1]] = value
    
    def _parse_env_value(self, value: str) -> Union[str, int, float, bool, None]:
        """
        Parse environment variable value to appropriate Python type.
        
        Args:
            value: String value from environment variable
            
        Returns:
            Parsed value with appropriate type
        """
        # Handle boolean values
        if value.lower() in ('true', 'yes', '1'):
            return True
        elif value.lower() in ('false', 'no', '0'):
            return False
        elif value.lower() in ('null', 'none', ''):
            return None
        
        # Try to parse as number
        try:
            if '.' in value:
                return float(value)
            else:
                return int(value)
        except ValueError:
            return value
    
    def get(self, key: str, default: Any = None) -> Any:
        """
        Get configuration value by key path.
        
        Args:
            key: Dot-separated key path (e.g., "spark.driver.memory")
            default: Default value if key not found
            
        Returns:
            Configuration value or default
        """
        keys = key.split('.')
        current = self.config_data
        
        try:
            for k in keys:
                current = current[k]
            return current
        except (KeyError, TypeError):
            return default
    
    def get_section(self, section: str) -> Dict[str, Any]:
        """
        Get entire configuration section.
        
        Args:
            section: Section name
            
        Returns:
            Configuration section dictionary
            
        Raises:
            ConfigurationError: If section not found
        """
        if section not in self.config_data:
            raise ConfigurationError(
                f"Configuration section not found: {section}",
                error_code="SECTION_NOT_FOUND",
                context={"section": section, "available_sections": list(self.config_data.keys())}
            )
        
        return self.config_data[section].copy()
    
    def get_spark_config(self) -> Dict[str, Any]:
        """
        Get Spark configuration for the current environment.
        
        Returns:
            Spark configuration dictionary
        """
        spark_config = self.get_section("spark_config")
        
        # Apply environment-specific Spark settings
        env_spark_key = f"spark_config_{self.environment}"
        if env_spark_key in self.config_data:
            env_spark_config = self.config_data[env_spark_key]
            self._deep_merge(spark_config, env_spark_config)
        
        return spark_config
    
    def validate_required_configs(self, required_keys: List[str]) -> None:
        """
        Validate that required configuration keys are present.
        
        Args:
            required_keys: List of required configuration key paths
            
        Raises:
            ConfigurationError: If any required keys are missing
        """
        missing_keys = []
        
        for key in required_keys:
            if self.get(key) is None:
                missing_keys.append(key)
        
        if missing_keys:
            raise ConfigurationError(
                f"Missing required configuration keys: {missing_keys}",
                error_code="MISSING_REQUIRED_CONFIG",
                context={"missing_keys": missing_keys}
            )
    
    def to_dict(self) -> Dict[str, Any]:
        """
        Return complete configuration as dictionary.
        
        Returns:
            Complete configuration dictionary
        """
        return self.config_data.copy()
    
    def __repr__(self) -> str:
        """Return string representation of configuration manager."""
        return f"ConfigurationManager(environment={self.environment}, config_path={self.config_path})"