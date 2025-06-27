#!/usr/bin/env python3
"""
IgniteFlow Main Application Entry Point.

This module serves as the main entry point for IgniteFlow jobs, providing
a modern, cloud-native approach to Spark ETL and ML pipeline execution.

The application follows SOLID principles and provides comprehensive
logging, error handling, and observability features.
"""

import argparse
import importlib
import logging
import os
import sys
import traceback
from pathlib import Path
from typing import Dict, Any, Optional

from igniteflow_core.config import ConfigurationManager
from igniteflow_core.logging import setup_logging
from igniteflow_core.spark import SparkSessionManager
from igniteflow_core.metrics import MetricsCollector
from igniteflow_core.exceptions import IgniteFlowError


class IgniteFlowApplication:
    """
    Main IgniteFlow application orchestrator.
    
    This class implements the Single Responsibility Principle by focusing
    solely on application orchestration and lifecycle management.
    
    Attributes:
        config: Configuration manager instance
        spark_manager: Spark session manager
        metrics: Metrics collector for observability
    """
    
    def __init__(self, config_path: str, environment: str) -> None:
        """
        Initialize the IgniteFlow application.
        
        Args:
            config_path: Path to configuration directory
            environment: Target environment (local, dev, staging, prod)
            
        Raises:
            IgniteFlowError: If initialization fails
        """
        self.config = ConfigurationManager(config_path, environment)
        self.spark_manager = SparkSessionManager(self.config)
        self.metrics = MetricsCollector()
        self.logger = logging.getLogger(__name__)
        
    def run_job(self, job_name: str) -> None:
        """
        Execute a specific job pipeline.
        
        This method implements the Open/Closed Principle by allowing
        new job types to be added without modifying existing code.
        
        Args:
            job_name: Name of the job to execute
            
        Raises:
            IgniteFlowError: If job execution fails
        """
        try:
            self.logger.info(f"Starting job execution: {job_name}")
            self.metrics.start_timer(f"job.{job_name}.duration")
            
            # Get Spark session
            spark = self.spark_manager.get_session()
            
            # Load and execute job module
            job_module = self._load_job_module(job_name)
            job_instance = job_module.create_job(spark, self.config)
            
            # Execute the job
            result = job_instance.run()
            
            self.logger.info(f"Job completed successfully: {job_name}")
            self.metrics.increment(f"job.{job_name}.success")
            
            return result
            
        except Exception as e:
            self.logger.error(f"Job execution failed: {job_name}", exc_info=True)
            self.metrics.increment(f"job.{job_name}.failure")
            raise IgniteFlowError(f"Job execution failed: {str(e)}") from e
        finally:
            self.metrics.stop_timer(f"job.{job_name}.duration")
            self._cleanup()
    
    def _load_job_module(self, job_name: str) -> Any:
        """
        Dynamically load a job module.
        
        Args:
            job_name: Name of the job module to load
            
        Returns:
            Loaded job module
            
        Raises:
            IgniteFlowError: If module loading fails
        """
        try:
            # Try to load from pipelines first, then fallback to legacy structure
            module_paths = [
                f"pipelines.{job_name}",
                f"job.{job_name}",
                f"pipebox.{job_name}.{job_name}_pipeline"
            ]
            
            for module_path in module_paths:
                try:
                    return importlib.import_module(module_path)
                except ImportError:
                    continue
            
            raise ImportError(f"Could not find job module: {job_name}")
            
        except ImportError as e:
            raise IgniteFlowError(f"Failed to load job module '{job_name}': {str(e)}") from e
    
    def _cleanup(self) -> None:
        """Clean up resources and publish metrics."""
        try:
            self.spark_manager.stop()
            self.metrics.publish()
        except Exception as e:
            self.logger.warning(f"Cleanup warning: {str(e)}")


class ArgumentParser:
    """
    Command line argument parser for IgniteFlow.
    
    This class follows the Single Responsibility Principle by handling
    only argument parsing and validation.
    """
    
    @staticmethod
    def parse_arguments() -> argparse.Namespace:
        """
        Parse command line arguments.
        
        Returns:
            Parsed arguments namespace
        """
        parser = argparse.ArgumentParser(
            description="IgniteFlow - Cloud-native Spark ETL & ML Pipeline Framework",
            formatter_class=argparse.RawDescriptionHelpFormatter,
            epilog="""
Examples:
  %(prog)s --job wordcount --environment local
  %(prog)s --job data_pipeline --environment dev --config-path /custom/config
  %(prog)s --job ml_training --environment prod --log-level DEBUG
            """
        )
        
        parser.add_argument(
            "--job",
            required=True,
            help="Name of the job/pipeline to execute"
        )
        
        parser.add_argument(
            "--environment",
            choices=["local", "dev", "staging", "prod"],
            default="local",
            help="Target environment (default: local)"
        )
        
        parser.add_argument(
            "--config-path",
            default=None,
            help="Path to configuration directory (default: auto-detect)"
        )
        
        parser.add_argument(
            "--log-level",
            choices=["DEBUG", "INFO", "WARNING", "ERROR", "CRITICAL"],
            default="INFO",
            help="Logging level (default: INFO)"
        )
        
        parser.add_argument(
            "--dry-run",
            action="store_true",
            help="Validate configuration without executing job"
        )
        
        parser.add_argument(
            "--verbose",
            action="store_true",
            help="Enable verbose output"
        )
        
        return parser.parse_args()


def setup_project_path() -> None:
    """
    Set up Python path for IgniteFlow modules.
    
    This ensures that all IgniteFlow modules can be imported correctly
    regardless of the execution context.
    """
    # Get project root directory
    current_dir = Path(__file__).parent
    project_root = current_dir.parent.parent
    src_path = project_root / "src"
    
    # Add to Python path if not already present
    src_path_str = str(src_path)
    if src_path_str not in sys.path:
        sys.path.insert(0, src_path_str)


def auto_detect_config_path() -> str:
    """
    Auto-detect configuration path based on project structure.
    
    Returns:
        Path to configuration directory
        
    Raises:
        IgniteFlowError: If config path cannot be determined
    """
    # Try environment variable first
    if config_path := os.getenv("IGNITEFLOW_CONFIG_PATH"):
        return config_path
    
    # Auto-detect based on script location
    current_dir = Path(__file__).parent
    project_root = current_dir.parent.parent
    config_path = project_root / "src" / "config"
    
    if config_path.exists():
        return str(config_path)
    
    # Fallback to legacy path
    legacy_config_path = current_dir.parent / "config"
    if legacy_config_path.exists():
        return str(legacy_config_path)
    
    raise IgniteFlowError(f"Could not auto-detect config path. Please specify --config-path")


def main() -> None:
    """
    Main application entry point.
    
    This function orchestrates the entire application lifecycle:
    1. Parse command line arguments
    2. Set up logging and paths
    3. Initialize and run the application
    4. Handle errors gracefully
    """
    try:
        # Parse arguments
        args = ArgumentParser.parse_arguments()
        
        # Set up project paths
        setup_project_path()
        
        # Determine config path
        config_path = args.config_path or auto_detect_config_path()
        
        # Set up logging
        setup_logging(
            level=args.log_level,
            verbose=args.verbose,
            environment=args.environment
        )
        
        logger = logging.getLogger(__name__)
        logger.info("IgniteFlow application starting...")
        
        # Log startup information
        logger.info(f"Job: {args.job}")
        logger.info(f"Environment: {args.environment}")
        logger.info(f"Config Path: {config_path}")
        logger.info(f"Python Path: {sys.path[:3]}...")  # Show first 3 entries
        
        # Dry run mode - validate configuration only
        if args.dry_run:
            logger.info("Dry run mode - validating configuration...")
            app = IgniteFlowApplication(config_path, args.environment)
            logger.info("Configuration validation successful")
            return
        
        # Initialize and run application
        app = IgniteFlowApplication(config_path, args.environment)
        app.run_job(args.job)
        
        logger.info("IgniteFlow application completed successfully")
        
    except KeyboardInterrupt:
        logging.getLogger(__name__).info("Application interrupted by user")
        sys.exit(1)
    except IgniteFlowError as e:
        logging.getLogger(__name__).error(f"IgniteFlow error: {str(e)}")
        sys.exit(1)
    except Exception as e:
        logging.getLogger(__name__).critical(
            f"Unexpected error: {str(e)}", 
            exc_info=True
        )
        sys.exit(1)


if __name__ == "__main__":
    main()
