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
from igniteflow_core.container import IgniteFlowContainer
from igniteflow_core.exceptions import IgniteFlowError
from igniteflow_core.utils import setup_project_path, auto_detect_config_path


class IgniteFlowApplication:
    """
    Main IgniteFlow application orchestrator.
    """
    
    def __init__(
        self,
        config_manager: ConfigurationManager,
        spark_manager: SparkSessionManager,
        metrics_collector: MetricsCollector,
        logger: logging.Logger,
    ) -> None:
        self.config_manager = config_manager
        self.spark_manager = spark_manager
        self.metrics_collector = metrics_collector
        self.logger = logger
        
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





def main() -> None:
    """
    Main application entry point.
    """
    try:
        # Parse arguments
        args = ArgumentParser.parse_arguments()
        
        # Set up project paths
        setup_project_path()
        
        # Determine config path
        config_path = args.config_path or auto_detect_config_path()
        
        # Initialize container
        container = IgniteFlowContainer(
            config__config_path=config_path,
            config__environment=args.environment,
        )

        # Set up logging
        setup_logging(
            level=args.log_level,
            verbose=args.verbose,
            environment=args.environment
        )
        
        logger = container.logger(name=__name__)
        logger.info("IgniteFlow application starting...")
        
        # Dry run mode - validate configuration only
        if args.dry_run:
            logger.info("Dry run mode - validating configuration...")
            container.config()
            logger.info("Configuration validation successful")
            return
        
        # Initialize and run application
        app = IgniteFlowApplication(
            config_manager=container.config(),
            spark_manager=container.spark_manager(),
            metrics_collector=container.metrics_collector(),
            logger=logger,
        )
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
