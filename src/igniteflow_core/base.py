"""
Base classes for IgniteFlow pipelines.

This module provides abstract base classes that define the common interface
and shared functionality for all IgniteFlow pipelines, following SOLID principles.
"""

from abc import ABC, abstractmethod
from typing import Dict, Any, Optional, Union
import logging

# Optional PySpark import
try:
    from pyspark.sql import SparkSession
    SPARK_AVAILABLE = True
except ImportError:
    SparkSession = None
    SPARK_AVAILABLE = False


class BasePipeline(ABC):
    """
    Abstract base class for all IgniteFlow pipelines.
    
    This class defines the common interface that all pipelines must implement,
    following the Template Method pattern and Single Responsibility Principle.
    
    Attributes:
        spark: Spark session instance
        config: Pipeline configuration dictionary
        logger: Logger instance for this pipeline
    """
    
    def __init__(self, spark: Optional[Union['SparkSession', Any]], config: Dict[str, Any]) -> None:
        """
        Initialize the pipeline with Spark session and configuration.
        
        Args:
            spark: Spark session instance (optional if PySpark not available)
            config: Configuration dictionary
        """
        if not SPARK_AVAILABLE and spark is not None:
            raise ImportError("PySpark is required but not available")
            
        self.spark = spark
        self.config = config
        self.logger = logging.getLogger(self.__class__.__name__)
        
        # Initialize pipeline-specific attributes
        self._initialize_pipeline()
    
    def _initialize_pipeline(self) -> None:
        """
        Initialize pipeline-specific attributes.
        
        Subclasses can override this method to perform custom initialization.
        """
        pass
    
    @abstractmethod
    def run(self) -> Dict[str, Any]:
        """
        Execute the pipeline.
        
        This is the main entry point for pipeline execution. Subclasses must
        implement this method to define their specific processing logic.
        
        Returns:
            Dictionary containing execution results and metadata
        """
        pass
    
    def get_config_value(self, key: str, default: Any = None) -> Any:
        """
        Get a configuration value using dot notation.
        
        Args:
            key: Configuration key in dot notation (e.g., "data.input.path")
            default: Default value if key is not found
            
        Returns:
            Configuration value or default
        """
        keys = key.split('.')
        value = self.config
        
        try:
            for k in keys:
                value = value[k]
            return value
        except (KeyError, TypeError):
            return default
    
    def validate_config(self, required_keys: list) -> None:
        """
        Validate that required configuration keys are present.
        
        Args:
            required_keys: List of required configuration keys
            
        Raises:
            ValueError: If any required keys are missing
        """
        missing_keys = []
        
        for key in required_keys:
            if self.get_config_value(key) is None:
                missing_keys.append(key)
        
        if missing_keys:
            raise ValueError(f"Missing required configuration keys: {missing_keys}")


class BaseETLPipeline(BasePipeline):
    """
    Base class for ETL (Extract, Transform, Load) pipelines.
    
    This class provides a template for ETL pipelines with standard
    extract, transform, and load phases.
    """
    
    @abstractmethod
    def extract(self) -> Any:
        """
        Extract data from source systems.
        
        Returns:
            Extracted data (typically a DataFrame or dictionary of DataFrames)
        """
        pass
    
    @abstractmethod
    def transform(self, data: Any) -> Any:
        """
        Transform the extracted data.
        
        Args:
            data: Data to transform
            
        Returns:
            Transformed data
        """
        pass
    
    @abstractmethod
    def load(self, data: Any) -> None:
        """
        Load the transformed data to target systems.
        
        Args:
            data: Transformed data to load
        """
        pass
    
    def run(self) -> Dict[str, Any]:
        """
        Execute the ETL pipeline using the template method pattern.
        
        Returns:
            Dictionary containing execution results
        """
        self.logger.info("Starting ETL pipeline execution")
        
        try:
            # Extract
            self.logger.info("Starting data extraction")
            extracted_data = self.extract()
            
            # Transform
            self.logger.info("Starting data transformation")
            transformed_data = self.transform(extracted_data)
            
            # Load
            self.logger.info("Starting data loading")
            self.load(transformed_data)
            
            self.logger.info("ETL pipeline completed successfully")
            
            return {
                "status": "success",
                "pipeline_type": "etl",
                "execution_time": "calculated_by_subclass"
            }
            
        except Exception as e:
            self.logger.error(f"ETL pipeline failed: {str(e)}", exc_info=True)
            raise


class BaseMLPipeline(BasePipeline):
    """
    Base class for Machine Learning pipelines.
    
    This class provides a template for ML pipelines with standard
    phases for data preparation, training, evaluation, and deployment.
    """
    
    @abstractmethod
    def prepare_data(self) -> Any:
        """
        Prepare data for machine learning.
        
        Returns:
            Prepared data for training and testing
        """
        pass
    
    @abstractmethod
    def train_model(self, data: Any) -> Any:
        """
        Train the machine learning model.
        
        Args:
            data: Prepared training data
            
        Returns:
            Trained model
        """
        pass
    
    @abstractmethod
    def evaluate_model(self, model: Any, data: Any) -> Dict[str, float]:
        """
        Evaluate the trained model.
        
        Args:
            model: Trained model
            data: Test data for evaluation
            
        Returns:
            Dictionary of evaluation metrics
        """
        pass
    
    def deploy_model(self, model: Any) -> Optional[str]:
        """
        Deploy the trained model (optional).
        
        Args:
            model: Trained model to deploy
            
        Returns:
            Deployment identifier or None if not implemented
        """
        self.logger.info("Model deployment not implemented in base class")
        return None
    
    def run(self) -> Dict[str, Any]:
        """
        Execute the ML pipeline using the template method pattern.
        
        Returns:
            Dictionary containing execution results
        """
        self.logger.info("Starting ML pipeline execution")
        
        try:
            # Prepare data
            self.logger.info("Starting data preparation")
            prepared_data = self.prepare_data()
            
            # Train model
            self.logger.info("Starting model training")
            trained_model = self.train_model(prepared_data)
            
            # Evaluate model
            self.logger.info("Starting model evaluation")
            evaluation_metrics = self.evaluate_model(trained_model, prepared_data)
            
            # Deploy model (optional)
            deployment_id = self.deploy_model(trained_model)
            
            self.logger.info("ML pipeline completed successfully")
            
            return {
                "status": "success",
                "pipeline_type": "ml",
                "metrics": evaluation_metrics,
                "deployment_id": deployment_id,
                "execution_time": "calculated_by_subclass"
            }
            
        except Exception as e:
            self.logger.error(f"ML pipeline failed: {str(e)}", exc_info=True)
            raise


class BaseStreamingPipeline(BasePipeline):
    """
    Base class for streaming data pipelines.
    
    This class provides a template for streaming pipelines that process
    continuous data streams.
    """
    
    @abstractmethod
    def setup_stream(self) -> Any:
        """
        Set up the streaming data source.
        
        Returns:
            Streaming DataFrame or data source
        """
        pass
    
    @abstractmethod
    def process_stream(self, stream: Any) -> Any:
        """
        Process the streaming data.
        
        Args:
            stream: Streaming data source
            
        Returns:
            Processed streaming data
        """
        pass
    
    @abstractmethod
    def write_stream(self, processed_stream: Any) -> Any:
        """
        Write the processed stream to output sinks.
        
        Args:
            processed_stream: Processed streaming data
            
        Returns:
            Streaming query or None
        """
        pass
    
    def run(self) -> Dict[str, Any]:
        """
        Execute the streaming pipeline.
        
        Returns:
            Dictionary containing execution results
        """
        self.logger.info("Starting streaming pipeline execution")
        
        try:
            # Setup stream
            self.logger.info("Setting up data stream")
            stream = self.setup_stream()
            
            # Process stream
            self.logger.info("Starting stream processing")
            processed_stream = self.process_stream(stream)
            
            # Write stream
            self.logger.info("Starting stream output")
            query = self.write_stream(processed_stream)
            
            self.logger.info("Streaming pipeline started successfully")
            
            return {
                "status": "started",
                "pipeline_type": "streaming",
                "query": query,
                "execution_time": "ongoing"
            }
            
        except Exception as e:
            self.logger.error(f"Streaming pipeline failed: {str(e)}", exc_info=True)
            raise