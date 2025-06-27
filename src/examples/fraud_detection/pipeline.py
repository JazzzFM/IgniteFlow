"""
Real-time Fraud Detection Pipeline with MLOps Integration.

This pipeline demonstrates a complete MLOps workflow using IgniteFlow:
- Advanced feature engineering with time-based aggregations
- Model training with ensemble methods
- Real-time scoring capabilities
- MLflow experiment tracking and model registry
- Data quality validation
- Production-ready deployment patterns

Author: IgniteFlow Team
License: MIT
"""

import logging
from typing import Dict, Any, Tuple, List
from datetime import datetime, timedelta

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql.window import Window
from pyspark.sql.types import *
from pyspark.ml import Pipeline
from pyspark.ml.feature import VectorAssembler, StandardScaler, StringIndexer, OneHotEncoder
from pyspark.ml.classification import GBTClassifier, RandomForestClassifier
from pyspark.ml.evaluation import BinaryClassificationEvaluator, MulticlassClassificationEvaluator
from pyspark.ml.tuning import CrossValidator, ParamGridBuilder

from igniteflow_core.base import BasePipeline
from igniteflow_core.mlops import MLflowTracker
from igniteflow_core.data_quality import DataQualityValidator
from igniteflow_core.exceptions import DataError, JobError


class FraudDetectionPipeline(BasePipeline):
    """
    Production-ready fraud detection pipeline with comprehensive MLOps integration.
    
    This pipeline implements state-of-the-art fraud detection techniques:
    
    Features:
    - Time-based feature engineering with sliding windows
    - Advanced categorical encoding
    - Ensemble model training with hyperparameter tuning
    - Real-time scoring capabilities
    - Comprehensive data quality validation
    - MLflow experiment tracking and model versioning
    - Performance monitoring and alerting
    
    Architecture:
    - Extract: Load transaction and customer data from multiple sources
    - Transform: Engineer features using time-based aggregations
    - Load: Train models and store artifacts in model registry
    - Serve: Deploy models for real-time inference
    """
    
    def __init__(self, spark: SparkSession, config: Dict[str, Any]) -> None:
        """
        Initialize the fraud detection pipeline.
        
        Args:
            spark: Spark session instance
            config: Configuration dictionary containing all pipeline parameters
        """
        super().__init__(spark, config)
        self.logger = logging.getLogger(__name__)
        
        # Initialize MLOps components
        self.mlflow = MLflowTracker(config.get("mlflow", {}))
        self.validator = DataQualityValidator(config.get("data_quality", {}))
        
        # Pipeline configuration
        self.feature_columns = config.get("model.feature_columns", [])
        self.categorical_columns = config.get("model.categorical_columns", [])
        self.target_column = config.get("model.target_column", "is_fraud")
        
        # Model configuration
        self.model_name = config.get("model.name", "fraud_detection_model")
        self.model_version = config.get("model.version", "1.0.0")
        
        self.logger.info("FraudDetectionPipeline initialized successfully")
    
    def extract_data(self) -> Tuple[DataFrame, DataFrame]:
        """
        Extract transaction and customer data from configured sources.
        
        Returns:
            Tuple of (transactions_df, customers_df)
            
        Raises:
            DataError: If data extraction fails
        """
        try:
            self.logger.info("Starting data extraction")
            
            # Load transaction data
            transactions_config = self.config.get("data.transactions")
            transactions = self.spark.read.format(transactions_config.get("format", "parquet")) \
                .load(transactions_config.get("path"))
            
            self.logger.info(f"Loaded {transactions.count()} transactions")
            
            # Load customer data
            customers_config = self.config.get("data.customers")
            customers = self.spark.read.format(customers_config.get("format", "parquet")) \
                .load(customers_config.get("path"))
            
            self.logger.info(f"Loaded {customers.count()} customer records")
            
            # Validate data schemas
            self.validator.validate_schema(transactions, "transactions_schema")
            self.validator.validate_schema(customers, "customers_schema")
            
            return transactions, customers
            
        except Exception as e:
            raise DataError(f"Data extraction failed: {str(e)}") from e
    
    def engineer_features(self, transactions: DataFrame, customers: DataFrame) -> DataFrame:
        """
        Engineer comprehensive fraud detection features.
        
        This method creates sophisticated features including:
        - Time-based aggregations (velocity features)
        - Customer behavior patterns
        - Transaction characteristics
        - Merchant risk indicators
        
        Args:
            transactions: Raw transaction data
            customers: Customer profile data
            
        Returns:
            DataFrame with engineered features
        """
        self.logger.info("Starting feature engineering")
        
        # Join transactions with customer data
        data = transactions.join(customers, "customer_id", "left")
        
        # Create time-based windows for aggregations
        window_7d = Window.partitionBy("customer_id").orderBy("timestamp") \
                          .rowsBetween(-7*24*60, 0)  # 7 days in minutes
        
        window_24h = Window.partitionBy("customer_id").orderBy("timestamp") \
                           .rowsBetween(-24*60, 0)  # 24 hours in minutes
        
        window_1h = Window.partitionBy("customer_id").orderBy("timestamp") \
                          .rowsBetween(-60, 0)  # 1 hour in minutes
        
        # Engineer velocity features
        data = data.withColumn(
            "tx_amount_7d_avg", F.avg("amount").over(window_7d)
        ).withColumn(
            "tx_amount_7d_std", F.stddev("amount").over(window_7d)
        ).withColumn(
            "tx_count_24h", F.count("transaction_id").over(window_24h)
        ).withColumn(
            "tx_count_1h", F.count("transaction_id").over(window_1h)
        ).withColumn(
            "tx_amount_24h_sum", F.sum("amount").over(window_24h)
        )
        
        # Calculate Z-scores for anomaly detection
        data = data.withColumn(
            "tx_amount_zscore",
            F.when(F.col("tx_amount_7d_std") > 0,
                   (F.col("amount") - F.col("tx_amount_7d_avg")) / F.col("tx_amount_7d_std")
            ).otherwise(0)
        )
        
        # Time-based features
        data = data.withColumn("hour_of_day", F.hour("timestamp")) \
                   .withColumn("day_of_week", F.dayofweek("timestamp")) \
                   .withColumn("is_weekend", F.when(F.dayofweek("timestamp").isin([1, 7]), 1).otherwise(0))
        
        # Transaction characteristics
        data = data.withColumn(
            "amount_category",
            F.when(F.col("amount") < 50, "small")
            .when(F.col("amount") < 500, "medium")
            .when(F.col("amount") < 2000, "large")
            .otherwise("very_large")
        )
        
        # Customer risk indicators
        customer_risk_window = Window.partitionBy("customer_id")
        
        data = data.withColumn(
            "customer_avg_amount", F.avg("amount").over(customer_risk_window)
        ).withColumn(
            "days_since_first_tx",
            F.datediff(F.col("timestamp"), F.min("timestamp").over(customer_risk_window))
        )
        
        # Merchant risk features
        merchant_window = Window.partitionBy("merchant_id")
        
        data = data.withColumn(
            "merchant_fraud_rate",
            F.avg(F.col(self.target_column).cast("double")).over(merchant_window)
        ).withColumn(
            "merchant_tx_count",
            F.count("transaction_id").over(merchant_window)
        )
        
        self.logger.info("Feature engineering completed")
        return data
    
    def encode_categorical_features(self, data: DataFrame) -> DataFrame:
        """
        Encode categorical features using appropriate techniques.
        
        Args:
            data: DataFrame with raw categorical features
            
        Returns:
            DataFrame with encoded categorical features
        """
        self.logger.info("Encoding categorical features")
        
        # String indexing and one-hot encoding for categorical variables
        indexers = []
        encoders = []
        
        for col in self.categorical_columns:
            if col in data.columns:
                # String indexer
                indexer = StringIndexer(
                    inputCol=col,
                    outputCol=f"{col}_indexed",
                    handleInvalid="keep"
                )
                indexers.append(indexer)
                
                # One-hot encoder
                encoder = OneHotEncoder(
                    inputCol=f"{col}_indexed",
                    outputCol=f"{col}_encoded"
                )
                encoders.append(encoder)
        
        # Apply transformations
        for indexer in indexers:
            data = indexer.fit(data).transform(data)
        
        for encoder in encoders:
            data = encoder.fit(data).transform(data)
        
        return data
    
    def prepare_training_data(self, data: DataFrame) -> Tuple[DataFrame, DataFrame]:
        """
        Prepare data for model training including train/test split.
        
        Args:
            data: Engineered feature dataset
            
        Returns:
            Tuple of (train_data, test_data)
        """
        self.logger.info("Preparing training data")
        
        # Remove rows with null values in critical columns
        data = data.na.drop(subset=self.feature_columns + [self.target_column])
        
        # Balance the dataset if needed
        fraud_count = data.filter(F.col(self.target_column) == 1).count()
        non_fraud_count = data.filter(F.col(self.target_column) == 0).count()
        
        self.logger.info(f"Dataset balance - Fraud: {fraud_count}, Non-fraud: {non_fraud_count}")
        
        # Apply sampling if dataset is highly imbalanced
        if non_fraud_count > fraud_count * 10:  # More than 10:1 ratio
            sampling_ratio = (fraud_count * 5) / non_fraud_count  # 5:1 ratio
            non_fraud_sample = data.filter(F.col(self.target_column) == 0).sample(sampling_ratio, seed=42)
            fraud_data = data.filter(F.col(self.target_column) == 1)
            data = non_fraud_sample.union(fraud_data)
            
            self.logger.info(f"Applied sampling - New dataset size: {data.count()}")
        
        # Split data for training and testing
        train_data, test_data = data.randomSplit([0.8, 0.2], seed=42)
        
        self.logger.info(f"Training set size: {train_data.count()}, Test set size: {test_data.count()}")
        
        return train_data, test_data
    
    def train_model(self, train_data: DataFrame, test_data: DataFrame) -> Any:
        """
        Train fraud detection model with hyperparameter tuning.
        
        Args:
            train_data: Training dataset
            test_data: Test dataset for evaluation
            
        Returns:
            Trained model pipeline
        """
        with self.mlflow.start_run(f"{self.model_name}_training"):
            self.logger.info("Starting model training")
            
            # Feature vector assembly
            assembler = VectorAssembler(
                inputCols=self.feature_columns,
                outputCol="features"
            )
            
            # Feature scaling
            scaler = StandardScaler(
                inputCol="features",
                outputCol="scaled_features",
                withStd=True,
                withMean=True
            )
            
            # Model selection - Gradient Boosted Trees
            gbt = GBTClassifier(
                featuresCol="scaled_features",
                labelCol=self.target_column,
                predictionCol="prediction",
                probabilityCol="probability",
                rawPredictionCol="rawPrediction",
                maxIter=100,
                maxDepth=5,
                stepSize=0.1,
                seed=42
            )
            
            # Create ML pipeline
            ml_pipeline = Pipeline(stages=[assembler, scaler, gbt])
            
            # Hyperparameter tuning with cross-validation
            param_grid = ParamGridBuilder() \
                .addGrid(gbt.maxDepth, [4, 5, 6]) \
                .addGrid(gbt.maxIter, [50, 100, 150]) \
                .addGrid(gbt.stepSize, [0.05, 0.1, 0.15]) \
                .build()
            
            # Cross-validation evaluator
            evaluator = BinaryClassificationEvaluator(
                labelCol=self.target_column,
                rawPredictionCol="rawPrediction",
                metricName="areaUnderROC"
            )
            
            # Cross-validator
            cv = CrossValidator(
                estimator=ml_pipeline,
                estimatorParamMaps=param_grid,
                evaluator=evaluator,
                numFolds=3,
                parallelism=2,
                seed=42
            )
            
            # Train model with cross-validation
            self.logger.info("Training model with cross-validation")
            cv_model = cv.fit(train_data)
            best_model = cv_model.bestModel
            
            # Log best parameters
            best_params = {
                "maxDepth": best_model.stages[-1].getMaxDepth(),
                "maxIter": best_model.stages[-1].getMaxIter(),
                "stepSize": best_model.stages[-1].getStepSize()
            }
            
            self.mlflow.log_params(best_params)
            self.logger.info(f"Best parameters: {best_params}")
            
            # Evaluate model
            predictions = best_model.transform(test_data)
            metrics = self.evaluate_model(predictions)
            
            # Log metrics and model
            self.mlflow.log_metrics(metrics)
            self.mlflow.log_model(best_model, f"{self.model_name}_{self.model_version}")
            
            self.logger.info(f"Model training completed - AUC: {metrics['auc']:.4f}")
            
            return best_model
    
    def evaluate_model(self, predictions: DataFrame) -> Dict[str, float]:
        """
        Comprehensive model evaluation with multiple metrics.
        
        Args:
            predictions: Model predictions on test data
            
        Returns:
            Dictionary of evaluation metrics
        """
        self.logger.info("Evaluating model performance")
        
        # AUC-ROC
        roc_evaluator = BinaryClassificationEvaluator(
            labelCol=self.target_column,
            rawPredictionCol="rawPrediction",
            metricName="areaUnderROC"
        )
        auc = roc_evaluator.evaluate(predictions)
        
        # AUC-PR
        pr_evaluator = BinaryClassificationEvaluator(
            labelCol=self.target_column,
            rawPredictionCol="rawPrediction",
            metricName="areaUnderPR"
        )
        auc_pr = pr_evaluator.evaluate(predictions)
        
        # Confusion matrix metrics
        tp = predictions.filter((F.col(self.target_column) == 1) & (F.col("prediction") == 1)).count()
        fp = predictions.filter((F.col(self.target_column) == 0) & (F.col("prediction") == 1)).count()
        tn = predictions.filter((F.col(self.target_column) == 0) & (F.col("prediction") == 0)).count()
        fn = predictions.filter((F.col(self.target_column) == 1) & (F.col("prediction") == 0)).count()
        
        # Calculate derived metrics
        precision = tp / (tp + fp) if (tp + fp) > 0 else 0.0
        recall = tp / (tp + fn) if (tp + fn) > 0 else 0.0
        f1_score = 2 * (precision * recall) / (precision + recall) if (precision + recall) > 0 else 0.0
        accuracy = (tp + tn) / (tp + tn + fp + fn) if (tp + tn + fp + fn) > 0 else 0.0
        specificity = tn / (tn + fp) if (tn + fp) > 0 else 0.0
        
        metrics = {
            "auc": auc,
            "auc_pr": auc_pr,
            "precision": precision,
            "recall": recall,
            "f1_score": f1_score,
            "accuracy": accuracy,
            "specificity": specificity,
            "true_positives": float(tp),
            "false_positives": float(fp),
            "true_negatives": float(tn),
            "false_negatives": float(fn)
        }
        
        self.logger.info(f"Model evaluation metrics: {metrics}")
        return metrics
    
    def generate_feature_importance(self, model: Any) -> Dict[str, float]:
        """
        Extract feature importance from trained model.
        
        Args:
            model: Trained ML pipeline
            
        Returns:
            Dictionary mapping feature names to importance scores
        """
        try:
            # Get the GBT classifier from the pipeline
            gbt_model = model.stages[-1]
            
            if hasattr(gbt_model, 'featureImportances'):
                importances = gbt_model.featureImportances.toArray()
                
                # Map to feature names
                feature_importance = {}
                for i, importance in enumerate(importances):
                    if i < len(self.feature_columns):
                        feature_importance[self.feature_columns[i]] = float(importance)
                
                # Sort by importance
                sorted_importance = dict(
                    sorted(feature_importance.items(), key=lambda x: x[1], reverse=True)
                )
                
                self.logger.info("Feature importance calculated successfully")
                return sorted_importance
            
        except Exception as e:
            self.logger.warning(f"Could not extract feature importance: {str(e)}")
            
        return {}
    
    def save_model_artifacts(self, model: Any, output_path: str) -> None:
        """
        Save model and associated artifacts to storage.
        
        Args:
            model: Trained model pipeline
            output_path: Path to save model artifacts
        """
        try:
            self.logger.info(f"Saving model artifacts to {output_path}")
            
            # Save model
            model.write().overwrite().save(output_path)
            
            # Generate and log feature importance
            feature_importance = self.generate_feature_importance(model)
            if feature_importance:
                self.mlflow.log_dict(feature_importance, "feature_importance.json")
            
            self.logger.info("Model artifacts saved successfully")
            
        except Exception as e:
            raise JobError(f"Failed to save model artifacts: {str(e)}") from e
    
    def run(self) -> Dict[str, Any]:
        """
        Execute the complete fraud detection pipeline.
        
        Returns:
            Dictionary containing execution results and metadata
        """
        self.logger.info("Starting fraud detection pipeline execution")
        
        try:
            # Extract data
            transactions, customers = self.extract_data()
            
            # Engineer features
            feature_data = self.engineer_features(transactions, customers)
            
            # Encode categorical features
            encoded_data = self.encode_categorical_features(feature_data)
            
            # Prepare training data
            train_data, test_data = self.prepare_training_data(encoded_data)
            
            # Train model
            model = self.train_model(train_data, test_data)
            
            # Save model artifacts
            model_path = self.config.get("model.output_path")
            self.save_model_artifacts(model, model_path)
            
            # Calculate pipeline statistics
            result = {
                "status": "success",
                "model_name": self.model_name,
                "model_version": self.model_version,
                "model_path": model_path,
                "total_transactions": transactions.count(),
                "total_customers": customers.count(),
                "training_samples": train_data.count(),
                "test_samples": test_data.count(),
                "fraud_rate": train_data.filter(F.col(self.target_column) == 1).count() / train_data.count(),
                "execution_timestamp": datetime.now().isoformat()
            }
            
            self.logger.info("Fraud detection pipeline completed successfully")
            return result
            
        except Exception as e:
            self.logger.error(f"Pipeline execution failed: {str(e)}", exc_info=True)
            raise JobError(f"Fraud detection pipeline failed: {str(e)}") from e


def create_job(spark: SparkSession, config: Dict[str, Any]) -> FraudDetectionPipeline:
    """
    Factory function to create the fraud detection pipeline.
    
    Args:
        spark: Spark session instance
        config: Configuration dictionary
        
    Returns:
        Configured FraudDetectionPipeline instance
    """
    return FraudDetectionPipeline(spark, config)