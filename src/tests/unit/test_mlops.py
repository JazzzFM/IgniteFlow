"""
Unit tests for the MLOps module.
"""

import pytest
import tempfile
import os
from unittest.mock import patch, MagicMock, call
from pathlib import Path
from typing import Dict, Any

from igniteflow_core.mlops import (
    MLflowTracker, SageMakerDeployer, ModelMetrics, ModelRegistry,
    ExperimentManager, create_mlflow_tracker, create_sagemaker_deployer
)
from igniteflow_core.exceptions import MLOpsError


class TestModelMetrics:
    """Test cases for ModelMetrics."""
    
    def test_init_basic(self):
        """Test ModelMetrics initialization."""
        metrics = ModelMetrics(
            accuracy=0.95,
            precision=0.92,
            recall=0.89
        )
        
        assert metrics.accuracy == 0.95
        assert metrics.precision == 0.92
        assert metrics.recall == 0.89
        assert metrics.f1_score is None
        assert metrics.auc is None
        assert metrics.custom_metrics == {}
    
    def test_init_complete(self):
        """Test ModelMetrics with all fields."""
        custom_metrics = {"business_metric": 0.87}
        
        metrics = ModelMetrics(
            accuracy=0.95,
            precision=0.92,
            recall=0.89,
            f1_score=0.905,
            auc=0.98,
            custom_metrics=custom_metrics
        )
        
        assert metrics.f1_score == 0.905
        assert metrics.auc == 0.98
        assert metrics.custom_metrics == custom_metrics
    
    def test_to_dict(self):
        """Test converting metrics to dictionary."""
        metrics = ModelMetrics(
            accuracy=0.95,
            precision=0.92,
            recall=0.89,
            custom_metrics={"business_metric": 0.87}
        )
        
        result = metrics.to_dict()
        
        assert result["accuracy"] == 0.95
        assert result["precision"] == 0.92
        assert result["recall"] == 0.89
        assert result["custom_metrics"] == {"business_metric": 0.87}
        
        # None values should be included
        assert "f1_score" in result
        assert result["f1_score"] is None


class TestMLflowTracker:
    """Test cases for MLflowTracker."""
    
    def test_init_default(self):
        """Test MLflowTracker initialization with defaults."""
        tracker = MLflowTracker()
        
        assert tracker.tracking_uri == "file:///tmp/mlruns"
        assert tracker.experiment_name == "Default"
        assert tracker.registry_uri is None
        assert tracker.enabled is True
    
    def test_init_custom_config(self):
        """Test MLflowTracker with custom configuration."""
        config = {
            "enabled": True,
            "tracking_uri": "http://mlflow:5000",
            "experiment_name": "fraud_detection",
            "registry_uri": "http://mlflow:5000",
            "artifact_location": "s3://my-bucket/artifacts"
        }
        
        tracker = MLflowTracker(config)
        
        assert tracker.tracking_uri == "http://mlflow:5000"
        assert tracker.experiment_name == "fraud_detection"
        assert tracker.registry_uri == "http://mlflow:5000"
        assert tracker.config["artifact_location"] == "s3://my-bucket/artifacts"
    
    def test_init_disabled(self):
        """Test MLflowTracker when disabled."""
        config = {"enabled": False}
        tracker = MLflowTracker(config)
        
        assert tracker.enabled is False
    
    @patch('igniteflow_core.mlops.MLFLOW_AVAILABLE', True)
    @patch('igniteflow_core.mlops.mlflow')
    def test_setup_mlflow(self, mock_mlflow):
        """Test MLflow setup."""
        config = {
            "tracking_uri": "http://mlflow:5000",
            "experiment_name": "test_experiment"
        }
        
        tracker = MLflowTracker(config)
        tracker._setup_mlflow()
        
        mock_mlflow.set_tracking_uri.assert_called_with("http://mlflow:5000")
        mock_mlflow.set_experiment.assert_called_with("test_experiment")
    
    @patch('igniteflow_core.mlops.MLFLOW_AVAILABLE', False)
    def test_setup_mlflow_not_available(self):
        """Test MLflow setup when not available."""
        tracker = MLflowTracker()
        
        # Should not raise exception
        tracker._setup_mlflow()
    
    @patch('igniteflow_core.mlops.MLFLOW_AVAILABLE', True)
    @patch('igniteflow_core.mlops.mlflow')
    def test_start_run(self, mock_mlflow):
        """Test starting an MLflow run."""
        tracker = MLflowTracker()
        mock_run = MagicMock()
        mock_mlflow.start_run.return_value = mock_run
        
        result = tracker.start_run("test_run")
        
        mock_mlflow.start_run.assert_called_with(run_name="test_run")
        assert result == mock_run
    
    def test_start_run_disabled(self):
        """Test starting run when tracker is disabled."""
        tracker = MLflowTracker({"enabled": False})
        
        result = tracker.start_run("test_run")
        
        assert result is None
    
    @patch('igniteflow_core.mlops.MLFLOW_AVAILABLE', True)
    @patch('igniteflow_core.mlops.mlflow')
    def test_log_metrics(self, mock_mlflow):
        """Test logging metrics."""
        tracker = MLflowTracker()
        metrics = ModelMetrics(accuracy=0.95, precision=0.92)
        
        tracker.log_metrics(metrics)
        
        # Should log all non-None metrics
        expected_calls = [
            call("accuracy", 0.95),
            call("precision", 0.92)
        ]
        mock_mlflow.log_metric.assert_has_calls(expected_calls, any_order=True)
    
    @patch('igniteflow_core.mlops.MLFLOW_AVAILABLE', True)
    @patch('igniteflow_core.mlops.mlflow')
    def test_log_params(self, mock_mlflow):
        """Test logging parameters."""
        tracker = MLflowTracker()
        params = {
            "learning_rate": 0.01,
            "batch_size": 32,
            "epochs": 100
        }
        
        tracker.log_params(params)
        
        for key, value in params.items():
            mock_mlflow.log_param.assert_any_call(key, value)
    
    @patch('igniteflow_core.mlops.MLFLOW_AVAILABLE', True)
    @patch('igniteflow_core.mlops.mlflow')
    def test_log_artifacts(self, mock_mlflow):
        """Test logging artifacts."""
        tracker = MLflowTracker()
        
        with tempfile.TemporaryDirectory() as temp_dir:
            artifact_path = Path(temp_dir) / "model.pkl"
            artifact_path.write_text("dummy model")
            
            tracker.log_artifacts(str(artifact_path))
            
            mock_mlflow.log_artifact.assert_called_with(str(artifact_path))
    
    @patch('igniteflow_core.mlops.MLFLOW_AVAILABLE', True)
    @patch('igniteflow_core.mlops.mlflow')
    def test_register_model(self, mock_mlflow):
        """Test registering a model."""
        tracker = MLflowTracker()
        mock_mlflow.active_run.return_value.info.run_id = "run123"
        
        result = tracker.register_model(
            model_name="fraud_detector",
            model_path="models/fraud_model"
        )
        
        mock_mlflow.register_model.assert_called_once()
        assert result is not None
    
    def test_register_model_disabled(self):
        """Test registering model when disabled."""
        tracker = MLflowTracker({"enabled": False})
        
        result = tracker.register_model("test_model", "path")
        
        assert result is None
    
    @patch('igniteflow_core.mlops.MLFLOW_AVAILABLE', True)
    @patch('igniteflow_core.mlops.mlflow')
    def test_end_run(self, mock_mlflow):
        """Test ending an MLflow run."""
        tracker = MLflowTracker()
        
        tracker.end_run()
        
        mock_mlflow.end_run.assert_called_once()
    
    @patch('igniteflow_core.mlops.MLFLOW_AVAILABLE', True)
    @patch('igniteflow_core.mlops.mlflow')
    def test_get_run_info(self, mock_mlflow):
        """Test getting run information."""
        tracker = MLflowTracker()
        mock_run = MagicMock()
        mock_mlflow.get_run.return_value = mock_run
        
        result = tracker.get_run_info("run123")
        
        mock_mlflow.get_run.assert_called_with("run123")
        assert result == mock_run
    
    @patch('igniteflow_core.mlops.MLFLOW_AVAILABLE', True)
    @patch('igniteflow_core.mlops.mlflow')
    def test_context_manager(self, mock_mlflow):
        """Test using tracker as context manager."""
        tracker = MLflowTracker()
        mock_run = MagicMock()
        mock_mlflow.start_run.return_value = mock_run
        
        with tracker.experiment_context("test_run") as run:
            assert run == mock_run
        
        mock_mlflow.start_run.assert_called_with(run_name="test_run")
        mock_mlflow.end_run.assert_called_once()


class TestSageMakerDeployer:
    """Test cases for SageMakerDeployer."""
    
    def test_init_default(self):
        """Test SageMakerDeployer initialization with defaults."""
        deployer = SageMakerDeployer()
        
        assert deployer.region == "us-east-1"
        assert deployer.role_arn is None
        assert deployer.enabled is True
    
    def test_init_custom_config(self):
        """Test SageMakerDeployer with custom configuration."""
        config = {
            "enabled": True,
            "region": "eu-west-1",
            "role_arn": "arn:aws:iam::123456789012:role/SageMakerRole",
            "instance_type": "ml.m5.xlarge",
            "endpoint_config_name": "my-endpoint-config"
        }
        
        deployer = SageMakerDeployer(config)
        
        assert deployer.region == "eu-west-1"
        assert deployer.role_arn == "arn:aws:iam::123456789012:role/SageMakerRole"
        assert deployer.config["instance_type"] == "ml.m5.xlarge"
    
    def test_init_disabled(self):
        """Test SageMakerDeployer when disabled."""
        config = {"enabled": False}
        deployer = SageMakerDeployer(config)
        
        assert deployer.enabled is False
    
    @patch('igniteflow_core.mlops.BOTO3_AVAILABLE', True)
    @patch('igniteflow_core.mlops.boto3')
    def test_setup_clients(self, mock_boto3):
        """Test setting up AWS clients."""
        config = {"region": "us-west-2"}
        deployer = SageMakerDeployer(config)
        
        deployer._setup_clients()
        
        mock_boto3.client.assert_any_call('sagemaker', region_name='us-west-2')
        mock_boto3.client.assert_any_call('s3', region_name='us-west-2')
    
    @patch('igniteflow_core.mlops.BOTO3_AVAILABLE', False)
    def test_setup_clients_not_available(self):
        """Test setting up clients when boto3 not available."""
        deployer = SageMakerDeployer()
        
        # Should not raise exception
        deployer._setup_clients()
        assert deployer.sagemaker_client is None
        assert deployer.s3_client is None
    
    @patch('igniteflow_core.mlops.BOTO3_AVAILABLE', True)
    def test_create_model(self):
        """Test creating a SageMaker model."""
        deployer = SageMakerDeployer()
        deployer.sagemaker_client = MagicMock()
        
        model_config = {
            "model_name": "fraud-detection-model",
            "image_uri": "123456789012.dkr.ecr.us-east-1.amazonaws.com/my-model:latest",
            "model_data_url": "s3://my-bucket/models/model.tar.gz",
            "role_arn": "arn:aws:iam::123456789012:role/SageMakerRole"
        }
        
        result = deployer.create_model(model_config)
        
        deployer.sagemaker_client.create_model.assert_called_once()
        assert result is not None
    
    def test_create_model_disabled(self):
        """Test creating model when disabled."""
        deployer = SageMakerDeployer({"enabled": False})
        
        result = deployer.create_model({})
        
        assert result is None
    
    @patch('igniteflow_core.mlops.BOTO3_AVAILABLE', True)
    def test_create_endpoint_config(self):
        """Test creating endpoint configuration."""
        deployer = SageMakerDeployer()
        deployer.sagemaker_client = MagicMock()
        
        config = {
            "endpoint_config_name": "fraud-detection-config",
            "model_name": "fraud-detection-model",
            "instance_type": "ml.m5.large",
            "initial_instance_count": 1
        }
        
        result = deployer.create_endpoint_config(config)
        
        deployer.sagemaker_client.create_endpoint_config.assert_called_once()
        assert result is not None
    
    @patch('igniteflow_core.mlops.BOTO3_AVAILABLE', True)
    def test_create_endpoint(self):
        """Test creating an endpoint."""
        deployer = SageMakerDeployer()
        deployer.sagemaker_client = MagicMock()
        
        config = {
            "endpoint_name": "fraud-detection-endpoint",
            "endpoint_config_name": "fraud-detection-config"
        }
        
        result = deployer.create_endpoint(config)
        
        deployer.sagemaker_client.create_endpoint.assert_called_once()
        assert result is not None
    
    @patch('igniteflow_core.mlops.BOTO3_AVAILABLE', True)
    def test_deploy_model(self):
        """Test complete model deployment."""
        deployer = SageMakerDeployer()
        deployer.sagemaker_client = MagicMock()
        
        # Mock successful responses
        deployer.sagemaker_client.create_model.return_value = {"ModelArn": "arn:model"}
        deployer.sagemaker_client.create_endpoint_config.return_value = {"EndpointConfigArn": "arn:config"}
        deployer.sagemaker_client.create_endpoint.return_value = {"EndpointArn": "arn:endpoint"}
        
        deployment_config = {
            "model_name": "test-model",
            "image_uri": "test-image",
            "model_data_url": "s3://bucket/model.tar.gz",
            "role_arn": "arn:role",
            "endpoint_name": "test-endpoint",
            "instance_type": "ml.m5.large"
        }
        
        result = deployer.deploy_model(deployment_config)
        
        # Should call all three methods
        deployer.sagemaker_client.create_model.assert_called_once()
        deployer.sagemaker_client.create_endpoint_config.assert_called_once()
        deployer.sagemaker_client.create_endpoint.assert_called_once()
        
        assert "endpoint_arn" in result
        assert "model_arn" in result
        assert "endpoint_config_arn" in result
    
    @patch('igniteflow_core.mlops.BOTO3_AVAILABLE', True)
    def test_delete_endpoint(self):
        """Test deleting an endpoint."""
        deployer = SageMakerDeployer()
        deployer.sagemaker_client = MagicMock()
        
        deployer.delete_endpoint("test-endpoint")
        
        deployer.sagemaker_client.delete_endpoint.assert_called_with(
            EndpointName="test-endpoint"
        )
    
    @patch('igniteflow_core.mlops.BOTO3_AVAILABLE', True)
    def test_get_endpoint_status(self):
        """Test getting endpoint status."""
        deployer = SageMakerDeployer()
        deployer.sagemaker_client = MagicMock()
        deployer.sagemaker_client.describe_endpoint.return_value = {
            "EndpointStatus": "InService"
        }
        
        status = deployer.get_endpoint_status("test-endpoint")
        
        deployer.sagemaker_client.describe_endpoint.assert_called_with(
            EndpointName="test-endpoint"
        )
        assert status == "InService"


class TestModelRegistry:
    """Test cases for ModelRegistry."""
    
    def test_init(self):
        """Test ModelRegistry initialization."""
        registry = ModelRegistry()
        
        assert registry.models == {}
    
    def test_register_model(self):
        """Test registering a model."""
        registry = ModelRegistry()
        
        model_info = {
            "name": "fraud_detector",
            "version": "v1.0",
            "metrics": ModelMetrics(accuracy=0.95),
            "artifacts": {"model_path": "/path/to/model"}
        }
        
        registry.register_model("fraud_detector", model_info)
        
        assert "fraud_detector" in registry.models
        assert registry.models["fraud_detector"] == model_info
    
    def test_get_model_existing(self):
        """Test getting an existing model."""
        registry = ModelRegistry()
        model_info = {"name": "test_model", "version": "v1.0"}
        registry.models["test_model"] = model_info
        
        result = registry.get_model("test_model")
        
        assert result == model_info
    
    def test_get_model_nonexistent(self):
        """Test getting a non-existent model."""
        registry = ModelRegistry()
        
        result = registry.get_model("nonexistent")
        
        assert result is None
    
    def test_list_models(self):
        """Test listing all models."""
        registry = ModelRegistry()
        registry.models = {
            "model1": {"name": "model1"},
            "model2": {"name": "model2"}
        }
        
        result = registry.list_models()
        
        assert "model1" in result
        assert "model2" in result
    
    def test_update_model(self):
        """Test updating a model."""
        registry = ModelRegistry()
        registry.models["test_model"] = {"version": "v1.0"}
        
        updates = {"version": "v1.1", "accuracy": 0.96}
        registry.update_model("test_model", updates)
        
        assert registry.models["test_model"]["version"] == "v1.1"
        assert registry.models["test_model"]["accuracy"] == 0.96
    
    def test_delete_model(self):
        """Test deleting a model."""
        registry = ModelRegistry()
        registry.models["test_model"] = {"name": "test_model"}
        
        registry.delete_model("test_model")
        
        assert "test_model" not in registry.models


class TestExperimentManager:
    """Test cases for ExperimentManager."""
    
    def test_init(self):
        """Test ExperimentManager initialization."""
        manager = ExperimentManager()
        
        assert manager.experiments == {}
    
    def test_create_experiment(self):
        """Test creating an experiment."""
        manager = ExperimentManager()
        
        experiment_config = {
            "name": "fraud_detection_exp",
            "description": "Fraud detection experiments",
            "tags": {"team": "data-science"}
        }
        
        manager.create_experiment("fraud_exp", experiment_config)
        
        assert "fraud_exp" in manager.experiments
        assert manager.experiments["fraud_exp"] == experiment_config
    
    def test_get_experiment(self):
        """Test getting an experiment."""
        manager = ExperimentManager()
        exp_config = {"name": "test_exp"}
        manager.experiments["test_exp"] = exp_config
        
        result = manager.get_experiment("test_exp")
        
        assert result == exp_config
    
    def test_list_experiments(self):
        """Test listing experiments."""
        manager = ExperimentManager()
        manager.experiments = {
            "exp1": {"name": "exp1"},
            "exp2": {"name": "exp2"}
        }
        
        result = manager.list_experiments()
        
        assert "exp1" in result
        assert "exp2" in result


class TestFactoryFunctions:
    """Test cases for factory functions."""
    
    def test_create_mlflow_tracker(self):
        """Test creating MLflow tracker."""
        config = {
            "enabled": True,
            "tracking_uri": "http://mlflow:5000",
            "experiment_name": "test_experiment"
        }
        
        tracker = create_mlflow_tracker(config)
        
        assert isinstance(tracker, MLflowTracker)
        assert tracker.tracking_uri == "http://mlflow:5000"
        assert tracker.experiment_name == "test_experiment"
    
    def test_create_mlflow_tracker_default(self):
        """Test creating MLflow tracker with defaults."""
        tracker = create_mlflow_tracker()
        
        assert isinstance(tracker, MLflowTracker)
        assert tracker.enabled is True
    
    def test_create_sagemaker_deployer(self):
        """Test creating SageMaker deployer."""
        config = {
            "enabled": True,
            "region": "eu-west-1",
            "role_arn": "arn:aws:iam::123456789012:role/SageMakerRole"
        }
        
        deployer = create_sagemaker_deployer(config)
        
        assert isinstance(deployer, SageMakerDeployer)
        assert deployer.region == "eu-west-1"
        assert deployer.role_arn == "arn:aws:iam::123456789012:role/SageMakerRole"
    
    def test_create_sagemaker_deployer_default(self):
        """Test creating SageMaker deployer with defaults."""
        deployer = create_sagemaker_deployer()
        
        assert isinstance(deployer, SageMakerDeployer)
        assert deployer.enabled is True
