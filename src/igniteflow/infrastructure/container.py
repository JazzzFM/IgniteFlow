"""
Dependency Injection Container for IgniteFlow.

This module defines the dependency injection container for the IgniteFlow
framework, using the `dependency-injector` library.

This container is responsible for wiring together all the core components
of the application, such as configuration, logging, Spark session, etc.
"""

from dependency_injector import containers, providers

from .config import ConfigurationManager
from .spark import SparkSessionManager
from .metrics import MetricsCollector
from .logging import get_logger


class IgniteFlowContainer(containers.DeclarativeContainer):
    """
    Main dependency injection container for the application.
    """

    # Configuration provider
    config = providers.Singleton(
        ConfigurationManager,
        config_path=providers.Dependency(),
        environment=providers.Dependency(),
    )

    # Spark session manager
    spark_manager = providers.Singleton(
        SparkSessionManager,
        config=config,
    )

    # Metrics collector
    metrics_collector = providers.Singleton(
        MetricsCollector,
        config=config.provided.get.call("metrics", default={}),
    )

    # Logger provider
    logger = providers.Factory(
        get_logger,
        name=providers.Dependency(),
    )
