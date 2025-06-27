"""
Fraud Detection Demo Script.

This script demonstrates how to use the IgniteFlow fraud detection pipeline
with synthetic data for testing and demonstration purposes.

Usage:
    python examples/fraud_detection_demo.py
"""

import os
import sys
from pathlib import Path
import logging
from datetime import datetime, timedelta
import random

# Add src to Python path
sys.path.insert(0, str(Path(__file__).parent.parent / "src"))

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import *
import json

from igniteflow_core.config import ConfigurationManager
from examples.fraud_detection.pipeline import FraudDetectionPipeline


def create_spark_session():
    """Create Spark session for local development."""
    return SparkSession.builder \
        .appName("FraudDetectionDemo") \
        .master("local[*]") \
        .config("spark.sql.adaptive.enabled", "true") \
        .config("spark.sql.adaptive.coalescePartitions.enabled", "true") \
        .getOrCreate()


def generate_synthetic_data(spark, num_customers=1000, num_transactions=10000):
    """
    Generate synthetic transaction and customer data for demonstration.
    
    Args:
        spark: Spark session
        num_customers: Number of customers to generate
        num_transactions: Number of transactions to generate
        
    Returns:
        Tuple of (transactions_df, customers_df)
    """
    print("Generating synthetic data...")
    
    # Generate customers
    customer_data = []
    for i in range(num_customers):
        customer_data.append({
            "customer_id": f"cust_{i:06d}",
            "age": random.randint(18, 80),
            "income": random.randint(25000, 150000),
            "credit_score": random.randint(300, 850),
            "account_age_days": random.randint(30, 3650)
        })
    
    customers_schema = StructType([
        StructField("customer_id", StringType(), False),
        StructField("age", IntegerType(), False),
        StructField("income", IntegerType(), False),
        StructField("credit_score", IntegerType(), False),
        StructField("account_age_days", IntegerType(), False)
    ])
    
    customers_df = spark.createDataFrame(customer_data, customers_schema)
    
    # Generate transactions
    merchant_categories = ["grocery", "gas", "restaurant", "retail", "online", "atm"]
    payment_methods = ["credit", "debit", "cash"]
    
    transaction_data = []
    base_time = datetime.now() - timedelta(days=30)
    
    for i in range(num_transactions):
        customer_id = f"cust_{random.randint(0, num_customers-1):06d}"
        
        # Generate realistic transaction patterns
        if random.random() < 0.1:  # 10% high-value transactions
            amount = random.uniform(500, 5000)
        else:
            amount = random.uniform(5, 500)
        
        # Add some fraudulent patterns
        is_fraud = 0
        if random.random() < 0.02:  # 2% fraud rate
            is_fraud = 1
            # Fraudulent transactions often have specific patterns
            amount = random.uniform(1000, 10000)  # Higher amounts
            merchant_category = random.choice(["online", "atm"])  # Risky categories
        else:
            merchant_category = random.choice(merchant_categories)
        
        # Generate timestamp within the last 30 days
        days_ago = random.randint(0, 29)
        hours_ago = random.randint(0, 23)
        minutes_ago = random.randint(0, 59)
        
        timestamp = base_time + timedelta(days=days_ago, hours=hours_ago, minutes=minutes_ago)
        
        transaction_data.append({
            "transaction_id": f"txn_{i:08d}",
            "customer_id": customer_id,
            "merchant_id": f"merchant_{random.randint(1, 1000):04d}",
            "amount": round(amount, 2),
            "timestamp": timestamp,
            "merchant_category": merchant_category,
            "payment_method": random.choice(payment_methods),
            "is_fraud": is_fraud
        })
    
    transactions_schema = StructType([
        StructField("transaction_id", StringType(), False),
        StructField("customer_id", StringType(), False),
        StructField("merchant_id", StringType(), False),
        StructField("amount", DoubleType(), False),
        StructField("timestamp", TimestampType(), False),
        StructField("merchant_category", StringType(), False),
        StructField("payment_method", StringType(), False),
        StructField("is_fraud", IntegerType(), False)
    ])
    
    transactions_df = spark.createDataFrame(transaction_data, transactions_schema)
    
    print(f"Generated {num_customers} customers and {num_transactions} transactions")
    print(f"Fraud rate: {transactions_df.filter(F.col('is_fraud') == 1).count() / num_transactions:.2%}")
    
    return transactions_df, customers_df


def save_synthetic_data(transactions_df, customers_df, output_dir):
    """Save synthetic data to local storage."""
    print(f"Saving synthetic data to {output_dir}")
    
    # Create output directory
    os.makedirs(output_dir, exist_ok=True)
    
    # Save as parquet files
    transactions_df.write.mode("overwrite").parquet(f"{output_dir}/transactions")
    customers_df.write.mode("overwrite").parquet(f"{output_dir}/customers")
    
    print("Synthetic data saved successfully")


def create_demo_config(data_dir, output_dir):
    """Create configuration for the demo."""
    config = {
        "fraud_detection": {
            "data": {
                "transactions": {
                    "path": f"{data_dir}/transactions",
                    "format": "parquet"
                },
                "customers": {
                    "path": f"{data_dir}/customers",
                    "format": "parquet"
                }
            },
            "model": {
                "name": "fraud_detection_demo",
                "version": "1.0.0",
                "target_column": "is_fraud",
                "feature_columns": [
                    "amount",
                    "hour_of_day",
                    "day_of_week",
                    "is_weekend",
                    "tx_amount_7d_avg",
                    "tx_count_24h",
                    "tx_count_1h",
                    "tx_amount_zscore",
                    "customer_avg_amount",
                    "days_since_first_tx"
                ],
                "categorical_columns": [
                    "merchant_category",
                    "payment_method"
                ],
                "output_path": f"{output_dir}/model",
                "min_interactions": 2
            }
        },
        "mlflow": {
            "enabled": False,  # Disable for demo
            "tracking_uri": "http://localhost:5000",
            "experiment_name": "fraud_detection_demo"
        },
        "data_quality": {
            "enabled": True,
            "fail_on_error": False,
            "rules": [
                {
                    "name": "completeness_check",
                    "type": "completeness",
                    "columns": ["transaction_id", "customer_id", "amount"],
                    "threshold": 0.95
                }
            ]
        }
    }
    
    return config


def run_demo():
    """Run the complete fraud detection demo."""
    print("=" * 60)
    print("IgniteFlow Fraud Detection Demo")
    print("=" * 60)
    
    # Setup paths
    demo_dir = Path(__file__).parent / "demo_output"
    data_dir = demo_dir / "data"
    output_dir = demo_dir / "output"
    
    # Create directories
    demo_dir.mkdir(exist_ok=True)
    data_dir.mkdir(exist_ok=True)
    output_dir.mkdir(exist_ok=True)
    
    # Setup logging
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
    )
    
    # Create Spark session
    spark = create_spark_session()
    
    try:
        # Generate synthetic data
        transactions_df, customers_df = generate_synthetic_data(spark)
        
        # Save synthetic data
        save_synthetic_data(transactions_df, customers_df, str(data_dir))
        
        # Create demo configuration
        config = create_demo_config(str(data_dir), str(output_dir))
        
        print("\n" + "=" * 60)
        print("Running Fraud Detection Pipeline")
        print("=" * 60)
        
        # Initialize and run pipeline
        pipeline = FraudDetectionPipeline(spark, config)
        result = pipeline.run()
        
        print("\n" + "=" * 60)
        print("Pipeline Results")
        print("=" * 60)
        
        for key, value in result.items():
            print(f"{key}: {value}")
        
        print("\n" + "=" * 60)
        print("Demo completed successfully!")
        print(f"Check outputs in: {demo_dir}")
        print("=" * 60)
        
    except Exception as e:
        print(f"\nDemo failed with error: {str(e)}")
        import traceback
        traceback.print_exc()
    
    finally:
        spark.stop()


if __name__ == "__main__":
    run_demo()