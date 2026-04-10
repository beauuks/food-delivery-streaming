"""
spark_config.py
---------------
Builds a configured SparkSession for the streaming pipeline.
Parquet and checkpoints are written locally. Upload to Azure Blob separately.
"""

import os
from pyspark.sql import SparkSession


def create_spark_session() -> SparkSession:
    """Create a SparkSession for local processing."""
    builder = (
        SparkSession.builder
        .appName("FoodDeliveryStreaming")
        .master("local[*]")
        .config("spark.sql.shuffle.partitions", "4")
        .config("spark.sql.streaming.schemaInference", "true")
        .config("spark.sql.session.timeZone", "UTC")
    )

    return builder.getOrCreate()


def get_checkpoint_path(name: str) -> str:
    """Return local checkpoint path."""
    return f"./data/checkpoints/{name}"


def get_output_path(name: str) -> str:
    """Return local Parquet output path."""
    return f"./data/output/{name}"
