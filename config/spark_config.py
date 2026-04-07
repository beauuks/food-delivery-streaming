"""
spark_config.py
---------------
Builds a configured SparkSession for the streaming pipeline.
"""

import os
from pyspark.sql import SparkSession


def create_spark_session() -> SparkSession:
    """Create a SparkSession configured for Event Hubs + Azure Blob Storage."""
    storage_account = os.getenv("AZURE_STORAGE_ACCOUNT", "")
    storage_key = os.getenv("AZURE_STORAGE_KEY", "")

    builder = (
        SparkSession.builder
        .appName("FoodDeliveryStreaming")
        .master("local[*]")
        .config("spark.sql.shuffle.partitions", "4")
        .config("spark.sql.streaming.schemaInference", "true")
    )

    # Azure Blob Storage config (for Parquet output + checkpoints)
    if storage_account and storage_key:
        builder = (
            builder
            .config(f"fs.azure.account.key.{storage_account}.blob.core.windows.net", storage_key)
        )

    return builder.getOrCreate()


def get_checkpoint_path(name: str) -> str:
    """Return checkpoint path — Azure Blob or local fallback."""
    storage_account = os.getenv("AZURE_STORAGE_ACCOUNT", "")
    container = os.getenv("AZURE_STORAGE_CONTAINER", "group6")

    if storage_account:
        return f"wasbs://{container}@{storage_account}.blob.core.windows.net/checkpoints/{name}"
    return f"./data/checkpoints/{name}"


def get_output_path(name: str) -> str:
    """Return Parquet output path — Azure Blob or local fallback."""
    storage_account = os.getenv("AZURE_STORAGE_ACCOUNT", "")
    container = os.getenv("AZURE_STORAGE_CONTAINER", "group6")

    if storage_account:
        return f"wasbs://{container}@{storage_account}.blob.core.windows.net/{name}"
    return f"./data/output/{name}"
