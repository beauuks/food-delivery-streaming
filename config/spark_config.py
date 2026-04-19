"""
spark_config.py
---------------
Builds a configured SparkSession for the streaming pipeline.
Writes Parquet to Azure Blob Storage and aggregated metrics to Supabase.
"""

import os
from pyspark.sql import SparkSession


def create_spark_session() -> SparkSession:
    """Create a SparkSession configured for Azure Blob Storage + Kafka."""
    storage_account = os.getenv("AZURE_STORAGE_ACCOUNT", "")
    storage_key = os.getenv("AZURE_STORAGE_KEY", "")

    builder = (
        SparkSession.builder
        .appName("FoodDeliveryStreaming")
        .master("local[*]")
        .config("spark.sql.shuffle.partitions", "4")
        .config("spark.sql.session.timeZone", "UTC")
        .config("spark.streaming.stopGracefullyOnShutdown", True)
    )

    if storage_account and storage_key:
        builder = builder.config(
            f"fs.azure.account.key.{storage_account}.blob.core.windows.net",
            storage_key,
        )

    return builder.getOrCreate()


def get_blob_path(name: str) -> str:
    """Return wasbs:// path for Azure Blob Storage."""
    storage_account = os.getenv("AZURE_STORAGE_ACCOUNT", "")
    container = os.getenv("AZURE_STORAGE_CONTAINER", "group6")
    return f"wasbs://{container}@{storage_account}.blob.core.windows.net/{name}"


def get_checkpoint_path(name: str) -> str:
    """Return checkpoint path on Azure Blob Storage."""
    return get_blob_path(f"checkpoints-v2/{name}")
