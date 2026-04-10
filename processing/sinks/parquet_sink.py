"""
parquet_sink.py
---------------
Writes Spark streaming micro-batches as Parquet to Azure Blob Storage.
Uses foreachBatch to partition by date/hour.
"""

import sys
import os

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", ".."))
from config.spark_config import get_output_path

from pyspark.sql import DataFrame
from pyspark.sql.functions import col, year, month, dayofmonth, hour, from_unixtime


def write_raw_parquet(batch_df: DataFrame, batch_id: int, feed_name: str):
    """
    Write raw events to Parquet, partitioned by year/month/day/hour.
    feed_name: 'orders' or 'couriers'
    """
    if batch_df.count() == 0:
        return

    # Add partition columns from event_time (epoch ms -> timestamp)
    partitioned = (
        batch_df
        .withColumn("event_ts", from_unixtime(col("event_time") / 1000))
        .withColumn("year", year("event_ts"))
        .withColumn("month", month("event_ts"))
        .withColumn("day", dayofmonth("event_ts"))
        .withColumn("hour", hour("event_ts"))
        .drop("event_ts")
    )

    output_path = get_output_path(f"raw/{feed_name}")
    (
        partitioned.write
        .mode("append")
        .partitionBy("year", "month", "day", "hour")
        .parquet(output_path)
    )


def write_aggregated_parquet(batch_df: DataFrame, batch_id: int, agg_name: str):
    """
    Write aggregated results to Parquet, partitioned by date.
    agg_name: 'windowed_kpis', 'demand_supply', etc.
    """
    if batch_df.count() == 0:
        return

    partitioned = (
        batch_df
        .withColumn("year", year("window_start"))
        .withColumn("month", month("window_start"))
        .withColumn("day", dayofmonth("window_start"))
    )

    output_path = get_output_path(f"aggregated/{agg_name}")
    (
        partitioned.write
        .mode("append")
        .partitionBy("year", "month", "day")
        .parquet(output_path)
    )
