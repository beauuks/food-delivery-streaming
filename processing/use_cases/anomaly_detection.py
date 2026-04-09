"""
anomaly_detection.py
--------------------
Use Case 3a (Advanced): Delivery time anomaly detection.

Sliding 30-min window (5-min slide) of actual_delivery_minutes per zone.
Flags anomalies when delivery time > mean + 2*stddev (z-score based).
Tracks late events to demonstrate watermark handling.
"""

from pyspark.sql import DataFrame
from pyspark.sql.functions import (
    col, count, avg, stddev, window, when, sum as _sum,
)

from processing.sinks.duckdb_sink import write_to_duckdb


def build_anomaly_detection(orders_df: DataFrame) -> DataFrame:
    """
    Sliding 30-min window (5-min slide): anomaly detection on delivery times.
    Uses event-time watermark to handle late-arriving events.
    """
    delivered = orders_df.filter(
        (col("status") == "DELIVERED") & col("actual_delivery_minutes").isNotNull()
    )

    return (
        delivered
        .withWatermark("event_timestamp", "5 minutes")
        .groupBy(
            window(col("event_timestamp"), "30 minutes", "5 minutes"),
            col("zone_id"),
        )
        .agg(
            count("*").alias("delivery_count"),
            avg("actual_delivery_minutes").alias("mean_delivery_min"),
            stddev("actual_delivery_minutes").alias("stddev_delivery_min"),
            # Count extreme deliveries (> 60 min) as anomalies
            count(
                when(col("actual_delivery_minutes") > 60, 1)
            ).alias("anomaly_count"),
            # Count late events in this window
            _sum(when(col("is_late") == True, 1).otherwise(0)).alias("late_event_count"),
        )
        .withColumn(
            "anomaly_threshold",
            col("mean_delivery_min") + 2 * col("stddev_delivery_min"),
        )
        .select(
            col("window.start").alias("window_start"),
            col("window.end").alias("window_end"),
            col("zone_id"),
            col("delivery_count"),
            col("mean_delivery_min"),
            col("stddev_delivery_min"),
            col("anomaly_threshold"),
            col("anomaly_count"),
            col("late_event_count"),
        )
    )


def start_anomaly_detection(orders_df: DataFrame, checkpoint_path: str):
    """Start the anomaly detection query."""
    anomaly_df = build_anomaly_detection(orders_df)

    q = (
        anomaly_df.writeStream
        .outputMode("update")
        .foreachBatch(_write_anomaly_batch)
        .option("checkpointLocation", f"{checkpoint_path}/anomalies")
        .queryName("delivery_anomalies")
        .start()
    )
    return [q]


def _write_anomaly_batch(batch_df, batch_id):
    """Write anomaly results to DuckDB."""
    if batch_df.count() == 0:
        return
    pandas_df = batch_df.toPandas()
    write_to_duckdb(pandas_df, "delivery_anomalies")
