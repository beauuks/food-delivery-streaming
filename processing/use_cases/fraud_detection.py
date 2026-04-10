"""
fraud_detection.py
------------------
Use Case 3b (Advanced): Fraud heuristics.

1-hour tumbling window grouping by device_id:
- Flag when same device_id has >= 3 cancellations
- Flag when same device_id has >= 2 distinct customer_ids (account hopping)
"""

from pyspark.sql import DataFrame
from pyspark.sql.functions import (
    col, count, approx_count_distinct, window, lit, when,
    sum as _sum, concat_ws,
)

from processing.sinks.duckdb_sink import write_to_duckdb


def build_fraud_detection(orders_df: DataFrame) -> DataFrame:
    """
    1-hour tumbling window: fraud heuristics per device_id.
    """
    # Include all order events that have a device_id
    with_device = orders_df.filter(col("device_id").isNotNull())

    agg = (
        with_device
        .withWatermark("event_timestamp", "5 minutes")
        .groupBy(
            window(col("event_timestamp"), "1 hour"),
            col("device_id"),
        )
        .agg(
            approx_count_distinct("customer_id").alias("distinct_customer_ids"),
            count(when(col("status") == "CANCELLED", 1)).alias("cancellation_count"),
            count(when(col("status") == "REFUNDED", 1)).alias("refund_count"),
            _sum("order_value_eur").alias("total_order_value"),
        )
    )

    # Build fraud flags
    result = (
        agg
        .withColumn(
            "fraud_flags",
            concat_ws(
                ", ",
                when(col("cancellation_count") >= 3, lit("HIGH_CANCELLATION")),
                when(col("distinct_customer_ids") >= 2, lit("ACCOUNT_HOPPING")),
                when(col("refund_count") >= 2, lit("HIGH_REFUNDS")),
            )
        )
        # Only keep rows with at least one flag
        .filter(
            (col("cancellation_count") >= 3) |
            (col("distinct_customer_ids") >= 2) |
            (col("refund_count") >= 2)
        )
        .select(
            col("window.start").alias("window_start"),
            col("window.end").alias("window_end"),
            col("device_id"),
            col("distinct_customer_ids"),
            col("cancellation_count"),
            col("refund_count"),
            col("total_order_value"),
            col("fraud_flags"),
        )
    )

    return result


def start_fraud_detection(orders_df: DataFrame, checkpoint_path: str):
    """Start the fraud detection query."""
    fraud_df = build_fraud_detection(orders_df)

    q = (
        fraud_df.writeStream
        .outputMode("update")
        .foreachBatch(_write_fraud_batch)
        .option("checkpointLocation", f"{checkpoint_path}/fraud")
        .queryName("fraud_alerts")
        .start()
    )
    return [q]


def _write_fraud_batch(batch_df, batch_id):
    """Write fraud alerts to DuckDB."""
    if batch_df.count() == 0:
        return
    pandas_df = batch_df.toPandas()
    write_to_duckdb(pandas_df, "fraud_alerts")
