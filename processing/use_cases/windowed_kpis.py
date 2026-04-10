"""
windowed_kpis.py
----------------
Use Case 1 (Basic): Windowed KPIs using tumbling and hopping windows.

- 5-min tumbling window: order count, revenue, avg prep time per zone
- 15-min hopping window (5-min slide): cancellation rate per zone
"""

from pyspark.sql import DataFrame
from pyspark.sql.functions import (
    col, count, sum as _sum, avg, window, lit, when,
)

from processing.sinks.duckdb_sink import write_to_duckdb
from processing.sinks.parquet_sink import write_aggregated_parquet


def build_tumbling_kpis(orders_df: DataFrame) -> DataFrame:
    """
    5-minute tumbling window: order count, total revenue, avg prep time per zone.
    Only counts PLACED events for order count.
    """
    placed = orders_df.filter(col("status") == "PLACED")

    return (
        placed
        .withWatermark("event_timestamp", "5 minutes")
        .groupBy(
            window(col("event_timestamp"), "5 minutes"),
            col("zone_id"),
        )
        .agg(
            count("*").alias("order_count"),
            _sum("order_value_eur").alias("total_revenue"),
            avg("estimated_prep_minutes").alias("avg_prep_minutes"),
        )
        .select(
            col("window.start").alias("window_start"),
            col("window.end").alias("window_end"),
            lit("tumbling_5min").alias("window_type"),
            col("zone_id"),
            col("order_count"),
            col("total_revenue"),
            col("avg_prep_minutes"),
            lit(0).cast("long").alias("cancellation_count"),
            lit(0.0).alias("cancellation_rate"),
        )
    )


def build_hopping_cancellation_rate(orders_df: DataFrame) -> DataFrame:
    """
    15-minute hopping window (5-min slide): cancellation rate per zone.
    """
    return (
        orders_df
        .filter(col("status").isin("PLACED", "CANCELLED"))
        .withWatermark("event_timestamp", "5 minutes")
        .groupBy(
            window(col("event_timestamp"), "15 minutes", "5 minutes"),
            col("zone_id"),
        )
        .agg(
            count("*").alias("order_count"),
            count(when(col("status") == "CANCELLED", 1)).alias("cancellation_count"),
        )
        .withColumn(
            "cancellation_rate",
            when(col("order_count") > 0,
                 col("cancellation_count") / col("order_count"))
            .otherwise(0.0)
        )
        .select(
            col("window.start").alias("window_start"),
            col("window.end").alias("window_end"),
            lit("hopping_15min_5slide").alias("window_type"),
            col("zone_id"),
            col("order_count"),
            lit(0.0).alias("total_revenue"),
            lit(None).cast("double").alias("avg_prep_minutes"),
            col("cancellation_count"),
            col("cancellation_rate"),
        )
    )


def start_windowed_kpis(orders_df: DataFrame, checkpoint_path: str):
    """Start both windowed KPI queries and return them."""
    queries = []

    # Tumbling KPIs
    tumbling = build_tumbling_kpis(orders_df)
    q1 = (
        tumbling.writeStream
        .outputMode("update")
        .foreachBatch(lambda df, bid: _write_batch(df, bid))
        .option("checkpointLocation", f"{checkpoint_path}/tumbling_kpis")
        .queryName("tumbling_kpis")
        .start()
    )
    queries.append(q1)

    # Hopping cancellation rate
    hopping = build_hopping_cancellation_rate(orders_df)
    q2 = (
        hopping.writeStream
        .outputMode("update")
        .foreachBatch(lambda df, bid: _write_batch(df, bid))
        .option("checkpointLocation", f"{checkpoint_path}/hopping_kpis")
        .queryName("hopping_cancellation_rate")
        .start()
    )
    queries.append(q2)

    return queries


def _write_batch(batch_df, batch_id):
    """Write a micro-batch to both DuckDB and Parquet."""
    if batch_df.count() == 0:
        return
    pandas_df = batch_df.toPandas()
    write_to_duckdb(pandas_df, "windowed_kpis")
    write_aggregated_parquet(batch_df, batch_id, "windowed_kpis")
