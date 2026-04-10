"""
restaurant_sla.py
-----------------
Use Case 2b (Intermediate): Restaurant SLA monitoring.

15-minute tumbling windows tracking prep-time percentiles (p50/p95/p99)
per restaurant. Flags SLA breaches when p95 exceeds tier threshold.

SLA thresholds: GOLD <= 15min, SILVER <= 25min, BRONZE <= 35min.
"""

from pyspark.sql import DataFrame
from pyspark.sql.functions import (
    col, count, window, when,
    percentile_approx, broadcast,
)

from processing.enrichment import get_restaurant_ref_df, SLA_THRESHOLDS
from processing.sinks.duckdb_sink import write_to_duckdb
from processing.sinks.parquet_sink import write_aggregated_parquet


def build_restaurant_sla(orders_df: DataFrame, spark) -> DataFrame:
    """
    15-min tumbling window: prep time percentiles per restaurant.
    Joins with restaurant reference data for SLA tier info.
    """
    # Only DELIVERED orders have actual_prep_minutes
    delivered = orders_df.filter(
        (col("status") == "DELIVERED") & col("actual_prep_minutes").isNotNull()
    )

    # Get restaurant reference data — drop zone_id to avoid ambiguity
    # (orders_df already has zone_id)
    restaurant_ref = broadcast(
        get_restaurant_ref_df(spark).drop("zone_id")
    )

    # Join with reference data first
    enriched = delivered.join(
        restaurant_ref,
        on="restaurant_id",
        how="left",
    )

    # Window aggregation with percentiles
    agg = (
        enriched
        .withWatermark("event_timestamp", "5 minutes")
        .groupBy(
            window(col("event_timestamp"), "15 minutes"),
            col("restaurant_id"),
            col("restaurant_name"),
            col("zone_id"),
            col("cuisine_type"),
            col("sla_tier"),
        )
        .agg(
            count("*").alias("order_count"),
            percentile_approx("actual_prep_minutes", 0.50).alias("p50_prep"),
            percentile_approx("actual_prep_minutes", 0.95).alias("p95_prep"),
            percentile_approx("actual_prep_minutes", 0.99).alias("p99_prep"),
        )
    )

    # Flag SLA breaches based on tier
    result = agg.withColumn(
        "sla_breached",
        when(
            (col("sla_tier") == "GOLD") & (col("p95_prep") > SLA_THRESHOLDS["GOLD"]), True
        ).when(
            (col("sla_tier") == "SILVER") & (col("p95_prep") > SLA_THRESHOLDS["SILVER"]), True
        ).when(
            (col("sla_tier") == "BRONZE") & (col("p95_prep") > SLA_THRESHOLDS["BRONZE"]), True
        ).otherwise(False)
    ).select(
        col("window.start").alias("window_start"),
        col("window.end").alias("window_end"),
        col("restaurant_id"),
        col("restaurant_name"),
        col("zone_id"),
        col("cuisine_type").alias("cuisine"),
        col("sla_tier"),
        col("order_count"),
        col("p50_prep").cast("double"),
        col("p95_prep").cast("double"),
        col("p99_prep").cast("double"),
        col("sla_breached"),
    )

    return result


def start_restaurant_sla(orders_df: DataFrame, spark, checkpoint_path: str):
    """Start the restaurant SLA monitoring query."""
    sla_df = build_restaurant_sla(orders_df, spark)

    q = (
        sla_df.writeStream
        .outputMode("update")
        .foreachBatch(_write_sla_batch)
        .option("checkpointLocation", f"{checkpoint_path}/restaurant_sla")
        .queryName("restaurant_sla")
        .start()
    )
    return [q]


def _write_sla_batch(batch_df, batch_id):
    """Write SLA results to DuckDB and Parquet."""
    if batch_df.count() == 0:
        return
    pandas_df = batch_df.toPandas()
    write_to_duckdb(pandas_df, "restaurant_sla")
    write_aggregated_parquet(batch_df, batch_id, "restaurant_sla")
