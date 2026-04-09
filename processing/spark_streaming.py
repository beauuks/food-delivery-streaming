"""
spark_streaming.py
------------------
Main entry point for the Spark Structured Streaming pipeline.

Reads from Azure Event Hubs (via Kafka protocol), parses/validates/enriches
events, runs all use cases, and writes to Parquet + DuckDB.

Usage:
  spark-submit \
    --packages org.apache.spark:spark-sql-kafka-0-10_2.13:4.1.1 \
    spark_streaming.py
"""

import sys
import os
import traceback

# Ensure project root is on the path
PROJECT_ROOT = os.path.join(os.path.dirname(__file__), "..")
sys.path.insert(0, PROJECT_ROOT)

from dotenv import load_dotenv
load_dotenv(os.path.join(PROJECT_ROOT, ".env"))

from pyspark.sql.functions import (
    col, from_json, from_unixtime, count, sum as _sum, avg, window, lit,
    when, stddev, percentile_approx, approx_count_distinct, concat_ws,
    broadcast,
)
from pyspark.sql import DataFrame, SparkSession

from config.spark_config import create_spark_session, get_checkpoint_path, get_output_path
from config.eventhub_config import get_kafka_conf, ORDER_TOPIC, COURIER_TOPIC
from processing.schemas import ORDER_SCHEMA, COURIER_SCHEMA
from processing.sinks.postgres_sink import init_tables, write_metrics
from processing.enrichment import get_restaurant_ref_df, SLA_THRESHOLDS


def read_eventhub_stream(spark, topic: str) -> DataFrame:
    """Read a stream from Azure Event Hubs via Kafka protocol."""
    conf = get_kafka_conf(topic)
    return (
        spark.readStream
        .format("kafka")
        .options(**conf)
        .load()
    )


def parse_and_validate(raw_df: DataFrame, schema) -> DataFrame:
    """Parse JSON from Kafka value column, validate, and add event_timestamp."""
    parsed = (
        raw_df
        .selectExpr("CAST(value AS STRING) AS json_body")
        .select(from_json(col("json_body"), schema).alias("data"))
        .select("data.*")
    )

    validated = parsed.filter(
        col("event_id").isNotNull() & col("event_time").isNotNull()
    )

    deduped = validated.filter(
        (col("is_duplicate") == False) | col("is_duplicate").isNull()
    )

    with_ts = deduped.withColumn(
        "event_timestamp",
        from_unixtime(col("event_time") / 1000).cast("timestamp"),
    )

    return with_ts


def process_order_batch(batch_df, batch_id, restaurant_ref_bc):
    """Process a single micro-batch of order events through all use cases."""
    try:
        if batch_df.count() == 0:
            return

        batch_df.cache()
        print(f"[orders] Batch {batch_id}: {batch_df.count()} events")

        # --- Use Case 1a: Tumbling KPIs ---
        placed = batch_df.filter(col("status").isin("PLACED", "CONFIRMED"))
        if placed.count() > 0:
            tumbling = (
                placed
                .groupBy(
                    window(col("event_timestamp"), "5 minutes"),
                    col("zone_id"),
                )
                .agg(
                    count(when(col("status") == "PLACED", 1)).alias("order_count"),
                    _sum(when(col("status") == "PLACED", col("order_value_eur"))).alias("total_revenue"),
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
            write_metrics(tumbling.toPandas(), "windowed_kpis")

        # --- Use Case 1b: Hopping Cancellation Rate ---
        placed_or_cancelled = batch_df.filter(col("status").isin("PLACED", "CANCELLED"))
        if placed_or_cancelled.count() > 0:
            hopping = (
                placed_or_cancelled
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
                    when(col("order_count") > 0, col("cancellation_count") / col("order_count")).otherwise(0.0)
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
            write_metrics(hopping.toPandas(), "windowed_kpis")

        # --- Use Case 2a: Demand (order side) ---
        pending_statuses = ["PLACED", "CONFIRMED", "PREPARING", "READY_FOR_PICKUP"]
        pending = batch_df.filter(col("status").isin(pending_statuses))
        if pending.count() > 0:
            demand = (
                pending
                .groupBy(
                    window(col("event_timestamp"), "5 minutes"),
                    col("zone_id"),
                )
                .agg(count("*").alias("pending_demand"))
                .select(
                    col("window.start").alias("window_start"),
                    col("window.end").alias("window_end"),
                    col("zone_id"),
                    col("pending_demand"),
                )
            )
            write_metrics(demand.toPandas(), "_demand_staging")

        # --- Use Case 2b: Restaurant SLA ---
        delivered = batch_df.filter(
            (col("status") == "DELIVERED") & col("actual_prep_minutes").isNotNull()
        )
        if delivered.count() > 0:
            enriched = delivered.join(restaurant_ref_bc, on="restaurant_id", how="left")
            sla = (
                enriched
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
                .withColumn(
                    "sla_breached",
                    when((col("sla_tier") == "GOLD") & (col("p95_prep") > SLA_THRESHOLDS["GOLD"]), True)
                    .when((col("sla_tier") == "SILVER") & (col("p95_prep") > SLA_THRESHOLDS["SILVER"]), True)
                    .when((col("sla_tier") == "BRONZE") & (col("p95_prep") > SLA_THRESHOLDS["BRONZE"]), True)
                    .otherwise(False)
                )
                .select(
                    col("window.start").alias("window_start"),
                    col("window.end").alias("window_end"),
                    "restaurant_id", "restaurant_name", "zone_id",
                    col("cuisine_type").alias("cuisine"), "sla_tier",
                    "order_count",
                    col("p50_prep").cast("double"),
                    col("p95_prep").cast("double"),
                    col("p99_prep").cast("double"),
                    "sla_breached",
                )
            )
            write_metrics(sla.toPandas(), "restaurant_sla")

        # --- Use Case 3a: Anomaly Detection ---
        delivered_del = batch_df.filter(
            (col("status") == "DELIVERED") & col("actual_delivery_minutes").isNotNull()
        )
        if delivered_del.count() > 0:
            anomaly = (
                delivered_del
                .groupBy(
                    window(col("event_timestamp"), "30 minutes", "5 minutes"),
                    col("zone_id"),
                )
                .agg(
                    count("*").alias("delivery_count"),
                    avg("actual_delivery_minutes").alias("mean_delivery_min"),
                    stddev("actual_delivery_minutes").alias("stddev_delivery_min"),
                    count(when(col("actual_delivery_minutes") > 60, 1)).alias("anomaly_count"),
                    _sum(when(col("is_late") == True, 1).otherwise(0)).alias("late_event_count"),
                )
                .withColumn("anomaly_threshold", col("mean_delivery_min") + 2 * col("stddev_delivery_min"))
                .select(
                    col("window.start").alias("window_start"),
                    col("window.end").alias("window_end"),
                    "zone_id", "delivery_count", "mean_delivery_min",
                    "stddev_delivery_min", "anomaly_threshold",
                    "anomaly_count", "late_event_count",
                )
            )
            write_metrics(anomaly.toPandas(), "delivery_anomalies")

        # --- Use Case 3b: Fraud Detection ---
        with_device = batch_df.filter(col("device_id").isNotNull())
        if with_device.count() > 0:
            fraud_agg = (
                with_device
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
                .withColumn(
                    "fraud_flags",
                    concat_ws(", ",
                        when(col("cancellation_count") >= 3, lit("HIGH_CANCELLATION")),
                        when(col("distinct_customer_ids") >= 2, lit("ACCOUNT_HOPPING")),
                        when(col("refund_count") >= 2, lit("HIGH_REFUNDS")),
                    )
                )
                .filter(
                    (col("cancellation_count") >= 3) |
                    (col("distinct_customer_ids") >= 2) |
                    (col("refund_count") >= 2)
                )
                .select(
                    col("window.start").alias("window_start"),
                    col("window.end").alias("window_end"),
                    "device_id", "distinct_customer_ids",
                    "cancellation_count", "refund_count",
                    "total_order_value", "fraud_flags",
                )
            )
            if fraud_agg.count() > 0:
                write_metrics(fraud_agg.toPandas(), "fraud_alerts")

        # --- Write raw Parquet ---
        output_path = get_output_path("raw/orders")
        (
            batch_df
            .withColumn("year", from_unixtime(col("event_time") / 1000, "yyyy").cast("int"))
            .withColumn("month", from_unixtime(col("event_time") / 1000, "MM").cast("int"))
            .withColumn("day", from_unixtime(col("event_time") / 1000, "dd").cast("int"))
            .withColumn("hour", from_unixtime(col("event_time") / 1000, "HH").cast("int"))
            .write.mode("append")
            .partitionBy("year", "month", "day", "hour")
            .parquet(output_path)
        )

        batch_df.unpersist()

    except Exception as e:
        print(f"[orders] Error in batch {batch_id}: {e}")
        traceback.print_exc()


def process_courier_batch(batch_df, batch_id):
    """Process a single micro-batch of courier events."""
    try:
        if batch_df.count() == 0:
            return

        print(f"[couriers] Batch {batch_id}: {batch_df.count()} events")

        # --- Write latest courier positions for the map ---
        positions = (
            batch_df
            .select("courier_id", "zone_id", "latitude", "longitude", "speed_kmh", "status", "vehicle_type")
            .dropDuplicates(["courier_id"])
        )
        if positions.count() > 0:
            write_metrics(positions.toPandas(), "courier_positions")

        # --- Use Case 2a: Supply (courier side) ---
        idle = batch_df.filter(col("status") == "ONLINE_IDLE")
        if idle.count() > 0:
            supply = (
                idle
                .groupBy(
                    window(col("event_timestamp"), "5 minutes"),
                    col("zone_id"),
                )
                .agg(count("*").alias("available_supply"))
                .select(
                    col("window.start").alias("window_start"),
                    col("window.end").alias("window_end"),
                    col("zone_id"),
                    col("available_supply"),
                )
            )

            from processing.sinks.postgres_sink import _get_conn, _TABLE_DDL
            conn = _get_conn()
            cur = conn.cursor()
            cur.execute(_TABLE_DDL["_demand_staging"])
            cur.execute(_TABLE_DDL["demand_supply_health"])

            # Write supply data and compute health by joining with demand staging
            pdf = supply.toPandas()
            for c in pdf.columns:
                if hasattr(pdf[c], 'dt'):
                    pdf[c] = pdf[c].astype(str)

            for _, row in pdf.iterrows():
                ws, we, zid, avail = row["window_start"], row["window_end"], row["zone_id"], int(row["available_supply"])
                # Get demand for this window+zone
                cur.execute(
                    "SELECT pending_demand FROM _demand_staging WHERE window_start=%s AND window_end=%s AND zone_id=%s",
                    (ws, we, zid)
                )
                result = cur.fetchone()
                demand_val = int(result[0]) if result else 0
                ratio = demand_val / max(avail, 1)
                if ratio < 1.0:
                    status = "healthy"
                elif ratio < 2.0:
                    status = "moderate"
                elif ratio < 4.0:
                    status = "stressed"
                else:
                    status = "critical"

                cur.execute("""
                    INSERT INTO demand_supply_health (window_start, window_end, zone_id, pending_demand, available_supply, health_ratio, health_status)
                    VALUES (%s,%s,%s,%s,%s,%s,%s)
                    ON CONFLICT (window_start, window_end, zone_id) DO UPDATE SET
                        pending_demand = EXCLUDED.pending_demand,
                        available_supply = EXCLUDED.available_supply,
                        health_ratio = EXCLUDED.health_ratio,
                        health_status = EXCLUDED.health_status
                """, (ws, we, zid, demand_val, avail, ratio, status))
            conn.commit()
            cur.close()
            conn.close()

        # --- Write raw Parquet ---
        output_path = get_output_path("raw/couriers")
        (
            batch_df
            .withColumn("year", from_unixtime(col("event_time") / 1000, "yyyy").cast("int"))
            .withColumn("month", from_unixtime(col("event_time") / 1000, "MM").cast("int"))
            .withColumn("day", from_unixtime(col("event_time") / 1000, "dd").cast("int"))
            .withColumn("hour", from_unixtime(col("event_time") / 1000, "HH").cast("int"))
            .write.mode("append")
            .partitionBy("year", "month", "day", "hour")
            .parquet(output_path)
        )

    except Exception as e:
        print(f"[couriers] Error in batch {batch_id}: {e}")
        traceback.print_exc()


def main():
    print("=" * 60)
    print("Food Delivery Streaming Pipeline - Starting")
    print("=" * 60)

    spark = create_spark_session()
    spark.sparkContext.setLogLevel("WARN")
    init_tables()

    checkpoint_base = get_checkpoint_path("")

    # Load restaurant reference data for broadcast
    rest_ref = broadcast(get_restaurant_ref_df(spark).drop("zone_id"))

    # --- Read streams ---
    print("[pipeline] Reading from Event Hubs via Kafka...")
    orders_raw = read_eventhub_stream(spark, ORDER_TOPIC)
    couriers_raw = read_eventhub_stream(spark, COURIER_TOPIC)

    # --- Parse and validate ---
    orders_df = parse_and_validate(orders_raw, ORDER_SCHEMA)
    couriers_df = parse_and_validate(couriers_raw, COURIER_SCHEMA)

    # --- Single order query handling all use cases ---
    print("[pipeline] Starting order processing (all use cases)...")
    order_query = (
        orders_df.writeStream
        .foreachBatch(lambda df, bid: process_order_batch(df, bid, rest_ref))
        .option("checkpointLocation", f"{checkpoint_base}/orders")
        .queryName("order_processing")
        .start()
    )

    # --- Single courier query ---
    print("[pipeline] Starting courier processing...")
    courier_query = (
        couriers_df.writeStream
        .foreachBatch(lambda df, bid: process_courier_batch(df, bid))
        .option("checkpointLocation", f"{checkpoint_base}/couriers")
        .queryName("courier_processing")
        .start()
    )

    print("=" * 60)
    print("[pipeline] 2 streaming queries started (orders + couriers)")
    print("[pipeline] Press Ctrl+C to stop.")
    print("=" * 60)

    spark.streams.awaitAnyTermination()


if __name__ == "__main__":
    main()
