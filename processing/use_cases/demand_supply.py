"""
demand_supply.py
----------------
Use Case 2a (Intermediate): Demand-supply health metric per zone.

Joins order stream (pending orders) with courier stream (available couriers)
in 5-minute windows to compute health ratio.

health_ratio = pending_demand / max(available_supply, 1)
- < 1.0: healthy
- 1.0-2.0: moderate
- 2.0-4.0: stressed
- > 4.0: critical
"""

from pyspark.sql import DataFrame
from pyspark.sql.functions import (
    col, count, window, lit, when,
)

from processing.sinks.duckdb_sink import write_to_duckdb


def build_demand_supply_health(
    orders_df: DataFrame,
    couriers_df: DataFrame,
) -> tuple[DataFrame, DataFrame]:
    """
    Build demand and supply aggregations separately.
    Returns (demand_agg, supply_agg) — joined in foreachBatch via DuckDB.
    """
    # Demand: count of orders in pending states per zone per 5-min window
    pending_statuses = ["PLACED", "CONFIRMED", "PREPARING", "READY_FOR_PICKUP"]
    demand = (
        orders_df
        .filter(col("status").isin(pending_statuses))
        .withWatermark("event_timestamp", "5 minutes")
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

    # Supply: count of idle couriers per zone per 5-min window
    supply = (
        couriers_df
        .filter(col("status") == "ONLINE_IDLE")
        .withWatermark("event_timestamp", "3 minutes")
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

    return demand, supply


def start_demand_supply(
    orders_df: DataFrame,
    couriers_df: DataFrame,
    checkpoint_path: str,
):
    """Start demand and supply queries, join results in foreachBatch."""
    demand, supply = build_demand_supply_health(orders_df, couriers_df)
    queries = []

    # Write demand counts to DuckDB staging
    q1 = (
        demand.writeStream
        .outputMode("update")
        .foreachBatch(lambda df, bid: _write_demand_batch(df, bid))
        .option("checkpointLocation", f"{checkpoint_path}/demand")
        .queryName("demand_count")
        .start()
    )
    queries.append(q1)

    # Write supply counts and compute health in foreachBatch
    q2 = (
        supply.writeStream
        .outputMode("update")
        .foreachBatch(lambda df, bid: _write_supply_and_compute_health(df, bid))
        .option("checkpointLocation", f"{checkpoint_path}/supply")
        .queryName("supply_count")
        .start()
    )
    queries.append(q2)

    return queries


def _write_demand_batch(batch_df, batch_id):
    """Write demand counts to a staging table in DuckDB."""
    if batch_df.count() == 0:
        return
    import duckdb
    from processing.sinks.duckdb_sink import DUCKDB_PATH, _ensure_dir
    _ensure_dir()
    con = duckdb.connect(DUCKDB_PATH)
    con.execute("""
        CREATE TABLE IF NOT EXISTS _demand_staging (
            window_start TIMESTAMP,
            window_end TIMESTAMP,
            zone_id VARCHAR,
            pending_demand BIGINT,
            PRIMARY KEY (window_start, window_end, zone_id)
        )
    """)
    pdf = batch_df.toPandas()
    con.execute("INSERT OR REPLACE INTO _demand_staging SELECT * FROM pdf")
    con.close()


def _write_supply_and_compute_health(batch_df, batch_id):
    """
    Write supply counts, join with demand staging, compute health ratio,
    and write to the final demand_supply_health table.
    """
    if batch_df.count() == 0:
        return
    import duckdb
    from processing.sinks.duckdb_sink import DUCKDB_PATH, _ensure_dir
    _ensure_dir()
    con = duckdb.connect(DUCKDB_PATH)

    # Ensure staging and target tables exist
    con.execute("""
        CREATE TABLE IF NOT EXISTS _demand_staging (
            window_start TIMESTAMP, window_end TIMESTAMP,
            zone_id VARCHAR, pending_demand BIGINT,
            PRIMARY KEY (window_start, window_end, zone_id)
        )
    """)
    con.execute("""
        CREATE TABLE IF NOT EXISTS demand_supply_health (
            window_start TIMESTAMP, window_end TIMESTAMP,
            zone_id VARCHAR, pending_demand BIGINT,
            available_supply BIGINT, health_ratio DOUBLE,
            health_status VARCHAR,
            PRIMARY KEY (window_start, window_end, zone_id)
        )
    """)

    # Write supply batch
    pdf = batch_df.toPandas()
    con.execute("""
        CREATE OR REPLACE TEMP TABLE _supply_batch AS SELECT * FROM pdf
    """)

    # Join with demand and compute health
    con.execute("""
        INSERT OR REPLACE INTO demand_supply_health
        SELECT
            COALESCE(s.window_start, d.window_start) AS window_start,
            COALESCE(s.window_end, d.window_end) AS window_end,
            COALESCE(s.zone_id, d.zone_id) AS zone_id,
            COALESCE(d.pending_demand, 0) AS pending_demand,
            COALESCE(s.available_supply, 0) AS available_supply,
            COALESCE(d.pending_demand, 0)::DOUBLE / GREATEST(COALESCE(s.available_supply, 0), 1) AS health_ratio,
            CASE
                WHEN COALESCE(d.pending_demand, 0)::DOUBLE / GREATEST(COALESCE(s.available_supply, 0), 1) < 1.0 THEN 'healthy'
                WHEN COALESCE(d.pending_demand, 0)::DOUBLE / GREATEST(COALESCE(s.available_supply, 0), 1) < 2.0 THEN 'moderate'
                WHEN COALESCE(d.pending_demand, 0)::DOUBLE / GREATEST(COALESCE(s.available_supply, 0), 1) < 4.0 THEN 'stressed'
                ELSE 'critical'
            END AS health_status
        FROM _supply_batch s
        FULL OUTER JOIN _demand_staging d
            ON s.window_start = d.window_start
            AND s.window_end = d.window_end
            AND s.zone_id = d.zone_id
    """)
    con.close()
