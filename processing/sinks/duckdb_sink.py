"""
duckdb_sink.py
--------------
Writes Spark micro-batch results to a local DuckDB database.
Each use case calls write_to_duckdb() in its foreachBatch handler.
"""

import os
import duckdb

DUCKDB_PATH = os.getenv("DUCKDB_PATH", "./data/metrics.duckdb")

# Table creation DDL for each use case
_TABLE_DDL = {
    "windowed_kpis": """
        CREATE TABLE IF NOT EXISTS windowed_kpis (
            window_start TIMESTAMP,
            window_end TIMESTAMP,
            window_type VARCHAR,
            zone_id VARCHAR,
            order_count BIGINT,
            total_revenue DOUBLE,
            avg_prep_minutes DOUBLE,
            cancellation_count BIGINT,
            cancellation_rate DOUBLE,
            PRIMARY KEY (window_start, window_end, window_type, zone_id)
        )
    """,
    "demand_supply_health": """
        CREATE TABLE IF NOT EXISTS demand_supply_health (
            window_start TIMESTAMP,
            window_end TIMESTAMP,
            zone_id VARCHAR,
            pending_demand BIGINT,
            available_supply BIGINT,
            health_ratio DOUBLE,
            health_status VARCHAR,
            PRIMARY KEY (window_start, window_end, zone_id)
        )
    """,
    "restaurant_sla": """
        CREATE TABLE IF NOT EXISTS restaurant_sla (
            window_start TIMESTAMP,
            window_end TIMESTAMP,
            restaurant_id VARCHAR,
            restaurant_name VARCHAR,
            zone_id VARCHAR,
            cuisine VARCHAR,
            sla_tier VARCHAR,
            order_count BIGINT,
            p50_prep DOUBLE,
            p95_prep DOUBLE,
            p99_prep DOUBLE,
            sla_breached BOOLEAN,
            PRIMARY KEY (window_start, window_end, restaurant_id)
        )
    """,
    "delivery_anomalies": """
        CREATE TABLE IF NOT EXISTS delivery_anomalies (
            window_start TIMESTAMP,
            window_end TIMESTAMP,
            zone_id VARCHAR,
            delivery_count BIGINT,
            mean_delivery_min DOUBLE,
            stddev_delivery_min DOUBLE,
            anomaly_threshold DOUBLE,
            anomaly_count BIGINT,
            late_event_count BIGINT,
            PRIMARY KEY (window_start, window_end, zone_id)
        )
    """,
    "fraud_alerts": """
        CREATE TABLE IF NOT EXISTS fraud_alerts (
            window_start TIMESTAMP,
            window_end TIMESTAMP,
            device_id VARCHAR,
            distinct_customer_ids BIGINT,
            cancellation_count BIGINT,
            refund_count BIGINT,
            total_order_value DOUBLE,
            fraud_flags VARCHAR,
            PRIMARY KEY (window_start, window_end, device_id)
        )
    """,
}


def _ensure_dir():
    os.makedirs(os.path.dirname(os.path.abspath(DUCKDB_PATH)), exist_ok=True)


def init_tables():
    """Create all DuckDB tables if they don't exist."""
    _ensure_dir()
    con = duckdb.connect(DUCKDB_PATH)
    for ddl in _TABLE_DDL.values():
        con.execute(ddl)
    con.close()


def write_to_duckdb(pandas_df, table_name: str):
    """
    Upsert a Pandas DataFrame into DuckDB.
    Called from Spark foreachBatch with batch_df.toPandas().
    """
    if pandas_df.empty:
        return

    _ensure_dir()
    con = duckdb.connect(DUCKDB_PATH)

    # Ensure table exists
    if table_name in _TABLE_DDL:
        con.execute(_TABLE_DDL[table_name])

    # Use INSERT OR REPLACE for upsert semantics
    con.execute(f"INSERT OR REPLACE INTO {table_name} SELECT * FROM pandas_df")
    con.close()
