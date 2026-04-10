"""
postgres_sink.py (historically duckdb_sink.py)
----------------------------------------------
Writes Spark micro-batch results to Supabase Postgres.
Uses accumulative upserts so windows build up across batches.
"""

import os
import psycopg2

DATABASE_URL = os.environ.get("DATABASE_URL", "")

# Table creation DDL
_TABLE_DDL = {
    "windowed_kpis": """
        CREATE TABLE IF NOT EXISTS windowed_kpis (
            window_start TIMESTAMPTZ,
            window_end TIMESTAMPTZ,
            window_type TEXT,
            zone_id TEXT,
            order_count BIGINT DEFAULT 0,
            total_revenue DOUBLE PRECISION DEFAULT 0,
            avg_prep_minutes DOUBLE PRECISION,
            cancellation_count BIGINT DEFAULT 0,
            cancellation_rate DOUBLE PRECISION DEFAULT 0,
            PRIMARY KEY (window_start, window_end, window_type, zone_id)
        )
    """,
    "demand_supply_health": """
        CREATE TABLE IF NOT EXISTS demand_supply_health (
            window_start TIMESTAMPTZ,
            window_end TIMESTAMPTZ,
            zone_id TEXT,
            pending_demand BIGINT DEFAULT 0,
            available_supply BIGINT DEFAULT 0,
            health_ratio DOUBLE PRECISION DEFAULT 0,
            health_status TEXT,
            PRIMARY KEY (window_start, window_end, zone_id)
        )
    """,
    "_demand_staging": """
        CREATE TABLE IF NOT EXISTS _demand_staging (
            window_start TIMESTAMPTZ,
            window_end TIMESTAMPTZ,
            zone_id TEXT,
            pending_demand BIGINT DEFAULT 0,
            PRIMARY KEY (window_start, window_end, zone_id)
        )
    """,
    "restaurant_sla": """
        CREATE TABLE IF NOT EXISTS restaurant_sla (
            window_start TIMESTAMPTZ,
            window_end TIMESTAMPTZ,
            restaurant_id TEXT,
            restaurant_name TEXT,
            zone_id TEXT,
            cuisine TEXT,
            sla_tier TEXT,
            order_count BIGINT DEFAULT 0,
            p50_prep DOUBLE PRECISION,
            p95_prep DOUBLE PRECISION,
            p99_prep DOUBLE PRECISION,
            sla_breached BOOLEAN DEFAULT FALSE,
            PRIMARY KEY (window_start, window_end, restaurant_id)
        )
    """,
    "delivery_anomalies": """
        CREATE TABLE IF NOT EXISTS delivery_anomalies (
            window_start TIMESTAMPTZ,
            window_end TIMESTAMPTZ,
            zone_id TEXT,
            delivery_count BIGINT DEFAULT 0,
            mean_delivery_min DOUBLE PRECISION,
            stddev_delivery_min DOUBLE PRECISION,
            anomaly_threshold DOUBLE PRECISION,
            anomaly_count BIGINT DEFAULT 0,
            late_event_count BIGINT DEFAULT 0,
            PRIMARY KEY (window_start, window_end, zone_id)
        )
    """,
    "fraud_alerts": """
        CREATE TABLE IF NOT EXISTS fraud_alerts (
            window_start TIMESTAMPTZ,
            window_end TIMESTAMPTZ,
            device_id TEXT,
            distinct_customer_ids BIGINT DEFAULT 0,
            cancellation_count BIGINT DEFAULT 0,
            refund_count BIGINT DEFAULT 0,
            total_order_value DOUBLE PRECISION DEFAULT 0,
            fraud_flags TEXT,
            PRIMARY KEY (window_start, window_end, device_id)
        )
    """,
    "courier_positions": """
        CREATE TABLE IF NOT EXISTS courier_positions (
            courier_id TEXT PRIMARY KEY,
            zone_id TEXT,
            latitude DOUBLE PRECISION,
            longitude DOUBLE PRECISION,
            speed_kmh DOUBLE PRECISION,
            status TEXT,
            vehicle_type TEXT,
            updated_at TIMESTAMPTZ DEFAULT NOW()
        )
    """,
}

# Accumulative upsert SQL per table
_ACCUMULATE_SQL = {
    "windowed_kpis": """
        INSERT INTO windowed_kpis (window_start, window_end, window_type, zone_id,
            order_count, total_revenue, avg_prep_minutes, cancellation_count, cancellation_rate)
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
        ON CONFLICT (window_start, window_end, window_type, zone_id) DO UPDATE SET
            order_count = windowed_kpis.order_count + COALESCE(EXCLUDED.order_count, 0),
            total_revenue = COALESCE(windowed_kpis.total_revenue, 0) + COALESCE(EXCLUDED.total_revenue, 0),
            avg_prep_minutes = COALESCE(EXCLUDED.avg_prep_minutes, windowed_kpis.avg_prep_minutes),
            cancellation_count = windowed_kpis.cancellation_count + COALESCE(EXCLUDED.cancellation_count, 0),
            cancellation_rate = (windowed_kpis.cancellation_count + COALESCE(EXCLUDED.cancellation_count, 0))::FLOAT
                / GREATEST(windowed_kpis.order_count + COALESCE(EXCLUDED.order_count, 0), 1)
    """,
    "_demand_staging": """
        INSERT INTO _demand_staging (window_start, window_end, zone_id, pending_demand)
        VALUES (%s, %s, %s, %s)
        ON CONFLICT (window_start, window_end, zone_id) DO UPDATE SET
            pending_demand = _demand_staging.pending_demand + EXCLUDED.pending_demand
    """,
    "restaurant_sla": """
        INSERT INTO restaurant_sla (window_start, window_end, restaurant_id, restaurant_name,
            zone_id, cuisine, sla_tier, order_count, p50_prep, p95_prep, p99_prep, sla_breached)
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
        ON CONFLICT (window_start, window_end, restaurant_id) DO UPDATE SET
            order_count = restaurant_sla.order_count + EXCLUDED.order_count,
            p50_prep = EXCLUDED.p50_prep,
            p95_prep = EXCLUDED.p95_prep,
            p99_prep = EXCLUDED.p99_prep,
            sla_breached = EXCLUDED.sla_breached
    """,
    "delivery_anomalies": """
        INSERT INTO delivery_anomalies (window_start, window_end, zone_id,
            delivery_count, mean_delivery_min, stddev_delivery_min,
            anomaly_threshold, anomaly_count, late_event_count)
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
        ON CONFLICT (window_start, window_end, zone_id) DO UPDATE SET
            delivery_count = delivery_anomalies.delivery_count + EXCLUDED.delivery_count,
            mean_delivery_min = EXCLUDED.mean_delivery_min,
            stddev_delivery_min = EXCLUDED.stddev_delivery_min,
            anomaly_threshold = EXCLUDED.anomaly_threshold,
            anomaly_count = delivery_anomalies.anomaly_count + EXCLUDED.anomaly_count,
            late_event_count = delivery_anomalies.late_event_count + EXCLUDED.late_event_count
    """,
    "fraud_alerts": """
        INSERT INTO fraud_alerts (window_start, window_end, device_id,
            distinct_customer_ids, cancellation_count, refund_count,
            total_order_value, fraud_flags)
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
        ON CONFLICT (window_start, window_end, device_id) DO UPDATE SET
            distinct_customer_ids = GREATEST(fraud_alerts.distinct_customer_ids, EXCLUDED.distinct_customer_ids),
            cancellation_count = fraud_alerts.cancellation_count + EXCLUDED.cancellation_count,
            refund_count = fraud_alerts.refund_count + EXCLUDED.refund_count,
            total_order_value = fraud_alerts.total_order_value + EXCLUDED.total_order_value,
            fraud_flags = EXCLUDED.fraud_flags
    """,
    "courier_positions": """
        INSERT INTO courier_positions (courier_id, zone_id, latitude, longitude, speed_kmh, status, vehicle_type, updated_at)
        VALUES (%s, %s, %s, %s, %s, %s, %s, NOW())
        ON CONFLICT (courier_id) DO UPDATE SET
            zone_id = EXCLUDED.zone_id,
            latitude = EXCLUDED.latitude,
            longitude = EXCLUDED.longitude,
            speed_kmh = EXCLUDED.speed_kmh,
            status = EXCLUDED.status,
            vehicle_type = EXCLUDED.vehicle_type,
            updated_at = NOW()
    """,
}


def _get_conn():
    """Get a Postgres connection."""
    return psycopg2.connect(DATABASE_URL)


def init_tables():
    """Create all tables if they don't exist."""
    conn = _get_conn()
    cur = conn.cursor()
    for ddl in _TABLE_DDL.values():
        cur.execute(ddl)
    conn.commit()
    cur.close()
    conn.close()


def write_metrics(pandas_df, table_name: str):
    """
    Upsert a Pandas DataFrame into Postgres.
    Uses accumulative upserts for count/sum columns.
    """
    if pandas_df.empty:
        return

    conn = _get_conn()
    cur = conn.cursor()

    # Ensure table exists
    if table_name in _TABLE_DDL:
        cur.execute(_TABLE_DDL[table_name])

    # Convert values for Postgres
    import math
    df = pandas_df.copy()
    for c in df.columns:
        if hasattr(df[c], 'dt'):
            df[c] = df[c].astype(str)

    # Replace NaN with None (Postgres NULL) to prevent NaN poisoning accumulations
    rows = []
    for row in df.itertuples(index=False, name=None):
        cleaned = tuple(None if (isinstance(v, float) and math.isnan(v)) else v for v in row)
        rows.append(cleaned)

    if table_name in _ACCUMULATE_SQL:
        cur.executemany(_ACCUMULATE_SQL[table_name], rows)
    else:
        # Fallback: simple upsert for demand_supply_health etc.
        cols = list(df.columns)
        placeholders = ", ".join(["%s"] * len(cols))
        col_names = ", ".join(cols)
        # Get primary key columns from DDL to build ON CONFLICT
        update_cols = [c for c in cols if c not in ("window_start", "window_end", "zone_id")]
        update_set = ", ".join([f"{c} = EXCLUDED.{c}" for c in update_cols])
        pk_cols = "window_start, window_end, zone_id"
        sql = f"INSERT INTO {table_name} ({col_names}) VALUES ({placeholders}) ON CONFLICT ({pk_cols}) DO UPDATE SET {update_set}"
        cur.executemany(sql, rows)

    conn.commit()
    cur.close()
    conn.close()
