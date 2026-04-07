# Milestone 2: Stream Analytics Implementation — Design Spec

## Overview

End-to-end streaming analytics pipeline for the food delivery platform. The generator (Milestone 1) pushes events to Azure Event Hubs, Spark Structured Streaming processes them locally, results land as Parquet in Azure Blob Storage and aggregated metrics in DuckDB, and a Streamlit dashboard visualizes everything live.

## Architecture

```
Generator (existing Python CLI)
  └─► Azure Event Hubs (2 topics, Kafka-compatible)
        └─► Spark Structured Streaming (local PySpark + Event Hubs connector)
              ├─► Raw + aggregated Parquet → Azure Blob Storage
              └─► Aggregated metrics → local DuckDB
                    └─► Streamlit Dashboard (auto-refresh)
```

---

## 2.1 Ingestion — Azure Event Hubs

### Topic Design

| Property | `order-lifecycle-events` | `courier-status-events` |
|---|---|---|
| Partitions | 4 | 4 |
| Partition key | `zone_id` | `zone_id` |
| Avg message size | ~800 bytes (JSON) | ~700 bytes (JSON) |
| Expected throughput | ~2 evt/s base, ~6 evt/s surge | ~1.5 evt/s |
| Retention | 1 day (default) | 1 day (default) |

### Partition Key Rationale

`zone_id` ensures all events for a zone land on the same partition. This enables:
- Ordered processing per zone (important for state machines)
- Efficient zone-level joins and aggregations without cross-partition shuffles
- 4 partitions match the 5 zones with acceptable skew (CENTRAL gets ~30% load)

### Consumer Groups

| Consumer Group | Purpose |
|---|---|
| `$Default` | Azure default, unused |
| `spark-processing` | Main Spark Structured Streaming job |

### Message Format

JSON serialization (matching Milestone 1 generator output). Messages include `event_time` (event-time semantics) and `ingestion_time` (processing-time for latency tracking).

### Producer Changes

Modify the existing generator's `simulator.py` to publish to Event Hubs using the `azure-eventhub` SDK instead of the current Kafka placeholder. The generator already has topic names and Kafka config — replace with Event Hubs connection strings via environment variables.

---

## 2.2 Stream Processing — Spark Structured Streaming

### Runtime

- Local PySpark (no cluster)
- `azure-eventhubs-spark` connector JAR for reading from Event Hubs
- `hadoop-azure` + `azure-storage` JARs for writing Parquet to Blob Storage

### Pipeline Stages

#### Stage 1: Parsing, Validation, Enrichment

- Read JSON from Event Hubs as structured streams
- Parse into typed schema using `from_json()`
- Validation: drop rows with null `event_id` or `event_time`
- Deduplicate: filter `is_duplicate == true` (generator marks these)
- Enrich orders with restaurant reference data (SLA tier, cuisine) via broadcast join on static DataFrame loaded from `reference_data.py`

#### Stage 2: Watermarks

| Stream | Watermark | Rationale |
|---|---|---|
| Orders | 5 minutes | Generator injects late events 60–300s late (5% rate). 5-min watermark captures most. |
| Couriers | 3 minutes | Courier events are more time-sensitive; heartbeats should arrive promptly. |

#### Stage 3: Windowed Processing (Use Cases)

See Use Cases section below.

#### Stage 4: Output Sinks

Two output paths run in parallel:
1. **Parquet → Azure Blob Storage** — raw events + aggregated results, partitioned by date/hour
2. **DuckDB → local file** — aggregated metrics for dashboard consumption, upserted per window

### Parquet Output Structure (Blob Storage)

```
fooddelivery-container/
├── raw/
│   ├── orders/year=YYYY/month=MM/day=DD/hour=HH/*.parquet
│   └── couriers/year=YYYY/month=MM/day=DD/hour=HH/*.parquet
└── aggregated/
    ├── windowed_kpis/year=YYYY/month=MM/day=DD/*.parquet
    ├── demand_supply/year=YYYY/month=MM/day=DD/*.parquet
    ├── restaurant_sla/year=YYYY/month=MM/day=DD/*.parquet
    ├── anomalies/year=YYYY/month=MM/day=DD/*.parquet
    └── fraud_alerts/year=YYYY/month=MM/day=DD/*.parquet
```

### Checkpoint Location

Spark checkpoints stored in Azure Blob Storage under `fooddelivery-container/checkpoints/` to enable restart recovery.

---

## 2.3 Use Cases

### Use Case 1: Basic — Windowed KPIs

**Windows:**
- **5-minute tumbling window:** order count, total revenue (`order_value_eur`), average `actual_prep_minutes` per `zone_id`
- **15-minute hopping window (5-min slide):** cancellation rate per `zone_id` (count of CANCELLED / total orders)

**Input:** Order lifecycle events (status = PLACED for counts, CANCELLED for cancellation rate, DELIVERED for prep/delivery times)

**Output:** Parquet + DuckDB table `windowed_kpis`

**Schema:**
```
window_start, window_end, window_type, zone_id,
order_count, total_revenue, avg_prep_minutes,
cancellation_count, cancellation_rate
```

### Use Case 2a: Intermediate — Demand-Supply Health per Zone

**Logic:**
- From order stream: count orders in state PLACED/CONFIRMED/PREPARING (not yet picked up) per zone per 5-min window = `pending_demand`
- From courier stream: count couriers in state ONLINE_IDLE per zone per 5-min window = `available_supply`
- Compute `health_ratio = pending_demand / max(available_supply, 1)`
- Classify: ratio < 1.0 = "healthy", 1.0–2.0 = "moderate", 2.0–4.0 = "stressed", > 4.0 = "critical"

**Implementation:** Stream-stream join on `zone_id` within the same 5-min window. Both streams watermarked.

**Output:** DuckDB table `demand_supply_health`

**Schema:**
```
window_start, window_end, zone_id,
pending_demand, available_supply, health_ratio, health_status
```

### Use Case 2b: Intermediate — Restaurant SLA Monitoring

**Logic:**
- Track `actual_prep_minutes` for DELIVERED orders per `restaurant_id` in 15-min tumbling windows
- Compute percentiles: p50, p95, p99 using `percentile_approx()`
- SLA thresholds by tier: GOLD ≤ 15min, SILVER ≤ 25min, BRONZE ≤ 35min
- Flag `sla_breached = true` when p95 exceeds tier threshold

**Input:** Order lifecycle events (status = DELIVERED, joined with restaurant reference data for SLA tier)

**Output:** Parquet + DuckDB table `restaurant_sla`

**Schema:**
```
window_start, window_end, restaurant_id, restaurant_name, zone_id,
cuisine, sla_tier, order_count, p50_prep, p95_prep, p99_prep, sla_breached
```

### Use Case 3a: Advanced — Delivery Time Anomaly Detection

**Logic:**
- Sliding 30-min window (5-min slide) of `actual_delivery_minutes` per `zone_id`
- Compute running mean and stddev
- Flag anomaly when any individual delivery time > mean + 2σ
- Late data handling: 5-min watermark allows late arrivals to update existing windows
- Track `late_event_count` per window to demonstrate late data handling

**Input:** Order lifecycle events (status = DELIVERED)

**Output:** DuckDB table `delivery_anomalies`

**Schema:**
```
window_start, window_end, zone_id,
delivery_count, mean_delivery_min, stddev_delivery_min,
anomaly_threshold, anomaly_count, late_event_count
```

### Use Case 3b: Advanced — Fraud Heuristics

**Logic:**
- 1-hour tumbling window
- Group by `device_id`: count CANCELLED + REFUNDED orders
- Flag suspicious when:
  - Same `device_id` has ≥ 3 cancellations in window, OR
  - Same `device_id` appears with ≥ 2 distinct `customer_id`s (account hopping)
- Cross-references generator's built-in fraud cluster injection (`FRAUD_BURST` config)

**Input:** Order lifecycle events (all statuses, keyed by `device_id`)

**Output:** DuckDB table `fraud_alerts`

**Schema:**
```
window_start, window_end, device_id,
distinct_customer_ids, cancellation_count, refund_count,
total_order_value, fraud_flags (array of string reasons)
```

---

## 2.4 Dashboard — Streamlit

### Pages

1. **Live KPIs** — auto-refresh every 10s
   - Cards: total orders, revenue, avg delivery time (current window)
   - Time-series line charts: orders/revenue over time per zone
   - Bar chart: cancellation rate by zone

2. **Demand-Supply Health** — auto-refresh every 10s
   - Zone heatmap/cards colored by health status (green/yellow/orange/red)
   - Time-series of health ratio per zone
   - Current status table

3. **Restaurant SLA** — auto-refresh every 30s
   - Table: restaurant name, zone, cuisine, SLA tier, p50/p95/p99, breach flag
   - Filterable by zone and cuisine dropdowns
   - Highlight rows with SLA breaches in red

4. **Alerts** — auto-refresh every 10s
   - Combined feed of delivery anomalies + fraud alerts
   - Sorted by timestamp descending
   - Color-coded by severity
   - Filterable by zone

### Global Controls

- Zone dropdown filter (applies across all pages)
- Time range selector (last 15min / 1hr / 4hr)

### Data Source

All pages read from local DuckDB file. Streamlit uses `st.cache_data` with TTL matching refresh interval.

---

## 2.5 Reflection & Discussion

Will be drafted based on actual implementation experience. Will cover:
- Streaming tradeoffs encountered (latency vs completeness, watermark tuning)
- Data design decisions that enabled/hindered analytics
- Production readiness gaps (reliability, scalability, maintainability)

---

## Project Structure (New Files)

```
food-delivery-streaming/
├── generator/
│   ├── simulator.py              # Modified: add Event Hubs publishing
│   ├── eventhub_producer.py      # New: Event Hubs producer wrapper
│   └── requirements.txt          # Updated: + azure-eventhub
├── processing/
│   ├── spark_streaming.py        # Main Spark job (all use cases)
│   ├── schemas.py                # Spark StructType definitions
│   ├── enrichment.py             # Reference data broadcast + join logic
│   ├── use_cases/
│   │   ├── windowed_kpis.py      # Use Case 1
│   │   ├── demand_supply.py      # Use Case 2a
│   │   ├── restaurant_sla.py     # Use Case 2b
│   │   ├── anomaly_detection.py  # Use Case 3a
│   │   └── fraud_detection.py    # Use Case 3b
│   ├── sinks/
│   │   ├── parquet_sink.py       # Blob Storage Parquet writer
│   │   └── duckdb_sink.py        # DuckDB upsert logic
│   └── requirements.txt          # pyspark, azure-eventhubs-spark, duckdb
├── dashboard/
│   ├── app.py                    # Streamlit main app
│   ├── pages/
│   │   ├── 1_live_kpis.py
│   │   ├── 2_demand_supply.py
│   │   ├── 3_restaurant_sla.py
│   │   └── 4_alerts.py
│   ├── components/
│   │   └── filters.py            # Shared zone/time filters
│   ├── db.py                     # DuckDB connection helper
│   └── requirements.txt          # streamlit, duckdb, plotly
├── config/
│   ├── eventhub_config.py        # Connection strings, topic config
│   └── spark_config.py           # Spark session + connector config
├── scripts/
│   ├── setup_azure.sh            # Create Event Hubs namespace + topics
│   └── setup_blob.sh             # Create storage account + container
└── docs/
    ├── milestone1_design.md
    └── milestone2_design.md       # Topic design documentation (for grading)
```

---

## Dependencies

### Processing (`processing/requirements.txt`)
```
pyspark>=3.5.0
azure-eventhubs-spark (Maven: com.microsoft.azure:azure-eventhubs-spark_2.12:2.3.22)
duckdb>=0.10.0
```

### Dashboard (`dashboard/requirements.txt`)
```
streamlit>=1.30.0
duckdb>=0.10.0
plotly>=5.18.0
pandas>=2.0.0
```

### Generator updates (`generator/requirements.txt`)
```
fastavro>=1.9.0
azure-eventhub>=5.11.0
```

---

## Azure Resources Required

1. **Event Hubs Namespace** (Standard tier, 1 TU sufficient for dev)
   - Topic: `order-lifecycle-events` (4 partitions)
   - Topic: `courier-status-events` (4 partitions)
   - Consumer group: `spark-processing`

2. **Storage Account** (Standard LRS)
   - Container: `fooddelivery`
   - Used for: Parquet output + Spark checkpoints

---

## Environment Variables

```bash
# Event Hubs
EVENTHUB_NAMESPACE=<namespace>.servicebus.windows.net
EVENTHUB_CONNECTION_STRING=Endpoint=sb://<namespace>.servicebus.windows.net/;SharedAccessKeyName=...
EVENTHUB_ORDER_TOPIC=order-lifecycle-events
EVENTHUB_COURIER_TOPIC=courier-status-events

# Blob Storage
AZURE_STORAGE_ACCOUNT=<account-name>
AZURE_STORAGE_KEY=<key>
AZURE_STORAGE_CONTAINER=fooddelivery

# DuckDB
DUCKDB_PATH=./data/metrics.duckdb
```
