# Milestone 2: Stream Analytics — Design & Documentation

## 2.1 Ingestion: Topic Design

### Topics

| Topic | Purpose | Data Source |
|---|---|---|
| `group_6_orders` | All order state transitions (PLACED → DELIVERED/CANCELLED) | Generator (simulator.py) |
| `group_6_couriers` | Courier availability, location, and session events | Generator (simulator.py) |

### Partitions & Keys

Both topics use **4 partitions** with **`zone_id` as the partition key**.

**Why zone_id?**
- All 5 analytics use cases aggregate by zone — co-locating zone data on the same partition avoids cross-partition shuffles
- 10 Madrid districts across 4 partitions provides reasonable distribution with consistent zone-level ordering
- Maintains per-zone ordering, important for demand-supply ratio calculations

### Message Sizing

| Topic | Avg Message | Max Message | Format |
|---|---|---|---|
| Orders | ~800 bytes | ~1.2 KB | JSON |
| Couriers | ~700 bytes | ~1.0 KB | JSON |

JSON was chosen over Avro for the streaming path because:
- Spark's `from_json()` is simpler to debug during development
- Event Hubs handles JSON natively in its capture feature
- Message sizes are well under the 1MB Event Hubs limit

### Expected Throughput

| Metric | Orders | Couriers |
|---|---|---|
| Base rate (--rate 3) | ~1-4 events/sec (jittered) | ~30-40 events/sec (heartbeats + delivery events) |
| Night (00:00-06:00) | 0-1 events/sec (probabilistic) | ~10-15 events/sec |
| Peak (lunch/dinner) | ~2-4 events/sec | ~35-45 events/sec |
| Surge mode (2 min burst) | ~6-12 events/sec (3x in CENTRO) | ~50+ events/sec |

### Consumer Group Strategy

| Consumer Group | Consumer | Purpose |
|---|---|---|
| `spark-processing` | Spark Structured Streaming | Main analytics pipeline (use cases + Parquet output) |

Spark manages consumer offsets internally via its checkpoint mechanism. A single consumer group is sufficient since all 4 streaming queries share the same Spark read streams.

---

## 2.2 Stream Processing — Spark Structured Streaming

### Architecture

```
Generator (Python CLI, AVRO serialization, event queue for realistic timing)
  └─► Azure Event Hubs (2 topics, 4 partitions each, AVRO messages)
        └─► Spark Structured Streaming (local PySpark 4.1.1, Kafka protocol)
              ├─► Azure Blob Storage (Parquet at rest, via wasbs://)
              └─► Supabase Postgres (aggregated metrics, accumulative upserts)
                    └─► Grafana Dashboard (auto-refresh, live Madrid geomap)
```

### Runtime

- Local PySpark 4.1.1 (Scala 2.13)
- `spark-sql-kafka-0-10` connector for reading from Event Hubs via Kafka-compatible endpoint (SASL_SSL, port 9093)
- `spark-avro` for AVRO deserialization using `from_avro()`
- `hadoop-azure` + `azure-storage` for writing Parquet to Azure Blob Storage via `wasbs://` protocol
- Supabase (hosted Postgres) for aggregated metrics
- All timestamps in UTC (`spark.sql.session.timeZone = UTC`)

### Serialization

Events are serialized as **AVRO** by the generator using `fastavro` and the schema files in `schemas/`. Spark deserializes them using `from_avro()` with the same schema definitions. This ensures:
- Schema enforcement at both producer and consumer
- Binary compactness (smaller messages than JSON)
- Consistent type handling (timestamp-millis, enums, unions)

### Event Queue (Realistic Timing)

The generator uses a deferred event queue so lifecycle events arrive at realistic times:
- **PLACED** → emitted immediately when order is created
- **CONFIRMED** → emitted ~30-60s later
- **PREPARING** → emitted ~35-65s later
- **READY_FOR_PICKUP** → emitted ~20 min later
- **DELIVERED** → emitted ~30-40 min later

This ensures Spark sees a realistic stream where each batch contains events from different lifecycle stages of different orders, not the entire lifecycle of a single order.

### Pipeline Stages

#### Stage 1: AVRO Deserialization, Validation, Enrichment

- Read AVRO bytes from Event Hubs via Kafka protocol (binary `value` column)
- Deserialize using `from_avro()` with the original AVRO schema definitions
- Validation: drop rows with null `event_id` or `event_time`
- Deduplicate: filter out events where `is_duplicate == true` (generator marks these)
- Enrich orders with restaurant reference data (SLA tier, cuisine) via broadcast join on static DataFrame

#### Stage 2: 4 Streaming Queries

Spark runs 4 concurrent streaming queries:

1. **order_processing** (`foreachBatch`) — runs all order-side use cases (UC1, UC2a demand side, UC2b, UC3a, UC3b), writes aggregated metrics to Supabase Postgres
2. **courier_processing** (`foreachBatch`) — runs courier-side use cases (UC2a supply side, courier positions), writes to Supabase Postgres
3. **orders_parquet_to_blob** (`.format("parquet")`) — writes raw parsed order events to Azure Blob Storage as Parquet
4. **couriers_parquet_to_blob** (`.format("parquet")`) — writes raw parsed courier events to Azure Blob Storage as Parquet

The `foreachBatch` handlers use accumulative upserts (`ON CONFLICT DO UPDATE SET count = count + excluded.count`) to correctly accumulate windowed results across micro-batches.

#### Stage 3: Output Sinks

Two output destinations, both written by Spark:
1. **Azure Blob Storage (Parquet at rest)** — raw parsed events written directly via `wasbs://` protocol using `hadoop-azure` connector
2. **Supabase Postgres** — aggregated metrics for dashboard consumption via `psycopg2`

### Parquet Output Structure (Azure Blob Storage)

Written by Spark Structured Streaming to container `group6` on storage account `iesstsabdbaa`:
```
wasbs://group6@iesstsabdbaa.blob.core.windows.net/
├── parquet/
│   ├── orders/*.parquet
│   └── couriers/*.parquet
└── checkpoints/
    ├── orders/
    ├── couriers/
    ├── parquet_orders/
    └── parquet_couriers/
```

---

## 2.3 Use Cases

### Use Case 1: Basic — Windowed KPIs

**Windows:**
- **5-minute tumbling window:** order count, total revenue, average prep time per zone
- **15-minute hopping window (5-min slide):** cancellation rate per zone

**Input:** Order lifecycle events (PLACED for counts/revenue, CONFIRMED for prep times, CANCELLED for cancellation rate)

**Output:** Postgres table `windowed_kpis` (accumulative upserts)

**Schema:**
```
window_start, window_end, window_type, zone_id,
order_count, total_revenue, avg_prep_minutes,
cancellation_count, cancellation_rate
```

### Use Case 2a: Intermediate — Demand-Supply Health per Zone

**Logic:**
- From order stream: count orders in state PLACED/CONFIRMED/PREPARING/READY_FOR_PICKUP per zone per 5-min window = `pending_demand`
- From courier stream: count couriers in state ONLINE_IDLE per zone per 5-min window = `available_supply`
- Compute `health_ratio = pending_demand / max(available_supply, 1)`
- Classify: ratio < 1.0 = "healthy", 1.0–2.0 = "moderate", 2.0–4.0 = "stressed", > 4.0 = "critical"

**Implementation:** Independent stream aggregations. Demand written to `_demand_staging`, supply joins with staging in Python to compute health ratio.

**Output:** Postgres table `demand_supply_health`

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

**Output:** Postgres table `restaurant_sla`

**Schema:**
```
window_start, window_end, restaurant_id, restaurant_name, zone_id,
cuisine, sla_tier, order_count, p50_prep, p95_prep, p99_prep, sla_breached
```

### Use Case 3a: Advanced — Delivery Time Anomaly Detection

**Logic:**
- Sliding 30-min window (5-min slide) of `actual_delivery_minutes` per `zone_id`
- Compute running mean and stddev
- Flag anomaly when delivery time > mean + 2σ
- Late data handling: events with `is_late = true` are tracked per window
- Track `late_event_count` per window to demonstrate watermark handling

**Input:** Order lifecycle events (status = DELIVERED)

**Output:** Postgres table `delivery_anomalies`

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

**Output:** Postgres table `fraud_alerts`

**Schema:**
```
window_start, window_end, device_id,
distinct_customer_ids, cancellation_count, refund_count,
total_order_value, fraud_flags
```

### Additional: Live Courier Positions

**Logic:**
- Extract latest position (lat/lon), status, speed, and vehicle type per courier from each micro-batch
- Upsert into `courier_positions` table (one row per courier, always latest)

**Output:** Postgres table `courier_positions`

**Schema:**
```
courier_id, zone_id, latitude, longitude, speed_kmh, status, vehicle_type, updated_at
```

---

## 2.4 Dashboard — Grafana

### Overview

Live dashboard built with Grafana, reading from Supabase Postgres. Auto-refreshes every 10 seconds. Features zone filtering via template variable.

### Sections

1. **Live KPIs** — 6 stat cards: total orders, revenue, avg prep time, cancellation rate, SLA breaches, fraud alerts. Color-coded thresholds (green/yellow/red).

2. **Live Courier Map** — Grafana Geomap panel showing real-time courier positions across Madrid's 10 districts. Courier dots sized by speed, colored by status. OpenStreetMap basemap centered on Madrid (40.42°N, 3.70°W).

3. **Order & Revenue Trends** — Stacked bar chart for orders per 5-min window by zone. Line chart for revenue per zone.

4. **Cancellation Analysis** — Time series of cancellation rate by zone (hopping window) with threshold shading. Bar chart of current cancellation rate by zone.

5. **Demand-Supply Health** — Gauge panels per zone showing health ratio (green < 2, orange < 4, red ≥ 4). Time series with threshold lines. Detail table with color-coded health status (HEALTHY/MODERATE/STRESSED/CRITICAL).

6. **Restaurant SLA Monitoring** — Table with prep time percentiles (p50/p95/p99) per restaurant, SLA tier, breach status highlighted in red. Bar chart of top restaurants by p95 prep time.

7. **Anomaly Detection** — Time series showing mean delivery time vs anomaly threshold (solid vs dashed lines). Stacked bar chart of anomaly count + late event count per window.

8. **Fraud Detection** — Table of fraud alerts with device_id, cancellation/refund counts, fraud flags highlighted in red.

### Data Source

All panels read from Supabase Postgres via Grafana's native PostgreSQL datasource. Zone filtering via Grafana template variable with regex matching.

---

## 2.5 Reflection & Discussion

### What We Learned About Streaming Tradeoffs

**Latency vs. Completeness**: The 5-minute watermark captures ~95% of late events (generator injects 60–300s late events at 5% rate) but adds latency before windows finalize. Using `foreachBatch` with accumulative upserts allows partial results to appear immediately while windows continue building.

**Window Size vs. Signal Quality**: Smaller windows (5-min tumbling) give faster updates but noisier signals. Larger windows (30-min sliding for anomaly detection) smooth out noise but delay detection. The hopping window (15-min with 5-min slide) provided a good middle ground for cancellation rate.

**Event-Time vs. Processing-Time**: Using `event_time` for windowing was critical for correctness. The event queue in the generator ensures events arrive at realistic wall-clock times, making the event-time and processing-time relationship natural rather than artificial.

**Realistic Data Flow**: Initially we emitted all lifecycle events simultaneously, which created unrealistic patterns. Implementing a deferred event queue that emits events at their real-time offset (PLACED now, DELIVERED 30 min later) made the data indistinguishable from a real food delivery platform's event stream.

### How We Designed Data to Enable Analytics

**Event-sourcing over snapshots**: Emitting every state transition enabled temporal analytics without replaying history. The demand-supply use case specifically benefits — we count orders in "pending" states at any point in time.

**Partition key selection**: Using `zone_id` as partition key aligned data layout with our most common aggregation dimension. All use cases group by zone, so co-located data reduced shuffle overhead.

**Edge case injection with ground truth flags**: Generator-level flags (`is_duplicate`, `is_late`, `is_heartbeat`) let the streaming pipeline handle edge cases correctly and provided ground truth for validation.

**Geographic realism**: Using real Madrid district coordinates (10 zones) with realistic restaurant distributions and courier movements enabled the Geomap visualization and made the entire simulation geographically grounded.

### What We Would Need to Improve for Production Readiness

**Reliability**:
- Add a dead-letter queue for malformed/unparseable events instead of silently dropping them
- Implement exactly-once semantics end-to-end — currently at-least-once with deduplication via generator flags
- Add health checks and automatic restart for failed streaming queries

**Scalability**:
- Move from local PySpark to a managed Spark cluster (Azure Databricks) for horizontal scaling
- Increase Event Hubs partitions dynamically based on throughput
- Add backpressure handling — if Spark falls behind, no mechanism to signal the generator to slow down

**Maintainability**:
- Add a schema registry (e.g., Azure Schema Registry) so schema changes are versioned and validated at both producer and consumer
- Add monitoring dashboards for pipeline health: processing lag, micro-batch duration, error rates
- Implement automated checkpoint cleanup
- Externalize configuration (window sizes, watermarks, SLA thresholds) to a config file rather than hardcoding

---

## Project Structure

```
food-delivery-streaming/
├── generator/
│   ├── simulator.py              # Event generator with deferred queue for realistic timing
│   ├── eventhub_producer.py      # Azure Event Hubs producer wrapper
│   ├── config.py                 # Madrid zones, cuisines, demand model, edge cases
│   ├── reference_data.py         # 150 restaurants + 120 couriers with Spanish names
│   ├── event_factories.py        # Order lifecycle + courier event construction
│   ├── avro_utils.py             # AVRO serialization
│   └── requirements.txt          # fastavro, azure-eventhub, python-dotenv
├── processing/
│   ├── spark_streaming.py        # Main Spark job — consolidated foreachBatch for all use cases
│   ├── schemas.py                # Spark StructType definitions
│   ├── enrichment.py             # Restaurant reference data for broadcast joins
│   ├── sinks/
│   │   └── postgres_sink.py      # Supabase Postgres upsert logic (accumulative)
│   └── requirements.txt          # pyspark, psycopg2-binary, python-dotenv
├── dashboard/
│   └── grafana/
│       └── food-delivery-dashboard.json  # Grafana dashboard with geomap + 20 panels
├── config/
│   ├── eventhub_config.py        # Event Hubs Kafka-compatible connection config
│   └── spark_config.py           # Spark session builder (UTC timezone)
├── schemas/
│   ├── order_lifecycle_event.avsc
│   └── courier_status_event.avsc
├── docs/
│   ├── milestone1_design.md
│   └── milestone2_design.md
├── .env.example
└── .gitignore
```

---

## Azure & Cloud Resources

| Resource | Name | Details |
|---|---|---|
| Event Hubs Namespace | `iesstsabdbaa-grp-06-10` | Shared class namespace |
| Event Hub (orders) | `group_6_orders` | 4 partitions, 24hr retention |
| Event Hub (couriers) | `group_6_couriers` | 4 partitions, 24hr retention |
| Consumer Group | `spark-processing` | On both event hubs |
| Storage Account | `iesstsabdbaa` | Shared class storage |
| Blob Container | `group6` | Parquet at rest (written by Spark via wasbs://) + checkpoints |
| Supabase Postgres | `zubtbmhmtlipkvcfimnk` | Aggregated metrics + courier positions |

---

## Environment Variables

```bash
# Event Hubs (per-hub connection strings with EntityPath)
EVENTHUB_ORDER_CONNECTION_STRING=Endpoint=sb://...;EntityPath=group_6_orders
EVENTHUB_COURIER_CONNECTION_STRING=Endpoint=sb://...;EntityPath=group_6_couriers
EVENTHUB_ORDER_TOPIC=group_6_orders
EVENTHUB_COURIER_TOPIC=group_6_couriers
EVENTHUB_CONSUMER_GROUP=spark-processing

# Azure Blob Storage
AZURE_STORAGE_ACCOUNT=iesstsabdbaa
AZURE_STORAGE_KEY=<key>
AZURE_STORAGE_CONTAINER=group6

# Supabase Postgres
DATABASE_URL=postgresql://postgres.<ref>:<password>@aws-1-eu-central-1.pooler.supabase.com:6543/postgres
```

---

## Simulation Model: Madrid

### Zones (10 Madrid Districts)

| Zone ID | District | Lat | Lon | Demand Weight |
|---|---|---|---|---|
| CENTRO | Centro | 40.4168 | -3.7038 | 18% |
| SALAMANCA | Salamanca | 40.4310 | -3.6830 | 14% |
| CHAMBERI | Chamberí | 40.4350 | -3.7050 | 12% |
| RETIRO | Retiro | 40.4100 | -3.6770 | 10% |
| LATINA | Latina | 40.4023 | -3.7150 | 8% |
| MONCLOA | Moncloa-Aravaca | 40.4350 | -3.7200 | 7% |
| TETUAN | Tetuán | 40.4600 | -3.6970 | 9% |
| ARGANZUELA | Arganzuela | 40.3950 | -3.6950 | 8% |
| CHAMARTIN | Chamartín | 40.4620 | -3.6770 | 7% |
| MALASANA | Malasaña | 40.4260 | -3.7060 | 7% |

### Cuisines (11 types)

SPANISH, ITALIAN, JAPANESE, AMERICAN, MEXICAN, CHINESE, INDIAN, MIDDLE_EASTERN, THAI, HEALTHY, DESSERT

### Entity Counts

- 150 restaurants (Spanish-themed names, distributed across zones by demand weight)
- 120 couriers (5 vehicle types: bicycle, scooter, motorcycle, car, walking)
- 2000 synthetic customers with fraud cluster simulation
