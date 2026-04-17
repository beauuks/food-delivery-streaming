# Real-Time Food Delivery Streaming Analytics

> **Course Project – Milestones 1 & 2: Stream Analytics Pipeline**
> Stream Analytics | Academic Year 2025/26

---

## Table of Contents

1. [Project Overview](#project-overview)
2. [Team Structure](#team-structure)
3. [Architecture Overview](#architecture-overview)
4. [Feed Design](#feed-design)
   - [Feed 1: Order Lifecycle Events](#feed-1-order-lifecycle-events)
   - [Feed 2: Courier Status Events](#feed-2-courier-status-events)
   - [Design Justification](#design-justification)
5. [Schema Design](#schema-design)
6. [Data Generator](#data-generator)
7. [Realism & Edge Cases](#realism--edge-cases)
8. [Milestone 2: Stream Analytics Pipeline](#milestone-2-stream-analytics-pipeline)
9. [Repository Structure](#repository-structure)
10. [Quick Start (Milestone 1)](#quick-start-milestone-1)
11. [Running the Full Pipeline (Milestone 2)](#running-the-full-pipeline-milestone-2)

---

## Project Overview

This project implements a **real-time analytics pipeline** for a food delivery platform (analogous to Uber Eats, Glovo, or Deliveroo) operating across 10 districts of Madrid. The platform connects customers, restaurants, and couriers — generating high-volume streaming data that must be processed, stored, and visualised with minimal latency.

**Milestone 1** delivers:
- Two streaming data feeds with full AVRO schemas
- A Python event generator with realistic distributions, configurable parameters, and comprehensive streaming edge cases
- Sample data in both JSON and AVRO formats
- A design document justifying architectural choices

**Milestone 2** delivers:
- Live ingestion into Azure Event Hubs with an event queue for realistic lifecycle timing
- Spark Structured Streaming processing with 5 analytical use cases (basic → advanced)
- Aggregated metrics persisted to Supabase Postgres
- Raw event Parquet storage on local disk + Azure Blob via Stream Analytics
- A live Grafana dashboard featuring a Madrid geomap of couriers, KPIs, SLA monitoring, anomaly detection, and fraud alerts

---

## Team Structure

| Name | Role | Responsibilities |
|------|------|-----------------|
| Beau | Lead Engineer & Architect | Core system architecture, generator engineering, AVRO serialisation layer, schema design, infrastructure setup, sample data generation, and overall technical implementation |
| Rania Mansouri | Project Lead & Feed Designer | Overall project coordination, feed design justification, analytics requirements mapping, milestone deliverable management |
| Maciej | Generator Engineer | Python simulator support, demand model tuning, batch and stream mode orchestration |
| Ana | Schema Designer | AVRO schema field definitions, type decisions, schema evolution strategy, null safety design |
| Leen | Data Quality Lead | Edge case injection logic, streaming correctness validation, duplicate and late event handling |
| Sebastian | Documentation Lead | README writing, milestone1_design.md, design notes, repository structure and organisation |


---

## Architecture Overview

```mermaid
flowchart TD
    subgraph M1 ["MILESTONE 1: Generator"]
        DM["Demand Model (temporal + surge)"]
        RD["Reference Data (Madrid zones + 150 restaurants + 120 couriers)"]
        EQ["Event Queue (deferred realistic timing)"]
        ECI["Edge Case Injector (late, duplicate, fraud)"]
        EF["Event Factories"]

        DM --> EF
        RD --> EF
        ECI --> EF
        EF --> EQ
    end

    subgraph M2 ["MILESTONE 2: Stream Analytics"]
        EH["Azure Event Hubs (2 topics, 4 partitions, AVRO)"]
        SPARK["Spark Structured Streaming (local PySpark, Kafka protocol)"]
        BLOB["Azure Blob Storage (Parquet at rest)"]
        PG["Supabase Postgres (aggregated metrics)"]
        GRAF["Grafana Dashboard (live map + 20 panels)"]

        EH --> SPARK
        SPARK --> BLOB
        SPARK --> PG --> GRAF
    end

    EQ --> EH
```

---

## Feed Design

### Feed 1: Order Lifecycle Events

**Topic:** `group_6_orders`
**Schema:** `schemas/order_lifecycle_event.avsc`

An **Order Lifecycle Event** is emitted every time an order transitions between states. Rather than emitting a single "completed order" record, we use the **event-sourcing pattern**: each state transition is its own immutable event. This is fundamental to streaming analytics because:

1. It enables **event-time processing** — each event carries the timestamp of the actual state transition, not when the pipeline processed it.
2. It enables **partial-order analytics** — we can detect SLA breaches in real-time (e.g., `PREPARING` for too long) without waiting for `DELIVERED`.
3. It produces a full audit trail for fraud detection.

**State Machine:**

```mermaid
flowchart LR
    classDef default fill:#e1f5fe,stroke:#0288d1,stroke-width:2px,color:#000
    classDef exception fill:#ffebee,stroke:#c62828,stroke-width:2px,color:#000

    PLACED --> CONFIRMED
    CONFIRMED --> PREPARING
    PREPARING --> READY_FOR_PICKUP
    READY_FOR_PICKUP --> PICKED_UP
    PICKED_UP --> IN_TRANSIT
    IN_TRANSIT --> DELIVERED

    PLACED --> CANCELLED:::exception
    CANCELLED --> REFUNDED:::exception
```

**Key fields for analytics:**

| Field | Purpose |
|-------|---------|
| `event_time` | Actual timestamp of transition — watermark anchor |
| `ingestion_time` | Pipeline arrival time — delta = latency / late arrival |
| `order_id` | Join key across all events in same order sequence |
| `zone_id` | Zone-level aggregation for demand/surge analytics |
| `restaurant_id` | SLA monitoring join key |
| `courier_id` | Supply-demand join with Feed 2 |
| `estimated_*` vs `actual_*` | SLA breach computation |
| `device_id` + `customer_id` | Fraud clustering |
| `is_duplicate` / `is_late` | Streaming correctness test flags |

---

### Feed 2: Courier Status Events

**Topic:** `group_6_couriers`
**Schema:** `schemas/courier_status_event.avsc`

A **Courier Status Event** is emitted when a courier's state changes (assignment, movement milestone, going offline) or on a **periodic heartbeat** to maintain availability presence. This feed is essential because:

1. It powers **supply-side analytics** — how many couriers are available per zone at any moment.
2. It enables **session window analytics** — a courier's "active session" (online → offline) is a natural session boundary.
3. It provides the **location stream** needed for zone-level demand-supply balance and the live Grafana map.
4. It enables detection of **mid-delivery drops** (courier goes offline while carrying an order).

**State Machine:**

```mermaid
flowchart LR
    classDef default fill:#e1f5fe,stroke:#0288d1,stroke-width:2px,color:#000

    OFFLINE --> ONLINE_IDLE
    ONLINE_IDLE --> ONLINE_ASSIGNED
    ONLINE_ASSIGNED --> HEADING_TO_RESTAURANT
    HEADING_TO_RESTAURANT --> AT_RESTAURANT
    AT_RESTAURANT --> HEADING_TO_CUSTOMER
    HEADING_TO_CUSTOMER --> COMPLETED_DELIVERY

    COMPLETED_DELIVERY --> ONLINE_IDLE
    COMPLETED_DELIVERY --> OFFLINE
```

---

### Design Justification

**Why two feeds?** Separating order lifecycle (demand) from courier status (supply) enables clean stream-to-stream joins for zone health analytics and avoids conflating fundamentally different signals.

**Why event-sourcing?** Emitting every state transition gives full temporal fidelity — we can detect SLA breaches, measure pending demand at any point in time, and track real-time throughput. A snapshot model would discard this information.

**Why AVRO?** Schema enforcement, evolution, binary compactness, and unambiguous logical timestamp types across languages.

---

## Schema Design

Both schemas are in `schemas/` as `.avsc` files. Design principles:

| Principle | Implementation |
|-----------|---------------|
| **Event-time semantics** | `event_time` (timestamp-millis) on every event |
| **Late arrival detection** | `ingestion_time` alongside `event_time` |
| **Deduplication** | `event_id` (UUID) on every event |
| **Join support** | `order_id`, `restaurant_id`, `courier_id`, `zone_id` as explicit FK-like fields |
| **Null safety** | Union types `["null", "string"]` with `default: null` for optional fields |
| **Enum stability** | All categorical fields use AVRO enums with explicit symbol lists |
| **Extensibility** | `metadata: map<string>` for future fields without schema break |
| **Schema evolution** | `schema_version` string field + namespace versioning |
| **Testing flags** | `is_duplicate`, `is_late` for streaming correctness validation |

---

## Data Generator

### Components

```
generator/
├── config.py           # All simulation parameters (env-var overridable)
├── reference_data.py   # Restaurant and Courier entity generation
├── event_factories.py  # Pure event construction functions
├── avro_utils.py       # AVRO serialisation layer
├── eventhub_producer.py# Azure Event Hubs producer wrapper
└── simulator.py        # Orchestration, demand model, event queue, CLI
```

### Demand Model

The temporal demand model uses a **24-hour hourly multiplier array** calibrated to real food delivery patterns:

- **Lunch peak:** 12:00–14:00 (multiplier ~1.0)
- **Dinner peak:** 19:00–21:00 (multiplier ~1.0)
- **Night trough:** 02:00–05:00 (multiplier ~0.02, probabilistic: 0 orders possible)
- **Weekend boost:** 1.35× applied on top of hourly multiplier
- **Per-tick jitter:** 50%–150% random variation to avoid patterned output

### Zone Model — Madrid Districts

| Zone ID | District | Demand Weight |
|---|---|---|
| CENTRO | Centro | 18% |
| SALAMANCA | Salamanca | 14% |
| CHAMBERI | Chamberí | 12% |
| RETIRO | Retiro | 10% |
| TETUAN | Tetuán | 9% |
| LATINA | Latina | 8% |
| ARGANZUELA | Arganzuela | 8% |
| MONCLOA | Moncloa-Aravaca | 7% |
| CHAMARTIN | Chamartín | 7% |
| MALASANA | Malasaña | 7% |

All zones use real Madrid latitude/longitude centres so couriers and restaurants appear correctly on the Grafana geomap.

### Cuisines (11 types)

SPANISH, ITALIAN, JAPANESE, AMERICAN, MEXICAN, CHINESE, INDIAN, MIDDLE_EASTERN, THAI, HEALTHY, DESSERT — each with calibrated prep time and order value distributions and themed restaurant names (e.g., "Casa del Sol", "Trattoria Artesano", "Sakura Madrid").

### Event Queue (Stream Mode)

In stream mode, lifecycle events are emitted at **realistic wall-clock times** via a deferred event queue:
- **PLACED** → emitted immediately
- **CONFIRMED** → ~30–60 s later
- **PREPARING** → ~65 s later
- **READY_FOR_PICKUP** → ~20 min later
- **DELIVERED** → ~30–40 min later

This ensures Spark sees a realistic stream — each micro-batch contains events from different lifecycle stages of different orders, not the entire lifecycle of one order all at once.

### Dynamic Surge Manager

Instead of a single fixed surge, the generator periodically triggers **random multi-zone surges**:
- Every 10–20 minutes a new surge starts in a random primary zone + 0–2 adjacent zones
- Random 1.5×–4× multiplier
- Random 1–5 minute duration
- Random reason: football match, concert ending, heavy rain, festival crowd, etc.

---

## Realism & Edge Cases

| Edge Case | Implementation | Rate |
|-----------|---------------|------|
| **Late events** | `event_time` backdated by 60–300s, `ingestion_time` = now | 5% |
| **Duplicate events** | Same event re-emitted with `is_duplicate: true` | 2% |
| **Missing steps** | Order jumps from READY_FOR_PICKUP → IN_TRANSIT (no PICKED_UP) | 1% |
| **Impossible durations** | Delivery in <2s or >2 hours (anomaly detection) | 0.8% |
| **Courier mid-delivery drop** | Courier OFFLINE during active delivery | 1.5% |
| **Fraud clusters** | Group of customers sharing a device_id | 0.5% |
| **Demand surge** | Rotating multi-zone surges, random intensity + duration | Periodic |
| **Order cancellations** | Full cancellation with reason codes | 8% |
| **Promo orders** | Orders with promotional codes | 20% |

### Why These Edge Cases Matter for Streaming

- **Late events + watermarks:** Spark Structured Streaming requires watermarks to handle late data. Our `is_late` flag lets us measure how many events fall outside the watermark threshold and tune accordingly.
- **Duplicates + idempotent sinks:** Without deduplication on `event_id`, windowed aggregations overcount. Our duplicates test dedup logic.
- **Missing steps:** Tests that analytics don't break when expected intermediate events are absent.
- **Impossible durations:** Seeds the anomaly detection use case.
- **Mid-delivery drops:** Tests the demand-supply health metric.

---

## Milestone 2: Stream Analytics Pipeline

### Pipeline

```
Generator (AVRO serialization, event queue for realistic timing)
  └─► Azure Event Hubs (2 topics, 4 partitions each, zone_id partition key)
        └─► Spark Structured Streaming (local PySpark, Kafka protocol)
              ├─► Azure Blob Storage (Parquet at rest via wasbs://)
              └─► Supabase Postgres (aggregated metrics, accumulative upserts)
                    └─► Grafana Dashboard (live map + 20 panels)
```

### Stream Processing (Spark Structured Streaming)

- **Serialization:** AVRO (generator serializes with `fastavro`, Spark deserializes with `from_avro()`)
- **Runtime:** Local PySpark 4.1.1 (Scala 2.13), `spark-sql-kafka-0-10` + `spark-avro` + `hadoop-azure` connectors, `spark.sql.session.timeZone=UTC`
- **Input:** Two Kafka sources reading from the Event Hubs Kafka-compatible endpoint (SASL_SSL, port 9093)
- **Processing:** 4 streaming queries:
  - `order_processing` — all order-side use cases → Supabase Postgres
  - `courier_processing` — courier-side use cases → Supabase Postgres
  - `orders_parquet_to_blob` — raw order events → Azure Blob Storage (Parquet)
  - `couriers_parquet_to_blob` — raw courier events → Azure Blob Storage (Parquet)
- **Output:**
  - Azure Blob Storage: `wasbs://group6@iesstsabdbaa.blob.core.windows.net/parquet/{orders,couriers}/`
  - Supabase Postgres: accumulative upserts for windowed metrics

### Use Cases Implemented

| # | Level | Use Case | Window | Output Table |
|---|---|---|---|---|
| 1a | Basic | Order count, revenue, avg prep time per zone | 5-min tumbling | `windowed_kpis` |
| 1b | Basic | Cancellation rate per zone | 15-min hopping (5-min slide) | `windowed_kpis` |
| 2a | Intermediate | Demand-supply health ratio per zone | 5-min tumbling (cross-stream) | `demand_supply_health` |
| 2b | Intermediate | Restaurant SLA monitoring (p50/p95/p99 prep time, tier-based breach detection) | 15-min tumbling | `restaurant_sla` |
| 3a | Advanced | Delivery time anomaly detection (z-score) with late event tracking | 30-min sliding (5-min slide) | `delivery_anomalies` |
| 3b | Advanced | Fraud heuristics (device-level cancellations, account hopping) | 1-hour tumbling | `fraud_alerts` |
| + | Live | Real-time courier positions (latest per courier) | Per batch | `courier_positions` |

### Dashboard (Grafana)

A single Grafana dashboard with 20+ panels powered by Supabase Postgres:

- **Live KPI stat cards:** total orders, revenue, avg prep time, cancellation rate, SLA breaches, fraud alerts
- **Live Courier Map — Madrid:** Geomap panel showing real-time courier positions with OpenStreetMap basemap, centred on Madrid (40.42°N, 3.70°W). Courier dots sized by speed.
- **Order & Revenue Trends:** stacked bar chart for orders by zone, line chart for revenue by zone
- **Cancellation Analysis:** time series with threshold shading + per-zone current bar chart
- **Demand-Supply Health:** zone gauges, trend chart with threshold lines, detail table with HEALTHY/MODERATE/STRESSED/CRITICAL colour coding
- **Restaurant SLA:** table with p50/p95/p99, breach highlighting, bar chart of worst offenders
- **Anomaly Detection:** mean vs threshold time series (solid vs dashed), anomaly count + late event bar chart
- **Fraud Alerts:** device-level table with fraud flags highlighted

Auto-refresh every 10 seconds. Zone filtering via Grafana template variable.

### Reflection & Production Readiness

See `docs/milestone2_design.md` for the full reflection on streaming tradeoffs, data design decisions, and production readiness gaps.

---

## Repository Structure

```
food-delivery-streaming/
├── README.md
├── .env.example                       # Template for environment variables
├── docs/
│   ├── milestone1_design.md           # Milestone 1 design document
│   └── milestone2_design.md           # Milestone 2 design + reflection
├── schemas/
│   ├── order_lifecycle_event.avsc
│   └── courier_status_event.avsc
├── generator/                         # Milestone 1 — event generator
│   ├── simulator.py                   # Main orchestrator + event queue + surge manager
│   ├── eventhub_producer.py           # Azure Event Hubs publishing
│   ├── config.py                      # Madrid zones, cuisines, demand model, edge cases
│   ├── reference_data.py              # 150 restaurants + 120 couriers
│   ├── event_factories.py
│   ├── avro_utils.py
│   └── requirements.txt
├── processing/                        # Milestone 2 — Spark Structured Streaming
│   ├── spark_streaming.py             # Main Spark job (consolidated foreachBatch)
│   ├── schemas.py                     # Spark StructType definitions
│   ├── enrichment.py                  # Restaurant reference data broadcast
│   ├── sinks/
│   │   └── postgres_sink.py           # Supabase Postgres upsert logic
│   └── requirements.txt
├── config/                            # Shared config modules
│   ├── eventhub_config.py             # Kafka-compatible EH connection
│   └── spark_config.py                # SparkSession builder (UTC)
├── dashboard/
│   └── grafana/
│       └── food-delivery-dashboard.json  # Grafana dashboard definition
└── sample_data/                       # Milestone 1 sample output
    ├── json/
    └── avro/
```

---

## Quick Start (Milestone 1)

```bash
# 1. Clone the repository
git clone https://github.com/beauuks/food-delivery-streaming.git
cd food-delivery-streaming

# 2. Install dependencies
pip install -r generator/requirements.txt

# 3. Generate sample data (batch mode)
cd generator
python simulator.py --mode batch --order-events 1000 --courier-events 500
```

---

## Running the Full Pipeline (Milestone 2)

### Prerequisites

- **Python 3.11+**, `pip`, `venv`
- **Apache Spark 4.x** with Scala 2.13 (`brew install apache-spark`)
- **Java 17+**
- **Grafana** (`brew install grafana`)
- **Azure Event Hubs** namespace with 2 event hubs (orders + couriers, 4 partitions each)
- **Azure Storage Account** with a Blob container
- **Supabase** project (free tier works, Postgres database)

### 1. Set up environment variables

Copy `.env.example` to `.env` and fill in your values:

```bash
cp .env.example .env
```

Required variables:

```bash
# Azure Event Hubs (per-hub connection strings with EntityPath)
export EVENTHUB_ORDER_CONNECTION_STRING="Endpoint=sb://<ns>.servicebus.windows.net/;SharedAccessKeyName=...;SharedAccessKey=...;EntityPath=<order-hub>"
export EVENTHUB_COURIER_CONNECTION_STRING="Endpoint=sb://<ns>.servicebus.windows.net/;SharedAccessKeyName=...;SharedAccessKey=...;EntityPath=<courier-hub>"
export EVENTHUB_ORDER_TOPIC=<order-hub-name>
export EVENTHUB_COURIER_TOPIC=<courier-hub-name>
export EVENTHUB_CONSUMER_GROUP=spark-processing

# Azure Blob Storage (for Stream Analytics output)
export AZURE_STORAGE_ACCOUNT=<account-name>
export AZURE_STORAGE_KEY=<key>
export AZURE_STORAGE_CONTAINER=<container-name>

# Supabase Postgres (transaction pooler connection string)
export DATABASE_URL="postgresql://postgres.<ref>:<password>@aws-1-<region>.pooler.supabase.com:6543/postgres"
```

### 2. Install Python dependencies

```bash
python3 -m venv venv
source venv/bin/activate
pip install -r generator/requirements.txt
pip install -r processing/requirements.txt
# psycopg2 needs to be installed globally for spark-submit
pip3 install psycopg2-binary --break-system-packages
```

### 3. Start the pipeline (3 terminals)

**Terminal 1 — Generator:**
```bash
cd food-delivery-streaming
source venv/bin/activate && source .env
cd generator
python3 simulator.py --mode stream --rate 3
```
The `--rate` flag is peak orders per second (default 3). Lifecycle events flow out of the event queue at realistic times (PLACED now, DELIVERED ~30 min later).

**Terminal 2 — Spark Streaming:**
```bash
cd food-delivery-streaming
source venv/bin/activate && source .env
cd processing
rm -rf data   # Clear checkpoints on first run or after schema changes
spark-submit \
  --packages org.apache.spark:spark-sql-kafka-0-10_2.13:4.1.1,org.apache.spark:spark-avro_2.13:4.1.1,org.apache.hadoop:hadoop-azure:3.3.1,com.microsoft.azure:azure-storage:8.6.6 \
  spark_streaming.py
```
You'll see `[orders] Batch N: X events` and `[couriers] Batch N: Y events` logs as data flows through. Parquet files will appear in Azure Blob Storage under `group6/parquet/`.

**Terminal 3 — Grafana:**
```bash
brew services start grafana
```
Then open `http://localhost:3000` (default login: `admin` / `admin`).

### 4. Configure Grafana (first time only)

1. **Add Postgres datasource:**
   - Left sidebar → **Connections** → **Data sources** → **Add data source** → **PostgreSQL**
   - Name: `Supabase`
   - Host: `aws-1-<region>.pooler.supabase.com:6543`
   - Database: `postgres`
   - User: `postgres.<project-ref>`
   - Password: `<your password>`
   - TLS/SSL Mode: `require`
   - Click **Save & test**

2. **Import the dashboard:**
   ```bash
   # From a fourth terminal in the project root
   curl -X POST http://admin:admin@localhost:3000/api/dashboards/db \
     -H "Content-Type: application/json" \
     -d @dashboard/grafana/food-delivery-dashboard.json
   ```
   The response includes the dashboard URL. Open it in your browser.

3. If panels show "No data", verify the datasource UID matches:
   ```bash
   curl -s http://admin:admin@localhost:3000/api/datasources | python3 -m json.tool
   ```
   If your datasource UID differs from `cfilvx0sixm2of`, replace it in `dashboard/grafana/food-delivery-dashboard.json` and re-import.

### 5. Let it run

Leave everything running for 30+ minutes. Panels populate in this order:
- **Immediately:** Order counts, revenue, courier positions on the map
- **~5 min:** Demand-supply health per zone
- **~20 min:** Restaurant SLA percentiles (needs DELIVERED events)
- **~30 min:** Delivery anomaly detection (needs full 30-min sliding window)
- **~1 hour:** Fraud alerts (needs 1-hour tumbling window)

### 6. Stopping the pipeline

- Ctrl+C in each terminal (generator, Spark)
- `brew services stop grafana`

---

## Available Environment Variables

Generator tunables (all optional, override via env or `.env`):

| Variable | Default | Description |
|----------|---------|-------------|
| `RESTAURANT_COUNT` | 150 | Number of synthetic restaurants |
| `COURIER_COUNT` | 120 | Number of synthetic couriers |
| `BASE_ORDERS_PER_SECOND` | 2.0 | Default peak throughput (CLI `--rate` overrides) |
| `CANCELLATION_RATE` | 0.08 | Fraction of orders cancelled |
| `LATE_EVENT_RATE` | 0.05 | Fraction of events arriving late |
| `DUPLICATE_RATE` | 0.02 | Fraction of events duplicated |
| `MISSING_STEP_RATE` | 0.01 | Fraction of orders with missing lifecycle step |
| `IMPOSSIBLE_DURATION_RATE` | 0.008 | Fraction with anomalous duration |
| `COURIER_MID_DELIVERY_DROP_RATE` | 0.015 | Fraction of couriers dropping mid-delivery |
| `SURGE_ENABLED` | true | Enable demand surge simulation |
| `SURGE_MIN_INTERVAL` | 600 | Seconds between surges (min) |
| `SURGE_MAX_INTERVAL` | 1200 | Seconds between surges (max) |
| `SURGE_MIN_DURATION` | 60 | Surge duration (min, seconds) |
| `SURGE_MAX_DURATION` | 300 | Surge duration (max, seconds) |
| `SURGE_MIN_MULTIPLIER` | 1.5 | Surge intensity (min) |
| `SURGE_MAX_MULTIPLIER` | 4.0 | Surge intensity (max) |
| `FRAUD_BURST` | true | Enable fraud cluster simulation |
