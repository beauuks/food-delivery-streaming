# Milestone 1 Design Document
## Streaming Data Feed Design & Generation

---

## 1. Feed Selection Rationale

### Why Order Lifecycle Events?

The order is the atomic unit of business value in a food delivery platform. Every business metric — revenue, SLA compliance, customer satisfaction, fraud loss — is ultimately traceable to an order. However, a single "order completed" record discards the temporal structure of the process.

We model the order as a **sequence of state transitions** rather than a single record because:

- **Real-time SLA monitoring** requires observing intermediate states. A restaurant that has been in PREPARING for 40 minutes is breaching its SLA right now — not when the order is eventually delivered.
- **Event-time processing** requires that each event carry the timestamp of when it actually occurred. If a courier picked up an order at 12:47 but the event arrived in the pipeline at 12:52, we must process it at 12:47 for correct windowed aggregation.
- **Partial failure detection** is only possible with intermediate events. A courier going offline mid-delivery creates a detectable gap between PICKED_UP and DELIVERED that would be invisible in a snapshot model.

### Why Courier Status Events?

The courier feed is the **supply side** of the marketplace. The fundamental operational challenge of food delivery is matching supply (available couriers) to demand (placed orders) in real time, by zone. Without a courier feed, we cannot compute:

- How many couriers are currently available in Zone X?
- Is Zone X experiencing a supply gap (many orders, few couriers)?
- How long was a courier's active session?
- Did a courier go offline while carrying an order?

The courier feed also provides a **session structure** — a courier's shift from first coming online to going offline — that naturally maps to session window analytics.

### Why Not a Third Feed?

A restaurant feed (prep status updates) was considered but rejected because:

- Restaurants update their prep status only at PREPARING and READY_FOR_PICKUP, information already captured in the order lifecycle feed via `estimated_prep_minutes` and `actual_prep_minutes`.
- Adding a third feed would require an additional join in every downstream query without providing analytically distinct information.

The two feeds are **complementary and sufficient**: together they cover demand-side dynamics (orders), supply-side dynamics (couriers), and the interaction between them (courier assignment, pickup, delivery).

---

## 2. Schema Design Decisions

### Temporal Fields

Every event carries two timestamps:

```json
"event_time":     1716300247000,  // When this happened in the real world
"ingestion_time": 1716300302481   // When the pipeline received this event
```

The **delta** between `ingestion_time` and `event_time` is our primary signal for late event detection. A delta of 55 seconds suggests a 55-second delivery delay from the mobile device; a delta of 5 minutes suggests network issues or intentional out-of-order simulation.

In Spark Structured Streaming, watermarks are defined on `event_time`:
```python
.withWatermark("event_time_parsed", "5 minutes")
```
This means events arriving more than 5 minutes late will be dropped from windowed aggregations. Our `is_late` flag (set when `ingestion_time - event_time > 60s`) allows us to measure what fraction of events fall into different latency buckets and tune the watermark threshold accordingly.

### Union Types for Optional Fields

AVRO represents optional fields as union types with null:
```json
"courier_id": ["null", "string"]
```

This is intentional: `courier_id` is genuinely absent for a PLACED order (no courier assigned yet) and genuinely present for a PICKED_UP order. Using null forces downstream code to handle the absent case explicitly, preventing silent errors in join logic.

### Enum Types for Categorical Fields

All status fields use AVRO enums rather than plain strings:
```json
{"type": "enum", "name": "OrderStatus", "symbols": ["PLACED", "CONFIRMED", ...]}
```

Benefits:
- **Schema enforcement**: an event with `status: "COMPLTED"` (typo) will fail AVRO validation at the producer.
- **Compact encoding**: enums are stored as integers in AVRO binary.
- **Self-documentation**: the schema is the source of truth for valid values.

### Metadata Map for Extensibility

Both schemas include:
```json
"metadata": {"type": "map", "values": "string", "default": {}}
```

This allows adding new string-valued fields in the future without a schema version bump — important for production systems where schema changes require coordination across teams. When a field becomes stable and typed, it can be promoted to a proper schema field in the next version.

### Schema Evolution Strategy

The `schema_version` field and `namespace` versioning (`com.fooddelivery.streams.v1`) allow us to evolve schemas without breaking existing consumers:

- **Backward compatible changes** (adding optional fields with defaults): increment minor version in `schema_version` string.
- **Breaking changes** (renaming fields, removing fields, changing types): bump namespace to `v2`, run both consumers in parallel during migration.

---

## 3. Realism Design

### Demand Distribution

The hourly demand multiplier array is calibrated to real food delivery patterns observed in published research and industry reports:

- Lunch peak (12–14h): 85–100% of peak demand
- Afternoon lull (15–17h): 55–65% of peak demand
- Dinner peak (19–21h): 85–100% of peak demand
- Night trough (02–05h): 2–3% of peak demand

The weekend multiplier (1.35×) reflects that weekend orders are typically higher volume but concentrated more in the dinner window.

### Zone-Level Skew

The five zones have demand weights (30%/25%/20%/15%/10%) that reflect realistic urban concentration: a city centre zone (ZONE_CENTRAL) handles a disproportionate share of orders due to higher restaurant density and population concentration. This zone-level skew is critical for:
- Testing zone-partitioned windowed aggregations
- Simulating surge conditions (a surge in ZONE_CENTRAL has a large platform-wide impact)

### Cuisine-Specific Distributions

Prep times and order values vary by cuisine to reflect real-world heterogeneity. Sushi restaurants genuinely take longer to prepare (25 min avg) and have higher average order values (€35) than burger places (12 min, €14). Using uniform distributions would hide the heterogeneity that makes SLA monitoring interesting.

---

## 4. Edge Case Design

### Late Events and Watermarks

**Why:** Mobile apps on courier and customer devices can lose connectivity and batch-send events when reconnected. In real deployments, 3–8% of events arrive late.

**Implementation:** The generator randomly backdates `event_time` by 60–300 seconds for 5% of events while setting `ingestion_time` to the current wall clock. The `is_late` flag enables measurement.

**Streaming impact:** Without watermarks, Spark would hold state indefinitely for late events. With our late event generation, we can demonstrate that a watermark of "5 minutes" correctly handles most late events while dropping those beyond the threshold.

### Duplicates and Idempotent Processing

**Why:** Network retries, at-least-once delivery guarantees in Kafka/Event Hubs, and mobile app crash-recoveries all produce duplicate events in production.

**Implementation:** 2% of events are re-emitted with `is_duplicate: true` (keeping all other fields identical). In practice, producers don't set this flag — we use it for testing our dedup logic.

**Streaming impact:** Without deduplication on `event_id`, a windowed count of "orders placed" would be inflated by ~2%. Downstream consumers must implement `dropDuplicates("event_id")` or use an idempotent state store.

### Missing Steps

**Why:** Network partitions, app bugs, or restaurant hardware failures can cause intermediate events to never be emitted. A delivery can go from READY_FOR_PICKUP directly to DELIVERED without a PICKED_UP event.

**Implementation:** 1% of orders skip the PICKED_UP step, jumping directly to IN_TRANSIT.

**Streaming impact:** Analytics that compute "time from pickup to delivery" must handle the case where the pickup event is absent. This tests our NULL handling in streaming SQL and stateful aggregations.

### Impossible Durations

**Why:** GPS spoofing, clock skew on devices, and data entry errors can produce events with logically impossible timestamps — a delivery completed in 1 second, or an order still "in transit" after 4 hours.

**Implementation:** 0.8% of orders get either a near-zero delivery time (<2 seconds) or an extremely long one (2–4 hours). These are our ground-truth anomalies for the anomaly detection use case.

**Streaming impact:** A z-score outlier detector on `actual_delivery_minutes` should flag these events. The challenge is that short windows may not have enough data to compute a stable mean and standard deviation.

### Courier Mid-Delivery Drop

**Why:** Battery death, app crashes, and couriers simply going offline mid-delivery are real operational events. They create an "orphaned delivery" — an order in IN_TRANSIT with no active courier.

**Implementation:** 1.5% of couriers emit an OFFLINE event with `offline_reason: MID_DELIVERY_DROP` during an active delivery assignment.

**Streaming impact:** The demand-supply health metric must detect this situation and re-surface the affected order as "awaiting courier reassignment." This requires joining the order stream with the courier stream on `order_id` and detecting the OFFLINE courier event.

### Fraud Clusters

**Why:** Multi-accounting fraud (one person using multiple accounts from the same device) and coordinated cancellation fraud are real problems for food delivery platforms.

**Implementation:** Five synthetic fraud clusters of 4 customers share the same `device_id`. With 0.5% probability per order, the event is generated from a fraud cluster customer. Additionally, fraud cluster orders have higher cancellation probability.

**Streaming impact:** A fraud detection query would look for `device_id` values associated with >3 different `customer_id` values within a 1-hour tumbling window, combined with a high cancellation rate.

---

## 5. Analytics Enablement Mapping

| Planned Use Case | Required Feed(s) | Required Fields | Window Type |
|-----------------|------------------|-----------------|-------------|
| Orders/min by zone | Order | zone_id, event_time, status=PLACED | Tumbling 1min |
| Revenue per hour | Order | order_value_eur, event_time, status=DELIVERED | Tumbling 1h |
| Courier utilisation | Courier | courier_id, status, event_time | Hopping 15min |
| Demand-supply gap | Order + Courier | zone_id, status (PLACED / ONLINE_IDLE) | Tumbling 5min |
| Restaurant SLA | Order | restaurant_id, estimated_prep_minutes, actual_prep_minutes | Tumbling 30min |
| Courier session length | Courier | active_session_id, session_start_time, status=OFFLINE | Session |
| Delivery time anomaly | Order | actual_delivery_minutes, zone_id, event_time | Hopping 10min, slide 2min |
| Fraud detection | Order | customer_id, device_id, status=CANCELLED | Tumbling 1h |
| Surge prediction | Order + Courier | zone_id, event_time, multiple statuses | Hopping 5min |

---

## 6. Assumptions and Limitations

1. **Geographic model:** The five zones are not real places. Latitude/longitude values are synthetic and are not used for routing — only for geo-clustering visualisation.
2. **Clock synchronisation:** We assume all devices have approximately synchronised clocks (< 10 minute drift). Larger clock skews would require a more complex late event model.
3. **Restaurant prep times:** We assume restaurants reliably emit READY_FOR_PICKUP events. In practice, a restaurant might never emit this event if their tablet is offline.
4. **Courier location:** GPS coordinates are jittered from zone centres. In Milestone 2, we could use actual routing APIs to simulate realistic paths.
5. **Event ordering within a session:** Events within a single order are generated in chronological order, but the batch output is shuffled. In streaming mode, events are emitted with wall-clock pacing which naturally introduces some out-of-order arrival.
