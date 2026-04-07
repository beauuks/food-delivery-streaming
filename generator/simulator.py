"""
simulator.py
------------
Orchestrates the full simulation: demand modelling, entity state management,
edge case injection, and event emission across both feeds.

Run modes:
  python simulator.py --mode batch   # Generate sample files and exit
  python simulator.py --mode stream  # Continuous streaming (for Milestone 2)
"""

import argparse
import json
import os
import random
import time
import uuid
from datetime import datetime
from typing import List, Dict, Any, Optional

from config import (
    ZONES, ZONE_IDS, ZONE_WEIGHTS,
    HOURLY_DEMAND, WEEKDAY_MULTIPLIER, WEEKEND_MULTIPLIER,
    BASE_ORDERS_PER_SECOND,
    EDGE_CASES, SURGE, OUTPUT,
    COURIER_COUNT,
)
from reference_data import RESTAURANTS, COURIERS, COURIER_MAP
from event_factories import (
    make_order_sequence,
    make_courier_event,
    _now_ms,
)
from avro_utils import (
    serialize_order_events_to_avro,
    serialize_courier_events_to_avro,
    AVRO_AVAILABLE,
)


# ---------------------------------------------------------------------------
# Demand model
# ---------------------------------------------------------------------------

def get_demand_multiplier(ts: Optional[float] = None) -> float:
    """Return demand multiplier [0, 1] based on time of day and day of week."""
    dt = datetime.fromtimestamp(ts or time.time())
    base = HOURLY_DEMAND[dt.hour]
    dow_mult = WEEKEND_MULTIPLIER if dt.weekday() >= 5 else WEEKDAY_MULTIPLIER
    return base * dow_mult


def pick_zone(surge_active: bool = False, surge_zone: str = "") -> str:
    """Pick a zone weighted by demand, with optional surge overlay."""
    weights = list(ZONE_WEIGHTS)
    if surge_active and surge_zone:
        idx = ZONE_IDS.index(surge_zone)
        weights[idx] *= SURGE.surge_multiplier
    total = sum(weights)
    weights = [w / total for w in weights]
    return random.choices(ZONE_IDS, weights=weights)[0]


# ---------------------------------------------------------------------------
# Customer / device pool (stateful for fraud simulation)
# ---------------------------------------------------------------------------

class CustomerPool:
    """Maintains a pool of synthetic customers with state for fraud detection."""

    def __init__(self, size: int = 2000):
        self.size = size
        self.customer_ids = [f"CUS-{uuid.uuid4().hex[:8].upper()}" for _ in range(size)]
        self.device_ids = [f"DEV-{uuid.uuid4().hex[:10].upper()}" for _ in range(size)]
        # Fraud clusters: small groups sharing device_id (multi-accounting)
        self.fraud_clusters = self._build_fraud_clusters()

    def _build_fraud_clusters(self, n_clusters: int = 5, cluster_size: int = 4) -> List[Dict]:
        clusters = []
        for _ in range(n_clusters):
            shared_device = f"DEV-FRAUD-{uuid.uuid4().hex[:6].upper()}"
            members = random.sample(self.customer_ids, cluster_size)
            clusters.append({"device_id": shared_device, "customer_ids": members})
        return clusters

    def sample(self, fraud_burst: bool = False):
        if fraud_burst and self.fraud_clusters and random.random() < EDGE_CASES.fraud_burst_probability:
            cluster = random.choice(self.fraud_clusters)
            return random.choice(cluster["customer_ids"]), cluster["device_id"]
        idx = random.randrange(self.size)
        return self.customer_ids[idx], self.device_ids[idx]


# ---------------------------------------------------------------------------
# Courier state manager
# ---------------------------------------------------------------------------

class CourierStateManager:
    """Tracks courier states and emits courier events accordingly."""

    def __init__(self):
        self.couriers = COURIERS
        # Session start times
        for c in self.couriers:
            c.session_start_time = _now_ms()

    def get_available_courier(self, zone_id: str) -> Optional[Any]:
        """Find an online idle courier, preferring the same zone."""
        same_zone = [c for c in self.couriers if c.is_online and c.current_zone_id == zone_id]
        any_zone  = [c for c in self.couriers if c.is_online]
        pool = same_zone if same_zone else any_zone
        return random.choice(pool) if pool else None

    def emit_heartbeats(self, base_time: int) -> List[Dict[str, Any]]:
        """Periodically emit heartbeat events for online couriers."""
        events = []
        for courier in self.couriers:
            if not courier.is_online:
                continue
            if random.random() < EDGE_CASES.heartbeat_rate:
                ev = make_courier_event(courier, "ONLINE_IDLE", base_event_time=base_time, is_heartbeat=True)
                if random.random() < EDGE_CASES.duplicate_rate:
                    ev_dup = dict(ev)
                    ev_dup["is_duplicate"] = True
                    events.append(ev_dup)
                events.append(ev)
        return events

    def possibly_drop_courier(self, courier: Any, order_id: str, base_time: int) -> Optional[Dict[str, Any]]:
        """Simulate courier going offline mid-delivery."""
        if random.random() < EDGE_CASES.courier_mid_delivery_drop_rate:
            courier.is_online = False
            ev = make_courier_event(
                courier, "OFFLINE",
                base_event_time=base_time + random.randint(30000, 120000),
                order_id=order_id,
                offline_reason="MID_DELIVERY_DROP",
            )
            return ev
        return None

    def courier_completed_delivery(self, courier: Any, base_time: int) -> List[Dict[str, Any]]:
        """Emit COMPLETED_DELIVERY event, update courier state."""
        courier.deliveries_this_session += 1
        events = [make_courier_event(courier, "COMPLETED_DELIVERY", base_event_time=base_time)]

        # Small chance courier goes offline after delivery
        if random.random() < 0.05:
            courier.is_online = False
            reason = random.choices(
                ["VOLUNTARY", "BATTERY_DEAD", "SHIFT_END"],
                weights=[0.5, 0.25, 0.25]
            )[0]
            events.append(make_courier_event(
                courier, "OFFLINE",
                base_event_time=base_time + random.randint(5000, 30000),
                offline_reason=reason,
            ))
        else:
            events.append(make_courier_event(courier, "ONLINE_IDLE", base_event_time=base_time + 15000))

        return events

    def bring_couriers_online(self, base_time: int) -> List[Dict[str, Any]]:
        """Randomly bring offline couriers back online (simulates shift starts)."""
        events = []
        for courier in self.couriers:
            if not courier.is_online and random.random() < 0.02:
                courier.is_online = True
                courier.current_session_id = str(uuid.uuid4())
                courier.session_start_time = base_time
                courier.deliveries_this_session = 0
                events.append(make_courier_event(courier, "ONLINE_IDLE", base_event_time=base_time))
        return events


# ---------------------------------------------------------------------------
# Batch generator
# ---------------------------------------------------------------------------

def generate_batch(
    n_order_events: int = OUTPUT.sample_order_events,
    n_courier_events: int = OUTPUT.sample_courier_events,
) -> tuple[List[Dict], List[Dict]]:
    """
    Generate n_order_events and n_courier_events as sample batches.
    Includes the full spectrum of edge cases.
    """
    customer_pool = CustomerPool()
    courier_mgr = CourierStateManager()

    order_events: List[Dict] = []
    courier_events: List[Dict] = []

    # Simulate across a 24-hour period at 1-minute resolution
    sim_start = int(time.time()) - 86400  # 24 hours ago
    sim_duration = 86400

    ticks = 0
    t = sim_start

    # Surge tracking
    surge_start = sim_start + SURGE.surge_trigger_seconds
    surge_end   = surge_start + SURGE.surge_duration_seconds

    while len(order_events) < n_order_events or len(courier_events) < n_courier_events:
        t += 60  # advance 1 minute per tick
        ticks += 1
        base_ms = t * 1000

        surge_active = SURGE.enabled and surge_start <= t <= surge_end

        demand = get_demand_multiplier(t)
        n_orders_this_tick = int(BASE_ORDERS_PER_SECOND * 60 * demand)
        if surge_active:
            n_orders_this_tick = int(n_orders_this_tick * SURGE.surge_multiplier)

        # --- Courier heartbeats ---
        if len(courier_events) < n_courier_events:
            hb_events = courier_mgr.emit_heartbeats(base_ms)
            courier_events.extend(hb_events)
            online_events = courier_mgr.bring_couriers_online(base_ms)
            courier_events.extend(online_events)

        # --- Order sequences ---
        for _ in range(n_orders_this_tick):
            if len(order_events) >= n_order_events:
                break

            zone_id = pick_zone(surge_active, SURGE.surge_zone)
            customer_id, device_id = customer_pool.sample(
                fraud_burst=EDGE_CASES.fraud_burst_enabled
            )

            # Pick restaurant in the same zone if possible
            zone_restaurants = [r for r in RESTAURANTS if r.zone_id == zone_id]
            restaurant = random.choice(zone_restaurants if zone_restaurants else RESTAURANTS)

            courier = courier_mgr.get_available_courier(zone_id)

            # Edge case injection flags
            force_cancel     = False
            skip_step        = random.random() < EDGE_CASES.missing_step_rate
            impossible_dur   = random.random() < EDGE_CASES.impossible_duration_rate
            fraud_cluster    = EDGE_CASES.fraud_burst_enabled and random.random() < EDGE_CASES.fraud_burst_probability

            events = make_order_sequence(
                restaurant=restaurant,
                customer_id=customer_id,
                device_id=device_id,
                zone_id=zone_id,
                courier=courier,
                base_event_time=base_ms,
                force_cancel=force_cancel,
                skip_step=skip_step,
                impossible_duration=impossible_dur,
                fraud_cluster=fraud_cluster,
            )

            # Inject duplicate for some events
            enriched = []
            for ev in events:
                enriched.append(ev)
                if random.random() < EDGE_CASES.duplicate_rate:
                    dup = dict(ev)
                    dup["is_duplicate"] = True
                    enriched.append(dup)

            order_events.extend(enriched)

            # Emit courier assignment + movement events for assigned orders
            if courier and len(events) > 2:
                delivered_event = next((e for e in events if e["status"] == "DELIVERED"), None)
                if delivered_event:
                    order_id = delivered_event["order_id"]
                    # HEADING_TO_RESTAURANT
                    courier_events.append(make_courier_event(
                        courier, "HEADING_TO_RESTAURANT",
                        base_event_time=base_ms + 30000,
                        order_id=order_id,
                    ))
                    # AT_RESTAURANT
                    courier_events.append(make_courier_event(
                        courier, "AT_RESTAURANT",
                        base_event_time=base_ms + 120000,
                        order_id=order_id,
                    ))
                    # Possibly drop mid-delivery
                    drop_ev = courier_mgr.possibly_drop_courier(courier, order_id, base_ms + 180000)
                    if drop_ev:
                        courier_events.append(drop_ev)
                    else:
                        # HEADING_TO_CUSTOMER
                        courier_events.append(make_courier_event(
                            courier, "HEADING_TO_CUSTOMER",
                            base_event_time=base_ms + 200000,
                            order_id=order_id,
                        ))
                        # Completed
                        completion_events = courier_mgr.courier_completed_delivery(
                            courier, base_ms + int(delivered_event.get("actual_delivery_minutes", 30) * 60000)
                        )
                        courier_events.extend(completion_events)

    # Shuffle to simulate out-of-order delivery (events not necessarily in order by time)
    random.shuffle(order_events)
    random.shuffle(courier_events)

    return order_events[:n_order_events], courier_events[:n_courier_events]


# ---------------------------------------------------------------------------
# Continuous stream generator (for Milestone 2)
# ---------------------------------------------------------------------------

def stream_continuously(
    orders_per_second: float = BASE_ORDERS_PER_SECOND,
    order_producer=None,
    courier_producer=None,
):
    """
    Continuously generate and emit events to Event Hubs or stdout.
    Respects real-time demand curve.
    """
    customer_pool = CustomerPool()
    courier_mgr   = CourierStateManager()

    print("[simulator] Starting continuous stream. Press Ctrl+C to stop.")
    tick = 0
    sim_start = time.time()
    surge_start = sim_start + SURGE.surge_trigger_seconds
    surge_end   = surge_start + SURGE.surge_duration_seconds

    try:
        while True:
            tick_start = time.time()
            surge_active = SURGE.enabled and surge_start <= tick_start <= surge_end

            demand = get_demand_multiplier(tick_start)
            n_orders = max(1, int(orders_per_second * demand))
            if surge_active:
                n_orders = int(n_orders * SURGE.surge_multiplier)

            base_ms = int(tick_start * 1000)

            # Heartbeats
            for ev in courier_mgr.emit_heartbeats(base_ms):
                _emit(ev, "courier", order_producer, courier_producer)
            for ev in courier_mgr.bring_couriers_online(base_ms):
                _emit(ev, "courier", order_producer, courier_producer)

            # Orders
            for _ in range(n_orders):
                zone_id = pick_zone(surge_active, SURGE.surge_zone)
                customer_id, device_id = customer_pool.sample(EDGE_CASES.fraud_burst_enabled)
                zone_restaurants = [r for r in RESTAURANTS if r.zone_id == zone_id]
                restaurant = random.choice(zone_restaurants if zone_restaurants else RESTAURANTS)
                courier = courier_mgr.get_available_courier(zone_id)

                events = make_order_sequence(
                    restaurant=restaurant, customer_id=customer_id, device_id=device_id,
                    zone_id=zone_id, courier=courier, base_event_time=base_ms,
                    skip_step=random.random() < EDGE_CASES.missing_step_rate,
                    impossible_duration=random.random() < EDGE_CASES.impossible_duration_rate,
                )
                for ev in events:
                    _emit(ev, "order", order_producer, courier_producer)
                    if random.random() < EDGE_CASES.duplicate_rate:
                        dup = dict(ev); dup["is_duplicate"] = True
                        _emit(dup, "order", order_producer, courier_producer)

            tick += 1
            if tick % 10 == 0:
                print(f"[simulator] Tick {tick}: {n_orders} orders, surge={'ON' if surge_active else 'OFF'}")

            elapsed = time.time() - tick_start
            sleep_time = max(0, 1.0 - elapsed)
            time.sleep(sleep_time)
    except KeyboardInterrupt:
        print("\n[simulator] Stopping...")
    finally:
        if order_producer:
            order_producer.flush()
            order_producer.close()
        if courier_producer:
            courier_producer.flush()
            courier_producer.close()


def _emit(event: Dict, feed: str, order_producer=None, courier_producer=None):
    """Emit a single event to stdout or Event Hubs."""
    if feed == "order" and order_producer:
        partition_key = event.get("zone_id", "default")
        order_producer.send(event, partition_key=partition_key)
    elif feed == "courier" and courier_producer:
        partition_key = event.get("zone_id", "default")
        courier_producer.send(event, partition_key=partition_key)
    else:
        label = event.get("status", "?")
        eid = event.get("order_id", event.get("courier_id"))
        print(f"[{feed.upper()}] {label} | {eid}")


# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------

def main():
    parser = argparse.ArgumentParser(description="Food Delivery Streaming Event Generator")
    parser.add_argument("--mode", choices=["batch", "stream"], default="batch")
    parser.add_argument("--order-events", type=int, default=OUTPUT.sample_order_events)
    parser.add_argument("--courier-events", type=int, default=OUTPUT.sample_courier_events)
    parser.add_argument("--seed", type=int, default=None, help="Random seed for reproducibility")
    args = parser.parse_args()

    if args.seed is not None:
        random.seed(args.seed)

    if args.mode == "batch":
        print(f"[simulator] Generating {args.order_events} order events and {args.courier_events} courier events...")
        order_events, courier_events = generate_batch(args.order_events, args.courier_events)

        # Write JSON
        if OUTPUT.json_enabled:
            os.makedirs(OUTPUT.json_output_dir, exist_ok=True)
            order_json_path   = os.path.join(OUTPUT.json_output_dir, "order_lifecycle_events.json")
            courier_json_path = os.path.join(OUTPUT.json_output_dir, "courier_status_events.json")
            with open(order_json_path, "w") as f:
                json.dump(order_events, f, indent=2)
            with open(courier_json_path, "w") as f:
                json.dump(courier_events, f, indent=2)
            print(f"[simulator] JSON written: {order_json_path}, {courier_json_path}")

        # Write AVRO
        if OUTPUT.avro_enabled and AVRO_AVAILABLE:
            os.makedirs(OUTPUT.avro_output_dir, exist_ok=True)
            order_avro_path   = os.path.join(OUTPUT.avro_output_dir, "order_lifecycle_events.avro")
            courier_avro_path = os.path.join(OUTPUT.avro_output_dir, "courier_status_events.avro")
            ok1 = serialize_order_events_to_avro(order_events, order_avro_path)
            ok2 = serialize_courier_events_to_avro(courier_events, courier_avro_path)
            if ok1 and ok2:
                print(f"[simulator] AVRO written: {order_avro_path}, {courier_avro_path}")
        elif OUTPUT.avro_enabled and not AVRO_AVAILABLE:
            print("[simulator] fastavro not installed. Skipping AVRO output. Run: pip install fastavro")

        print(f"[simulator] Done. {len(order_events)} order events, {len(courier_events)} courier events.")

    elif args.mode == "stream":
        order_producer, courier_producer = None, None
        if os.getenv("EVENTHUB_ORDER_CONNECTION_STRING"):
            from eventhub_producer import create_producers
            order_producer, courier_producer = create_producers()
            print("[simulator] Publishing to Azure Event Hubs")
        else:
            print("[simulator] No EVENTHUB_ORDER_CONNECTION_STRING set, printing to stdout")
        stream_continuously(
            order_producer=order_producer,
            courier_producer=courier_producer,
        )


if __name__ == "__main__":
    main()
