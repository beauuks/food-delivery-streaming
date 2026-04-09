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
import heapq
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


class SurgeManager:
    """Manages periodic, multi-zone demand surges."""

    def __init__(self):
        self.active_surges = []  # list of {zones, multiplier, end_time, reason}
        self.next_surge_time = time.time() + random.uniform(
            SURGE.min_interval_seconds / 2,  # first surge comes sooner
            SURGE.min_interval_seconds,
        )

    _SURGE_REASONS = [
        "Football match nearby",
        "Concert ending",
        "Heavy rain",
        "Festival crowd",
        "Office lunch rush",
        "Late-night bar crowd",
        "Weekend brunch wave",
        "Metro disruption",
    ]

    def update(self, current_time: float):
        """Check for new surges and expire old ones."""
        # Expire finished surges
        expired = [s for s in self.active_surges if current_time >= s["end_time"]]
        for s in expired:
            print(f"[surge] Ended in {', '.join(s['zones'])} ({s['reason']})")
        self.active_surges = [s for s in self.active_surges if current_time < s["end_time"]]

        # Trigger new surge?
        if SURGE.enabled and current_time >= self.next_surge_time:
            # Pick primary zone
            primary_zone = random.choice(ZONE_IDS)
            # Pick 0-2 adjacent zones
            n_extra = random.randint(0, SURGE.max_zones_per_surge - 1)
            adjacent = SURGE.ADJACENT_ZONES.get(primary_zone, [])
            extra_zones = random.sample(adjacent, min(n_extra, len(adjacent)))
            surge_zones = [primary_zone] + extra_zones

            multiplier = round(random.uniform(SURGE.min_multiplier, SURGE.max_multiplier), 1)
            duration = random.uniform(SURGE.min_duration_seconds, SURGE.max_duration_seconds)
            reason = random.choice(self._SURGE_REASONS)

            self.active_surges.append({
                "zones": surge_zones,
                "multiplier": multiplier,
                "end_time": current_time + duration,
                "reason": reason,
            })
            print(f"[surge] Started in {', '.join(surge_zones)} | {multiplier}x for {int(duration)}s | {reason}")

            # Schedule next surge
            self.next_surge_time = current_time + random.uniform(
                SURGE.min_interval_seconds,
                SURGE.max_interval_seconds,
            )

    def is_surging(self, zone_id: str) -> tuple[bool, float]:
        """Check if a zone is currently surging. Returns (is_active, multiplier)."""
        for s in self.active_surges:
            if zone_id in s["zones"]:
                return True, s["multiplier"]
        return False, 1.0

    def get_surge_zones(self) -> list[str]:
        """Return all currently surging zones."""
        zones = set()
        for s in self.active_surges:
            zones.update(s["zones"])
        return list(zones)


def pick_zone(surge_manager: "SurgeManager" = None) -> str:
    """Pick a zone weighted by demand, with surge overlay."""
    weights = list(ZONE_WEIGHTS)
    if surge_manager:
        for i, zone_id in enumerate(ZONE_IDS):
            is_surging, mult = surge_manager.is_surging(zone_id)
            if is_surging:
                weights[i] *= mult
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
# Event queue for deferred emission (stream mode)
# ---------------------------------------------------------------------------

class EventQueue:
    """Priority queue that holds events until their scheduled emit time."""

    def __init__(self):
        self._queue: List = []  # min-heap of (emit_time_ms, counter, event, feed)
        self._counter = 0

    def schedule(self, emit_time_ms: int, event: Dict, feed: str):
        """Schedule an event for future emission."""
        heapq.heappush(self._queue, (emit_time_ms, self._counter, event, feed))
        self._counter += 1

    def get_ready(self, current_time_ms: int) -> List[tuple]:
        """Pop and return all events whose emit time has arrived."""
        ready = []
        while self._queue and self._queue[0][0] <= current_time_ms:
            _, _, event, feed = heapq.heappop(self._queue)
            ready.append((event, feed))
        return ready

    def __len__(self):
        return len(self._queue)


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

            zone_id = pick_zone()
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
    Uses an event queue so lifecycle events are emitted at realistic times:
    PLACED is emitted immediately, DELIVERED is emitted ~30-40 min later.
    """
    customer_pool = CustomerPool()
    courier_mgr   = CourierStateManager()
    event_queue   = EventQueue()

    print("[simulator] Starting continuous stream. Press Ctrl+C to stop.")
    tick = 0
    emitted_this_tick = 0
    surge_mgr = SurgeManager()

    try:
        while True:
            tick_start = time.time()
            current_ms = int(tick_start * 1000)
            emitted_this_tick = 0

            # Update surge state (trigger new surges, expire old ones)
            surge_mgr.update(tick_start)

            # --- 1. Drain the event queue: emit events whose time has arrived ---
            for event, feed in event_queue.get_ready(current_ms):
                event["ingestion_time"] = int(time.time() * 1000)
                _emit(event, feed, order_producer, courier_producer)
                emitted_this_tick += 1

            # --- 2. Courier heartbeats (emit immediately, they're real-time) ---
            for ev in courier_mgr.emit_heartbeats(current_ms):
                _emit(ev, "courier", order_producer, courier_producer)
                emitted_this_tick += 1
            for ev in courier_mgr.bring_couriers_online(current_ms):
                _emit(ev, "courier", order_producer, courier_producer)
                emitted_this_tick += 1

            # --- 3. Generate new orders and schedule lifecycle events ---
            demand = get_demand_multiplier(tick_start)
            raw = orders_per_second * demand * random.uniform(0.5, 1.5)
            # Allow 0 orders on low-demand ticks (e.g., 3am)
            # Use probabilistic rounding: 0.3 means 30% chance of 1 order
            n_orders = int(raw) if raw >= 1 else (1 if random.random() < raw else 0)
            # Boost order count if any zone is surging
            surge_zones = surge_mgr.get_surge_zones()
            if surge_zones:
                avg_mult = sum(surge_mgr.is_surging(z)[1] for z in surge_zones) / len(surge_zones)
                n_orders = max(n_orders, int(n_orders * avg_mult * 0.5))

            for _ in range(n_orders):
                zone_id = pick_zone(surge_mgr)
                customer_id, device_id = customer_pool.sample(EDGE_CASES.fraud_burst_enabled)
                zone_restaurants = [r for r in RESTAURANTS if r.zone_id == zone_id]
                restaurant = random.choice(zone_restaurants if zone_restaurants else RESTAURANTS)
                courier = courier_mgr.get_available_courier(zone_id)

                events = make_order_sequence(
                    restaurant=restaurant, customer_id=customer_id, device_id=device_id,
                    zone_id=zone_id, courier=courier, base_event_time=current_ms,
                    skip_step=random.random() < EDGE_CASES.missing_step_rate,
                    impossible_duration=random.random() < EDGE_CASES.impossible_duration_rate,
                )

                for ev in events:
                    # Compute offset from now: how far in the future is this event?
                    offset = ev["event_time"] - current_ms

                    if offset <= 0:
                        # PLACED event or late events: emit now
                        ev["ingestion_time"] = int(time.time() * 1000)
                        _emit(ev, "order", order_producer, courier_producer)
                        emitted_this_tick += 1
                    else:
                        # Future event (CONFIRMED, DELIVERED, etc.): schedule
                        event_queue.schedule(ev["event_time"], ev, "order")

                    # Duplicate injection
                    if random.random() < EDGE_CASES.duplicate_rate:
                        dup = dict(ev)
                        dup["is_duplicate"] = True
                        if offset <= 0:
                            dup["ingestion_time"] = int(time.time() * 1000)
                            _emit(dup, "order", order_producer, courier_producer)
                        else:
                            event_queue.schedule(ev["event_time"], dup, "order")

                # --- Schedule courier delivery events for this order ---
                if courier and len(events) > 2:
                    delivered_event = next((e for e in events if e["status"] == "DELIVERED"), None)
                    if delivered_event:
                        order_id = delivered_event["order_id"]

                        # HEADING_TO_RESTAURANT: ~30s after order placed
                        ev_h2r = make_courier_event(
                            courier, "HEADING_TO_RESTAURANT",
                            base_event_time=current_ms + 30000,
                            order_id=order_id,
                        )
                        event_queue.schedule(current_ms + 30000, ev_h2r, "courier")

                        # AT_RESTAURANT: ~2 min after order placed
                        ev_at = make_courier_event(
                            courier, "AT_RESTAURANT",
                            base_event_time=current_ms + 120000,
                            order_id=order_id,
                        )
                        event_queue.schedule(current_ms + 120000, ev_at, "courier")

                        # Possibly drop mid-delivery
                        drop_ev = courier_mgr.possibly_drop_courier(courier, order_id, current_ms + 180000)
                        if drop_ev:
                            event_queue.schedule(current_ms + 180000, drop_ev, "courier")
                        else:
                            # HEADING_TO_CUSTOMER: ~3.3 min after order placed
                            ev_h2c = make_courier_event(
                                courier, "HEADING_TO_CUSTOMER",
                                base_event_time=current_ms + 200000,
                                order_id=order_id,
                            )
                            event_queue.schedule(current_ms + 200000, ev_h2c, "courier")

                            # COMPLETED_DELIVERY: at delivery time
                            delivery_ms = current_ms + int(delivered_event.get("actual_delivery_minutes", 30) * 60000)
                            completion_events = courier_mgr.courier_completed_delivery(courier, delivery_ms)
                            for cev in completion_events:
                                event_queue.schedule(delivery_ms, cev, "courier")

            tick += 1
            orders_since_log = getattr(stream_continuously, '_orders_since_log', 0) + n_orders
            stream_continuously._orders_since_log = orders_since_log
            if tick % 10 == 0:
                surge_info = f"surge={', '.join(surge_zones)}" if surge_zones else "no surge"
                print(f"[simulator] Tick {tick}: {orders_since_log} orders (last 10s), {emitted_this_tick} emitted, queue={len(event_queue)}, {surge_info}")
                stream_continuously._orders_since_log = 0

            # Flush producer periodically
            if order_producer and tick % 5 == 0:
                order_producer.flush()
            if courier_producer and tick % 5 == 0:
                courier_producer.flush()

            elapsed = time.time() - tick_start
            sleep_time = max(0, 1.0 - elapsed)
            time.sleep(sleep_time)
    except KeyboardInterrupt:
        print(f"\n[simulator] Stopping... {len(event_queue)} events still in queue")
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
    parser.add_argument("--rate", type=float, default=3.0, help="Orders per second at peak demand (default: 3)")
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
            orders_per_second=args.rate,
            order_producer=order_producer,
            courier_producer=courier_producer,
        )


if __name__ == "__main__":
    main()
