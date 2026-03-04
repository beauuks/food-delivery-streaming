"""
event_factories.py
------------------
Pure-function factories that construct order lifecycle and courier status
event dictionaries. Separate from I/O and simulation orchestration.
"""

import uuid
import random
import time
from typing import Optional, Dict, Any, List

from config import (
    EDGE_CASES, ZONES,
    ORDER_VALUE_PARAMS, PREP_TIME_PARAMS, DELIVERY_TIME_PARAMS,
)
from reference_data import Restaurant, Courier


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _now_ms() -> int:
    return int(time.time() * 1000)


def _ts(dt_offset_seconds: float = 0.0) -> int:
    """Return current epoch milliseconds offset by dt_offset_seconds."""
    return int((_now_ms() / 1000 + dt_offset_seconds) * 1000)


def _maybe_inject_late(event_time_ms: int) -> tuple[int, int, bool]:
    """
    Possibly delay the ingestion time to simulate late events.
    Returns (event_time_ms, ingestion_time_ms, is_late).
    """
    if random.random() < EDGE_CASES.late_event_rate:
        delay = random.uniform(60, EDGE_CASES.max_late_delay_seconds)
        # Ingestion time is now, event_time is in the past
        return event_time_ms - int(delay * 1000), _now_ms(), True
    return event_time_ms, _now_ms(), False


def _jitter_coord(center: float, radius: float = 0.01) -> float:
    return center + random.uniform(-radius, radius)


PROMO_CODES = ["FIRST10", "LUNCH20", "SPEED15", "PARTY30", "NEWZONE", "FLASH25"]
PAYMENT_METHODS = ["CARD", "CASH", "WALLET", "VOUCHER"]
PAYMENT_WEIGHTS = [0.65, 0.10, 0.20, 0.05]
PLATFORMS = ["IOS", "ANDROID", "WEB"]
PLATFORM_WEIGHTS = [0.45, 0.45, 0.10]
CANCELLATION_REASONS = ["CUSTOMER_REQUEST", "RESTAURANT_UNAVAILABLE", "NO_COURIER", "FRAUD_SUSPECTED", "TIMEOUT"]
CANCELLATION_WEIGHTS = [0.50, 0.20, 0.15, 0.05, 0.10]
OFFLINE_REASONS = ["VOLUNTARY", "BATTERY_DEAD", "APP_CRASH", "MID_DELIVERY_DROP", "SHIFT_END"]
OFFLINE_WEIGHTS = [0.40, 0.15, 0.10, 0.15, 0.20]


# ---------------------------------------------------------------------------
# Order Lifecycle Event factory
# ---------------------------------------------------------------------------

def make_order_placed_event(
    restaurant: Restaurant,
    customer_id: str,
    device_id: str,
    zone_id: str,
    base_event_time: Optional[int] = None,
) -> Dict[str, Any]:
    """Create the initial PLACED event for a new order."""
    order_id = f"ORD-{uuid.uuid4().hex[:12].upper()}"
    cuisine = restaurant.cuisine_type
    val_mean, val_std = ORDER_VALUE_PARAMS[cuisine]
    order_value = max(5.0, random.gauss(val_mean, val_std))

    is_promo = random.random() < EDGE_CASES.promo_rate
    promo_code = random.choice(PROMO_CODES) if is_promo else None

    raw_ts = base_event_time or _now_ms()
    event_time, ingestion_time, is_late = _maybe_inject_late(raw_ts)

    event = {
        "schema_version": "1.0.0",
        "event_id": str(uuid.uuid4()),
        "order_id": order_id,
        "customer_id": customer_id,
        "restaurant_id": restaurant.restaurant_id,
        "courier_id": None,
        "zone_id": zone_id,
        "status": "PLACED",
        "event_time": event_time,
        "ingestion_time": ingestion_time,
        "order_value_eur": round(order_value, 2),
        "item_count": random.randint(1, 8),
        "is_promo": is_promo,
        "promo_code": promo_code,
        "payment_method": random.choices(PAYMENT_METHODS, weights=PAYMENT_WEIGHTS)[0],
        "estimated_prep_minutes": None,
        "actual_prep_minutes": None,
        "estimated_delivery_minutes": None,
        "actual_delivery_minutes": None,
        "cancellation_reason": None,
        "device_id": device_id,
        "platform": random.choices(PLATFORMS, weights=PLATFORM_WEIGHTS)[0],
        "is_duplicate": False,
        "is_late": is_late,
        "metadata": {},
    }
    return event


def make_order_status_event(
    base_event: Dict[str, Any],
    new_status: str,
    dt_seconds: float,
    courier: Optional[Courier] = None,
    extra_fields: Optional[Dict] = None,
) -> Dict[str, Any]:
    """Advance an order to a new status. dt_seconds after the base event."""
    event = dict(base_event)
    event["event_id"] = str(uuid.uuid4())
    event["status"] = new_status

    raw_ts = base_event["event_time"] + int(dt_seconds * 1000)
    event_time, ingestion_time, is_late = _maybe_inject_late(raw_ts)
    event["event_time"] = event_time
    event["ingestion_time"] = ingestion_time
    event["is_late"] = is_late
    event["is_duplicate"] = False

    if courier:
        event["courier_id"] = courier.courier_id

    if extra_fields:
        event.update(extra_fields)

    return event


def make_order_sequence(
    restaurant: Restaurant,
    customer_id: str,
    device_id: str,
    zone_id: str,
    courier: Optional[Courier],
    base_event_time: Optional[int] = None,
    force_cancel: bool = False,
    skip_step: bool = False,
    impossible_duration: bool = False,
    fraud_cluster: bool = False,
) -> List[Dict[str, Any]]:
    """
    Generate the full sequence of events for one order lifecycle.
    Returns a list of events in chronological order (may be emitted out-of-order later).
    """
    events: List[Dict[str, Any]] = []
    cuisine = restaurant.cuisine_type
    prep_mean, prep_std = PREP_TIME_PARAMS[cuisine]
    vehicle = courier.vehicle_type if courier else "SCOOTER"
    del_mean, del_std = DELIVERY_TIME_PARAMS[vehicle]

    # Durations in seconds
    confirm_lag   = random.uniform(10, 60)
    prep_time     = max(2, random.gauss(prep_mean, prep_std)) * 60  # to seconds
    pickup_lag    = random.uniform(30, 180)
    delivery_time = max(2, random.gauss(del_mean, del_std)) * 60

    if impossible_duration:
        # Inject an anomalously fast or slow delivery for anomaly detection
        delivery_time = random.choice([
            random.uniform(0.5, 2),   # 0.5-2 seconds (impossible fast)
            random.uniform(7200, 14400),  # 2-4 hours (stuck courier)
        ])

    est_prep_min = int(prep_time / 60)
    est_del_min  = int(delivery_time / 60)

    # --- PLACED ---
    placed = make_order_placed_event(restaurant, customer_id, device_id, zone_id, base_event_time)
    events.append(placed)

    # --- CANCELLED (early exit) ---
    if force_cancel or random.random() < EDGE_CASES.cancellation_rate:
        reason = "FRAUD_SUSPECTED" if fraud_cluster else random.choices(
            CANCELLATION_REASONS, weights=CANCELLATION_WEIGHTS)[0]
        cancel_dt = random.uniform(30, confirm_lag + 60)
        cancelled = make_order_status_event(placed, "CANCELLED", cancel_dt, extra_fields={
            "cancellation_reason": reason,
        })
        events.append(cancelled)
        return events

    # --- CONFIRMED ---
    confirmed = make_order_status_event(placed, "CONFIRMED", confirm_lag, extra_fields={
        "estimated_prep_minutes": est_prep_min,
        "estimated_delivery_minutes": est_del_min,
    })
    events.append(confirmed)

    # --- PREPARING ---
    preparing = make_order_status_event(confirmed, "PREPARING", 5)
    events.append(preparing)

    # --- READY_FOR_PICKUP ---
    actual_prep = int(prep_time / 60)
    ready = make_order_status_event(preparing, "READY_FOR_PICKUP", prep_time, extra_fields={
        "actual_prep_minutes": actual_prep,
    })
    events.append(ready)

    # Skip PICKED_UP step (missing step edge case)
    if skip_step and courier:
        in_transit = make_order_status_event(ready, "IN_TRANSIT", pickup_lag, courier=courier)
        events.append(in_transit)
        final_dt = delivery_time
        actual_del = int((pickup_lag + delivery_time) / 60)
        delivered = make_order_status_event(in_transit, "DELIVERED", final_dt, extra_fields={
            "actual_delivery_minutes": actual_del,
        })
        events.append(delivered)
        return events

    # --- PICKED_UP ---
    if courier:
        picked_up = make_order_status_event(ready, "PICKED_UP", pickup_lag, courier=courier)
        events.append(picked_up)

        # --- IN_TRANSIT ---
        in_transit = make_order_status_event(picked_up, "IN_TRANSIT", 10, courier=courier)
        events.append(in_transit)

        # --- DELIVERED ---
        actual_del = int((pickup_lag + delivery_time) / 60)
        delivered = make_order_status_event(in_transit, "DELIVERED", delivery_time, courier=courier, extra_fields={
            "actual_delivery_minutes": actual_del,
        })
        events.append(delivered)

    return events


# ---------------------------------------------------------------------------
# Courier Status Event factory
# ---------------------------------------------------------------------------

def make_courier_event(
    courier: Courier,
    status: str,
    base_event_time: Optional[int] = None,
    order_id: Optional[str] = None,
    is_heartbeat: bool = False,
    offline_reason: Optional[str] = None,
    go_offline_mid_delivery: bool = False,
) -> Dict[str, Any]:
    """Create a courier status event."""
    raw_ts = base_event_time or _now_ms()
    event_time, ingestion_time, is_late = _maybe_inject_late(raw_ts)

    zone = ZONES[courier.current_zone_id]
    lat = _jitter_coord(zone["lat_center"], radius=0.02)
    lon = _jitter_coord(zone["lon_center"], radius=0.02)

    vehicle = courier.vehicle_type
    speed_map = {
        "BICYCLE": (15, 5),
        "SCOOTER": (28, 6),
        "MOTORCYCLE": (35, 8),
        "CAR": (30, 10),
        "WALKING": (5, 1),
    }
    speed_mean, speed_std = speed_map.get(vehicle, (20, 5))

    if status == "OFFLINE":
        speed = 0.0
    elif is_heartbeat:
        speed = max(0, random.gauss(speed_mean / 2, speed_std))
    else:
        speed = max(0, random.gauss(speed_mean, speed_std))

    # Anomaly: occasional impossible speed
    if random.random() < 0.005:
        speed = random.uniform(200, 500)

    event = {
        "schema_version": "1.0.0",
        "event_id": str(uuid.uuid4()),
        "courier_id": courier.courier_id,
        "order_id": order_id,
        "zone_id": courier.current_zone_id,
        "status": status,
        "event_time": event_time,
        "ingestion_time": ingestion_time,
        "latitude": round(lat, 6),
        "longitude": round(lon, 6),
        "location_accuracy_meters": round(random.uniform(3, 25), 1),
        "speed_kmh": round(speed, 1),
        "battery_pct": random.randint(5, 100),
        "vehicle_type": vehicle,
        "active_session_id": courier.current_session_id,
        "session_start_time": int(courier.session_start_time),
        "deliveries_completed_this_session": courier.deliveries_this_session,
        "is_heartbeat": is_heartbeat,
        "is_duplicate": False,
        "is_late": is_late,
        "offline_reason": offline_reason,
        "metadata": {},
    }

    return event
