"""
config.py
---------
Centralised simulation configuration for the food delivery streaming generator.
All knobs for realism, edge-case injection, and throughput are here.
"""

from dataclasses import dataclass, field
from typing import Dict, List, Tuple
import os


# ---------------------------------------------------------------------------
# Zone definitions (fictional city grid)
# ---------------------------------------------------------------------------
ZONES: Dict[str, Dict] = {
    "ZONE_NORTH":    {"lat_center": 48.870, "lon_center": 2.340, "demand_weight": 0.20, "restaurant_density": "high"},
    "ZONE_SOUTH":    {"lat_center": 48.820, "lon_center": 2.340, "demand_weight": 0.15, "restaurant_density": "medium"},
    "ZONE_EAST":     {"lat_center": 48.845, "lon_center": 2.400, "demand_weight": 0.25, "restaurant_density": "high"},
    "ZONE_WEST":     {"lat_center": 48.845, "lon_center": 2.280, "demand_weight": 0.10, "restaurant_density": "low"},
    "ZONE_CENTRAL":  {"lat_center": 48.855, "lon_center": 2.345, "demand_weight": 0.30, "restaurant_density": "very_high"},
}

ZONE_IDS = list(ZONES.keys())
ZONE_WEIGHTS = [z["demand_weight"] for z in ZONES.values()]


# ---------------------------------------------------------------------------
# Restaurant pool
# ---------------------------------------------------------------------------
CUISINE_TYPES = ["PIZZA", "SUSHI", "BURGER", "INDIAN", "THAI", "MEXICAN", "SALAD", "RAMEN"]

RESTAURANT_COUNT = int(os.getenv("RESTAURANT_COUNT", "50"))
COURIER_COUNT    = int(os.getenv("COURIER_COUNT", "80"))


# ---------------------------------------------------------------------------
# Temporal demand model
# ---------------------------------------------------------------------------
# Hourly demand multipliers (index = hour of day, 0-23)
HOURLY_DEMAND = [
    0.05, 0.03, 0.02, 0.02, 0.02, 0.03,   # 00-05 (night)
    0.05, 0.10, 0.15, 0.12, 0.12, 0.18,   # 06-11 (morning ramp)
    0.90, 1.00, 0.80, 0.60, 0.55, 0.65,   # 12-17 (lunch peak)
    0.85, 1.00, 0.90, 0.70, 0.40, 0.15,   # 18-23 (dinner peak)
]

# Weekend multiplier applied on top of hourly demand
WEEKDAY_MULTIPLIER = 1.0
WEEKEND_MULTIPLIER = 1.35

# Base orders per second at peak demand (multiplied by hourly factor)
BASE_ORDERS_PER_SECOND = float(os.getenv("BASE_ORDERS_PER_SECOND", "2.0"))


# ---------------------------------------------------------------------------
# Order value distributions (by cuisine)
# ---------------------------------------------------------------------------
ORDER_VALUE_PARAMS: Dict[str, Tuple[float, float]] = {
    # (mean_eur, std_eur)
    "PIZZA":   (18.5, 5.0),
    "SUSHI":   (35.0, 10.0),
    "BURGER":  (14.0, 4.0),
    "INDIAN":  (22.0, 6.0),
    "THAI":    (20.0, 5.5),
    "MEXICAN": (16.0, 4.5),
    "SALAD":   (12.0, 3.0),
    "RAMEN":   (17.5, 4.0),
}

# Prep time distributions (mean_minutes, std_minutes) by cuisine
PREP_TIME_PARAMS: Dict[str, Tuple[float, float]] = {
    "PIZZA":   (18, 4),
    "SUSHI":   (25, 6),
    "BURGER":  (12, 3),
    "INDIAN":  (22, 5),
    "THAI":    (20, 5),
    "MEXICAN": (15, 4),
    "SALAD":   (8,  2),
    "RAMEN":   (16, 4),
}

# Delivery time params (mean_minutes, std_minutes) 
DELIVERY_TIME_PARAMS: Dict[str, Tuple[float, float]] = {
    "BICYCLE":    (22, 6),
    "SCOOTER":    (17, 4),
    "MOTORCYCLE": (15, 3),
    "CAR":        (20, 5),
    "WALKING":    (35, 8),
}


# ---------------------------------------------------------------------------
# Edge case injection rates (probabilities per event)
# ---------------------------------------------------------------------------
@dataclass
class EdgeCaseConfig:
    # Probability that an emitted event is a duplicate of the previous
    duplicate_rate: float = float(os.getenv("DUPLICATE_RATE", "0.02"))

    # Probability that an event arrives late (event_time << ingestion_time)
    late_event_rate: float = float(os.getenv("LATE_EVENT_RATE", "0.05"))

    # Max seconds an event can be delayed (uniform between 60 and this)
    max_late_delay_seconds: int = int(os.getenv("MAX_LATE_DELAY_SECONDS", "300"))

    # Probability that a courier goes OFFLINE mid-delivery
    courier_mid_delivery_drop_rate: float = float(os.getenv("COURIER_MID_DELIVERY_DROP_RATE", "0.015"))

    # Probability that an order skips a step (e.g., DELIVERED without PICKED_UP)
    missing_step_rate: float = float(os.getenv("MISSING_STEP_RATE", "0.01"))

    # Probability that an order has an "impossible" prep or delivery duration
    impossible_duration_rate: float = float(os.getenv("IMPOSSIBLE_DURATION_RATE", "0.008"))

    # Probability that an order is cancelled
    cancellation_rate: float = float(os.getenv("CANCELLATION_RATE", "0.08"))

    # Probability that an order uses a promo code
    promo_rate: float = float(os.getenv("PROMO_RATE", "0.20"))

    # Probability that a courier heartbeat is emitted (per courier per interval)
    heartbeat_rate: float = float(os.getenv("HEARTBEAT_RATE", "0.30"))

    # Fraud cluster: surge of cancellations from a few users
    fraud_burst_enabled: bool = os.getenv("FRAUD_BURST", "true").lower() == "true"
    fraud_burst_probability: float = float(os.getenv("FRAUD_BURST_PROB", "0.005"))


# ---------------------------------------------------------------------------
# Surge / demand events
# ---------------------------------------------------------------------------
@dataclass
class SurgeConfig:
    enabled: bool = os.getenv("SURGE_ENABLED", "true").lower() == "true"
    # Zone that will experience surge
    surge_zone: str = os.getenv("SURGE_ZONE", "ZONE_CENTRAL")
    # Multiplier applied to demand_weight during surge
    surge_multiplier: float = float(os.getenv("SURGE_MULTIPLIER", "3.0"))
    # Duration of surge in seconds
    surge_duration_seconds: int = int(os.getenv("SURGE_DURATION_SECONDS", "120"))
    # After how many seconds from start to trigger surge
    surge_trigger_seconds: int = int(os.getenv("SURGE_TRIGGER_SECONDS", "60"))


# ---------------------------------------------------------------------------
# Output configuration
# ---------------------------------------------------------------------------
@dataclass
class OutputConfig:
    # Write JSON events
    json_enabled: bool = True
    json_output_dir: str = os.getenv("JSON_OUTPUT_DIR", "sample_data/json")

    # Write AVRO events
    avro_enabled: bool = True
    avro_output_dir: str = os.getenv("AVRO_OUTPUT_DIR", "sample_data/avro")

    # Kafka/Event Hubs output (milestone 2)
    kafka_enabled: bool = os.getenv("KAFKA_ENABLED", "false").lower() == "true"
    kafka_bootstrap_servers: str = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
    order_topic: str = os.getenv("ORDER_TOPIC", "order-lifecycle-events")
    courier_topic: str = os.getenv("COURIER_TOPIC", "courier-status-events")

    # Number of sample events to generate in batch mode
    sample_order_events: int = int(os.getenv("SAMPLE_ORDER_EVENTS", "500"))
    sample_courier_events: int = int(os.getenv("SAMPLE_COURIER_EVENTS", "500"))


EDGE_CASES = EdgeCaseConfig()
SURGE      = SurgeConfig()
OUTPUT     = OutputConfig()
