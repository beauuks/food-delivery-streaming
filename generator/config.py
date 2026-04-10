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
# Madrid district coordinates
ZONES: Dict[str, Dict] = {
    "CENTRO":      {"lat_center": 40.4168, "lon_center": -3.7038, "demand_weight": 0.18, "restaurant_density": "very_high", "district": "Centro"},
    "SALAMANCA":   {"lat_center": 40.4310, "lon_center": -3.6830, "demand_weight": 0.14, "restaurant_density": "high",      "district": "Salamanca"},
    "CHAMBERI":    {"lat_center": 40.4350, "lon_center": -3.7050, "demand_weight": 0.12, "restaurant_density": "high",      "district": "Chamberí"},
    "RETIRO":      {"lat_center": 40.4100, "lon_center": -3.6770, "demand_weight": 0.10, "restaurant_density": "medium",    "district": "Retiro"},
    "LATINA":      {"lat_center": 40.4023, "lon_center": -3.7150, "demand_weight": 0.08, "restaurant_density": "medium",    "district": "Latina"},
    "MONCLOA":     {"lat_center": 40.4350, "lon_center": -3.7200, "demand_weight": 0.07, "restaurant_density": "low",       "district": "Moncloa-Aravaca"},
    "TETUAN":      {"lat_center": 40.4600, "lon_center": -3.6970, "demand_weight": 0.09, "restaurant_density": "high",      "district": "Tetuán"},
    "ARGANZUELA":  {"lat_center": 40.3950, "lon_center": -3.6950, "demand_weight": 0.08, "restaurant_density": "medium",    "district": "Arganzuela"},
    "CHAMARTIN":   {"lat_center": 40.4620, "lon_center": -3.6770, "demand_weight": 0.07, "restaurant_density": "medium",    "district": "Chamartín"},
    "MALASANA":    {"lat_center": 40.4260, "lon_center": -3.7060, "demand_weight": 0.07, "restaurant_density": "very_high", "district": "Malasaña"},
}

ZONE_IDS = list(ZONES.keys())
ZONE_WEIGHTS = [z["demand_weight"] for z in ZONES.values()]


# ---------------------------------------------------------------------------
# Restaurant pool
# ---------------------------------------------------------------------------
CUISINE_TYPES = ["SPANISH", "ITALIAN", "JAPANESE", "AMERICAN", "MEXICAN", "CHINESE", "INDIAN", "MIDDLE_EASTERN", "THAI", "HEALTHY", "DESSERT"]

RESTAURANT_COUNT = int(os.getenv("RESTAURANT_COUNT", "150"))
COURIER_COUNT    = int(os.getenv("COURIER_COUNT", "120"))


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
    "SPANISH":        (24.0, 7.0),
    "ITALIAN":        (19.0, 5.0),
    "JAPANESE":       (32.0, 9.0),
    "AMERICAN":       (15.0, 4.0),
    "MEXICAN":        (16.0, 4.5),
    "CHINESE":        (14.0, 4.0),
    "INDIAN":         (20.0, 5.5),
    "MIDDLE_EASTERN": (13.0, 3.5),
    "THAI":           (18.0, 5.0),
    "HEALTHY":        (12.0, 3.0),
    "DESSERT":        (9.0, 3.0),
}

# Prep time distributions (mean_minutes, std_minutes) by cuisine
PREP_TIME_PARAMS: Dict[str, Tuple[float, float]] = {
    "SPANISH":        (22, 6),
    "ITALIAN":        (18, 4),
    "JAPANESE":       (25, 6),
    "AMERICAN":       (12, 3),
    "MEXICAN":        (15, 4),
    "CHINESE":        (14, 3),
    "INDIAN":         (22, 5),
    "MIDDLE_EASTERN": (12, 3),
    "THAI":           (20, 5),
    "HEALTHY":        (8,  2),
    "DESSERT":        (10, 3),
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
    # How often a new surge can trigger (seconds between surges)
    min_interval_seconds: int = int(os.getenv("SURGE_MIN_INTERVAL", "600"))   # 10 min
    max_interval_seconds: int = int(os.getenv("SURGE_MAX_INTERVAL", "1200"))  # 20 min
    # Duration range for each surge (seconds)
    min_duration_seconds: int = int(os.getenv("SURGE_MIN_DURATION", "60"))    # 1 min
    max_duration_seconds: int = int(os.getenv("SURGE_MAX_DURATION", "300"))   # 5 min
    # Multiplier range
    min_multiplier: float = float(os.getenv("SURGE_MIN_MULTIPLIER", "1.5"))
    max_multiplier: float = float(os.getenv("SURGE_MAX_MULTIPLIER", "4.0"))
    # Max zones affected per surge (1 to 3)
    max_zones_per_surge: int = int(os.getenv("SURGE_MAX_ZONES", "3"))

    # Adjacent zone mapping for multi-zone surges (zones that are geographically close)
    ADJACENT_ZONES: Dict = None

    def __post_init__(self):
        self.ADJACENT_ZONES = {
            "CENTRO":     ["MALASANA", "LATINA", "ARGANZUELA", "SALAMANCA"],
            "SALAMANCA":  ["CENTRO", "RETIRO", "CHAMARTIN", "CHAMBERI"],
            "CHAMBERI":   ["MALASANA", "TETUAN", "MONCLOA", "CENTRO"],
            "RETIRO":     ["SALAMANCA", "ARGANZUELA", "CENTRO"],
            "LATINA":     ["CENTRO", "ARGANZUELA", "MONCLOA"],
            "MONCLOA":    ["CHAMBERI", "LATINA", "CENTRO"],
            "TETUAN":     ["CHAMBERI", "CHAMARTIN", "MALASANA"],
            "ARGANZUELA": ["CENTRO", "LATINA", "RETIRO"],
            "CHAMARTIN":  ["TETUAN", "SALAMANCA", "CHAMBERI"],
            "MALASANA":   ["CENTRO", "CHAMBERI", "TETUAN"],
        }


# ---------------------------------------------------------------------------
# Output configuration
# ---------------------------------------------------------------------------
@dataclass
class OutputConfig:
    # Write JSON events
    json_enabled: bool = True
    json_output_dir: str = os.getenv(
        "JSON_OUTPUT_DIR",
        os.path.join(os.path.dirname(__file__), "..", "sample_data", "json"),
    )

    # Write AVRO events
    avro_enabled: bool = True
    avro_output_dir: str = os.getenv(
        "AVRO_OUTPUT_DIR",
        os.path.join(os.path.dirname(__file__), "..", "sample_data", "avro"),
    )

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
