"""
reference_data.py
-----------------
Generates stable reference entities: restaurants and couriers.
These are generated once at startup and reused across the simulation.
"""

import uuid
import random
from dataclasses import dataclass, field
from typing import List, Dict

from config import (
    ZONES, ZONE_IDS, CUISINE_TYPES,
    RESTAURANT_COUNT, COURIER_COUNT,
    ORDER_VALUE_PARAMS, PREP_TIME_PARAMS,
)


@dataclass
class Restaurant:
    restaurant_id: str
    name: str
    zone_id: str
    cuisine_type: str
    latitude: float
    longitude: float
    avg_prep_mean: float
    avg_prep_std: float
    avg_order_value_mean: float
    avg_order_value_std: float
    # SLA tier (used for SLA monitoring analytics)
    sla_tier: str  # "GOLD", "SILVER", "BRONZE"
    is_active: bool = True


@dataclass
class Courier:
    courier_id: str
    name: str
    vehicle_type: str
    home_zone_id: str
    current_zone_id: str
    latitude: float
    longitude: float
    is_online: bool = True
    current_session_id: str = field(default_factory=lambda: str(uuid.uuid4()))
    session_start_time: float = field(default_factory=lambda: 0.0)
    deliveries_this_session: int = 0


_FIRST_NAMES = [
    "Carlos", "Lucia", "Pablo", "Maria", "Javier", "Ana", "Diego", "Elena",
    "Alejandro", "Sofia", "Miguel", "Carmen", "Daniel", "Laura", "Sergio", "Marta"
]
_LAST_NAMES = [
    "Garcia", "Rodriguez", "Martinez", "Lopez", "Gonzalez", "Hernandez", "Fernandez",
    "Sanchez", "Perez", "Gomez", "Moreno", "Ruiz", "Jimenez", "Diaz"
]
_RESTAURANT_NAMES_BY_CUISINE = {
    "SPANISH":        ["Casa", "Taberna", "Mesón", "Bodega", "Tasca", "Rincón", "Bar"],
    "ITALIAN":        ["Trattoria", "Pizzeria", "Osteria", "Ristorante", "Bella", "La Dolce"],
    "JAPANESE":       ["Sakura", "Hanami", "Koi", "Zen", "Mizu", "Nori", "Tanuki"],
    "AMERICAN":       ["Big", "Lucky", "Star", "Golden", "Eagle", "Liberty", "Rocky"],
    "MEXICAN":        ["El Ranchero", "La Cantina", "Taco", "Azteca", "Jalapeño", "Maiz"],
    "CHINESE":        ["Dragon", "Jade", "Golden Wok", "Bamboo", "Lotus", "Ming", "Phoenix"],
    "INDIAN":         ["Taj", "Namaste", "Saffron", "Masala", "Curry", "Bombay", "Delhi"],
    "MIDDLE_EASTERN": ["Aladdin", "Sultan", "Habibi", "Beirut", "Falafel", "Shawarma", "Oasis"],
    "THAI":           ["Bangkok", "Siam", "Pad Thai", "Orchid", "Mango", "Lotus", "Spice"],
    "HEALTHY":        ["Green", "Fresh", "Vita", "Pure", "Bowl", "Leaf", "Glow"],
    "DESSERT":        ["Sweet", "Sugar", "Choco", "Dulce", "Crème", "Helado", "Pastel"],
}
_RESTAURANT_SUFFIXES = [
    "Madrid", "del Sol", "de Oro", "Real", "Gourmet", "Express", "Premium",
    "Fusión", "Original", "Artesano"
]

VEHICLE_TYPES = ["BICYCLE", "SCOOTER", "MOTORCYCLE", "CAR", "WALKING"]
VEHICLE_WEIGHTS = [0.30, 0.35, 0.20, 0.10, 0.05]
SLA_TIERS = ["GOLD", "SILVER", "BRONZE"]
SLA_TIER_WEIGHTS = [0.20, 0.50, 0.30]


def _random_name() -> str:
    return f"{random.choice(_FIRST_NAMES)} {random.choice(_LAST_NAMES)}"


def _jitter_coord(center: float, radius: float = 0.03) -> float:
    return center + random.uniform(-radius, radius)


def build_restaurants() -> List[Restaurant]:
    restaurants = []
    for i in range(RESTAURANT_COUNT):
        zone_id = random.choices(ZONE_IDS, weights=[
            ZONES[z]["demand_weight"] for z in ZONE_IDS
        ])[0]
        zone = ZONES[zone_id]
        cuisine = random.choice(CUISINE_TYPES)
        prep_mean, prep_std = PREP_TIME_PARAMS[cuisine]
        val_mean, val_std = ORDER_VALUE_PARAMS[cuisine]

        r = Restaurant(
            restaurant_id=f"R{str(i+1).zfill(4)}",
            name=f"{random.choice(_RESTAURANT_NAMES_BY_CUISINE.get(cuisine, ['Restaurant']))} {random.choice(_RESTAURANT_SUFFIXES)}",
            zone_id=zone_id,
            cuisine_type=cuisine,
            latitude=_jitter_coord(zone["lat_center"]),
            longitude=_jitter_coord(zone["lon_center"]),
            avg_prep_mean=prep_mean,
            avg_prep_std=prep_std,
            avg_order_value_mean=val_mean,
            avg_order_value_std=val_std,
            sla_tier=random.choices(SLA_TIERS, weights=SLA_TIER_WEIGHTS)[0],
        )
        restaurants.append(r)
    return restaurants


def build_couriers() -> List[Courier]:
    couriers = []
    for i in range(COURIER_COUNT):
        zone_id = random.choices(ZONE_IDS, weights=[
            ZONES[z]["demand_weight"] for z in ZONE_IDS
        ])[0]
        zone = ZONES[zone_id]
        vehicle = random.choices(VEHICLE_TYPES, weights=VEHICLE_WEIGHTS)[0]

        c = Courier(
            courier_id=f"C{str(i+1).zfill(4)}",
            name=_random_name(),
            vehicle_type=vehicle,
            home_zone_id=zone_id,
            current_zone_id=zone_id,
            latitude=_jitter_coord(zone["lat_center"]),
            longitude=_jitter_coord(zone["lon_center"]),
            is_online=random.random() < 0.70,  # 70% start online
        )
        couriers.append(c)
    return couriers


# Singletons for the simulation run
RESTAURANTS: List[Restaurant] = build_restaurants()
COURIERS: List[Courier] = build_couriers()

# Lookup maps
RESTAURANT_MAP: Dict[str, Restaurant] = {r.restaurant_id: r for r in RESTAURANTS}
COURIER_MAP: Dict[str, Courier] = {c.courier_id: c for c in COURIERS}
