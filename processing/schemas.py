"""
schemas.py
----------
Spark StructType schemas matching the Avro definitions from Milestone 1.
Used to parse JSON from Event Hubs.
"""

from pyspark.sql.types import (
    StructType, StructField, StringType, LongType, DoubleType,
    IntegerType, BooleanType, FloatType, MapType,
)


ORDER_SCHEMA = StructType([
    StructField("schema_version", StringType(), True),
    StructField("event_id", StringType(), False),
    StructField("order_id", StringType(), False),
    StructField("customer_id", StringType(), False),
    StructField("restaurant_id", StringType(), False),
    StructField("courier_id", StringType(), True),
    StructField("zone_id", StringType(), False),
    StructField("status", StringType(), False),
    StructField("event_time", LongType(), False),
    StructField("ingestion_time", LongType(), False),
    StructField("order_value_eur", DoubleType(), True),
    StructField("item_count", IntegerType(), True),
    StructField("is_promo", BooleanType(), True),
    StructField("promo_code", StringType(), True),
    StructField("payment_method", StringType(), True),
    StructField("estimated_prep_minutes", IntegerType(), True),
    StructField("actual_prep_minutes", IntegerType(), True),
    StructField("estimated_delivery_minutes", IntegerType(), True),
    StructField("actual_delivery_minutes", IntegerType(), True),
    StructField("cancellation_reason", StringType(), True),
    StructField("device_id", StringType(), True),
    StructField("platform", StringType(), True),
    StructField("is_duplicate", BooleanType(), True),
    StructField("is_late", BooleanType(), True),
    StructField("metadata", MapType(StringType(), StringType()), True),
])


COURIER_SCHEMA = StructType([
    StructField("schema_version", StringType(), True),
    StructField("event_id", StringType(), False),
    StructField("courier_id", StringType(), False),
    StructField("order_id", StringType(), True),
    StructField("zone_id", StringType(), False),
    StructField("status", StringType(), False),
    StructField("event_time", LongType(), False),
    StructField("ingestion_time", LongType(), False),
    StructField("latitude", DoubleType(), True),
    StructField("longitude", DoubleType(), True),
    StructField("location_accuracy_meters", FloatType(), True),
    StructField("speed_kmh", FloatType(), True),
    StructField("battery_pct", IntegerType(), True),
    StructField("vehicle_type", StringType(), True),
    StructField("active_session_id", StringType(), True),
    StructField("session_start_time", LongType(), True),
    StructField("deliveries_completed_this_session", IntegerType(), True),
    StructField("is_heartbeat", BooleanType(), True),
    StructField("is_duplicate", BooleanType(), True),
    StructField("is_late", BooleanType(), True),
    StructField("offline_reason", StringType(), True),
    StructField("metadata", MapType(StringType(), StringType()), True),
])
