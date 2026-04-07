"""
enrichment.py
-------------
Loads restaurant reference data as a Spark DataFrame for broadcast joins.
"""

import sys
import os

from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.types import StructType, StructField, StringType, DoubleType

# Add generator to path so we can import reference_data
sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "generator"))
from reference_data import RESTAURANTS


RESTAURANT_REF_SCHEMA = StructType([
    StructField("restaurant_id", StringType(), False),
    StructField("restaurant_name", StringType(), True),
    StructField("zone_id", StringType(), True),
    StructField("cuisine_type", StringType(), True),
    StructField("sla_tier", StringType(), True),
    StructField("avg_prep_mean", DoubleType(), True),
])

# SLA thresholds: p95 prep time must be under these (minutes)
SLA_THRESHOLDS = {
    "GOLD": 15,
    "SILVER": 25,
    "BRONZE": 35,
}


def get_restaurant_ref_df(spark: SparkSession) -> DataFrame:
    """Load restaurant reference data as a broadcast-ready DataFrame."""
    rows = [
        (
            r.restaurant_id,
            r.name,
            r.zone_id,
            r.cuisine_type,
            r.sla_tier,
            float(r.avg_prep_mean),
        )
        for r in RESTAURANTS
    ]
    return spark.createDataFrame(rows, schema=RESTAURANT_REF_SCHEMA)
