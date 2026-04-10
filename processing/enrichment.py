"""
enrichment.py
-------------
Loads restaurant reference data as a Spark DataFrame for broadcast joins.
"""

import importlib.util
import os

from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.types import StructType, StructField, StringType, DoubleType

# Load generator modules by file path to avoid config package name clash
_GENERATOR_DIR = os.path.join(os.path.dirname(__file__), "..", "generator")


def _load_module(name, filepath):
    spec = importlib.util.spec_from_file_location(name, filepath)
    mod = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(mod)
    return mod


# Load generator config first (reference_data depends on it)
_gen_config = _load_module("gen_config", os.path.join(_GENERATOR_DIR, "config.py"))
import sys
sys.modules["config"] = _gen_config  # so reference_data's "from config import ..." works

_ref_data = _load_module("reference_data", os.path.join(_GENERATOR_DIR, "reference_data.py"))
RESTAURANTS = _ref_data.RESTAURANTS

# Restore: remove the fake "config" entry so it doesn't break our real config package
del sys.modules["config"]


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
