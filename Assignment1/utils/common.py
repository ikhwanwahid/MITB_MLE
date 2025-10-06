# utils/common.py
from typing import Optional, List
from pyspark.sql import SparkSession

BASE = "datamart"
BRONZE = f"{BASE}/bronze"
SILVER = f"{BASE}/silver"
GOLD   = f"{BASE}/gold"

# Stores for training/serving
FEATURE_STORE = f"{BASE}/feature_store"
LABEL_STORE   = f"{BASE}/label_store"

def get_spark(app: str = "Medallion-Underwriting") -> SparkSession:
    return (
        SparkSession.builder
        .appName(app)
        .config("spark.sql.sources.partitionOverwriteMode", "dynamic")
        .getOrCreate()
    )

def parse_changed_dates(arg: Optional[str]) -> Optional[List[str]]:
    if not arg:
        return None
    parts = [x.strip() for x in arg.split(",")]
    return [p for p in parts if p]

