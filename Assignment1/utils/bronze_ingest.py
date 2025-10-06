# utils/bronze_ingest.py
# Ingest the 4 CSVs from /mnt/data into Bronze Parquet, partitioned by snapshot_date
import argparse
from pyspark.sql import functions as F
from .common import get_spark, BRONZE, parse_changed_dates

SRC_ROOT = "data"

def _read_csv(spark, path):
    return spark.read.option("header", True).csv(path)

def ingest_all(changed_dates=None):
    spark = get_spark("BronzeIngest")

    # Loans
    loans = _read_csv(spark, f"{SRC_ROOT}/lms_loan_daily.csv") \
        .withColumn("snapshot_date", F.to_date("snapshot_date")) \
        .withColumn("loan_start_date", F.to_date("loan_start_date"))
    if changed_dates:
        loans = loans.filter(F.col("snapshot_date").isin(changed_dates))
    loans.write.mode("overwrite").partitionBy("snapshot_date") \
        .parquet(f"{BRONZE}/loans/lms_loan_daily")

    # Financials
    fins = _read_csv(spark, f"{SRC_ROOT}/features_financials.csv") \
        .withColumn("snapshot_date", F.to_date("snapshot_date"))
    if changed_dates:
        fins = fins.filter(F.col("snapshot_date").isin(changed_dates))
    fins.write.mode("overwrite").partitionBy("snapshot_date") \
        .parquet(f"{BRONZE}/users/financials")

    # Attributes
    attrs = _read_csv(spark, f"{SRC_ROOT}/features_attributes.csv") \
        .withColumn("snapshot_date", F.to_date("snapshot_date"))
    if changed_dates:
        attrs = attrs.filter(F.col("snapshot_date").isin(changed_dates))
    attrs.write.mode("overwrite").partitionBy("snapshot_date") \
        .parquet(f"{BRONZE}/users/attributes")

    # Clickstream
    clicks = _read_csv(spark, f"{SRC_ROOT}/feature_clickstream.csv") \
        .withColumn("snapshot_date", F.to_date("snapshot_date"))
    if changed_dates:
        clicks = clicks.filter(F.col("snapshot_date").isin(changed_dates))
    clicks.write.mode("overwrite").partitionBy("snapshot_date") \
        .parquet(f"{BRONZE}/users/clickstream")

if __name__ == "__main__":
    ap = argparse.ArgumentParser()
    ap.add_argument("--changed_dates", type=str, default=None)
    args = ap.parse_args()
    ingest_all(parse_changed_dates(args.changed_dates))
