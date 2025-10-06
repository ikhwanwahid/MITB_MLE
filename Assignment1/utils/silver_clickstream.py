# utils/silver_clickstream.py
import argparse, re
from pyspark.sql import functions as F
from .common import get_spark, BRONZE, SILVER, parse_changed_dates

def build_click_agg(changed_dates=None):
    spark = get_spark("SilverClickstream")
    df = spark.read.parquet(f"{BRONZE}/users/clickstream")
    if changed_dates:
        df = df.filter(F.col("snapshot_date").isin(changed_dates))

    clicks = df.withColumn("snapshot_date", F.to_date("snapshot_date"))

    # Cast fe_1..fe_20 to double if present
    fe_cols = [c for c in clicks.columns if re.fullmatch(r"fe_\d+", c)]
    for c in fe_cols:
        clicks = clicks.withColumn(c, F.col(c).cast("double"))

    # Derive simple month-level aggregates we can roll up later (≤ application_date)
    clicks = clicks.withColumn("fe_sum", F.expr("+".join([f"COALESCE({c},0.0)" for c in fe_cols]) if fe_cols else "0.0")) \
                   .withColumn("fe_avg", F.when(F.lit(len(fe_cols)) > 0, F.col("fe_sum")/F.lit(len(fe_cols))).otherwise(F.lit(0.0)))

    # Ensure one row per Customer_ID × snapshot_date (source is already at that grain)
    out = clicks.select(["Customer_ID","snapshot_date"] + fe_cols + ["fe_sum","fe_avg"]).dropDuplicates(["Customer_ID","snapshot_date"])

    out.write.mode("overwrite").partitionBy("snapshot_date").parquet(f"{SILVER}/users/click_agg_month")

if __name__ == "__main__":
    ap = argparse.ArgumentParser()
    ap.add_argument("--changed_dates", type=str, default=None)
    args = ap.parse_args()
    build_click_agg(parse_changed_dates(args.changed_dates))

