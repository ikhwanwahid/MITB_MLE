import argparse
from pyspark.sql import functions as F
from common import get_spark, BRONZE, parse_changed_dates

# Source CSV paths (from your uploads)
LOANS_CSV  = "/data/lms_loan_daily.csv"
FINS_CSV   = "/data/features_financials.csv"
ATTRS_CSV  = "/data/features_attributes.csv"
CLICKS_CSV = "/data/feature_clickstream.csv"

def read_csv(spark, path):
    return spark.read.option("header", True).csv(path)

def write_partitioned_by_date(df, out_dir, changed_dates=None):
    # Ensure snapshot_date is typed as date for partitioning
    df = df.withColumn("snapshot_date", F.to_date("snapshot_date"))
    if changed_dates:
        df = df.filter(F.col("snapshot_date").isin(changed_dates))
    (
        df.write
        .mode("overwrite")
        .partitionBy("snapshot_date")
        .parquet(out_dir)
    )

def ingest_all(changed_dates=None):
    spark = get_spark("BronzeIngest")

    loans_bz  = read_csv(spark, LOANS_CSV)
    fins_bz   = read_csv(spark, FINS_CSV)
    attrs_bz  = read_csv(spark, ATTRS_CSV)
    clicks_bz = read_csv(spark, CLICKS_CSV)

    write_partitioned_by_date(loans_bz,  f"{BRONZE}/loans/lms_loan_daily", changed_dates)
    write_partitioned_by_date(fins_bz,   f"{BRONZE}/users/financials",     changed_dates)
    write_partitioned_by_date(attrs_bz,  f"{BRONZE}/users/attributes",     changed_dates)
    write_partitioned_by_date(clicks_bz, f"{BRONZE}/users/clickstream",    changed_dates)

if __name__ == "__main__":
    ap = argparse.ArgumentParser()
    ap.add_argument("--changed_dates", type=str, default=None,
                    help="Comma-separated YYYY-MM-DD (e.g. 2023-08-01,2023-09-01)")
    args = ap.parse_args()
    ingest_all(parse_changed_dates(args.changed_dates))

