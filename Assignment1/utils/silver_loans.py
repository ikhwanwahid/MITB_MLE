# utils/silver_loans.py
import argparse
from pyspark.sql import functions as F, Window as W
from .common import get_spark, BRONZE, SILVER, parse_changed_dates

def build_loans_silver(changed_dates=None):
    """
    Emits THREE Silver tables:
      0) silver/loans_monthly_clean   ← per-loan monthly timeline (cleaned/typed)
      1) silver/loans_app_terms       : one row per loan_id at application (MOB=0)
      2) silver/loans_customer_month  : Customer_ID × snapshot_date aggregates (no 'mob' in key)
    """
    spark = get_spark("SilverLoans-Underwriting")

    # --- Read Bronze loans and cast/clean once here ---
    df = spark.read.parquet(f"{BRONZE}/loans/lms_loan_daily")
    if changed_dates:
        df = df.filter(F.col("snapshot_date").isin(changed_dates))

    loans = (
        df
        .withColumn("snapshot_date",   F.to_date("snapshot_date"))
        .withColumn("loan_start_date", F.to_date("loan_start_date"))
        .withColumn("tenure",          F.col("tenure").cast("int"))
        .withColumn("installment_num", F.col("installment_num").cast("int"))
        .withColumn("loan_amt",        F.col("loan_amt").cast("double"))
        .withColumn("due_amt",         F.col("due_amt").cast("double"))
        .withColumn("paid_amt",        F.col("paid_amt").cast("double"))
        .withColumn("overdue_amt",     F.col("overdue_amt").cast("double"))
        .withColumn("balance",         F.col("balance").cast("double"))
        .withColumn("dpd30_flag", (F.col("overdue_amt") > 0).cast("int"))
    )

    # (0) NEW: per-loan monthly timeline (clean, typed) → source of truth for Gold
    loans_monthly_clean = loans.select(
        "loan_id", "Customer_ID",
        "snapshot_date", "loan_start_date",
        "installment_num", "tenure",
        "loan_amt", "due_amt", "paid_amt", "overdue_amt", "balance",
        "dpd30_flag"
    )
    (loans_monthly_clean.write
        .mode("overwrite")
        .partitionBy("snapshot_date")
        .parquet(f"{SILVER}/loans_monthly_clean"))

    # (1) Application terms per loan (earliest snapshot only)
    w_first = W.partitionBy("loan_id").orderBy(F.col("snapshot_date").asc())
    loans_app_terms = (
        loans.withColumn("rn", F.row_number().over(w_first))
        .filter("rn = 1")
        .select(
            "loan_id", "Customer_ID",
            F.col("loan_start_date").alias("application_date"),
            F.col("loan_amt").alias("orig_loan_amt"),
            F.col("tenure").alias("orig_tenure"),
            F.col("installment_num").alias("orig_installment_num")
        )
    )
    loans_app_terms.write.mode("overwrite").parquet(f"{SILVER}/loans_app_terms")

    # (2) Customer × month aggregates (no loan-level mob in the key)
    cust_month = (
        loans.groupBy("Customer_ID", "snapshot_date")
        .agg(
            F.countDistinct("loan_id").alias("active_loans"),
            F.sum("balance").alias("total_balance"),
            F.sum("paid_amt").alias("total_paid_amt"),
            F.sum("overdue_amt").alias("total_overdue_amt"),
            F.max("dpd30_flag").alias("any_dpd30"),
        )
    )
    (cust_month.write
        .mode("overwrite")
        .partitionBy("snapshot_date")
        .parquet(f"{SILVER}/loans_customer_month"))

if __name__ == "__main__":
    ap = argparse.ArgumentParser()
    ap.add_argument("--changed_dates", type=str, default=None)
    args = ap.parse_args()
    build_loans_silver(parse_changed_dates(args.changed_dates))


