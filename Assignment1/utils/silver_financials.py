# utils/silver_financials.py
import argparse, re
from pyspark.sql import functions as F, types as T, Window as W
from .common import get_spark, BRONZE, SILVER, parse_changed_dates

def _to_double(col):
    # strip anything that isn't digit, dot, or minus; cast to double
    return F.regexp_replace(col, r"[^0-9\.\-]", "").cast("double")

def _parse_credit_history(col):
    # "10 Years and 9 Months" -> 10*12 + 9 = 129
    years = F.regexp_extract(col, r"(\d+)\s*Year", 1).cast("int")
    months = F.regexp_extract(col, r"(\d+)\s*Month", 1).cast("int")
    return (F.coalesce(years, F.lit(0)) * F.lit(12) + F.coalesce(months, F.lit(0))).alias("credit_history_months")

def build_financials_scd2(changed_dates=None):
    spark = get_spark("SilverFinancials")
    df = spark.read.parquet(f"{BRONZE}/users/financials")
    if changed_dates:
        df = df.filter(F.col("snapshot_date").isin(changed_dates))

    fins = (
        df
        .withColumn("snapshot_date", F.to_date("snapshot_date"))
        .withColumn("Annual_Income", _to_double(F.col("Annual_Income")))
        .withColumn("Monthly_Inhand_Salary", _to_double(F.col("Monthly_Inhand_Salary")))
        .withColumn("Num_Bank_Accounts", F.col("Num_Bank_Accounts").cast("int"))
        .withColumn("Num_Credit_Card", F.col("Num_Credit_Card").cast("int"))
        .withColumn("Num_of_Loan", _to_double(F.col("Num_of_Loan")).cast("int"))
        .withColumn("Num_of_Delayed_Payment", _to_double(F.col("Num_of_Delayed_Payment")).cast("int"))
        .withColumn("Changed_Credit_Limit", _to_double(F.col("Changed_Credit_Limit")))
        .withColumn("Outstanding_Debt", _to_double(F.col("Outstanding_Debt")))
        .withColumn("Credit_Utilization_Ratio", _to_double(F.col("Credit_Utilization_Ratio")))
        .withColumn("credit_history_months", _parse_credit_history(F.col("Credit_History_Age")))
        .withColumn("Payment_of_Min_Amount_flag",
                    F.when(F.lower(F.col("Payment_of_Min_Amount")).isin("yes","y","true"), 1)
                     .when(F.lower(F.col("Payment_of_Min_Amount")).isin("no","n","false"), 0)
                     .otherwise(None).cast("int"))
    )

    # Portfolio “Type_of_Loan” flags (best-effort normalization)
    loan_types_raw = F.coalesce(F.col("Type_of_Loan"), F.lit(""))

    # Normalize: lower, turn hyphens/underscores into spaces, convert "and" to commas
    norm = F.lower(loan_types_raw)
    norm = F.regexp_replace(norm, r"[-_]+", " ")
    norm = F.regexp_replace(norm, r"\band\b", ",")  # split on commas later
    
    # Split to array on commas, trim tokens, drop empties
    arr      = F.split(norm, r",")
    trimmed  = F.transform(arr, lambda x: F.trim(x))
    cleaned  = F.array_remove(trimmed, "")  # remove empty strings
    
    fins = fins.withColumn("loan_types_arr", cleaned)
    
    # Canonical tokens (match our normalization: lower, spaces instead of hyphens)
    tokens = [
    "auto loan",
    "mortgage",
    "personal loan",
    "student loan",
    "home loan",
    "credit builder loan",
    "debt consolidation loan",
    "payday loan",
    "not specified",
    ]
    
    for t in tokens:
        colname = "has_" + t.replace(" ", "_")
        fins = fins.withColumn(colname, F.array_contains(F.col("loan_types_arr"), F.lit(t)).cast("int"))

    # SCD2 bands (for future-proof "as-of" joins; currently one snapshot per customer)
    w = W.partitionBy("Customer_ID").orderBy("snapshot_date")
    fins_scd2 = (fins
        .dropDuplicates(["Customer_ID","snapshot_date"])
        .withColumn("valid_from", F.col("snapshot_date"))
        .withColumn("valid_to", F.lead("snapshot_date").over(w))
        .drop("snapshot_date")
    )

    fins_scd2.write.mode("overwrite").parquet(f"{SILVER}/users/financials_scd2")

if __name__ == "__main__":
    ap = argparse.ArgumentParser()
    ap.add_argument("--changed_dates", type=str, default=None)
    args = ap.parse_args()
    build_financials_scd2(parse_changed_dates(args.changed_dates))

