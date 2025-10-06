# utils/gold_build_loan_app_mob6.py
"""
Underwriting-safe GOLD table:
- Grain: one row per loan_id
- Features: ONLY information available at application time (MOB=0)
- Label: DPD30 proxy (overdue_amt > 0) at that loan's MOB=6 date
- Inputs: all from SILVER (cleaned)
"""

from pyspark.sql import functions as F, Window as W
from .common import get_spark, SILVER, GOLD


def build_loan_app_mob6():
    spark = get_spark("Gold-LoanAppMOB6")

    # --- 1) Source: Silver loans monthly timeline (cleaned) ---
    loans_sv = (
        spark.read.parquet(f"{SILVER}/loans_monthly_clean")
        .withColumn("mob", F.months_between(F.col("snapshot_date"), F.col("loan_start_date")).cast("int"))
    )

    # --- 2) Anchors per loan: application date and calendar MOB6 date ---
    loan_targets = (
        loans_sv.select("loan_id", "Customer_ID", "loan_start_date").dropDuplicates()
        .withColumn("application_date", F.col("loan_start_date"))
        .withColumn("loan_mob6_target", F.add_months("loan_start_date", 6))
    )

    # Snap to first available snapshot >= target
    loan_snaps = loans_sv.select("loan_id", "snapshot_date").dropDuplicates()
    candidates = loan_targets.join(loan_snaps, "loan_id").where(
        F.col("snapshot_date") >= F.col("loan_mob6_target")
    )
    w_anchor = W.partitionBy("loan_id").orderBy(F.col("snapshot_date").asc())
    loan_anchor = (
        candidates.withColumn("rn", F.row_number().over(w_anchor)).filter("rn=1")
        .select(
            "loan_id",
            "Customer_ID",
            "application_date",
            F.col("snapshot_date").alias("loan_mob6_date"),
        )
    )

    # --- 3) Label at MOB=6 for THIS loan ---
    loan_label = (
        loans_sv.alias("l")
        .join(
            loan_anchor.alias("a"),
            (F.col("l.loan_id") == F.col("a.loan_id"))
            & (F.col("l.snapshot_date") == F.col("a.loan_mob6_date")),
            "left",
        )
        .select(
            "a.loan_id",
            "a.Customer_ID",
            "a.application_date",
            "a.loan_mob6_date",
            F.col("l.dpd30_flag").alias("is_default_at_loan6"),
            F.col("l.balance").alias("loan_balance_at_6"),
            F.col("l.paid_amt").alias("loan_paid_at_6"),
            F.col("l.overdue_amt").alias("loan_overdue_at_6"),
        )
    )

    # --- 4) Features known at application time (MOB=0) ---

    # 4A. Origination terms (per-loan, MOB=0)
    loan_terms = spark.read.parquet(f"{SILVER}/loans_app_terms")

    # 4B. Prior customer history ≤ application (exclude THIS loan)
    hist = (
        loans_sv.alias("l")
        .join(loan_anchor.alias("a"), "Customer_ID")
        .where(
            (F.col("l.loan_id") != F.col("a.loan_id"))
            & (F.col("l.loan_start_date") < F.col("a.application_date"))
            & (F.col("l.snapshot_date") <= F.col("a.application_date"))
        )
        .groupBy("a.loan_id", "a.Customer_ID", "a.application_date", "a.loan_mob6_date")
        .agg(
            F.countDistinct("l.loan_id").alias("prior_loans_count"),
            F.sum("dpd30_flag").alias("prior_months_with_dpd30"),
            F.sum("overdue_amt").alias("prior_cum_overdue"),
            F.sum("paid_amt").alias("prior_cum_paid"),
            F.max(F.when(F.col("balance") > 0, 1).otherwise(0)).alias(
                "prior_any_active_balance"
            ),
        )
    )

    # 4C. Financials as-of application (SCD2)
    fins2 = spark.read.parquet(f"{SILVER}/users/financials_scd2")
    fin_asof = (
        fins2.alias("f")
        .join(loan_anchor.alias("a"), "Customer_ID")
        .where(
            (F.col("f.valid_from") <= F.col("a.application_date"))
            & (
                (F.col("f.valid_to") > F.col("a.application_date"))
                | F.col("f.valid_to").isNull()
            )
        )
    )
    wf = W.partitionBy("Customer_ID", "application_date").orderBy(
        F.col("f.valid_from").desc()
    )
    fin_at_app = (
        fin_asof.withColumn("rn", F.row_number().over(wf))
        .filter("rn=1")
        .drop("rn", "valid_from", "valid_to")
    )

    # 4D. Clickstream ≤ application_date (sum/avg per feature + months counted)
    clicks = spark.read.parquet(f"{SILVER}/users/click_agg_month")
    fe_cols = [c for c in clicks.columns if c.startswith("fe_")]
    agg_exprs = []
    for c in fe_cols:
        agg_exprs += [
            F.sum(F.col(c)).alias(f"{c}_sum_to_app"),
            F.avg(F.col(c)).alias(f"{c}_avg_to_app"),
        ]
    click_feats = (
        clicks.alias("c")
        .join(loan_anchor.alias("a"), "Customer_ID")
        .where(F.col("c.snapshot_date") <= F.col("a.application_date"))
        .groupBy("a.loan_id", "a.Customer_ID", "a.application_date", "a.loan_mob6_date")
        .agg(*agg_exprs, F.count(F.lit(1)).alias("months_with_clicks_to_app"))
    )

    # 4E. Attributes (SCD0)
    attrs = spark.read.parquet(f"{SILVER}/users/attributes_current")

    # --- 5) Assemble GOLD ---
    gold = (
        loan_anchor
        .join(
            loan_label,
            ["loan_id", "Customer_ID", "application_date", "loan_mob6_date"],
            "left",
        )
        .join(loan_terms, ["loan_id", "Customer_ID", "application_date"], "left")
        .join(
            hist,
            ["loan_id", "Customer_ID", "application_date", "loan_mob6_date"],
            "left",
        )
        .join(fin_at_app, ["Customer_ID", "application_date"], "left")
        .join(
            click_feats,
            ["loan_id", "Customer_ID", "application_date", "loan_mob6_date"],
            "left",
        )
        .join(attrs, "Customer_ID", "left")
    )

    # --- 6) Make column names unique by position, then drop duplicate keys ---
    names = gold.columns
    seen = {}
    new_names = []
    for n in names:
        if n in seen:
            seen[n] += 1
            new_names.append(f"{n}__dup{seen[n]}")
        else:
            seen[n] = 0
            new_names.append(n)
    gold = gold.toDF(*new_names)  # rename by POSITION (no ambiguous refs)

    # drop any duplicated join-key columns (keep the first occurrence)
    key_prefixes = {"loan_id", "Customer_ID", "application_date", "loan_mob6_date"}
    drop_cols = [n for n in new_names if "__dup" in n and n.split("__dup")[0] in key_prefixes]
    if drop_cols:
        gold = gold.drop(*drop_cols)

    # --- 7) Write GOLD table ---
    gold.write.mode("overwrite").parquet(f"{GOLD}/loan_app_mob6")


if __name__ == "__main__":
    build_loan_app_mob6()



