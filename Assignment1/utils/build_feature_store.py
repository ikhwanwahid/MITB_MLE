# utils/build_feature_store_loan_app_mob6.py
from pyspark.sql import functions as F
from .common import get_spark, GOLD, FEATURE_STORE

def build_feature_store():
    spark = get_spark("FS-LoanAppMOB6")
    g = spark.read.parquet(f"{GOLD}/loan_app_mob6")

    # Label-ish columns to drop from features
    drop_cols = {"is_default_at_loan6","loan_balance_at_6","loan_paid_at_6","loan_overdue_at_6","loan_mob6_date"}
    feat_cols = [c for c in g.columns if c not in drop_cols]

    fs = (g.select(*feat_cols)
            .withColumn("event_time", F.col("application_date"))
            .withColumn("event_date", F.to_date("application_date")))

    (fs.write.mode("overwrite")
       .partitionBy("event_date")
       .parquet(f"{FEATURE_STORE}/offline/loan_app_mob6_features"))

if __name__ == "__main__":
    build_feature_store()
