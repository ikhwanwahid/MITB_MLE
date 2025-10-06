# utils/build_label_store_loan_app_mob6.py
from pyspark.sql import functions as F
from .common import get_spark, GOLD, LABEL_STORE

def build_label_store():
    spark = get_spark("LS-LoanAppMOB6")
    g = spark.read.parquet(f"{GOLD}/loan_app_mob6")

    ls = (g.select(
            "loan_id","Customer_ID","application_date",
            F.col("loan_mob6_date").alias("label_event_time"),
            "is_default_at_loan6")
          .withColumn("label_event_date", F.to_date("label_event_time"))
    )

    (ls.write.mode("overwrite")
       .partitionBy("label_event_date")
       .parquet(f"{LABEL_STORE}/loan_app_mob6_labels"))

if __name__ == "__main__":
    build_label_store()
