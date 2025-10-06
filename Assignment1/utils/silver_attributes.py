# utils/silver_attributes.py
from pyspark.sql import functions as F
from .common import get_spark, BRONZE, SILVER

def build_attributes_current():
    spark = get_spark("SilverAttributes")
    df = spark.read.parquet(f"{BRONZE}/users/attributes")

    attrs = (
        df.withColumn("snapshot_date", F.to_date("snapshot_date"))
          .withColumn("Age", F.col("Age").cast("int"))
          .withColumn("SSN_masked", F.when(F.col("SSN").isNotNull(), F.lit("***-**-****")).otherwise(None))
          .select("Customer_ID", "Age", "Occupation", "SSN_masked")
          .dropDuplicates(["Customer_ID"])
    )
    attrs.write.mode("overwrite").parquet(f"{SILVER}/users/attributes_current")
