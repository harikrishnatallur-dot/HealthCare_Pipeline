from utils.spark_session import get_spark
from transformations.staging_claims import stage_claims
from utils.data_quality import validate_not_empty

spark = get_spark("Claims_ETL")

raw_claims = spark.read.option("header", True).csv("data/raw/claims/")
staged_claims = stage_claims(raw_claims)

validate_not_empty(staged_claims)

staged_claims.write.mode("append").partitionBy("service_date").saveAsTable("healthcare.stg_claims")
