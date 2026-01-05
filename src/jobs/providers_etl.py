from utils.spark_session import get_spark
from transformations.staging_providers import stage_providers
from utils.data_quality import validate_not_empty

spark = get_spark("Providers_ETL")

raw_providers = spark.read.option("header", True).csv("data/raw/providers/")
staged_providers = stage_providers(raw_providers)

validate_not_empty(staged_providers)

staged_providers.write.mode("append").partitionBy("credential_date").saveAsTable("healthcare.stg_providers")
