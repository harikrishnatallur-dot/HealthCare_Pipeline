from utils.spark_session import get_spark
from transformations.staging_members import stage_members
from utils.data_quality import validate_not_empty

spark = get_spark("Members_ETL")

raw_members = spark.read.option("header", True).csv("data/raw/members/")
staged_members = stage_members(raw_members)

validate_not_empty(staged_members)

staged_members.write.mode("append").partitionBy("enrollment_date").saveAsTable("healthcare.stg_members")
