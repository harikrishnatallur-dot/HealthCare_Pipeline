from pyspark.sql.functions import col, current_date

def stage_members(df):
    return df.withColumn("date_of_birth", col("date_of_birth").cast("date")) \
             .withColumn("enrollment_date", col("enrollment_date").cast("date")) \
             .withColumn("load_date", current_date()) \
             .dropDuplicates(["member_id"])
