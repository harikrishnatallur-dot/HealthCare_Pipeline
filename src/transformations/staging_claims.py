from pyspark.sql.functions import col, current_date

def stage_claims(df):
    return df.withColumn("amount", col("amount").cast("double")) \
             .withColumn("service_date", col("service_date").cast("date")) \
             .withColumn("load_date", current_date()) \
             .dropDuplicates(["claim_id"])
