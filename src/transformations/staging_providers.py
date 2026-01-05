from pyspark.sql.functions import col, current_date

def stage_providers(df):
    return df.withColumn("credential_date", col("credential_date").cast("date")) \
             .withColumn("load_date", current_date()) \
             .dropDuplicates(["provider_id"])
