def get_last_processed_date(spark, job_name):
    df = spark.sql(f"SELECT last_processed_date FROM healthcare.etl_metadata WHERE job_name='{job_name}'")
    return df.collect()[0][0]
