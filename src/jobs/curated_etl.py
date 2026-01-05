from utils.spark_session import get_spark
from transformations.curated_claims import (
    curate_claims_fact,
    curate_dim_members,
    curate_dim_providers
)
from utils.data_quality import (
    validate_not_empty,
    validate_referential_integrity,
    validate_no_nulls,
    profile_dataframe
)

spark = get_spark("Curated_ETL")

print("\n" + "="*80)
print("Starting Curated ETL Job")
print("="*80)

# Read from staging tables
stg_claims = spark.table("healthcare.stg_claims")
print("\n### STAGE 5: Curated Transformations ###")
curated_members = curate_dim_members(stg_members)
curated_providers = curate_dim_providers(stg_providers)

# Create curated fact table
curated_claims = curate_claims_fact(stg_claims, stg_members, stg_providers)

# Profile curated data
print("\n### STAGE 6: Curated Data Profiling ###")
profile_dataframe(curated_members, "Curated Members Dimension")
profile_dataframe(curated_providers, "Curated Providers Dimension")
profile_dataframe(curated_claims, "Curated Claims Fact")

# Load to curated tables
print("\n### STAGE 7: Loading to Curated Tables ###")
print("Writing curated_dim_members...")
curated_members.write.mode("overwrite").saveAsTable("healthcare.curated_dim_members")

print("Writing curated_dim_providers...")
curated_providers.write.mode("overwrite").saveAsTable("healthcare.curated_dim_providers")

print("Writing curated_claims_fact (partitioned by service_year, service_month)...")
curated_claims.write.mode("append").partitionBy("service_year", "service_month").saveAsTable("healthcare.curated_claims_fact")

print("\n" + "="*80)
print("✓ Curated ETL Completed Successfully!")
print("="*80)
print(f"Records Loaded:")
print(f"  - Members Dimension:  {curated_members.count():,}")
print(f"  - Providers Dimension: {curated_providers.count():,}")
print(f"  - Claims Fact:        {curated_claims.count():,}")
print("="*80 + "\n
# Validate referential integrity
print("\n### STAGE 3: Referential Integrity Checks ###")
validate_referential_integrity(
    stg_claims, stg_members,
    "patient_id", "member_id",
    "Claims -> Members"
)
validate_referential_integrity(
    stg_claims, stg_providers,
    "provider_id", "provider_id",
    "Claims -> Providers"
)

# Validate required fields are not null
print("\n### STAGE 4: Null Value Checks ###")
validate_no_nulls(stg_claims, ["claim_id", "patient_id", "provider_id", "amount", "service_date"], "Claims")
validate_no_nulls(stg_members, ["member_id", "first_name", "last_name", "date_of_birth"], "Members")
validate_no_nulls(stg_providers, ["provider_id", "provider_name", "specialty"], "Providers")

# Create curated dimensions
curated_members = curate_dim_members(stg_members)
curated_providers = curate_dim_providers(stg_providers)

# Create curated fact table
curated_claims = curate_claims_fact(stg_claims, stg_members, stg_providers)

# Load to curated tables
curated_members.write.mode("overwrite").saveAsTable("healthcare.curated_dim_members")
curated_providers.write.mode("overwrite").saveAsTable("healthcare.curated_dim_providers")
curated_claims.write.mode("append").partitionBy("service_year", "service_month").saveAsTable("healthcare.curated_claims_fact")

print(f"Curated ETL completed:")
print(f"  - Members: {curated_members.count()} records")
print(f"  - Providers: {curated_providers.count()} records")
print(f"  - Claims: {curated_claims.count()} records")
