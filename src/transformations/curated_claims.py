from pyspark.sql.functions import (
    col, year, month, current_timestamp, 
    concat_ws, datediff, floor
)

def curate_claims_fact(claims_df, members_df, providers_df):
    """
    Create curated claims fact table by joining with dimensions
    and calculating derived metrics
    """
    # Calculate member age at service
    claims_with_members = claims_df.alias("c").join(
        members_df.alias("m"),
        col("c.patient_id") == col("m.member_id"),
        "left"
    ).select(
        col("c.claim_id"),
        col("c.patient_id").alias("member_id"),
        col("c.provider_id"),
        col("c.diagnosis_code"),
        col("c.procedure_code"),
        col("c.amount").alias("claim_amount"),
        col("c.claim_status"),
        col("c.service_date"),
        col("m.plan_type").alias("member_plan_type"),
        col("m.date_of_birth"),
        floor(datediff(col("c.service_date"), col("m.date_of_birth")) / 365.25).alias("member_age_at_service")
    )
    
    # Join with providers
    curated = claims_with_members.alias("cm").join(
        providers_df.alias("p"),
        col("cm.provider_id") == col("p.provider_id"),
        "left"
    ).select(
        col("cm.claim_id"),
        col("cm.member_id"),
        col("cm.provider_id"),
        col("cm.diagnosis_code"),
        col("cm.procedure_code"),
        col("cm.claim_amount"),
        col("cm.claim_status"),
        col("cm.service_date"),
        col("cm.member_age_at_service"),
        col("cm.member_plan_type"),
        col("p.specialty").alias("provider_specialty"),
        col("p.network_status").alias("provider_network_status"),
        datediff(current_timestamp(), col("cm.service_date")).alias("days_to_process"),
        current_timestamp().alias("load_timestamp"),
        year(col("cm.service_date")).alias("service_year"),
        month(col("cm.service_date")).alias("service_month")
    )
    
    return curated


def curate_dim_members(members_df):
    """Create curated members dimension with derived attributes"""
    return members_df.select(
        col("member_id"),
        concat_ws(" ", col("first_name"), col("last_name")).alias("full_name"),
        col("date_of_birth"),
        floor(datediff(current_timestamp(), col("date_of_birth")) / 365.25).alias("age"),
        col("gender"),
        concat_ws(", ", col("address"), col("city"), col("state"), col("zip_code")).alias("full_address"),
        col("city"),
        col("state"),
        col("zip_code"),
        col("phone").alias("contact_phone"),
        col("email").alias("contact_email"),
        col("plan_type"),
        col("enrollment_date"),
        col("status"),
        (col("status") == "active").alias("is_active"),
        current_timestamp().alias("load_timestamp")
    )


def curate_dim_providers(providers_df):
    """Create curated providers dimension with derived attributes"""
    return providers_df.select(
        col("provider_id"),
        col("npi"),
        col("provider_name"),
        col("specialty"),
        col("provider_type"),
        concat_ws(", ", col("address"), col("city"), col("state"), col("zip_code")).alias("full_address"),
        col("city"),
        col("state"),
        col("zip_code"),
        col("phone").alias("contact_phone"),
        col("email").alias("contact_email"),
        col("network_status"),
        (col("network_status") == "in-network").alias("is_in_network"),
        col("credential_date"),
        floor(datediff(current_timestamp(), col("credential_date")) / 365.25).alias("years_credentialed"),
        current_timestamp().alias("load_timestamp")
    )
