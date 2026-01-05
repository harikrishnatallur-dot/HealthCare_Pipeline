USE healthcare;

-- Curated Claims Fact Table (with dimensional foreign keys)
CREATE TABLE IF NOT EXISTS curated_claims_fact (
    claim_id STRING,
    member_id STRING,
    provider_id STRING,
    diagnosis_code STRING,
    procedure_code STRING,
    claim_amount DOUBLE,
    claim_status STRING,
    service_date DATE,
    member_age_at_service INT,
    member_plan_type STRING,
    provider_specialty STRING,
    provider_network_status STRING,
    days_to_process INT,
    load_timestamp TIMESTAMP
)
PARTITIONED BY (service_year INT, service_month INT)
STORED AS PARQUET
TBLPROPERTIES ('parquet.compression'='SNAPPY');

-- Curated Members Dimension
CREATE TABLE IF NOT EXISTS curated_dim_members (
    member_id STRING,
    full_name STRING,
    date_of_birth DATE,
    age INT,
    gender STRING,
    full_address STRING,
    city STRING,
    state STRING,
    zip_code STRING,
    contact_phone STRING,
    contact_email STRING,
    plan_type STRING,
    enrollment_date DATE,
    status STRING,
    is_active BOOLEAN,
    load_timestamp TIMESTAMP
)
STORED AS PARQUET
TBLPROPERTIES ('parquet.compression'='SNAPPY');

-- Curated Providers Dimension
CREATE TABLE IF NOT EXISTS curated_dim_providers (
    provider_id STRING,
    npi STRING,
    provider_name STRING,
    specialty STRING,
    provider_type STRING,
    full_address STRING,
    city STRING,
    state STRING,
    zip_code STRING,
    contact_phone STRING,
    contact_email STRING,
    network_status STRING,
    is_in_network BOOLEAN,
    credential_date DATE,
    years_credentialed INT,
    load_timestamp TIMESTAMP
)
STORED AS PARQUET
TBLPROPERTIES ('parquet.compression'='SNAPPY');

-- Curated Claims Summary (Aggregated)
CREATE TABLE IF NOT EXISTS curated_claims_summary (
    summary_date DATE,
    total_claims INT,
    total_amount DOUBLE,
    avg_claim_amount DOUBLE,
    approved_claims INT,
    pending_claims INT,
    unique_members INT,
    unique_providers INT,
    top_diagnosis_code STRING,
    top_procedure_code STRING,
    load_timestamp TIMESTAMP
)
PARTITIONED BY (summary_year INT, summary_month INT)
STORED AS PARQUET
TBLPROPERTIES ('parquet.compression'='SNAPPY');
