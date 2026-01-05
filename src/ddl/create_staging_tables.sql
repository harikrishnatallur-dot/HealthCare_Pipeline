-- Create healthcare database
CREATE DATABASE IF NOT EXISTS healthcare
COMMENT 'Healthcare data warehouse'
LOCATION '/user/hive/warehouse/healthcare.db';

USE healthcare;

-- Staging Claims Table
CREATE TABLE IF NOT EXISTS stg_claims (
    claim_id STRING,
    patient_id STRING,
    provider_id STRING,
    diagnosis_code STRING,
    procedure_code STRING,
    amount DOUBLE,
    claim_status STRING,
    load_date DATE
)
PARTITIONED BY (service_date DATE)
STORED AS PARQUET
TBLPROPERTIES ('parquet.compression'='SNAPPY');

-- Staging Members Table
CREATE TABLE IF NOT EXISTS stg_members (
    member_id STRING,
    first_name STRING,
    last_name STRING,
    date_of_birth DATE,
    gender STRING,
    address STRING,
    city STRING,
    state STRING,
    zip_code STRING,
    phone STRING,
    email STRING,
    plan_type STRING,
    status STRING,
    load_date DATE
)
PARTITIONED BY (enrollment_date DATE)
STORED AS PARQUET
TBLPROPERTIES ('parquet.compression'='SNAPPY');

-- Staging Providers Table
CREATE TABLE IF NOT EXISTS stg_providers (
    provider_id STRING,
    npi STRING,
    provider_name STRING,
    specialty STRING,
    provider_type STRING,
    address STRING,
    city STRING,
    state STRING,
    zip_code STRING,
    phone STRING,
    email STRING,
    network_status STRING,
    load_date DATE
)
PARTITIONED BY (credential_date DATE)
STORED AS PARQUET
TBLPROPERTIES ('parquet.compression'='SNAPPY');
