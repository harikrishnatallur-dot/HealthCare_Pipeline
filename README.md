# Healthcare Spark ETL Pipeline

A production-grade PySpark ETL pipeline for processing healthcare claims, member, and provider data with comprehensive data quality checks and dimensional modeling.

---

## 📋 Table of Contents

- [Overview](#overview)
- [Architecture](#architecture)
- [Technology Stack](#technology-stack)
- [Project Structure](#project-structure)
- [Data Model](#data-model)
- [Setup & Installation](#setup--installation)
- [Execution Flow](#execution-flow)
- [Data Quality Framework](#data-quality-framework)
- [Configuration](#configuration)
- [Running the Pipeline](#running-the-pipeline)
- [Monitoring & Troubleshooting](#monitoring--troubleshooting)

---

## Overview

This ETL pipeline processes healthcare data through a three-layered architecture:

1. **Raw Layer**: Source CSV files (claims, members, providers)
2. **Staging Layer**: Cleaned, typed, and deduplicated data in Hive tables
3. **Curated Layer**: Enriched dimensional model with business metrics

**Key Features:**
- ✅ Multi-layered data architecture (Raw → Staging → Curated)
- ✅ Referential integrity validation
- ✅ Comprehensive data quality checks
- ✅ Partitioned storage for query optimization
- ✅ Data profiling and statistics
- ✅ Dimensional modeling (Facts & Dimensions)
- ✅ Production-ready error handling

---

## Architecture

### Data Flow Diagram

```
┌─────────────────────────────────────────────────────────────────┐
│                         RAW LAYER                                │
│  data/raw/claims/*.csv  |  members/*.csv  |  providers/*.csv    │
└────────────────────────────┬────────────────────────────────────┘
                             │ Extract (CSV)
                             ↓
┌─────────────────────────────────────────────────────────────────┐
│                      STAGING LAYER (Hive)                        │
│  ┌──────────────┐   ┌──────────────┐   ┌──────────────┐        │
│  │ stg_claims   │   │ stg_members  │   │stg_providers │        │
│  │ - Type cast  │   │ - Type cast  │   │ - Type cast  │        │
│  │ - Dedupe     │   │ - Dedupe     │   │ - Dedupe     │        │
│  │ - Load date  │   │ - Load date  │   │ - Load date  │        │
│  └──────────────┘   └──────────────┘   └──────────────┘        │
└────────────────────────────┬────────────────────────────────────┘
                             │ Transform (Join, Enrich, Derive)
                             ↓
┌─────────────────────────────────────────────────────────────────┐
│                     CURATED LAYER (Hive)                         │
│  ┌──────────────────┐  ┌─────────────────┐  ┌────────────────┐ │
│  │ curated_claims   │  │curated_dim_     │  │curated_dim_    │ │
│  │     _fact        │  │   members       │  │  providers     │ │
│  │ - Age calc       │  │ - Full name     │  │ - Full address │ │
│  │ - Joins dims     │  │ - Age           │  │ - Years cred   │ │
│  │ - Business KPIs  │  │ - is_active     │  │ - is_in_network│ │
│  └──────────────────┘  └─────────────────┘  └────────────────┘ │
└─────────────────────────────────────────────────────────────────┘
```

### Layered Architecture

| Layer | Purpose | Storage | Partitioning |
|-------|---------|---------|--------------|
| **Raw** | Source data ingestion | CSV files | N/A |
| **Staging** | Data cleansing & typing | Hive/Parquet | By date fields |
| **Curated** | Business-ready analytics | Hive/Parquet | Year/Month |

---

## Technology Stack

- **Apache Spark 3.x**: Distributed data processing
- **PySpark**: Python API for Spark
- **Apache Hive**: Metastore and data warehouse
- **Parquet**: Columnar storage format with Snappy compression
- **YARN**: Resource management and job scheduling
- **Python 3.x**: ETL scripting

---

## Project Structure

```
healthcare-spark-etl/
├── README.md                           # This file
├── config/
│   ├── dev.yaml                       # Development environment config
│   └── prod.yaml                      # Production environment config
├── data/
│   └── raw/
│       ├── claims/
│       │   └── claims_2026_01.csv    # Source claims data
│       ├── members/
│       │   └── members_2026_01.csv   # Source member data
│       └── providers/
│           └── providers_2026_01.csv # Source provider data
├── scripts/
│   ├── setup_database.sh             # Initialize Hive tables
│   └── run_pipeline.sh               # YARN job submission
├── src/
│   ├── ddl/
│   │   ├── create_staging_tables.sql # Staging table DDL
│   │   └── create_curated_tables.sql # Curated table DDL
│   ├── jobs/
│   │   ├── claims_etl.py            # Claims staging job
│   │   ├── members_etl.py           # Members staging job
│   │   ├── providers_etl.py         # Providers staging job
│   │   └── curated_etl.py           # Curated layer job
│   ├── transformations/
│   │   ├── staging_claims.py        # Claims staging logic
│   │   ├── staging_members.py       # Members staging logic
│   │   ├── staging_providers.py     # Providers staging logic
│   │   └── curated_claims.py        # Curated transformations
│   └── utils/
│       ├── spark_session.py         # Spark session factory
│       ├── data_quality.py          # Data quality checks
│       └── etl_metadata.py          # Metadata tracking
```

---

## Data Model

### Staging Tables

#### `healthcare.stg_claims`
| Column | Type | Description |
|--------|------|-------------|
| claim_id | STRING | Unique claim identifier |
| patient_id | STRING | Member/patient ID (FK) |
| provider_id | STRING | Provider ID (FK) |
| diagnosis_code | STRING | ICD-10 diagnosis code |
| procedure_code | STRING | CPT procedure code |
| amount | DOUBLE | Claim amount |
| claim_status | STRING | approved/pending/denied |
| load_date | DATE | ETL load date |
| service_date | DATE | **Partition Key** |

#### `healthcare.stg_members`
| Column | Type | Description |
|--------|------|-------------|
| member_id | STRING | Unique member identifier (PK) |
| first_name | STRING | Member first name |
| last_name | STRING | Member last name |
| date_of_birth | DATE | Date of birth |
| gender | STRING | M/F |
| plan_type | STRING | PPO/HMO/EPO |
| status | STRING | active/inactive |
| enrollment_date | DATE | **Partition Key** |
| load_date | DATE | ETL load date |

#### `healthcare.stg_providers`
| Column | Type | Description |
|--------|------|-------------|
| provider_id | STRING | Unique provider identifier (PK) |
| npi | STRING | National Provider Identifier |
| provider_name | STRING | Provider name |
| specialty | STRING | Medical specialty |
| network_status | STRING | in-network/out-of-network |
| credential_date | DATE | **Partition Key** |
| load_date | DATE | ETL load date |

### Curated Tables

#### `healthcare.curated_claims_fact`
Fact table with enriched claims data joined with dimensions.

**Derived Metrics:**
- `member_age_at_service`: Calculated age at service date
- `days_to_process`: Days from service to ETL
- `service_year`, `service_month`: Partition keys

**Partitioned by:** `service_year`, `service_month`

#### `healthcare.curated_dim_members`
Member dimension with enriched attributes.

**Derived Attributes:**
- `full_name`: Concatenated first + last name
- `age`: Current age
- `full_address`: Complete address string
- `is_active`: Boolean flag (status == 'active')

#### `healthcare.curated_dim_providers`
Provider dimension with enriched attributes.

**Derived Attributes:**
- `full_address`: Complete address string
- `is_in_network`: Boolean flag
- `years_credentialed`: Years since credential date

---

## Setup & Installation

### Prerequisites

1. **Apache Spark 3.x** installed and configured
2. **Hadoop/YARN** cluster (or local mode for development)
3. **Python 3.7+** with PySpark
4. **Hive metastore** configured with Spark

### Environment Setup

```bash
# Set Spark home
export SPARK_HOME=/opt/spark

# Set Python path
export PYTHONPATH=$SPARK_HOME/python:$PYTHONPATH

# Verify installation
$SPARK_HOME/bin/spark-submit --version
```

### Database Initialization

Run this **once** to create all Hive tables:

```bash
cd healthcare-spark-etl/scripts
chmod +x setup_database.sh
./setup_database.sh
```

This creates:
- `healthcare` database
- 3 staging tables (`stg_claims`, `stg_members`, `stg_providers`)
- 4 curated tables (`curated_claims_fact`, `curated_dim_members`, `curated_dim_providers`, `curated_claims_summary`)

---

## Execution Flow

### Manual Execution

#### Step 1: Run Staging Jobs (Parallel)

```bash
# Claims staging
spark-submit \
  --master yarn \
  --deploy-mode cluster \
  src/jobs/claims_etl.py

# Members staging
spark-submit \
  --master yarn \
  --deploy-mode cluster \
  src/jobs/members_etl.py

# Providers staging
spark-submit \
  --master yarn \
  --deploy-mode cluster \
  src/jobs/providers_etl.py
```

#### Step 2: Run Curated Job (After all staging jobs complete)

```bash
spark-submit \
  --master yarn \
  --deploy-mode cluster \
  src/jobs/curated_etl.py
```

### Control-M Job Flow

```
Job Dependency Chain:

setup_database (one-time)
       ↓
┌──────────────┬──────────────┬──────────────┐
│ claims_stg   │ members_stg  │ providers_stg│  (run in parallel)
└──────────────┴──────────────┴──────────────┘
                     ↓
               curated_etl
```

**Job Definitions:**

1. **Job: `HEALTHCARE_STAGING_CLAIMS`**
   - Command: `spark-submit src/jobs/claims_etl.py`
   - Depends: None

2. **Job: `HEALTHCARE_STAGING_MEMBERS`**
   - Command: `spark-submit src/jobs/members_etl.py`
   - Depends: None

3. **Job: `HEALTHCARE_STAGING_PROVIDERS`**
   - Command: `spark-submit src/jobs/providers_etl.py`
   - Depends: None

4. **Job: `HEALTHCARE_CURATED`**
   - Command: `spark-submit src/jobs/curated_etl.py`
   - Depends: All 3 staging jobs

---

## Data Quality Framework

### Built-in Validation Checks

The pipeline includes comprehensive data quality checks at multiple stages:

#### 1. **Empty DataFrame Check**
```python
validate_not_empty(df)
```
Ensures no empty datasets are processed.

#### 2. **Referential Integrity Check**
```python
validate_referential_integrity(
    child_df, parent_df, 
    child_key, parent_key, 
    relationship_name
)
```
Validates foreign key relationships:
- All `patient_id` in claims exist in members
- All `provider_id` in claims exist in providers

**Behavior:** Job fails if orphaned records found, displays sample orphaned IDs.

#### 3. **Null Value Check**
```python
validate_no_nulls(df, columns, context)
```
Validates critical fields are not null:
- Claims: `claim_id`, `patient_id`, `provider_id`, `amount`, `service_date`
- Members: `member_id`, `first_name`, `last_name`, `date_of_birth`
- Providers: `provider_id`, `provider_name`, `specialty`

#### 4. **Data Profiling**
```python
profile_dataframe(df, name)
```
Generates statistics:
- Record counts
- Distinct value counts
- Duplicate detection
- Null value percentages per column

### Quality Check Execution Flow

```
Curated ETL Stages:
├── Stage 1: Data Profiling (staging tables)
├── Stage 2: Empty Check
├── Stage 3: Referential Integrity ⚠️ CRITICAL
├── Stage 4: Null Value Checks ⚠️ CRITICAL
├── Stage 5: Curated Transformations
├── Stage 6: Curated Data Profiling
└── Stage 7: Load to Curated Tables
```

---

## Configuration

### Environment Configuration Files

#### `config/dev.yaml` (Development)
```yaml
environment: dev
spark:
  app_name: Healthcare_ETL_Dev
  master: local[*]
  warehouse_dir: /tmp/spark-warehouse
paths:
  raw_data: data/raw
  staging: /user/hive/warehouse/healthcare.db
```

#### `config/prod.yaml` (Production)
```yaml
environment: prod
spark:
  app_name: Healthcare_ETL_Prod
  master: yarn
  deploy_mode: cluster
  warehouse_dir: /user/hive/warehouse
paths:
  raw_data: hdfs:///healthcare/raw
  staging: /user/hive/warehouse/healthcare.db
```

### Spark Configuration

Default Spark session settings in `src/utils/spark_session.py`:
- Hive support enabled
- Dynamic allocation enabled
- Parquet compression: SNAPPY
- Warehouse directory: `/user/hive/warehouse`

---

## Running the Pipeline

### Development/Local Mode

```bash
# Set local mode
export SPARK_MASTER=local[*]

# Run staging
python src/jobs/claims_etl.py
python src/jobs/members_etl.py
python src/jobs/providers_etl.py

# Run curated
python src/jobs/curated_etl.py
```

### Production/Cluster Mode

```bash
# Use provided script
cd scripts
chmod +x run_pipeline.sh
./run_pipeline.sh
```

### Verify Results

```sql
-- Check staging tables
SELECT COUNT(*) FROM healthcare.stg_claims;
SELECT COUNT(*) FROM healthcare.stg_members;
SELECT COUNT(*) FROM healthcare.stg_providers;

-- Check curated tables
SELECT COUNT(*) FROM healthcare.curated_claims_fact;
SELECT COUNT(*) FROM healthcare.curated_dim_members;
SELECT COUNT(*) FROM healthcare.curated_dim_providers;

-- Sample curated data
SELECT 
    claim_id,
    member_age_at_service,
    claim_amount,
    provider_specialty,
    service_date
FROM healthcare.curated_claims_fact
LIMIT 10;
```

---

## Monitoring & Troubleshooting

### Common Issues

#### 1. **Referential Integrity Failure**
```
ERROR: Referential integrity check failed: 5 orphaned records in Claims -> Members
```
**Solution:** Check source data for invalid patient_ids or ensure members data is loaded first.

#### 2. **Null Value Failure**
```
ERROR: Null value check failed for Claims: amount: 3 nulls
```
**Solution:** Investigate source data quality, add imputation logic if needed.

#### 3. **Hive Table Not Found**
```
pyspark.sql.utils.AnalysisException: Table or view not found: healthcare.stg_claims
```
**Solution:** Run `setup_database.sh` to create tables.

#### 4. **Partition Already Exists**
```
AnalysisException: Partition already exists
```
**Solution:** Change write mode from `append` to `overwrite` or delete existing partitions.

### Monitoring Queries

```sql
-- Check data freshness
SELECT 
    MAX(load_date) as last_load,
    COUNT(*) as record_count
FROM healthcare.stg_claims;

-- Check partitions
SHOW PARTITIONS healthcare.curated_claims_fact;

-- Check for data gaps
SELECT 
    service_date,
    COUNT(*) as claim_count
FROM healthcare.stg_claims
GROUP BY service_date
ORDER BY service_date;
```

### Log Locations

- **YARN Application Logs**: `yarn logs -applicationId <app_id>`
- **Spark History Server**: `http://<spark-history-server>:18080`
- **Local Logs**: `$SPARK_HOME/logs/`

---

## Best Practices

1. **Always run `setup_database.sh` before first execution**
2. **Run staging jobs in parallel** for better performance
3. **Monitor data quality reports** after each run
4. **Use partitioning** to optimize query performance
5. **Set appropriate Spark memory** based on data volume
6. **Test with sample data** before production deployment
7. **Implement incremental loads** using `etl_metadata.py` for large datasets
8. **Schedule regular maintenance** (ANALYZE TABLE, vacuum, etc.)

---

## Future Enhancements

- [ ] Incremental load support using metadata tracking
- [ ] SCD Type 2 for dimension history
- [ ] Additional aggregation tables (claims_summary)
- [ ] Real-time streaming ingestion
- [ ] ML model integration for fraud detection
- [ ] API endpoints for data access
- [ ] Automated data lineage tracking
- [ ] Delta Lake integration for ACID transactions

---

## Contact & Support

For questions or issues:
- Create an issue in the repository
- Contact the data engineering team
- Review Spark documentation: https://spark.apache.org/docs/latest/

---

**Version:** 1.0  
**Last Updated:** January 4, 2026  
**Maintained by:** Hari Krisha Tallur - Healthcare Data Engineering Team
