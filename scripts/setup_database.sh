#!/bin/bash

# Setup Healthcare Data Warehouse
# This script creates the database and all staging/curated tables

echo "Setting up Healthcare Data Warehouse..."

# Set Spark home (adjust if needed)
SPARK_HOME=${SPARK_HOME:-/opt/spark}

# Create staging tables
echo "Creating staging tables..."
$SPARK_HOME/bin/spark-sql -f ../src/ddl/create_staging_tables.sql

# Create curated tables
echo "Creating curated tables..."
$SPARK_HOME/bin/spark-sql -f ../src/ddl/create_curated_tables.sql

echo "Database setup complete!"
echo "Tables created in 'healthcare' database:"
echo "  Staging: stg_claims, stg_members, stg_providers"
echo "  Curated: curated_claims_fact, curated_dim_members, curated_dim_providers, curated_claims_summary"
