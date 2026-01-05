from pyspark.sql.functions import col, isnan, when, count as spark_count

def validate_not_empty(df):
    """Validate dataframe is not empty"""
    if df.count() == 0:
        raise Exception("Data quality check failed: empty dataframe")


def validate_referential_integrity(child_df, parent_df, child_key, parent_key, relationship_name):
    """
    Validate referential integrity between child and parent dataframes
    Returns count of orphaned records
    """
    orphans = child_df.join(
        parent_df.select(col(parent_key)),
        child_df[child_key] == parent_df[parent_key],
        "left_anti"
    )
    
    orphan_count = orphans.count()
    
    if orphan_count > 0:
        print(f"WARNING: {orphan_count} orphaned records found in {relationship_name}")
        print(f"Sample orphaned {child_key}s:")
        orphans.select(child_key).distinct().show(10, truncate=False)
        raise Exception(f"Referential integrity check failed: {orphan_count} orphaned records in {relationship_name}")
    
    print(f"✓ Referential integrity check passed: {relationship_name}")
    return orphan_count


def validate_no_nulls(df, columns, context=""):
    """
    Validate specified columns have no null values
    """
    null_checks = []
    for column in columns:
        null_count = df.filter(
            col(column).isNull() | isnan(col(column))
        ).count()
        
        if null_count > 0:
            null_checks.append(f"{column}: {null_count} nulls")
    
    if null_checks:
        error_msg = f"Null value check failed for {context}: " + ", ".join(null_checks)
        print(f"ERROR: {error_msg}")
        raise Exception(error_msg)
    
    print(f"✓ Null check passed for {context}: {columns}")


def profile_dataframe(df, name):
    """
    Profile dataframe and print statistics
    """
    record_count = df.count()
    column_count = len(df.columns)
    
    print(f"\n{'='*60}")
    print(f"Data Profile: {name}")
    print(f"{'='*60}")
    print(f"Total Records: {record_count:,}")
    print(f"Total Columns: {column_count}")
    
    # Check for duplicates on first column (assuming it's the key)
    if df.columns:
        key_col = df.columns[0]
        distinct_count = df.select(key_col).distinct().count()
        duplicate_count = record_count - distinct_count
        print(f"Distinct {key_col}: {distinct_count:,}")
        if duplicate_count > 0:
            print(f"⚠ Duplicates found: {duplicate_count}")
    
    # Null counts per column
    null_counts = df.select([
        spark_count(when(col(c).isNull() | isnan(col(c)), c)).alias(c) 
        for c in df.columns
    ]).collect()[0].asDict()
    
    null_columns = {k: v for k, v in null_counts.items() if v > 0}
    if null_columns:
        print(f"\nNull values found:")
        for col_name, null_count in null_columns.items():
            print(f"  - {col_name}: {null_count:,} ({null_count/record_count*100:.2f}%)")
    else:
        print(f"✓ No null values found")
    
    print(f"{'='*60}\n")
    
    return {
        'name': name,
        'record_count': record_count,
        'column_count': column_count,
        'null_columns': null_columns
    }
