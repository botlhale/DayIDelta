"""
DayIDelta: SCD2 for Time Series Data in Delta Lake (Azure Databricks Unity Catalog)

A robust Python function for maintaining slowly-changing dimension (SCD2) history 
for time series observation tables in Delta Lake on Azure Databricks Unity Catalog.

Tracks changes to time series records with proper batch-based SCD2 semantics, 
using surrogate start_day_id and end_day_id for each record.
"""

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, lit, max as spark_max, when, coalesce
from pyspark.sql.types import IntegerType, TimestampType
from datetime import datetime
import logging

# Set up logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


def DayIDelta(new_data_df, key_cols, tracked_cols, dest_catalog, dest_sch, dest_tb_obs, dim_day_table=None):
    """
    Maintain SCD2 history for time series data in Azure Databricks Unity Catalog.
    
    Parameters:
    -----------
    new_data_df : pyspark.sql.DataFrame
        DataFrame containing the new batch of data to process
    key_cols : list
        List of column names that form the unique key for records
    tracked_cols : list  
        List of column names whose changes should trigger SCD2 updates
    dest_catalog : str
        Unity Catalog catalog name
    dest_sch : str
        Schema name within the catalog
    dest_tb_obs : str
        Table name for the SCD2 observation table
    dim_day_table : str, optional
        Table name for the day dimension table. Defaults to 'dim_day'
    
    Returns:
    --------
    None
        Function performs table operations and doesn't return data
    """
    
    spark = SparkSession.builder.getOrCreate()
    
    # Set default dim_day table name
    if dim_day_table is None:
        dim_day_table = "dim_day"
    
    # Full table names with Unity Catalog 3-level naming
    full_obs_table = f"{dest_catalog}.{dest_sch}.{dest_tb_obs}"
    full_dim_day_table = f"{dest_catalog}.{dest_sch}.{dim_day_table}"
    
    logger.info(f"Processing SCD2 update for table: {full_obs_table}")
    logger.info(f"Key columns: {key_cols}")
    logger.info(f"Tracked columns: {tracked_cols}")
    
    # Ensure the dim_day table exists and get/create new day_id
    current_day_id = _ensure_dim_day_and_get_current_id(spark, full_dim_day_table)
    logger.info(f"Current day_id: {current_day_id}")
    
    # Check if observation table exists
    obs_table_exists = _table_exists(spark, full_obs_table)
    
    if not obs_table_exists:
        # Initial load - create table with SCD2 columns
        logger.info("Creating new SCD2 observation table")
        new_data_with_scd2 = new_data_df.withColumn("start_day_id", lit(current_day_id).cast(IntegerType())) \
                                       .withColumn("end_day_id", lit(0).cast(IntegerType()))
        
        new_data_with_scd2.write.format("delta").mode("overwrite").saveAsTable(full_obs_table)
        logger.info(f"Initial load complete: {new_data_with_scd2.count()} records")
        return
    
    # Table exists - perform SCD2 processing
    existing_df = spark.table(full_obs_table)
    
    # Get data sources in the current batch for multi-source support
    data_source_cols = [col for col in key_cols if 'source' in col.lower() or 'src' in col.lower()]
    batch_sources = []
    if data_source_cols:
        batch_sources = [row[0] for row in new_data_df.select(data_source_cols[0]).distinct().collect()]
        logger.info(f"Processing sources: {batch_sources}")
    
    # Step 1: Close records that have changed or are missing from the batch
    _close_changed_records(spark, existing_df, new_data_df, key_cols, tracked_cols, 
                          current_day_id, full_obs_table, batch_sources)
    
    # Step 2: Insert new/changed records
    _insert_new_records(spark, new_data_df, current_day_id, full_obs_table)
    
    logger.info("SCD2 processing complete")


def _ensure_dim_day_and_get_current_id(spark, full_dim_day_table):
    """
    Ensure the dim_day table exists and return the current day_id.
    Creates the table with day_id=0 if it doesn't exist.
    """
    if not _table_exists(spark, full_dim_day_table):
        logger.info("Creating dim_day table")
        # Create dim_day table with initial record (day_id=0, far future date)
        dim_day_data = [(0, datetime(9999, 12, 31, 0, 0, 0))]
        dim_day_df = spark.createDataFrame(dim_day_data, ["day_id", "date"])
        dim_day_df.write.format("delta").mode("overwrite").saveAsTable(full_dim_day_table)
        current_day_id = 1
    else:
        # Get the next day_id
        max_day_id = spark.table(full_dim_day_table).agg(spark_max("day_id")).collect()[0][0]
        current_day_id = max_day_id + 1
    
    # Insert the new day_id record
    new_day_record = [(current_day_id, datetime.now())]
    new_day_df = spark.createDataFrame(new_day_record, ["day_id", "date"])
    new_day_df.write.format("delta").mode("append").saveAsTable(full_dim_day_table)
    
    return current_day_id


def _table_exists(spark, table_name):
    """Check if a table exists in Unity Catalog."""
    try:
        spark.table(table_name)
        return True
    except Exception:
        return False


def _close_changed_records(spark, existing_df, new_data_df, key_cols, tracked_cols, 
                          current_day_id, full_obs_table, batch_sources):
    """
    Close records that have changed values or are missing from the current batch.
    Only processes records for data sources present in the current batch.
    """
    # Filter existing active records
    active_existing = existing_df.filter(col("end_day_id") == 0)
    
    # Filter by data sources if applicable
    if batch_sources:
        data_source_col = [col for col in key_cols if 'source' in col.lower() or 'src' in col.lower()][0]
        active_existing = active_existing.filter(col(data_source_col).isin(batch_sources))
    
    # Create comparison key for existing records
    existing_with_key = active_existing.withColumn(
        "_comparison_key", 
        _create_comparison_key(active_existing, key_cols + tracked_cols)
    )
    
    # Create comparison key for new records  
    new_with_key = new_data_df.withColumn(
        "_comparison_key",
        _create_comparison_key(new_data_df, key_cols + tracked_cols)
    )
    
    # Find records that exist in current table but not in new batch (logical deletes)
    # or have different tracked column values
    new_keys = new_with_key.select(*key_cols).distinct()
    existing_keys = existing_with_key.select(*key_cols).distinct()
    
    # Records to close: exist in current but not in new batch, or have changed values
    records_to_close_df = existing_with_key.join(
        new_with_key.select("_comparison_key"), 
        on="_comparison_key", 
        how="left_anti"
    )
    
    if records_to_close_df.count() > 0:
        logger.info(f"Closing {records_to_close_df.count()} changed/deleted records")
        
        # Update end_day_id for records to close
        # Use Delta Lake MERGE operation
        merge_condition = " AND ".join([
            f"target.{col} = source.{col}" for col in key_cols
        ]) + " AND target.end_day_id = 0"
        
        records_to_close_df.createOrReplaceTempView("records_to_close")
        
        spark.sql(f"""
            MERGE INTO {full_obs_table} AS target
            USING records_to_close AS source
            ON {merge_condition}
            WHEN MATCHED THEN UPDATE SET end_day_id = {current_day_id}
        """)


def _insert_new_records(spark, new_data_df, current_day_id, full_obs_table):
    """Insert new records with current day_id as start_day_id."""
    new_records = new_data_df.withColumn("start_day_id", lit(current_day_id).cast(IntegerType())) \
                            .withColumn("end_day_id", lit(0).cast(IntegerType()))
    
    logger.info(f"Inserting {new_records.count()} new records")
    new_records.write.format("delta").mode("append").saveAsTable(full_obs_table)


def _create_comparison_key(df, cols):
    """
    Create a comparison key by concatenating specified columns.
    Handles null values appropriately.
    """
    from pyspark.sql.functions import concat_ws, coalesce, lit
    
    # Convert all columns to string and handle nulls
    col_expressions = []
    for col_name in cols:
        col_expr = coalesce(col(col_name).cast("string"), lit("__NULL__"))
        col_expressions.append(col_expr)
    
    # Concatenate with a delimiter
    return concat_ws("|", *col_expressions)


# Additional utility functions for Unity Catalog specific operations

def create_unity_catalog_if_not_exists(spark, catalog_name):
    """Create Unity Catalog if it doesn't exist."""
    try:
        spark.sql(f"CREATE CATALOG IF NOT EXISTS {catalog_name}")
        logger.info(f"Catalog {catalog_name} ensured")
    except Exception as e:
        logger.warning(f"Could not create catalog {catalog_name}: {e}")


def create_schema_if_not_exists(spark, catalog_name, schema_name):
    """Create schema in Unity Catalog if it doesn't exist."""
    try:
        spark.sql(f"CREATE SCHEMA IF NOT EXISTS {catalog_name}.{schema_name}")
        logger.info(f"Schema {catalog_name}.{schema_name} ensured")
    except Exception as e:
        logger.warning(f"Could not create schema {catalog_name}.{schema_name}: {e}")


def setup_unity_catalog_environment(spark, catalog_name, schema_name):
    """
    Set up Unity Catalog environment by creating catalog and schema if needed.
    """
    create_unity_catalog_if_not_exists(spark, catalog_name)
    create_schema_if_not_exists(spark, catalog_name, schema_name)
    
    # Set the current catalog and schema for the session
    spark.sql(f"USE CATALOG {catalog_name}")
    spark.sql(f"USE SCHEMA {schema_name}")
    logger.info(f"Using catalog: {catalog_name}, schema: {schema_name}")