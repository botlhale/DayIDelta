"""
Core SCD2 Engine - Platform-agnostic SCD2 implementation.

This module contains the core SCD2 logic separated from platform-specific details.
The engine works with platform adapters to handle different Spark environments.
"""

import time
import logging
from typing import List, Optional
from datetime import datetime
try:
    from pyspark.sql import SparkSession, DataFrame
    from pyspark.sql.functions import col, lit, max as spark_max, when, coalesce, concat_ws
    from pyspark.sql.types import IntegerType
    PYSPARK_AVAILABLE = True
except ImportError:
    # Mock types for environments without PySpark
    PYSPARK_AVAILABLE = False
    SparkSession = None
    DataFrame = None
    col = None
    lit = None
    spark_max = None
    when = None
    coalesce = None
    concat_ws = None
    IntegerType = None

from .interfaces import PlatformAdapter
from .models import TableSchema, SCD2Config, SCD2ProcessingResult

# Set up logging
logger = logging.getLogger(__name__)


class SCD2Engine:
    """
    Core SCD2 engine that handles slowly changing dimension logic.
    
    This engine is platform-agnostic and works with platform adapters
    to handle the specifics of different Spark environments.
    """
    
    def __init__(self, platform_adapter: PlatformAdapter):
        """
        Initialize the SCD2 engine with a platform adapter.
        
        Args:
            platform_adapter: Platform-specific adapter for table operations
        """
        if not PYSPARK_AVAILABLE:
            raise ImportError("PySpark is required for SCD2Engine. Please install pyspark.")
        self.platform_adapter = platform_adapter
    
    def process(self, spark: "SparkSession", new_data_df: "DataFrame", 
                table_schema: TableSchema, scd2_config: SCD2Config) -> SCD2ProcessingResult:
        """
        Process a new batch of data using SCD2 logic.
        
        Args:
            spark: Active SparkSession
            new_data_df: DataFrame containing new batch of data
            table_schema: Schema information for the SCD2 table
            scd2_config: SCD2 configuration
            
        Returns:
            SCD2ProcessingResult with processing metrics and status
        """
        start_time = time.time()
        result = SCD2ProcessingResult(
            records_processed=new_data_df.count(),
            records_closed=0,
            records_inserted=0,
            current_day_id=0,
            processing_time_seconds=0.0
        )
        
        try:
            logger.info(f"Processing SCD2 update for table: {table_schema.full_table_name}")
            logger.info(f"Key columns: {table_schema.key_columns}")
            logger.info(f"Tracked columns: {table_schema.tracked_columns}")
            
            # Setup platform environment
            self.platform_adapter.setup_environment(spark, scd2_config)
            
            # Get current day_id and ensure dim_day table exists
            current_day_id = self._ensure_dim_day_and_get_current_id(spark, scd2_config)
            result.current_day_id = current_day_id
            
            # Get full table names
            obs_table_name = self.platform_adapter.get_full_table_name(scd2_config, table_schema.table)
            
            # Check if observation table exists
            obs_table_exists = self.platform_adapter.table_exists(spark, obs_table_name)
            
            if not obs_table_exists:
                # Initial load
                result.records_inserted = self._perform_initial_load(
                    spark, new_data_df, current_day_id, obs_table_name
                )
                logger.info(f"Initial load complete: {result.records_inserted} records")
            else:
                # Incremental SCD2 processing
                closed_count, inserted_count = self._perform_scd2_update(
                    spark, new_data_df, table_schema, current_day_id, obs_table_name
                )
                result.records_closed = closed_count
                result.records_inserted = inserted_count
                logger.info(f"SCD2 update complete: {closed_count} closed, {inserted_count} inserted")
            
            result.processing_time_seconds = time.time() - start_time
            logger.info(f"SCD2 processing completed in {result.processing_time_seconds:.2f} seconds")
            
        except Exception as e:
            error_msg = f"SCD2 processing failed: {str(e)}"
            logger.error(error_msg)
            result.errors.append(error_msg)
            result.processing_time_seconds = time.time() - start_time
        
        return result
    
    def _ensure_dim_day_and_get_current_id(self, spark: "SparkSession", scd2_config: SCD2Config) -> int:
        """
        Ensure the dim_day table exists and return the current day_id.
        Creates the table with day_id=0 if it doesn't exist.
        """
        dim_day_table = scd2_config.get_full_dim_day_table()
        
        if not self.platform_adapter.table_exists(spark, dim_day_table):
            logger.info("Creating dim_day table")
            # Create dim_day table with initial record (day_id=0, far future date)
            dim_day_data = [(0, datetime(9999, 12, 31, 0, 0, 0))]
            dim_day_df = spark.createDataFrame(dim_day_data, ["day_id", "date"])
            self.platform_adapter.create_table(spark, dim_day_df, dim_day_table, mode="overwrite")
            current_day_id = 1
        else:
            # Get the next day_id
            max_day_id = spark.table(dim_day_table).agg(spark_max("day_id")).collect()[0][0]
            current_day_id = max_day_id + 1
        
        # Insert the new day_id record
        new_day_record = [(current_day_id, datetime.now())]
        new_day_df = spark.createDataFrame(new_day_record, ["day_id", "date"])
        self.platform_adapter.append_to_table(spark, new_day_df, dim_day_table)
        
        return current_day_id
    
    def _perform_initial_load(self, spark: "SparkSession", new_data_df: "DataFrame", 
                            current_day_id: int, obs_table_name: str) -> int:
        """Perform initial load of SCD2 table."""
        new_data_with_scd2 = new_data_df.withColumn("start_day_id", lit(current_day_id).cast(IntegerType())) \
                                       .withColumn("end_day_id", lit(0).cast(IntegerType()))
        
        record_count = new_data_with_scd2.count()
        self.platform_adapter.create_table(spark, new_data_with_scd2, obs_table_name, mode="overwrite")
        
        return record_count
    
    def _perform_scd2_update(self, spark: "SparkSession", new_data_df: "DataFrame", 
                           table_schema: TableSchema, current_day_id: int, obs_table_name: str) -> tuple[int, int]:
        """Perform incremental SCD2 update."""
        existing_df = spark.table(obs_table_name)
        
        # Get data sources in the current batch for multi-source support
        data_source_cols = [col for col in table_schema.key_columns if 'source' in col.lower() or 'src' in col.lower()]
        batch_sources = []
        if data_source_cols:
            batch_sources = [row[0] for row in new_data_df.select(data_source_cols[0]).distinct().collect()]
            logger.info(f"Processing sources: {batch_sources}")
        
        # Step 1: Close records that have changed or are missing from the batch
        closed_count = self._close_changed_records(
            spark, existing_df, new_data_df, table_schema, current_day_id, obs_table_name, batch_sources
        )
        
        # Step 2: Insert new/changed records
        inserted_count = self._insert_new_records(spark, new_data_df, current_day_id, obs_table_name)
        
        return closed_count, inserted_count
    
    def _close_changed_records(self, spark: "SparkSession", existing_df: "DataFrame", new_data_df: "DataFrame",
                             table_schema: TableSchema, current_day_id: int, obs_table_name: str, 
                             batch_sources: List[str]) -> int:
        """
        Close records that have changed values or are missing from the current batch.
        Only processes records for data sources present in the current batch.
        """
        # Filter existing active records
        active_existing = existing_df.filter(col("end_day_id") == 0)
        
        # Filter by data sources if applicable
        if batch_sources:
            data_source_col = [col for col in table_schema.key_columns if 'source' in col.lower() or 'src' in col.lower()][0]
            active_existing = active_existing.filter(col(data_source_col).isin(batch_sources))
        
        # Create comparison key for existing records
        existing_with_key = active_existing.withColumn(
            "_comparison_key", 
            self._create_comparison_key(active_existing, table_schema.key_columns + table_schema.tracked_columns)
        )
        
        # Create comparison key for new records  
        new_with_key = new_data_df.withColumn(
            "_comparison_key",
            self._create_comparison_key(new_data_df, table_schema.key_columns + table_schema.tracked_columns)
        )
        
        # Find records that exist in current table but not in new batch (logical deletes)
        # or have different tracked column values
        records_to_close_df = existing_with_key.join(
            new_with_key.select("_comparison_key"), 
            on="_comparison_key", 
            how="left_anti"
        )
        
        closed_count = records_to_close_df.count()
        
        if closed_count > 0:
            logger.info(f"Closing {closed_count} changed/deleted records")
            
            # Update end_day_id for records to close using merge operation
            merge_condition = " AND ".join([
                f"target.{col} = source.{col}" for col in table_schema.key_columns
            ]) + " AND target.end_day_id = 0"
            
            update_clause = f"end_day_id = {current_day_id}"
            
            self.platform_adapter.merge_table(
                spark, records_to_close_df, obs_table_name, merge_condition, update_clause
            )
        
        return closed_count
    
    def _insert_new_records(self, spark: "SparkSession", new_data_df: "DataFrame", 
                          current_day_id: int, obs_table_name: str) -> int:
        """Insert new records with current day_id as start_day_id."""
        new_records = new_data_df.withColumn("start_day_id", lit(current_day_id).cast(IntegerType())) \
                                .withColumn("end_day_id", lit(0).cast(IntegerType()))
        
        record_count = new_records.count()
        logger.info(f"Inserting {record_count} new records")
        
        self.platform_adapter.append_to_table(spark, new_records, obs_table_name)
        
        return record_count
    
    def _create_comparison_key(self, df: "DataFrame", cols: List[str]) -> "col":
        """
        Create a comparison key by concatenating specified columns.
        Handles null values appropriately.
        """
        # Convert all columns to string and handle nulls
        col_expressions = []
        for col_name in cols:
            col_expr = coalesce(col(col_name).cast("string"), lit("__NULL__"))
            col_expressions.append(col_expr)
        
        # Concatenate with a delimiter
        return concat_ws("|", *col_expressions)


# Backward compatibility function
def DayIDelta(new_data_df, key_cols, tracked_cols, dest_catalog=None, dest_sch=None, 
              dest_tb_obs=None, dim_day_table=None, **kwargs):
    """
    Backward compatibility function that maintains the original DayIDelta API.
    
    This function wraps the new SCD2Engine for existing users.
    """
    from ..platforms.unity_catalog import UnityCatalogAdapter
    
    # Handle both old parameter names and new ones
    catalog = dest_catalog or kwargs.get('catalog')
    schema = dest_sch or kwargs.get('schema') 
    table = dest_tb_obs or kwargs.get('table')
    
    if not all([catalog, schema, table]):
        raise ValueError("catalog, schema, and table must be provided")
    
    # Create configuration
    scd2_config = SCD2Config(
        catalog=catalog,
        schema=schema,
        dim_day_table=dim_day_table or "dim_day"
    )
    
    # Create table schema
    table_schema = TableSchema(
        catalog=catalog,
        schema=schema,
        table=table,
        key_columns=key_cols,
        tracked_columns=tracked_cols
    )
    
    # Create engine with Unity Catalog adapter (default)
    platform_adapter = UnityCatalogAdapter()
    engine = SCD2Engine(platform_adapter)
    
    # Get SparkSession
    from pyspark.sql import SparkSession
    spark = SparkSession.builder.getOrCreate()
    
    # Process the data
    result = engine.process(spark, new_data_df, table_schema, scd2_config)
    
    if not result.success:
        raise RuntimeError(f"SCD2 processing failed: {'; '.join(result.errors)}")
    
    logger.info(f"SCD2 processing completed successfully: {result.records_inserted} records processed")