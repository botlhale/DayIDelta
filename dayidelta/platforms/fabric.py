"""
Microsoft Fabric platform adapter.

Handles Microsoft Fabric specific operations including lakehouse integration,
environment management, and Fabric-specific table operations.
"""

import logging
from typing import Dict

try:
    from pyspark.sql import SparkSession, DataFrame
    PYSPARK_AVAILABLE = True
except ImportError:
    PYSPARK_AVAILABLE = False
    SparkSession = None
    DataFrame = None

from .base import BasePlatformAdapter
from ..core.models import SCD2Config

logger = logging.getLogger(__name__)


class FabricAdapter(BasePlatformAdapter):
    """
    Platform adapter for Microsoft Fabric.
    
    Handles Microsoft Fabric specific features like:
    - Lakehouse integration
    - Fabric environment resources
    - Schema-level table naming (2-level)
    """
    
    def setup_environment(self, spark: SparkSession, config: SCD2Config) -> None:
        """Setup Microsoft Fabric environment."""
        # In Microsoft Fabric, ensure the schema exists if specified
        if config.schema:
            self._create_schema_if_not_exists(spark, config.schema)
            logger.info(f"Using Fabric schema: {config.schema}")
        
        logger.info("Microsoft Fabric environment setup complete")
    
    def get_full_table_name(self, config: SCD2Config, table_name: str) -> str:
        """Get the full qualified table name for Microsoft Fabric (2-level naming)."""
        if config.schema:
            return f"{config.schema}.{table_name}"
        else:
            return table_name
    
    def create_table(self, spark: SparkSession, df: DataFrame, table_name: str, mode: str = "overwrite") -> None:
        """Create a table in Microsoft Fabric using Delta format."""
        logger.info(f"Creating Fabric table: {table_name}")
        df.write.format("delta").mode(mode).saveAsTable(table_name)
    
    def merge_table(self, spark: SparkSession, source_df: DataFrame, target_table: str, 
                   merge_condition: str, update_clause: str) -> None:
        """
        Perform a merge operation in Microsoft Fabric.
        
        Microsoft Fabric supports Delta Lake MERGE operations.
        """
        # Create a temporary view for the source data
        temp_view = f"fabric_merge_source_{abs(hash(target_table)) % 100000}"
        source_df.createOrReplaceTempView(temp_view)
        
        try:
            merge_sql = f"""
            MERGE INTO {target_table} AS target
            USING {temp_view} AS source
            ON {merge_condition}
            WHEN MATCHED THEN UPDATE SET {update_clause}
            """
            
            logger.debug(f"Executing Fabric merge SQL: {merge_sql}")
            spark.sql(merge_sql)
            
        finally:
            # Clean up temp view
            try:
                spark.catalog.dropTempView(temp_view)
            except Exception:
                pass  # Ignore cleanup errors
    
    def _create_schema_if_not_exists(self, spark: SparkSession, schema_name: str) -> None:
        """Create schema in Microsoft Fabric if it doesn't exist."""
        try:
            spark.sql(f"CREATE SCHEMA IF NOT EXISTS {schema_name}")
            logger.info(f"Fabric schema {schema_name} ensured")
        except Exception as e:
            logger.warning(f"Could not create Fabric schema {schema_name}: {e}")
    
    def get_table_schema(self, spark: SparkSession, table_name: str) -> Dict[str, str]:
        """Get the schema of a Microsoft Fabric table."""
        try:
            # Try to get table info using DESCRIBE
            describe_df = spark.sql(f"DESCRIBE TABLE {table_name}")
            schema_dict = {}
            
            for row in describe_df.collect():
                if row['col_name'] and not row['col_name'].startswith('#'):
                    schema_dict[row['col_name']] = row['data_type']
            
            return schema_dict
            
        except Exception as e:
            logger.error(f"Failed to get schema for Fabric table {table_name}: {e}")
            # Fallback to parent method
            return super().get_table_schema(spark, table_name)