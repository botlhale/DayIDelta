"""
Unity Catalog platform adapter for Azure Databricks.

Handles Unity Catalog specific operations including 3-level naming,
catalog and schema management, and Delta Lake operations.
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


class UnityCatalogAdapter(BasePlatformAdapter):
    """
    Platform adapter for Azure Databricks Unity Catalog.
    
    Handles Unity Catalog specific features like:
    - 3-level naming (catalog.schema.table)
    - Catalog and schema creation
    - Unity Catalog permissions and access control
    """
    
    def setup_environment(self, spark: "SparkSession", config: SCD2Config) -> None:
        """Setup Unity Catalog environment by creating catalog and schema if needed."""
        if config.catalog:
            self._create_catalog_if_not_exists(spark, config.catalog)
        
        if config.catalog and config.schema:
            self._create_schema_if_not_exists(spark, config.catalog, config.schema)
            
            # Set the current catalog and schema for the session
            spark.sql(f"USE CATALOG {config.catalog}")
            spark.sql(f"USE SCHEMA {config.schema}")
            logger.info(f"Using catalog: {config.catalog}, schema: {config.schema}")
    
    def get_full_table_name(self, config: SCD2Config, table_name: str) -> str:
        """Get the full 3-level qualified table name for Unity Catalog."""
        if config.catalog and config.schema:
            return f"{config.catalog}.{config.schema}.{table_name}"
        elif config.schema:
            return f"{config.schema}.{table_name}"
        else:
            return table_name
    
    def table_exists(self, spark: "SparkSession", table_name: str) -> bool:
        """Check if a table exists in Unity Catalog."""
        try:
            spark.table(table_name)
            return True
        except Exception:
            return False
    
    def create_table(self, spark: "SparkSession", df: "DataFrame", table_name: str, mode: str = "overwrite") -> None:
        """Create a table in Unity Catalog using Delta format."""
        logger.info(f"Creating Unity Catalog table: {table_name}")
        df.write.format("delta").mode(mode).saveAsTable(table_name)
    
    def merge_table(self, spark: "SparkSession", source_df: "DataFrame", target_table: str, 
                   merge_condition: str, update_clause: str) -> None:
        """
        Perform a merge operation using Unity Catalog MERGE syntax.
        
        Unity Catalog supports advanced MERGE operations with Delta Lake.
        """
        # Create a temporary view for the source data
        temp_view = f"temp_merge_source_{abs(hash(target_table)) % 100000}"
        source_df.createOrReplaceTempView(temp_view)
        
        try:
            merge_sql = f"""
            MERGE INTO {target_table} AS target
            USING {temp_view} AS source
            ON {merge_condition}
            WHEN MATCHED THEN UPDATE SET {update_clause}
            """
            
            logger.debug(f"Executing merge SQL: {merge_sql}")
            spark.sql(merge_sql)
            
        finally:
            # Clean up temp view
            try:
                spark.catalog.dropTempView(temp_view)
            except Exception:
                pass  # Ignore cleanup errors
    
    def get_table_schema(self, spark: "SparkSession", table_name: str) -> Dict[str, str]:
        """Get the schema of a Unity Catalog table."""
        try:
            # Use DESCRIBE to get detailed table information in Unity Catalog
            describe_df = spark.sql(f"DESCRIBE TABLE {table_name}")
            schema_dict = {}
            
            for row in describe_df.collect():
                if row['col_name'] and not row['col_name'].startswith('#'):
                    schema_dict[row['col_name']] = row['data_type']
            
            return schema_dict
            
        except Exception as e:
            logger.error(f"Failed to get schema for Unity Catalog table {table_name}: {e}")
            # Fallback to parent method
            return super().get_table_schema(spark, table_name)
    
    def _create_catalog_if_not_exists(self, spark: "SparkSession", catalog_name: str) -> None:
        """Create Unity Catalog if it doesn't exist."""
        try:
            spark.sql(f"CREATE CATALOG IF NOT EXISTS {catalog_name}")
            logger.info(f"Unity Catalog {catalog_name} ensured")
        except Exception as e:
            logger.warning(f"Could not create Unity Catalog {catalog_name}: {e}")
    
    def _create_schema_if_not_exists(self, spark: "SparkSession", catalog_name: str, schema_name: str) -> None:
        """Create schema in Unity Catalog if it doesn't exist."""
        try:
            spark.sql(f"CREATE SCHEMA IF NOT EXISTS {catalog_name}.{schema_name}")
            logger.info(f"Unity Catalog schema {catalog_name}.{schema_name} ensured")
        except Exception as e:
            logger.warning(f"Could not create Unity Catalog schema {catalog_name}.{schema_name}: {e}")


# Convenience function for backward compatibility
def setup_unity_catalog_environment(spark: "SparkSession", catalog_name: str, schema_name: str) -> None:
    """
    Set up Unity Catalog environment by creating catalog and schema if needed.
    
    This is a convenience function that maintains backward compatibility
    with the original setup_unity_catalog_environment function.
    """
    config = SCD2Config(catalog=catalog_name, schema=schema_name)
    adapter = UnityCatalogAdapter()
    adapter.setup_environment(spark, config)