"""
Base platform adapter providing common functionality.
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

from ..core.interfaces import PlatformAdapter
from ..core.models import SCD2Config

logger = logging.getLogger(__name__)


class BasePlatformAdapter(PlatformAdapter):
    """
    Base implementation of PlatformAdapter with common functionality.
    
    Platform-specific adapters can inherit from this class and override
    specific methods as needed.
    """
    
    def get_table_schema(self, spark: "SparkSession", table_name: str) -> Dict[str, str]:
        """Get the schema of a table."""
        if not PYSPARK_AVAILABLE:
            raise ImportError("PySpark is required for table operations.")
        try:
            df = spark.table(table_name)
            return {field.name: field.dataType.simpleString() for field in df.schema.fields}
        except Exception as e:
            logger.error(f"Failed to get schema for table {table_name}: {e}")
            return {}
    
    def create_table(self, spark: "SparkSession", df: "DataFrame", table_name: str, mode: str = "overwrite") -> None:
        """Create a table using Delta format."""
        if not PYSPARK_AVAILABLE:
            raise ImportError("PySpark is required for table operations.")
        df.write.format("delta").mode(mode).saveAsTable(table_name)
    
    def append_to_table(self, spark: "SparkSession", df: "DataFrame", table_name: str) -> None:
        """Append data to an existing table."""
        if not PYSPARK_AVAILABLE:
            raise ImportError("PySpark is required for table operations.")
        df.write.format("delta").mode("append").saveAsTable(table_name)
    
    def table_exists(self, spark: "SparkSession", table_name: str) -> bool:
        """Check if a table exists."""
        if not PYSPARK_AVAILABLE:
            raise ImportError("PySpark is required for table operations.")
        try:
            spark.table(table_name)
            return True
        except Exception:
            return False
    
    def merge_table(self, spark: "SparkSession", source_df: "DataFrame", target_table: str, 
                   merge_condition: str, update_clause: str) -> None:
        """Perform a merge operation using SQL."""
        if not PYSPARK_AVAILABLE:
            raise ImportError("PySpark is required for table operations.")
        # Create a temporary view for the source data
        temp_view = f"temp_source_{hash(merge_condition) % 10000}"
        source_df.createOrReplaceTempView(temp_view)
        
        merge_sql = f"""
        MERGE INTO {target_table} AS target
        USING {temp_view} AS source
        ON {merge_condition}
        WHEN MATCHED THEN UPDATE SET {update_clause}
        """
        
        spark.sql(merge_sql)
        
        # Clean up temp view
        spark.catalog.dropTempView(temp_view)