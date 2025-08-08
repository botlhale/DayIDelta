"""
SQL and Python query patterns for SCD2 operations.

Contains reusable query templates and patterns for different types of SCD2 queries.
"""

from typing import List, Optional
from ..core.models import TableSchema


class SCD2QueryPatterns:
    """Contains SQL and Python code patterns for different SCD2 query types."""
    
    @staticmethod
    def current_data_sql(table_schema: TableSchema, filters: Optional[str] = None) -> str:
        """Generate SQL for current/active data."""
        where_clause = "WHERE end_day_id = 0"
        if filters:
            where_clause += f" AND {filters}"
            
        return f"""-- Query for current/active data
SELECT {', '.join(table_schema.all_data_columns)}
FROM {table_schema.full_table_name}
{where_clause}
ORDER BY {', '.join(table_schema.key_columns[:2])}"""

    @staticmethod
    def current_data_python(table_schema: TableSchema, filters: Optional[str] = None) -> str:
        """Generate Python code for current/active data."""
        filter_condition = "col('end_day_id') == 0"
        if filters:
            filter_condition += f" & ({filters})"
            
        return f"""# Query for current/active data using PySpark
from pyspark.sql import SparkSession
from pyspark.sql.functions import col

spark = SparkSession.builder.getOrCreate()

# Read current/active records
current_data = spark.table("{table_schema.full_table_name}") \\
    .filter({filter_condition}) \\
    .select({', '.join([f'"{col}"' for col in table_schema.all_data_columns])}) \\
    .orderBy({', '.join([f'"{col}"' for col in table_schema.key_columns[:2]])})

current_data.show()"""

    @staticmethod
    def point_in_time_sql(table_schema: TableSchema, day_id: int, filters: Optional[str] = None) -> str:
        """Generate SQL for point-in-time historical query."""
        where_clause = f"WHERE start_day_id <= {day_id} AND (end_day_id = 0 OR end_day_id > {day_id})"
        if filters:
            where_clause += f" AND {filters}"
            
        return f"""-- Query for data as it looked at day_id {day_id}
SELECT {', '.join(table_schema.all_data_columns)}
FROM {table_schema.full_table_name}
{where_clause}
ORDER BY {', '.join(table_schema.key_columns[:2])}"""

    @staticmethod
    def point_in_time_python(table_schema: TableSchema, day_id: int, filters: Optional[str] = None) -> str:
        """Generate Python code for point-in-time historical query."""
        filter_condition = f"(col('start_day_id') <= {day_id}) & ((col('end_day_id') == 0) | (col('end_day_id') > {day_id}))"
        if filters:
            filter_condition = f"({filter_condition}) & ({filters})"
            
        return f"""# Query for data as it looked at day_id {day_id}
from pyspark.sql import SparkSession
from pyspark.sql.functions import col

spark = SparkSession.builder.getOrCreate()

# Read point-in-time data
historical_data = spark.table("{table_schema.full_table_name}") \\
    .filter({filter_condition}) \\
    .select({', '.join([f'"{col}"' for col in table_schema.all_data_columns])}) \\
    .orderBy({', '.join([f'"{col}"' for col in table_schema.key_columns[:2]])})

historical_data.show()"""

    @staticmethod
    def time_series_history_sql(table_schema: TableSchema, start_day_id: int, end_day_id: int, 
                               time_series_filter: Optional[str] = None, other_filters: Optional[str] = None) -> str:
        """Generate SQL for time series history within a period."""
        where_conditions = [
            f"start_day_id <= {end_day_id}",
            f"(end_day_id = 0 OR end_day_id > {start_day_id})"
        ]
        
        if time_series_filter:
            where_conditions.append(time_series_filter)
        if other_filters:
            where_conditions.append(other_filters)
            
        where_clause = "WHERE " + " AND ".join(where_conditions)
        
        return f"""-- Query for time series history from day_id {start_day_id} to {end_day_id}
SELECT {', '.join(table_schema.all_data_columns)},
       start_day_id,
       end_day_id
FROM {table_schema.full_table_name}
{where_clause}
ORDER BY {', '.join(table_schema.key_columns[:2])}, start_day_id"""

    @staticmethod
    def time_series_history_python(table_schema: TableSchema, start_day_id: int, end_day_id: int,
                                  time_series_filter: Optional[str] = None, other_filters: Optional[str] = None) -> str:
        """Generate Python code for time series history within a period."""
        filter_conditions = [
            f"col('start_day_id') <= {end_day_id}",
            f"(col('end_day_id') == 0) | (col('end_day_id') > {start_day_id})"
        ]
        
        if time_series_filter:
            filter_conditions.append(time_series_filter)
        if other_filters:
            filter_conditions.append(other_filters)
            
        filter_condition = " & ".join([f"({cond})" for cond in filter_conditions])
        
        columns_to_select = table_schema.all_data_columns + ['start_day_id', 'end_day_id']
        order_columns = table_schema.key_columns[:2] + ['start_day_id']
        
        return f"""# Query for time series history from day_id {start_day_id} to {end_day_id}
from pyspark.sql import SparkSession
from pyspark.sql.functions import col

spark = SparkSession.builder.getOrCreate()

# Read time series history
history_data = spark.table("{table_schema.full_table_name}") \\
    .filter({filter_condition}) \\
    .select({', '.join([f'"{col}"' for col in columns_to_select])}) \\
    .orderBy({', '.join([f'"{col}"' for col in order_columns])})

history_data.show()"""

    @staticmethod
    def data_comparison_sql(table_schema: TableSchema, day_id_1: int, day_id_2: int, 
                           comparison_columns: Optional[List[str]] = None) -> str:
        """Generate SQL for comparing data between two time periods."""
        if not comparison_columns:
            comparison_columns = table_schema.tracked_columns
            
        key_join = " AND ".join([f"t1.{col} = t2.{col}" for col in table_schema.key_columns])
        
        comparison_select = []
        for col in comparison_columns:
            comparison_select.extend([
                f"t1.{col} as {col}_day_{day_id_1}",
                f"t2.{col} as {col}_day_{day_id_2}",
                f"CASE WHEN t1.{col} != t2.{col} THEN 'CHANGED' ELSE 'SAME' END as {col}_status"
            ])
            
        return f"""-- Compare data between day_id {day_id_1} and day_id {day_id_2}
WITH data_day_{day_id_1} AS (
    SELECT {', '.join(table_schema.all_data_columns)}
    FROM {table_schema.full_table_name}
    WHERE start_day_id <= {day_id_1} AND (end_day_id = 0 OR end_day_id > {day_id_1})
),
data_day_{day_id_2} AS (
    SELECT {', '.join(table_schema.all_data_columns)}
    FROM {table_schema.full_table_name}
    WHERE start_day_id <= {day_id_2} AND (end_day_id = 0 OR end_day_id > {day_id_2})
)
SELECT {', '.join(table_schema.key_columns)},
       {', '.join(comparison_select)}
FROM data_day_{day_id_1} t1
FULL OUTER JOIN data_day_{day_id_2} t2 ON {key_join}
ORDER BY {', '.join(table_schema.key_columns[:2])}"""

    @staticmethod
    def data_comparison_python(table_schema: TableSchema, day_id_1: int, day_id_2: int,
                              comparison_columns: Optional[List[str]] = None) -> str:
        """Generate Python code for comparing data between two time periods."""
        if not comparison_columns:
            comparison_columns = table_schema.tracked_columns
            
        return f"""# Compare data between day_id {day_id_1} and day_id {day_id_2}
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, when

spark = SparkSession.builder.getOrCreate()

# Read data for day_id {day_id_1}
data_day_{day_id_1} = spark.table("{table_schema.full_table_name}") \\
    .filter((col('start_day_id') <= {day_id_1}) & 
            ((col('end_day_id') == 0) | (col('end_day_id') > {day_id_1}))) \\
    .select({', '.join([f'"{col}"' for col in table_schema.all_data_columns])})

# Read data for day_id {day_id_2}
data_day_{day_id_2} = spark.table("{table_schema.full_table_name}") \\
    .filter((col('start_day_id') <= {day_id_2}) & 
            ((col('end_day_id') == 0) | (col('end_day_id') > {day_id_2}))) \\
    .select({', '.join([f'"{col}"' for col in table_schema.all_data_columns])})

# Join and compare
join_keys = {table_schema.key_columns}
comparison = data_day_{day_id_1}.alias('t1').join(
    data_day_{day_id_2}.alias('t2'), 
    join_keys, 
    'full_outer'
).select(
    {', '.join([f'"t1.{col}"' for col in table_schema.key_columns])},
    {', '.join([f'"t1.{col}".alias("{col}_day_{day_id_1}")' for col in comparison_columns])},
    {', '.join([f'"t2.{col}".alias("{col}_day_{day_id_2}")' for col in comparison_columns])},
    {', '.join([f'when(col("t1.{col}") != col("t2.{col}"), "CHANGED").otherwise("SAME").alias("{col}_status")' for col in comparison_columns])}
)

comparison.show()"""