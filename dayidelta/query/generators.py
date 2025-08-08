"""
Query generators for SQL and Python code generation.

Provides generators that create SQL statements and Python code
for different types of SCD2 queries.
"""

from typing import List, Optional
from ..core.interfaces import QueryGenerator
from ..core.models import TableSchema
from .patterns import SCD2QueryPatterns


class SQLQueryGenerator(QueryGenerator):
    """Generator for SQL queries against SCD2 tables."""
    
    def __init__(self):
        self.patterns = SCD2QueryPatterns()
    
    def generate_current_data_query(self, schema: TableSchema, filters: Optional[str] = None) -> str:
        """Generate SQL query for current/active data."""
        return self.patterns.current_data_sql(schema, filters)
    
    def generate_point_in_time_query(self, schema: TableSchema, day_id: int, 
                                   filters: Optional[str] = None) -> str:
        """Generate SQL query for point-in-time historical data."""
        return self.patterns.point_in_time_sql(schema, day_id, filters)
    
    def generate_time_series_history_query(self, schema: TableSchema, start_day_id: int, 
                                         end_day_id: int, filters: Optional[str] = None) -> str:
        """Generate SQL query for time series history within a period."""
        return self.patterns.time_series_history_sql(schema, start_day_id, end_day_id, filters)
    
    def generate_data_comparison_query(self, schema: TableSchema, day_id_1: int, day_id_2: int,
                                     comparison_columns: Optional[List[str]] = None) -> str:
        """Generate SQL query for comparing data between two time periods."""
        return self.patterns.data_comparison_sql(schema, day_id_1, day_id_2, comparison_columns)


class PythonQueryGenerator(QueryGenerator):
    """Generator for Python/PySpark code for SCD2 queries."""
    
    def __init__(self):
        self.patterns = SCD2QueryPatterns()
    
    def generate_current_data_query(self, schema: TableSchema, filters: Optional[str] = None) -> str:
        """Generate Python code for current/active data."""
        return self.patterns.current_data_python(schema, filters)
    
    def generate_point_in_time_query(self, schema: TableSchema, day_id: int, 
                                   filters: Optional[str] = None) -> str:
        """Generate Python code for point-in-time historical data."""
        return self.patterns.point_in_time_python(schema, day_id, filters)
    
    def generate_time_series_history_query(self, schema: TableSchema, start_day_id: int, 
                                         end_day_id: int, filters: Optional[str] = None) -> str:
        """Generate Python code for time series history within a period."""
        return self.patterns.time_series_history_python(schema, start_day_id, end_day_id, filters)
    
    def generate_data_comparison_query(self, schema: TableSchema, day_id_1: int, day_id_2: int,
                                     comparison_columns: Optional[List[str]] = None) -> str:
        """Generate Python code for comparing data between two time periods."""
        return self.patterns.data_comparison_python(schema, day_id_1, day_id_2, comparison_columns)