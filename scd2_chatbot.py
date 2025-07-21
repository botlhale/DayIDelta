"""
SCD2 AI Chatbot for DayIDelta

An AI-powered chatbot that helps users generate SQL statements and Python code
to query SCD2 (Slowly Changing Dimension Type 2) tables created by DayIDelta.

The chatbot understands natural language queries and generates appropriate SQL
and PySpark code for common SCD2 query patterns including:
- Current/active data queries
- Point-in-time historical queries
- Time series history for specific periods
- Data comparison between different time periods
"""

import re
import datetime
from typing import Dict, List, Tuple, Optional, Union
from dataclasses import dataclass
from enum import Enum


class QueryType(Enum):
    """Types of SCD2 queries the chatbot can generate."""
    CURRENT_DATA = "current_data"
    POINT_IN_TIME = "point_in_time"
    TIME_SERIES_HISTORY = "time_series_history"
    DATA_COMPARISON = "data_comparison"
    CUSTOM = "custom"


@dataclass
class TableSchema:
    """Represents the schema of an SCD2 table."""
    catalog: str
    schema: str
    table: str
    key_columns: List[str]
    tracked_columns: List[str]
    other_columns: List[str] = None
    
    @property
    def full_table_name(self) -> str:
        """Return the full 3-level table name."""
        return f"{self.catalog}.{self.schema}.{self.table}"
    
    @property
    def all_data_columns(self) -> List[str]:
        """Return all data columns (excluding SCD2 system columns)."""
        columns = self.key_columns + self.tracked_columns
        if self.other_columns:
            columns.extend(self.other_columns)
        return columns


@dataclass
class QueryRequest:
    """Represents a user's query request."""
    user_input: str
    table_schema: TableSchema
    query_type: QueryType = None
    parameters: Dict = None


@dataclass
class QueryResponse:
    """Response containing generated SQL and Python code."""
    sql_query: str
    python_code: str
    explanation: str
    query_type: QueryType
    parameters: Dict


class SCD2QueryPatterns:
    """Contains SQL and Python code patterns for different SCD2 query types."""
    
    @staticmethod
    def current_data_sql(table_schema: TableSchema, filters: str = "") -> str:
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
    def current_data_python(table_schema: TableSchema, filters: str = "") -> str:
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
    def point_in_time_sql(table_schema: TableSchema, day_id: int, filters: str = "") -> str:
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
    def point_in_time_python(table_schema: TableSchema, day_id: int, filters: str = "") -> str:
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
                               time_series_filter: str = "", other_filters: str = "") -> str:
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
                                  time_series_filter: str = "", other_filters: str = "") -> str:
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
                           comparison_columns: List[str] = None) -> str:
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
                              comparison_columns: List[str] = None) -> str:
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


class SCD2Chatbot:
    """AI chatbot for generating SCD2 queries."""
    
    def __init__(self):
        self.query_patterns = SCD2QueryPatterns()
        
    def parse_user_input(self, user_input: str, table_schema: TableSchema) -> QueryRequest:
        """Parse user input to determine query type and extract parameters."""
        user_input_lower = user_input.lower()
        
        # Patterns for different query types
        current_patterns = [
            r"current\s+data", r"show\s+me\s+all\s+current", r"active\s+records",
            r"latest\s+data", r"what\s+is\s+the\s+current", r"show\s+current",
            r"current.*temperature", r"current.*humidity", r"current.*sensor"
        ]
        
        historical_patterns = [
            r"how.*looked\s+in", r"data\s+as\s+of", r"point[\s-]in[\s-]time",
            r"historical\s+data", r"data\s+at\s+day", r"data.*for.*december",
            r"as\s+of\s+day", r"at\s+day_id", r"view\s+for", r"look.*in.*q\d",
            r"readings.*look.*in", r"how.*did.*look", r"look.*q\d"
        ]
        
        time_series_patterns = [
            r"history\s+of", r"time\s+series.*for", r"changes\s+over",
            r"evolution\s+of", r"track\s+changes", r"history.*for.*sensor",
            r"show.*history", r"temperature\s+history", r"over\s+q\d"
        ]
        
        comparison_patterns = [
            r"difference.*between", r"compare.*and", r"changes\s+from",
            r"what\s+changed", r"delta\s+between"
        ]
        
        # Determine query type
        query_type = QueryType.CUSTOM
        parameters = {}
        
        if any(re.search(pattern, user_input_lower) for pattern in current_patterns):
            query_type = QueryType.CURRENT_DATA
        elif any(re.search(pattern, user_input_lower) for pattern in historical_patterns):
            query_type = QueryType.POINT_IN_TIME
            parameters = self._extract_time_parameters(user_input_lower, table_schema)
        elif any(re.search(pattern, user_input_lower) for pattern in time_series_patterns):
            query_type = QueryType.TIME_SERIES_HISTORY
            parameters = self._extract_time_parameters(user_input_lower, table_schema)
        elif any(re.search(pattern, user_input_lower) for pattern in comparison_patterns):
            query_type = QueryType.DATA_COMPARISON
            parameters = self._extract_comparison_parameters(user_input_lower, table_schema)
        
        return QueryRequest(
            user_input=user_input,
            table_schema=table_schema,
            query_type=query_type,
            parameters=parameters
        )
    
    def _extract_time_parameters(self, user_input: str, table_schema: TableSchema = None) -> Dict:
        """Extract time-related parameters from user input."""
        parameters = {}
        
        # Look for quarters (e.g., "2024 q4", "2023 q2")
        quarter_match = re.search(r'(\d{4})\s+q(\d)', user_input)
        if quarter_match:
            year = int(quarter_match.group(1))
            quarter = int(quarter_match.group(2))
            parameters['year'] = year
            parameters['quarter'] = quarter
            # Convert to approximate day_id range (this would need mapping to actual day_id values)
            if table_schema:
                parameters['estimated_day_id'] = self._estimate_day_id_from_quarter(year, quarter, table_schema)
        
        # Look for specific day_id mentions
        day_id_match = re.search(r'day[_\s]*id[_\s]*(\d+)', user_input)
        if day_id_match:
            parameters['day_id'] = int(day_id_match.group(1))
        
        # Look for "x days ago"
        days_ago_match = re.search(r'(\d+)\s+days?\s+ago', user_input)
        if days_ago_match:
            days_ago = int(days_ago_match.group(1))
            parameters['days_ago'] = days_ago
            # This would need mapping to actual day_id
            if table_schema:
                parameters['estimated_day_id'] = f"(SELECT MAX(day_id) - {days_ago} FROM {table_schema.catalog}.{table_schema.schema}.dim_day)"
        
        return parameters
    
    def _extract_comparison_parameters(self, user_input: str, table_schema: TableSchema = None) -> Dict:
        """Extract comparison parameters from user input."""
        parameters = {}
        
        # Look for "between X and Y" patterns
        between_match = re.search(r'between\s+(.+?)\s+and\s+(.+?)(?:\s|$)', user_input)
        if between_match:
            param1 = between_match.group(1).strip()
            param2 = between_match.group(2).strip()
            
            # Try to extract day_ids or time periods
            day_id_1 = self._extract_day_id_from_text(param1)
            day_id_2 = self._extract_day_id_from_text(param2)
            
            if day_id_1:
                parameters['day_id_1'] = day_id_1
            if day_id_2:
                parameters['day_id_2'] = day_id_2
        
        return parameters
    
    def _extract_day_id_from_text(self, text: str) -> Optional[int]:
        """Extract day_id from text fragment."""
        # Look for explicit day_id
        day_id_match = re.search(r'day[_\s]*id[_\s]*(\d+)', text)
        if day_id_match:
            return int(day_id_match.group(1))
        
        # Look for "today"
        if 'today' in text:
            return "CURRENT_DAY_ID"  # Placeholder
        
        # Look for "X days ago"
        days_ago_match = re.search(r'(\d+)\s+days?\s+ago', text)
        if days_ago_match:
            return f"CURRENT_DAY_ID - {days_ago_match.group(1)}"
        
        return None
    
    def _estimate_day_id_from_quarter(self, year: int, quarter: int, table_schema: TableSchema) -> str:
        """Estimate day_id from year and quarter (placeholder implementation)."""
        # This would need to query the actual dim_day table
        return f"(SELECT day_id FROM {table_schema.catalog}.{table_schema.schema}.dim_day WHERE YEAR(date) = {year} AND QUARTER(date) = {quarter} ORDER BY day_id DESC LIMIT 1)"
    
    def generate_response(self, request: QueryRequest) -> QueryResponse:
        """Generate SQL and Python code based on the request."""
        
        if request.query_type == QueryType.CURRENT_DATA:
            return self._generate_current_data_response(request)
        elif request.query_type == QueryType.POINT_IN_TIME:
            return self._generate_point_in_time_response(request)
        elif request.query_type == QueryType.TIME_SERIES_HISTORY:
            return self._generate_time_series_history_response(request)
        elif request.query_type == QueryType.DATA_COMPARISON:
            return self._generate_data_comparison_response(request)
        else:
            return self._generate_custom_response(request)
    
    def _generate_current_data_response(self, request: QueryRequest) -> QueryResponse:
        """Generate response for current data queries."""
        sql_query = self.query_patterns.current_data_sql(request.table_schema)
        python_code = self.query_patterns.current_data_python(request.table_schema)
        
        explanation = f"""This query retrieves all current/active records from the SCD2 table {request.table_schema.full_table_name}.

Key points:
- Filters for end_day_id = 0 (active records)
- Includes all data columns: {', '.join(request.table_schema.all_data_columns)}
- Ordered by key columns for consistent results"""
        
        return QueryResponse(
            sql_query=sql_query,
            python_code=python_code,
            explanation=explanation,
            query_type=QueryType.CURRENT_DATA,
            parameters=request.parameters or {}
        )
    
    def _generate_point_in_time_response(self, request: QueryRequest) -> QueryResponse:
        """Generate response for point-in-time queries."""
        params = request.parameters or {}
        day_id = params.get('day_id', params.get('estimated_day_id', 'YOUR_DAY_ID'))
        
        sql_query = self.query_patterns.point_in_time_sql(request.table_schema, day_id)
        python_code = self.query_patterns.point_in_time_python(request.table_schema, day_id)
        
        explanation = f"""This query retrieves data as it looked at a specific point in time (day_id = {day_id}).

Key points:
- Uses SCD2 logic: start_day_id <= {day_id} AND (end_day_id = 0 OR end_day_id > {day_id})
- Shows the active version of each record at that time
- Excludes records that hadn't started yet or had already ended"""
        
        return QueryResponse(
            sql_query=sql_query,
            python_code=python_code,
            explanation=explanation,
            query_type=QueryType.POINT_IN_TIME,
            parameters=params
        )
    
    def _generate_time_series_history_response(self, request: QueryRequest) -> QueryResponse:
        """Generate response for time series history queries."""
        params = request.parameters or {}
        
        # Use parameters or defaults
        if 'year' in params and 'quarter' in params:
            year = params['year']
            quarter = params['quarter']
            # Estimate day_id range for the quarter
            start_day_id = f"(SELECT MIN(day_id) FROM {request.table_schema.catalog}.{request.table_schema.schema}.dim_day WHERE YEAR(date) = {year} AND QUARTER(date) = {quarter})"
            end_day_id = f"(SELECT MAX(day_id) FROM {request.table_schema.catalog}.{request.table_schema.schema}.dim_day WHERE YEAR(date) = {year} AND QUARTER(date) = {quarter})"
            period_desc = f"{year} Q{quarter}"
        else:
            start_day_id = params.get('start_day_id', 'START_DAY_ID')
            end_day_id = params.get('end_day_id', 'END_DAY_ID')
            period_desc = f"day_id {start_day_id} to {end_day_id}"
        
        sql_query = self.query_patterns.time_series_history_sql(
            request.table_schema, start_day_id, end_day_id
        )
        python_code = self.query_patterns.time_series_history_python(
            request.table_schema, start_day_id, end_day_id
        )
        
        explanation = f"""This query retrieves the complete history of records for {period_desc}.

Key points:
- Shows all versions of records that were active during this period
- Includes start_day_id and end_day_id for tracking version lifecycles
- Useful for understanding how data evolved over time"""
        
        return QueryResponse(
            sql_query=sql_query,
            python_code=python_code,
            explanation=explanation,
            query_type=QueryType.TIME_SERIES_HISTORY,
            parameters=params
        )
    
    def _generate_data_comparison_response(self, request: QueryRequest) -> QueryResponse:
        """Generate response for data comparison queries."""
        params = request.parameters or {}
        day_id_1 = params.get('day_id_1', 'DAY_ID_1')
        day_id_2 = params.get('day_id_2', 'DAY_ID_2')
        
        sql_query = self.query_patterns.data_comparison_sql(
            request.table_schema, day_id_1, day_id_2
        )
        python_code = self.query_patterns.data_comparison_python(
            request.table_schema, day_id_1, day_id_2
        )
        
        explanation = f"""This query compares data between two points in time (day_id {day_id_1} vs day_id {day_id_2}).

Key points:
- Uses FULL OUTER JOIN to capture records that exist in either time period
- Shows values for tracked columns at both time periods
- Includes status indicators showing which values changed
- Useful for identifying what data changed between two time periods"""
        
        return QueryResponse(
            sql_query=sql_query,
            python_code=python_code,
            explanation=explanation,
            query_type=QueryType.DATA_COMPARISON,
            parameters=params
        )
    
    def _generate_custom_response(self, request: QueryRequest) -> QueryResponse:
        """Generate response for custom queries."""
        sql_template = f"""-- Custom query for {request.table_schema.full_table_name}
-- Available columns: {', '.join(request.table_schema.all_data_columns)}
-- SCD2 columns: start_day_id, end_day_id

-- For current data:
-- WHERE end_day_id = 0

-- For historical data at specific day_id X:
-- WHERE start_day_id <= X AND (end_day_id = 0 OR end_day_id > X)

SELECT *
FROM {request.table_schema.full_table_name}
WHERE end_day_id = 0  -- Modify this condition as needed
"""
        
        python_template = f"""# Custom query for {request.table_schema.full_table_name}
from pyspark.sql import SparkSession
from pyspark.sql.functions import col

spark = SparkSession.builder.getOrCreate()

# Load the SCD2 table
df = spark.table("{request.table_schema.full_table_name}")

# Apply your custom filters here
# Examples:
# - Current data: .filter(col('end_day_id') == 0)
# - Historical: .filter((col('start_day_id') <= day_id) & ((col('end_day_id') == 0) | (col('end_day_id') > day_id)))

result = df.filter(col('end_day_id') == 0)  # Modify as needed
result.show()
"""
        
        explanation = f"""I couldn't determine the specific query type from your request: "{request.user_input}"

Here's a template to help you build a custom query for the SCD2 table {request.table_schema.full_table_name}.

Common patterns:
1. Current data: end_day_id = 0
2. Point-in-time: start_day_id <= X AND (end_day_id = 0 OR end_day_id > X)
3. History range: start_day_id <= END_ID AND (end_day_id = 0 OR end_day_id > START_ID)

Please modify the WHERE conditions to match your specific needs."""
        
        return QueryResponse(
            sql_query=sql_template,
            python_code=python_template,
            explanation=explanation,
            query_type=QueryType.CUSTOM,
            parameters=request.parameters or {}
        )
    
    def chat(self, user_input: str, table_schema: TableSchema) -> QueryResponse:
        """Main chat interface - parse input and generate response."""
        request = self.parse_user_input(user_input, table_schema)
        return self.generate_response(request)


# Convenience function for quick usage
def quick_query(user_input: str, catalog: str, schema: str, table: str,
                key_columns: List[str], tracked_columns: List[str],
                other_columns: List[str] = None) -> QueryResponse:
    """
    Quick interface for generating SCD2 queries.
    
    Example usage:
    response = quick_query(
        "show me all current data",
        catalog="my_catalog",
        schema="my_schema", 
        table="my_table",
        key_columns=["id", "timestamp"],
        tracked_columns=["value", "status"]
    )
    print(response.sql_query)
    """
    table_schema = TableSchema(
        catalog=catalog,
        schema=schema,
        table=table,
        key_columns=key_columns,
        tracked_columns=tracked_columns,
        other_columns=other_columns
    )
    
    chatbot = SCD2Chatbot()
    return chatbot.chat(user_input, table_schema)


if __name__ == "__main__":
    # Example usage
    table_schema = TableSchema(
        catalog="cata",
        schema="sch", 
        table="tabl1",
        key_columns=["TIME_SERIES_NAME", "DATETIME", "DATA_SOURCE"],
        tracked_columns=["VALUE"]
    )
    
    chatbot = SCD2Chatbot()
    
    # Test different query types
    test_queries = [
        "can you show me all current data",
        "Can you show me how the data looked in 2024 q4",
        "can you show me the history of time series x for 2023 q2", 
        "Show me the differences in data between today and 5 days ago"
    ]
    
    for query in test_queries:
        print(f"\n{'='*60}")
        print(f"Query: {query}")
        print('='*60)
        
        response = chatbot.chat(query, table_schema)
        print(f"\nQuery Type: {response.query_type.value}")
        print(f"\nExplanation:\n{response.explanation}")
        print(f"\nSQL Query:\n{response.sql_query}")
        print(f"\nPython Code:\n{response.python_code}")