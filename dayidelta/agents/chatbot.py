"""
SCD2 AI Chatbot agent - modular and improved implementation.

This module provides an AI-powered chatbot that generates SQL statements and Python code
for querying SCD2 tables. It's been refactored to be more modular and extensible.
"""

import logging
from typing import Dict, Any, Optional
from ..core.interfaces import Agent
from ..core.models import TableSchema, QueryRequest, QueryResponse, QueryType
from ..query.parsers import NaturalLanguageParser
from ..query.generators import SQLQueryGenerator, PythonQueryGenerator

logger = logging.getLogger(__name__)


class SCD2Chatbot(Agent):
    """
    AI-powered chatbot for generating SCD2 queries.
    
    Features:
    - Natural language understanding
    - Multi-query support (current, historical, time series, comparison)
    - Dual output (SQL + Python)
    - Smart parameter extraction
    """
    
    def __init__(self, sql_generator: Optional[SQLQueryGenerator] = None,
                 python_generator: Optional[PythonQueryGenerator] = None,
                 parser: Optional[NaturalLanguageParser] = None):
        """
        Initialize the chatbot with configurable components.
        
        Args:
            sql_generator: SQL query generator (optional, creates default if None)
            python_generator: Python code generator (optional, creates default if None)
            parser: Natural language parser (optional, creates default if None)
        """
        self.sql_generator = sql_generator or SQLQueryGenerator()
        self.python_generator = python_generator or PythonQueryGenerator()
        self.parser = parser or NaturalLanguageParser()
    
    def process_request(self, user_input: str, schema: TableSchema) -> QueryResponse:
        """Process a user request and return a response."""
        return self.chat(user_input, schema)
    
    def chat(self, user_input: str, table_schema: TableSchema) -> QueryResponse:
        """Main chat interface - parse input and generate response."""
        try:
            # Parse the user input
            request = self.parser.parse_query(user_input, table_schema)
            
            # Generate the response
            response = self._generate_response(request)
            
            logger.info(f"Generated {response.query_type.value} query for: {user_input}")
            return response
            
        except Exception as e:
            logger.error(f"Error processing chat request: {e}")
            return self._generate_error_response(user_input, table_schema, str(e))
    
    def format_response(self, response: QueryResponse) -> str:
        """Format a response for presentation to the user."""
        output = []
        output.append(f"🤖 Query Type: {response.query_type.value.replace('_', ' ').title()}")
        
        if response.parameters:
            output.append(f"📊 Parameters: {response.parameters}")
        
        output.append(f"\n💡 Explanation:")
        output.append(response.explanation)
        
        output.append(f"\n📄 SQL Query:")
        output.append("-" * 50)
        output.append(response.sql_query)
        
        output.append(f"\n🐍 Python Code:")
        output.append("-" * 50)
        output.append(response.python_code)
        
        return "\n".join(output)
    
    def _generate_response(self, request: QueryRequest) -> QueryResponse:
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
        sql_query = self.sql_generator.generate_current_data_query(request.table_schema)
        python_code = self.python_generator.generate_current_data_query(request.table_schema)
        
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
        
        sql_query = self.sql_generator.generate_point_in_time_query(request.table_schema, day_id)
        python_code = self.python_generator.generate_point_in_time_query(request.table_schema, day_id)
        
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
        
        sql_query = self.sql_generator.generate_time_series_history_query(
            request.table_schema, start_day_id, end_day_id
        )
        python_code = self.python_generator.generate_time_series_history_query(
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
        
        sql_query = self.sql_generator.generate_data_comparison_query(
            request.table_schema, day_id_1, day_id_2
        )
        python_code = self.python_generator.generate_data_comparison_query(
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
    
    def _generate_error_response(self, user_input: str, schema: TableSchema, error: str) -> QueryResponse:
        """Generate an error response."""
        error_sql = f"""-- Error processing query: {error}
-- Original input: {user_input}
-- Please try rephrasing your question

SELECT 'Error: {error}' as error_message;"""
        
        error_python = f"""# Error processing query: {error}
# Original input: {user_input}
# Please try rephrasing your question

print("Error: {error}")"""
        
        return QueryResponse(
            sql_query=error_sql,
            python_code=error_python,
            explanation=f"An error occurred while processing your request: {error}",
            query_type=QueryType.CUSTOM,
            parameters={}
        )


# Convenience function for quick usage
def quick_query(user_input: str, catalog: str, schema: str, table: str,
                key_columns: list, tracked_columns: list,
                other_columns: Optional[list] = None) -> QueryResponse:
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