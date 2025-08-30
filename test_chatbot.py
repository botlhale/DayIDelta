"""
Test Suite for SCD2 AI Chatbot

Comprehensive tests for the SCD2 chatbot functionality including
query parsing, SQL generation, and Python code generation.
"""

import unittest
from datetime import datetime

# Use modular imports - this provides better maintainability and functionality
try:
    from dayidelta.agents.chatbot import SCD2Chatbot, quick_query
    from dayidelta.core.models import TableSchema, QueryType, QueryRequest, QueryResponse  
    from dayidelta.query.patterns import SCD2QueryPatterns
except ImportError:
    # Fallback to legacy imports if modular structure not available
    from scd2_chatbot import (
        SCD2Chatbot, TableSchema, QueryType, QueryRequest, QueryResponse,
        SCD2QueryPatterns, quick_query
    )


class TestTableSchema(unittest.TestCase):
    """Test TableSchema class."""
    
    def setUp(self):
        self.schema = TableSchema(
            catalog="test_catalog",
            schema="test_schema", 
            table="test_table",
            key_columns=["id", "timestamp"],
            tracked_columns=["value", "status"],
            other_columns=["metadata"]
        )
    
    def test_full_table_name(self):
        """Test full table name generation."""
        expected = "test_catalog.test_schema.test_table"
        self.assertEqual(self.schema.full_table_name, expected)
    
    def test_all_data_columns(self):
        """Test all data columns property."""
        expected = ["id", "timestamp", "value", "status", "metadata"]
        self.assertEqual(self.schema.all_data_columns, expected)
    
    def test_schema_without_other_columns(self):
        """Test schema without other_columns."""
        schema = TableSchema(
            catalog="cat",
            schema="sch",
            table="tab",
            key_columns=["key1"],
            tracked_columns=["track1"]
        )
        expected = ["key1", "track1"]
        self.assertEqual(schema.all_data_columns, expected)


class TestSCD2QueryPatterns(unittest.TestCase):
    """Test SQL and Python query pattern generation."""
    
    def setUp(self):
        self.schema = TableSchema(
            catalog="test_cat",
            schema="test_sch",
            table="test_tab",
            key_columns=["sensor_id", "timestamp"],
            tracked_columns=["temperature"],
            other_columns=["location"]
        )
        self.patterns = SCD2QueryPatterns()
    
    def test_current_data_sql(self):
        """Test current data SQL generation."""
        sql = self.patterns.current_data_sql(self.schema)
        
        # Check key components
        self.assertIn("SELECT sensor_id, timestamp, temperature, location", sql)
        self.assertIn("FROM test_cat.test_sch.test_tab", sql)
        self.assertIn("WHERE end_day_id = 0", sql)
        self.assertIn("ORDER BY sensor_id, timestamp", sql)
    
    def test_current_data_sql_with_filters(self):
        """Test current data SQL with additional filters."""
        sql = self.patterns.current_data_sql(self.schema, "sensor_id = 'ABC123'")
        
        self.assertIn("WHERE end_day_id = 0 AND sensor_id = 'ABC123'", sql)
    
    def test_point_in_time_sql(self):
        """Test point-in-time SQL generation."""
        sql = self.patterns.point_in_time_sql(self.schema, 100)
        
        self.assertIn("WHERE start_day_id <= 100", sql)
        self.assertIn("(end_day_id = 0 OR end_day_id > 100)", sql)
    
    def test_time_series_history_sql(self):
        """Test time series history SQL generation."""
        sql = self.patterns.time_series_history_sql(self.schema, 50, 150)
        
        self.assertIn("start_day_id <= 150", sql)
        self.assertIn("(end_day_id = 0 OR end_day_id > 50)", sql)
        self.assertIn("start_day_id,", sql)
        self.assertIn("end_day_id", sql)
    
    def test_data_comparison_sql(self):
        """Test data comparison SQL generation."""
        sql = self.patterns.data_comparison_sql(self.schema, 100, 200)
        
        self.assertIn("WITH data_day_100 AS", sql)
        self.assertIn("data_day_200 AS", sql)
        self.assertIn("FULL OUTER JOIN", sql)
        self.assertIn("temperature_day_100", sql)
        self.assertIn("temperature_day_200", sql)
        self.assertIn("temperature_status", sql)
    
    def test_current_data_python(self):
        """Test current data Python generation."""
        python_code = self.patterns.current_data_python(self.schema)
        
        self.assertIn("from pyspark.sql import SparkSession", python_code)
        self.assertIn("from pyspark.sql.functions import col", python_code)
        self.assertIn("filter(col('end_day_id') == 0)", python_code)
        self.assertIn("test_cat.test_sch.test_tab", python_code)
    
    def test_point_in_time_python(self):
        """Test point-in-time Python generation."""
        python_code = self.patterns.point_in_time_python(self.schema, 100)
        
        self.assertIn("col('start_day_id') <= 100", python_code)
        self.assertIn("col('end_day_id') == 0", python_code)
        self.assertIn("col('end_day_id') > 100", python_code)


class TestSCD2Chatbot(unittest.TestCase):
    """Test SCD2Chatbot functionality."""
    
    def setUp(self):
        self.chatbot = SCD2Chatbot()
        self.schema = TableSchema(
            catalog="cata",
            schema="sch",
            table="tabl1",
            key_columns=["TIME_SERIES_NAME", "DATETIME", "DATA_SOURCE"],
            tracked_columns=["VALUE"]
        )
    
    def test_parse_current_data_query(self):
        """Test parsing current data queries."""
        queries = [
            "show me all current data",
            "what is the current state",
            "give me active records",
            "latest data please"
        ]
        
        for query in queries:
            request = self.chatbot.parse_user_input(query, self.schema)
            self.assertEqual(request.query_type, QueryType.CURRENT_DATA)
    
    def test_parse_historical_query(self):
        """Test parsing historical queries."""
        queries = [
            "how did the data look in 2024 q4",
            "show me data as of day_id 150",
            "point-in-time view for December",
            "historical data at day 100"
        ]
        
        for query in queries:
            request = self.chatbot.parse_user_input(query, self.schema)
            self.assertEqual(request.query_type, QueryType.POINT_IN_TIME)
    
    def test_parse_time_series_query(self):
        """Test parsing time series history queries."""
        queries = [
            "show me the history of time series x for 2023 q2",
            "track changes over the last quarter",
            "evolution of data in Q1",
            "time series history from day 100 to 200"
        ]
        
        for query in queries:
            request = self.chatbot.parse_user_input(query, self.schema)
            self.assertEqual(request.query_type, QueryType.TIME_SERIES_HISTORY)
    
    def test_parse_comparison_query(self):
        """Test parsing comparison queries."""
        queries = [
            "show me the differences in data between today and 5 days ago",
            "compare data between Q3 and Q4",
            "what changed from last month to this month",
            "delta between day_id 100 and day_id 200"
        ]
        
        for query in queries:
            request = self.chatbot.parse_user_input(query, self.schema)
            self.assertEqual(request.query_type, QueryType.DATA_COMPARISON)
    
    def test_parse_custom_query(self):
        """Test parsing unrecognized queries."""
        queries = [
            "do something complex",
            "random request",
            "explain the table structure"
        ]
        
        for query in queries:
            request = self.chatbot.parse_user_input(query, self.schema)
            self.assertEqual(request.query_type, QueryType.CUSTOM)
    
    def test_extract_quarter_parameters(self):
        """Test quarter parameter extraction."""
        query = "show me data for 2024 q4"
        request = self.chatbot.parse_user_input(query, self.schema)
        
        # The quarter pattern should be detected in "for 2024 q4"
        self.assertEqual(request.parameters.get('year'), 2024)
        self.assertEqual(request.parameters.get('quarter'), 4)
    
    def test_extract_day_id_parameters(self):
        """Test day_id parameter extraction."""
        query = "show data at day_id 150"
        request = self.chatbot.parse_user_input(query, self.schema)
        
        self.assertEqual(request.parameters.get('day_id'), 150)
    
    def test_extract_days_ago_parameters(self):
        """Test 'days ago' parameter extraction."""
        query = "compare data between today and 30 days ago"
        request = self.chatbot.parse_user_input(query, self.schema)
        
        # The pattern should extract the "30 days ago" part
        self.assertEqual(request.parameters.get('days_ago'), 30)
    
    def test_generate_current_data_response(self):
        """Test current data response generation."""
        request = QueryRequest(
            user_input="show current data",
            table_schema=self.schema,
            query_type=QueryType.CURRENT_DATA
        )
        
        response = self.chatbot.generate_response(request)
        
        self.assertEqual(response.query_type, QueryType.CURRENT_DATA)
        self.assertIn("end_day_id = 0", response.sql_query)
        self.assertIn("cata.sch.tabl1", response.sql_query)
        self.assertIn("from pyspark.sql", response.python_code)
        self.assertIn("active records", response.explanation.lower())
    
    def test_generate_point_in_time_response(self):
        """Test point-in-time response generation."""
        request = QueryRequest(
            user_input="show data at day_id 100",
            table_schema=self.schema,
            query_type=QueryType.POINT_IN_TIME,
            parameters={'day_id': 100}
        )
        
        response = self.chatbot.generate_response(request)
        
        self.assertEqual(response.query_type, QueryType.POINT_IN_TIME)
        self.assertIn("start_day_id <= 100", response.sql_query)
        self.assertIn("end_day_id > 100", response.sql_query)
    
    def test_chat_interface(self):
        """Test the main chat interface."""
        response = self.chatbot.chat("show me all current data", self.schema)
        
        self.assertIsInstance(response, QueryResponse)
        self.assertEqual(response.query_type, QueryType.CURRENT_DATA)
        self.assertTrue(len(response.sql_query) > 0)
        self.assertTrue(len(response.python_code) > 0)
        self.assertTrue(len(response.explanation) > 0)


class TestQuickQuery(unittest.TestCase):
    """Test quick_query convenience function."""
    
    def test_quick_query_current_data(self):
        """Test quick_query for current data."""
        response = quick_query(
            user_input="show me all current data",
            catalog="test_cat",
            schema="test_sch",
            table="test_tab",
            key_columns=["id"],
            tracked_columns=["value"]
        )
        
        self.assertEqual(response.query_type, QueryType.CURRENT_DATA)
        self.assertIn("test_cat.test_sch.test_tab", response.sql_query)
    
    def test_quick_query_with_other_columns(self):
        """Test quick_query with other_columns."""
        response = quick_query(
            user_input="current data",
            catalog="cat",
            schema="sch",
            table="tab",
            key_columns=["key1"],
            tracked_columns=["track1"],
            other_columns=["other1", "other2"]
        )
        
        self.assertIn("key1, track1, other1, other2", response.sql_query)


class TestErrorHandling(unittest.TestCase):
    """Test error handling and edge cases."""
    
    def setUp(self):
        self.chatbot = SCD2Chatbot()
        self.schema = TableSchema(
            catalog="test",
            schema="test", 
            table="test",
            key_columns=["id"],
            tracked_columns=["value"]
        )
    
    def test_empty_query(self):
        """Test handling of empty queries."""
        response = self.chatbot.chat("", self.schema)
        self.assertEqual(response.query_type, QueryType.CUSTOM)
    
    def test_very_long_query(self):
        """Test handling of very long queries."""
        long_query = "show me " + "very " * 100 + "current data"
        response = self.chatbot.chat(long_query, self.schema)
        self.assertEqual(response.query_type, QueryType.CURRENT_DATA)
    
    def test_special_characters(self):
        """Test handling of special characters."""
        query = "show current data @#$%^&*()"
        response = self.chatbot.chat(query, self.schema)
        # Should still detect current data pattern
        self.assertEqual(response.query_type, QueryType.CURRENT_DATA)
    
    def test_case_insensitive_parsing(self):
        """Test case insensitive query parsing."""
        queries = [
            "SHOW ME CURRENT DATA",
            "Show Me Current Data",
            "show me CURRENT data"
        ]
        
        for query in queries:
            response = self.chatbot.chat(query, self.schema)
            self.assertEqual(response.query_type, QueryType.CURRENT_DATA)


class TestSQLValidation(unittest.TestCase):
    """Test basic SQL syntax validation."""
    
    def setUp(self):
        self.chatbot = SCD2Chatbot()
        self.schema = TableSchema(
            catalog="test_catalog",
            schema="test_schema",
            table="test_table", 
            key_columns=["id", "timestamp"],
            tracked_columns=["value", "status"]
        )
    
    def test_sql_basic_syntax(self):
        """Test that generated SQL has basic valid syntax."""
        queries = [
            "show current data",
            "historical data at day_id 100",
            "compare day_id 50 and day_id 100"
        ]
        
        for query in queries:
            response = self.chatbot.chat(query, self.schema)
            sql = response.sql_query
            
            # Basic syntax checks
            self.assertIn("SELECT", sql.upper())
            self.assertIn("FROM", sql.upper())
            self.assertIn(self.schema.full_table_name, sql)
            
            # No obvious syntax errors
            self.assertNotIn(";;", sql)  # Double semicolons
            self.assertEqual(sql.count("("), sql.count(")"))  # Balanced parentheses
    
    def test_python_basic_syntax(self):
        """Test that generated Python code has basic valid syntax."""
        response = self.chatbot.chat("show current data", self.schema)
        python_code = response.python_code
        
        # Basic Python syntax checks
        self.assertIn("from pyspark.sql", python_code)
        self.assertIn("SparkSession", python_code)
        self.assertIn("spark.table", python_code)
        
        # No obvious syntax errors
        lines = python_code.split('\n')
        for line in lines:
            if line.strip() and not line.strip().startswith('#'):
                # Check for basic Python syntax
                self.assertNotIn('\\\\', line)  # No double backslashes


def run_specific_test_examples():
    """Run specific test examples from the problem statement."""
    print("Running specific test examples from problem statement...")
    print("=" * 60)
    
    # Create table schema matching problem statement
    schema = TableSchema(
        catalog="cata",
        schema="sch",
        table="tabl1",
        key_columns=["TIME_SERIES_NAME", "DATETIME", "DATA_SOURCE"],
        tracked_columns=["VALUE"]
    )
    
    chatbot = SCD2Chatbot()
    
    # Test the exact queries from problem statement
    test_queries = [
        "can you show me all current data",
        "Can you show me how the data looked in 2024 q4",
        "can you show me the history of time series x for 2023 q2",
        "Show me the differences in data between today and x days ago"
    ]
    
    for i, query in enumerate(test_queries, 1):
        print(f"\nTest {i}: '{query}'")
        print("-" * 40)
        
        response = chatbot.chat(query, schema)
        print(f"Detected Type: {response.query_type.value}")
        print(f"Parameters: {response.parameters}")
        
        # Validate response
        assert response.sql_query is not None and len(response.sql_query) > 0
        assert response.python_code is not None and len(response.python_code) > 0
        assert response.explanation is not None and len(response.explanation) > 0
        assert "cata.sch.tabl1" in response.sql_query
        
        print("✅ Response generated successfully")
    
    print(f"\n🎉 All {len(test_queries)} test queries passed!")


if __name__ == "__main__":
    print("SCD2 Chatbot Test Suite")
    print("=" * 40)
    
    # Run specific examples first
    run_specific_test_examples()
    
    print(f"\n\nRunning comprehensive unit tests...")
    print("=" * 40)
    
    # Run unit tests
    unittest.main(verbosity=2, exit=False)
    
    print(f"\n✅ All tests completed!")