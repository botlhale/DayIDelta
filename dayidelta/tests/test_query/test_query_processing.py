"""
Tests for query generation and parsing functionality.
"""

import unittest
from dayidelta.core.models import TableSchema, QueryType
from dayidelta.query.parsers import NaturalLanguageParser
from dayidelta.query.generators import SQLQueryGenerator, PythonQueryGenerator


class TestNaturalLanguageParser(unittest.TestCase):
    """Test the natural language parser."""
    
    def setUp(self):
        """Set up test fixtures."""
        self.parser = NaturalLanguageParser()
        self.schema = TableSchema(
            catalog="test_catalog",
            schema="test_schema",
            table="test_table",
            key_columns=["sensor_id", "timestamp"],
            tracked_columns=["temperature", "status"]
        )
    
    def test_current_data_parsing(self):
        """Test parsing of current data queries."""
        queries = [
            "show me all current data",
            "get current records",
            "what is the current temperature",
            "active data please"
        ]
        
        for query in queries:
            with self.subTest(query=query):
                request = self.parser.parse_query(query, self.schema)
                self.assertEqual(request.query_type, QueryType.CURRENT_DATA)
    
    def test_historical_data_parsing(self):
        """Test parsing of historical/point-in-time queries."""
        queries = [
            "how did the data look in 2024 q4",
            "data as of day_id 100",
            "point in time view for last month",
            "how did temperature look in december"
        ]
        
        for query in queries:
            with self.subTest(query=query):
                request = self.parser.parse_query(query, self.schema)
                self.assertEqual(request.query_type, QueryType.POINT_IN_TIME)
    
    def test_time_series_parsing(self):
        """Test parsing of time series history queries."""
        queries = [
            "show me temperature history for 2023",
            "track changes over Q2",
            "evolution of sensor data",
            "history over the last quarter"
        ]
        
        for query in queries:
            with self.subTest(query=query):
                request = self.parser.parse_query(query, self.schema)
                self.assertEqual(request.query_type, QueryType.TIME_SERIES_HISTORY)
    
    def test_comparison_parsing(self):
        """Test parsing of data comparison queries."""
        queries = [
            "compare data between today and yesterday",
            "difference between Q3 and Q4",
            "what changed from day_id 10 to day_id 20"
        ]
        
        for query in queries:
            with self.subTest(query=query):
                request = self.parser.parse_query(query, self.schema)
                self.assertEqual(request.query_type, QueryType.DATA_COMPARISON)
    
    def test_quarter_parameter_extraction(self):
        """Test extraction of quarter parameters."""
        query = "show me data for 2024 q3"
        request = self.parser.parse_query(query, self.schema)
        
        self.assertIn('year', request.parameters)
        self.assertIn('quarter', request.parameters)
        self.assertEqual(request.parameters['year'], 2024)
        self.assertEqual(request.parameters['quarter'], 3)
    
    def test_day_id_parameter_extraction(self):
        """Test extraction of day_id parameters."""
        query = "show me data at day_id 150"
        request = self.parser.parse_query(query, self.schema)
        
        self.assertIn('day_id', request.parameters)
        self.assertEqual(request.parameters['day_id'], 150)
    
    def test_days_ago_parameter_extraction(self):
        """Test extraction of 'days ago' parameters."""
        query = "show me data from 30 days ago"
        request = self.parser.parse_query(query, self.schema)
        
        self.assertIn('days_ago', request.parameters)
        self.assertEqual(request.parameters['days_ago'], 30)


class TestQueryGenerators(unittest.TestCase):
    """Test SQL and Python query generators."""
    
    def setUp(self):
        """Set up test fixtures."""
        self.sql_generator = SQLQueryGenerator()
        self.python_generator = PythonQueryGenerator()
        self.schema = TableSchema(
            catalog="test_catalog",
            schema="test_schema", 
            table="test_table",
            key_columns=["id", "timestamp"],
            tracked_columns=["value", "status"]
        )
    
    def test_current_data_sql_generation(self):
        """Test SQL generation for current data."""
        sql = self.sql_generator.generate_current_data_query(self.schema)
        
        self.assertIn("SELECT", sql)
        self.assertIn("end_day_id = 0", sql)
        self.assertIn("test_catalog.test_schema.test_table", sql)
        self.assertIn("id, timestamp, value, status", sql)
    
    def test_current_data_python_generation(self):
        """Test Python generation for current data."""
        python = self.python_generator.generate_current_data_query(self.schema)
        
        self.assertIn("from pyspark.sql import SparkSession", python)
        self.assertIn("col('end_day_id') == 0", python)
        self.assertIn("test_catalog.test_schema.test_table", python)
    
    def test_point_in_time_sql_generation(self):
        """Test SQL generation for point-in-time queries."""
        sql = self.sql_generator.generate_point_in_time_query(self.schema, 100)
        
        self.assertIn("start_day_id <= 100", sql)
        self.assertIn("end_day_id > 100", sql)
        self.assertIn("test_catalog.test_schema.test_table", sql)
    
    def test_time_series_history_sql_generation(self):
        """Test SQL generation for time series history."""
        sql = self.sql_generator.generate_time_series_history_query(self.schema, 50, 150)
        
        self.assertIn("start_day_id <= 150", sql)
        self.assertIn("end_day_id > 50", sql)
        self.assertIn("start_day_id,", sql)  # Should include SCD2 columns
        self.assertIn("end_day_id", sql)
    
    def test_data_comparison_sql_generation(self):
        """Test SQL generation for data comparison."""
        sql = self.sql_generator.generate_data_comparison_query(self.schema, 100, 200)
        
        self.assertIn("WITH data_day_100", sql)
        self.assertIn("WITH data_day_200", sql) 
        self.assertIn("FULL OUTER JOIN", sql)
        self.assertIn("value_day_100", sql)
        self.assertIn("value_day_200", sql)


if __name__ == '__main__':
    unittest.main()