"""
Tests for the core SCD2 engine functionality.
"""

import unittest
from unittest.mock import Mock, MagicMock
from datetime import datetime
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, IntegerType

from dayidelta.core.scd2_engine import SCD2Engine
from dayidelta.core.models import TableSchema, SCD2Config
from dayidelta.core.interfaces import PlatformAdapter


class MockPlatformAdapter(PlatformAdapter):
    """Mock platform adapter for testing."""
    
    def __init__(self):
        self.tables = {}
        self.setup_called = False
    
    def table_exists(self, spark, table_name):
        return table_name in self.tables
    
    def create_table(self, spark, df, table_name, mode="overwrite"):
        self.tables[table_name] = df
    
    def append_to_table(self, spark, df, table_name):
        if table_name in self.tables:
            # In real implementation this would append, here we just replace for simplicity
            self.tables[table_name] = df
        else:
            self.tables[table_name] = df
    
    def merge_table(self, spark, source_df, target_table, merge_condition, update_clause):
        # Mock merge operation
        pass
    
    def get_table_schema(self, spark, table_name):
        return {"col1": "string", "col2": "int"}
    
    def setup_environment(self, spark, config):
        self.setup_called = True
    
    def get_full_table_name(self, config, table_name):
        if config.catalog and config.schema:
            return f"{config.catalog}.{config.schema}.{table_name}"
        return table_name


class TestSCD2Engine(unittest.TestCase):
    """Test the SCD2Engine class."""
    
    def setUp(self):
        """Set up test fixtures."""
        self.mock_adapter = MockPlatformAdapter()
        self.engine = SCD2Engine(self.mock_adapter)
        
        self.table_schema = TableSchema(
            catalog="test_catalog",
            schema="test_schema",
            table="test_table",
            key_columns=["id", "timestamp"],
            tracked_columns=["value", "status"]
        )
        
        self.scd2_config = SCD2Config(
            catalog="test_catalog",
            schema="test_schema",
            platform="unity_catalog"
        )
    
    def test_engine_initialization(self):
        """Test SCD2Engine initialization."""
        self.assertIsInstance(self.engine.platform_adapter, MockPlatformAdapter)
    
    def test_config_full_dim_day_table(self):
        """Test SCD2Config full dim_day table name generation."""
        # With catalog and schema
        config = SCD2Config(catalog="cat", schema="sch", dim_day_table="custom_dim")
        self.assertEqual(config.get_full_dim_day_table(), "cat.sch.custom_dim")
        
        # With schema only
        config = SCD2Config(schema="sch", dim_day_table="custom_dim")
        self.assertEqual(config.get_full_dim_day_table(), "sch.custom_dim")
        
        # Default
        config = SCD2Config(dim_day_table="custom_dim")
        self.assertEqual(config.get_full_dim_day_table(), "custom_dim")


class TestTableSchema(unittest.TestCase):
    """Test the TableSchema model."""
    
    def test_full_table_name(self):
        """Test full table name generation."""
        schema = TableSchema(
            catalog="cat", schema="sch", table="tab",
            key_columns=["key1"], tracked_columns=["track1"]
        )
        self.assertEqual(schema.full_table_name, "cat.sch.tab")
    
    def test_all_data_columns(self):
        """Test all data columns property."""
        schema = TableSchema(
            catalog="cat", schema="sch", table="tab",
            key_columns=["key1", "key2"],
            tracked_columns=["track1"],
            other_columns=["other1", "other2"]
        )
        expected = ["key1", "key2", "track1", "other1", "other2"]
        self.assertEqual(schema.all_data_columns, expected)
    
    def test_all_data_columns_without_other(self):
        """Test all data columns without other_columns."""
        schema = TableSchema(
            catalog="cat", schema="sch", table="tab",
            key_columns=["key1"],
            tracked_columns=["track1"]
        )
        expected = ["key1", "track1"]
        self.assertEqual(schema.all_data_columns, expected)


if __name__ == '__main__':
    unittest.main()