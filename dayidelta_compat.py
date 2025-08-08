"""
Backward compatibility module for DayIDelta.

This module provides the original API while using the new modular structure underneath.
This ensures existing code continues to work without changes.
"""

# Import everything from the new structure but maintain original names
from dayidelta.core.models import TableSchema as DayIDeltaTableSchema
from dayidelta.core.models import SCD2Config, QueryType
from dayidelta.agents.chatbot import SCD2Chatbot, quick_query

# Re-export with original names for backward compatibility
try:
    from dayidelta.core.scd2_engine import DayIDelta
    from dayidelta.platforms.unity_catalog import setup_unity_catalog_environment
    PYSPARK_FUNCTIONS_AVAILABLE = True
except ImportError:
    PYSPARK_FUNCTIONS_AVAILABLE = False
    
    def DayIDelta(*args, **kwargs):
        raise ImportError("PySpark is required for DayIDelta function. Please install pyspark.")
    
    def setup_unity_catalog_environment(*args, **kwargs):
        raise ImportError("PySpark is required for Unity Catalog operations. Please install pyspark.")

# Re-export the query patterns and generators for backward compatibility
from dayidelta.query.patterns import SCD2QueryPatterns
from dayidelta.query.generators import SQLQueryGenerator, PythonQueryGenerator

# Original chatbot classes with same interface
class SCD2Chatbot(SCD2Chatbot):
    """Backward compatible SCD2Chatbot - same as the new implementation."""
    pass

# Export everything that was available in the original implementation
__all__ = [
    'DayIDelta', 'setup_unity_catalog_environment',
    'SCD2Chatbot', 'SCD2QueryPatterns', 'quick_query',
    'QueryType', 'TableSchema'
]

# Alias for TableSchema to maintain compatibility
TableSchema = DayIDeltaTableSchema