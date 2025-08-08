"""
Core DayIDelta functionality.

This module contains the platform-agnostic SCD2 engine and core abstractions.
"""

from .models import TableSchema, SCD2Config, QueryType, QueryRequest, QueryResponse, SCD2ProcessingResult
from .interfaces import PlatformAdapter, QueryGenerator, QueryParser, Agent

# PySpark-dependent imports
try:
    from .scd2_engine import SCD2Engine, DayIDelta
    PYSPARK_AVAILABLE = True
except ImportError:
    PYSPARK_AVAILABLE = False
    SCD2Engine = None
    DayIDelta = None

__all__ = [
    'TableSchema', 'SCD2Config', 'QueryType', 'QueryRequest', 'QueryResponse', 'SCD2ProcessingResult',
    'PlatformAdapter', 'QueryGenerator', 'QueryParser', 'Agent'
]

if PYSPARK_AVAILABLE:
    __all__.extend(['SCD2Engine', 'DayIDelta'])