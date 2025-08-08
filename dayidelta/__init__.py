"""
DayIDelta: SCD2 for Time Series Data in Delta Lake

A modular, robust Python library for maintaining slowly-changing dimension (SCD2) 
history for time series observation tables in Delta Lake.

This package provides:
- Core SCD2 engine with platform abstractions
- Platform-specific implementations (Unity Catalog, Microsoft Fabric)
- AI-powered query generation and chatbot agents
- Flexible and extensible architecture
"""

__version__ = "2.0.0"
__author__ = "13668754 Canada Inc"

# Core exports
from .core.models import TableSchema, SCD2Config, QueryType, QueryRequest, QueryResponse, SCD2ProcessingResult
from .core.interfaces import PlatformAdapter, QueryGenerator, QueryParser, Agent

# Query generation (no PySpark dependency)
from .query.generators import SQLQueryGenerator, PythonQueryGenerator
from .query.patterns import SCD2QueryPatterns
from .query.parsers import NaturalLanguageParser

# Agents (no PySpark dependency)
from .agents.chatbot import SCD2Chatbot, quick_query
from .agents.cli import SCD2CLI

# Platform-specific imports (require PySpark)
try:
    from .core.scd2_engine import SCD2Engine, DayIDelta
    from .platforms.unity_catalog import UnityCatalogAdapter, setup_unity_catalog_environment
    from .platforms.fabric import FabricAdapter
    PYSPARK_IMPORTS_AVAILABLE = True
except ImportError:
    # PySpark not available - only core functionality will work
    PYSPARK_IMPORTS_AVAILABLE = False
    SCD2Engine = None
    DayIDelta = None
    UnityCatalogAdapter = None
    FabricAdapter = None
    setup_unity_catalog_environment = None

__all__ = [
    # Core models and interfaces (always available)
    'TableSchema', 'SCD2Config', 'QueryType', 'QueryRequest', 'QueryResponse', 'SCD2ProcessingResult',
    'PlatformAdapter', 'QueryGenerator', 'QueryParser', 'Agent',
    # Query generation (always available)
    'SQLQueryGenerator', 'PythonQueryGenerator', 'SCD2QueryPatterns', 'NaturalLanguageParser',
    # Agents (always available)
    'SCD2Chatbot', 'SCD2CLI', 'quick_query',
]

# Add PySpark-dependent exports if available
if PYSPARK_IMPORTS_AVAILABLE:
    __all__.extend([
        'SCD2Engine', 'DayIDelta',
        'UnityCatalogAdapter', 'FabricAdapter', 'setup_unity_catalog_environment'
    ])