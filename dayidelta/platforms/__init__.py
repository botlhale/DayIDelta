"""
Platform adapters for different Spark environments.
"""

from .base import BasePlatformAdapter

# Only import PySpark-dependent adapters if PySpark is available
try:
    from .unity_catalog import UnityCatalogAdapter, setup_unity_catalog_environment
    from .fabric import FabricAdapter
    PYSPARK_ADAPTERS_AVAILABLE = True
except ImportError:
    PYSPARK_ADAPTERS_AVAILABLE = False
    UnityCatalogAdapter = None
    FabricAdapter = None
    setup_unity_catalog_environment = None

__all__ = ['BasePlatformAdapter']

if PYSPARK_ADAPTERS_AVAILABLE:
    __all__.extend([
        'UnityCatalogAdapter', 'setup_unity_catalog_environment',
        'FabricAdapter'
    ])