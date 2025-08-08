"""
Core interfaces and protocols for DayIDelta.

Defines the abstract interfaces that platform adapters and query generators
must implement to work with the SCD2 engine.
"""

from abc import ABC, abstractmethod
from typing import Dict, List, Optional, Any

try:
    from pyspark.sql import DataFrame, SparkSession
    PYSPARK_AVAILABLE = True
except ImportError:
    # Mock types for environments without PySpark
    PYSPARK_AVAILABLE = False
    DataFrame = None
    SparkSession = None

from .models import TableSchema, SCD2Config, QueryRequest, QueryResponse


class PlatformAdapter(ABC):
    """
    Abstract interface for platform-specific implementations.
    
    Platform adapters handle the specifics of different Spark environments
    like Unity Catalog, Microsoft Fabric, Azure Synapse, etc.
    """
    
    @abstractmethod
    def table_exists(self, spark: "SparkSession", table_name: str) -> bool:
        """Check if a table exists in the platform."""
        pass
    
    @abstractmethod
    def create_table(self, spark: "SparkSession", df: "DataFrame", table_name: str, mode: str = "overwrite") -> None:
        """Create a table in the platform."""
        pass
    
    @abstractmethod
    def append_to_table(self, spark: "SparkSession", df: "DataFrame", table_name: str) -> None:
        """Append data to an existing table."""
        pass
    
    @abstractmethod
    def merge_table(self, spark: "SparkSession", source_df: "DataFrame", target_table: str, 
                   merge_condition: str, update_clause: str) -> None:
        """Perform a merge operation on the table."""
        pass
    
    @abstractmethod
    def get_table_schema(self, spark: "SparkSession", table_name: str) -> Dict[str, str]:
        """Get the schema of a table."""
        pass
    
    @abstractmethod
    def setup_environment(self, spark: "SparkSession", config: SCD2Config) -> None:
        """Setup the platform environment (catalogs, schemas, etc.)."""
        pass
    
    @abstractmethod
    def get_full_table_name(self, config: SCD2Config, table_name: str) -> str:
        """Get the full qualified table name for this platform."""
        pass


class QueryGenerator(ABC):
    """
    Abstract interface for query generators.
    
    Query generators create SQL statements and Python code for different
    types of SCD2 queries.
    """
    
    @abstractmethod
    def generate_current_data_query(self, schema: TableSchema, filters: Optional[str] = None) -> str:
        """Generate query for current/active data."""
        pass
    
    @abstractmethod
    def generate_point_in_time_query(self, schema: TableSchema, day_id: int, 
                                   filters: Optional[str] = None) -> str:
        """Generate query for point-in-time historical data."""
        pass
    
    @abstractmethod
    def generate_time_series_history_query(self, schema: TableSchema, start_day_id: int, 
                                         end_day_id: int, filters: Optional[str] = None) -> str:
        """Generate query for time series history within a period."""
        pass
    
    @abstractmethod
    def generate_data_comparison_query(self, schema: TableSchema, day_id_1: int, day_id_2: int,
                                     comparison_columns: Optional[List[str]] = None) -> str:
        """Generate query for comparing data between two time periods."""
        pass


class QueryParser(ABC):
    """
    Abstract interface for parsing natural language queries.
    """
    
    @abstractmethod
    def parse_query(self, user_input: str, schema: TableSchema) -> QueryRequest:
        """Parse natural language input into a structured query request."""
        pass
    
    @abstractmethod
    def extract_parameters(self, user_input: str, schema: TableSchema) -> Dict[str, Any]:
        """Extract parameters from natural language input."""
        pass


class Agent(ABC):
    """
    Abstract interface for agents that interact with users.
    """
    
    @abstractmethod
    def process_request(self, user_input: str, schema: TableSchema) -> QueryResponse:
        """Process a user request and return a response."""
        pass
    
    @abstractmethod
    def format_response(self, response: QueryResponse) -> str:
        """Format a response for presentation to the user."""
        pass