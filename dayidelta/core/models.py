"""
Data models and schemas for DayIDelta.

Contains the core data structures used throughout the library.
"""

from dataclasses import dataclass, field
from typing import Dict, List, Optional, Any
from enum import Enum


class QueryType(Enum):
    """Types of SCD2 queries the system can generate."""
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
    other_columns: Optional[List[str]] = None
    
    @property
    def full_table_name(self) -> str:
        """Return the full 3-level table name."""
        return f"{self.catalog}.{self.schema}.{self.table}"
    
    @property
    def all_data_columns(self) -> List[str]:
        """Return all data columns (excluding SCD2 system columns)."""
        columns = self.key_columns.copy()
        columns.extend(self.tracked_columns)
        if self.other_columns:
            columns.extend(self.other_columns)
        return columns


@dataclass  
class SCD2Config:
    """Configuration for SCD2 operations."""
    catalog: Optional[str] = None
    schema: Optional[str] = None
    dim_day_table: str = "dim_day"
    platform: str = "unity_catalog"  # unity_catalog, fabric, synapse
    
    # Platform-specific configuration
    platform_config: Dict[str, Any] = field(default_factory=dict)
    
    def get_full_dim_day_table(self) -> str:
        """Get the full qualified dim_day table name."""
        if self.catalog and self.schema:
            return f"{self.catalog}.{self.schema}.{self.dim_day_table}"
        elif self.schema:
            return f"{self.schema}.{self.dim_day_table}"
        else:
            return self.dim_day_table


@dataclass
class QueryRequest:
    """Represents a user's query request."""
    user_input: str
    table_schema: TableSchema
    query_type: Optional[QueryType] = None
    parameters: Optional[Dict[str, Any]] = None


@dataclass
class QueryResponse:
    """Response containing generated SQL and Python code."""
    sql_query: str
    python_code: str
    explanation: str
    query_type: QueryType
    parameters: Dict[str, Any]


@dataclass
class SCD2ProcessingResult:
    """Result of an SCD2 processing operation."""
    records_processed: int
    records_closed: int
    records_inserted: int
    current_day_id: int
    processing_time_seconds: float
    errors: List[str] = field(default_factory=list)
    warnings: List[str] = field(default_factory=list)
    
    @property
    def success(self) -> bool:
        """Return True if processing was successful."""
        return len(self.errors) == 0