"""
Validation utilities for DayIDelta.
"""

import re
from typing import List, Dict, Any, Optional
from ..core.models import TableSchema, SCD2Config


def validate_table_schema(schema: TableSchema) -> List[str]:
    """
    Validate a table schema and return list of validation errors.
    
    Args:
        schema: TableSchema to validate
        
    Returns:
        List of validation error messages (empty if valid)
    """
    errors = []
    
    # Check required fields
    if not schema.catalog:
        errors.append("Catalog name is required")
    if not schema.schema:
        errors.append("Schema name is required") 
    if not schema.table:
        errors.append("Table name is required")
    if not schema.key_columns:
        errors.append("At least one key column is required")
    if not schema.tracked_columns:
        errors.append("At least one tracked column is required")
    
    # Check naming conventions
    if schema.catalog and not _is_valid_identifier(schema.catalog):
        errors.append(f"Invalid catalog name: {schema.catalog}")
    if schema.schema and not _is_valid_identifier(schema.schema):
        errors.append(f"Invalid schema name: {schema.schema}")
    if schema.table and not _is_valid_identifier(schema.table):
        errors.append(f"Invalid table name: {schema.table}")
    
    # Check column names
    all_columns = schema.key_columns + schema.tracked_columns
    if schema.other_columns:
        all_columns.extend(schema.other_columns)
    
    for col in all_columns:
        if not _is_valid_identifier(col):
            errors.append(f"Invalid column name: {col}")
    
    # Check for overlapping columns
    key_set = set(schema.key_columns)
    tracked_set = set(schema.tracked_columns)
    
    overlap = key_set.intersection(tracked_set)
    if overlap:
        errors.append(f"Columns cannot be both key and tracked: {list(overlap)}")
    
    # Check for reserved SCD2 column names
    reserved_columns = {'start_day_id', 'end_day_id'}
    used_reserved = reserved_columns.intersection(set(all_columns))
    if used_reserved:
        errors.append(f"Cannot use reserved SCD2 column names: {list(used_reserved)}")
    
    return errors


def validate_scd2_config(config: SCD2Config) -> List[str]:
    """
    Validate SCD2 configuration and return list of validation errors.
    
    Args:
        config: SCD2Config to validate
        
    Returns:
        List of validation error messages (empty if valid)
    """
    errors = []
    
    # Check platform
    valid_platforms = {'unity_catalog', 'fabric', 'synapse'}
    if config.platform not in valid_platforms:
        errors.append(f"Invalid platform: {config.platform}. Must be one of: {valid_platforms}")
    
    # Check dim_day table name
    if not _is_valid_identifier(config.dim_day_table):
        errors.append(f"Invalid dim_day table name: {config.dim_day_table}")
    
    # Platform-specific validation
    if config.platform == 'unity_catalog':
        if not config.catalog:
            errors.append("Unity Catalog platform requires catalog name")
        if not config.schema:
            errors.append("Unity Catalog platform requires schema name")
    
    return errors


def validate_query_parameters(parameters: Dict[str, Any]) -> List[str]:
    """
    Validate query parameters and return list of validation errors.
    
    Args:
        parameters: Query parameters to validate
        
    Returns:
        List of validation error messages (empty if valid)
    """
    errors = []
    
    # Validate day_id parameters
    day_id_params = ['day_id', 'day_id_1', 'day_id_2', 'start_day_id', 'end_day_id']
    for param in day_id_params:
        if param in parameters:
            value = parameters[param]
            if isinstance(value, int) and value < 0:
                errors.append(f"{param} must be non-negative: {value}")
    
    # Validate year parameters
    if 'year' in parameters:
        year = parameters['year']
        if not isinstance(year, int) or year < 1900 or year > 2100:
            errors.append(f"Invalid year: {year}")
    
    # Validate quarter parameters
    if 'quarter' in parameters:
        quarter = parameters['quarter']
        if not isinstance(quarter, int) or quarter < 1 or quarter > 4:
            errors.append(f"Invalid quarter: {quarter}. Must be 1-4")
    
    # Validate days_ago parameters
    if 'days_ago' in parameters:
        days_ago = parameters['days_ago']
        if not isinstance(days_ago, int) or days_ago < 0:
            errors.append(f"days_ago must be non-negative: {days_ago}")
    
    return errors


def _is_valid_identifier(name: str) -> bool:
    """
    Check if a name is a valid SQL identifier.
    
    Args:
        name: Identifier name to check
        
    Returns:
        True if valid, False otherwise
    """
    if not name:
        return False
    
    # Allow alphanumeric, underscore, and some special characters commonly used in identifiers
    # This is a basic check - actual SQL identifier rules vary by platform
    pattern = r'^[a-zA-Z_][a-zA-Z0-9_]*$'
    return bool(re.match(pattern, name)) and len(name) <= 128


def sanitize_sql_identifier(name: str) -> str:
    """
    Sanitize a string to be a valid SQL identifier.
    
    Args:
        name: String to sanitize
        
    Returns:
        Sanitized identifier name
    """
    if not name:
        return "unnamed"
    
    # Replace invalid characters with underscores
    sanitized = re.sub(r'[^a-zA-Z0-9_]', '_', name)
    
    # Ensure it starts with a letter or underscore
    if sanitized and sanitized[0].isdigit():
        sanitized = '_' + sanitized
    
    # Truncate if too long
    if len(sanitized) > 128:
        sanitized = sanitized[:128]
    
    return sanitized or "unnamed"


def validate_sql_query(sql: str) -> List[str]:
    """
    Basic validation of SQL query syntax.
    
    Args:
        sql: SQL query string to validate
        
    Returns:
        List of validation warnings/errors
    """
    warnings = []
    
    if not sql or not sql.strip():
        warnings.append("Empty SQL query")
        return warnings
    
    sql_upper = sql.upper()
    
    # Check for potentially dangerous operations
    dangerous_keywords = ['DROP', 'DELETE', 'TRUNCATE', 'ALTER', 'CREATE', 'INSERT', 'UPDATE']
    for keyword in dangerous_keywords:
        if f' {keyword} ' in sql_upper or sql_upper.startswith(keyword):
            warnings.append(f"SQL contains potentially dangerous keyword: {keyword}")
    
    # Check for basic SQL structure
    if 'SELECT' not in sql_upper:
        warnings.append("SQL query should contain SELECT statement")
    
    # Check for unbalanced parentheses
    if sql.count('(') != sql.count(')'):
        warnings.append("Unbalanced parentheses in SQL query")
    
    return warnings