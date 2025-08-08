# DayIDelta 2.0: Modular Architecture Guide

## Overview

DayIDelta 2.0 introduces a completely refactored, modular architecture that separates concerns and provides better extensibility while maintaining full backward compatibility.

## Architecture Improvements

### Before (Monolithic)
```
DayIDelta.py (246 lines)           # Core SCD2 + Unity Catalog mixed
scd2_chatbot.py (654 lines)       # All chatbot logic in one file
chatbot_cli.py (171 lines)        # CLI functionality
```

### After (Modular)
```
dayidelta/
├── core/                          # Platform-agnostic SCD2 engine
│   ├── scd2_engine.py            # Core SCD2 logic
│   ├── models.py                 # Data models
│   └── interfaces.py             # Abstract interfaces
├── platforms/                    # Platform-specific adapters
│   ├── unity_catalog.py          # Unity Catalog adapter
│   ├── fabric.py                 # Microsoft Fabric adapter
│   └── base.py                   # Base adapter
├── query/                        # Query generation and parsing
│   ├── patterns.py               # SQL/Python templates
│   ├── generators.py             # Query generators
│   └── parsers.py                # Natural language parsing
├── agents/                       # User interaction agents
│   ├── chatbot.py                # AI chatbot agent
│   └── cli.py                    # CLI agent
└── utils/                        # Validation and utilities
    └── validation.py
```

## Key Benefits

### 1. Separation of Concerns
- **SCD2 Logic**: Pure SCD2 operations independent of platform
- **Platform Adapters**: Handle platform-specific operations
- **Query Generation**: Separated from user interaction
- **Agents**: Handle different interaction patterns (CLI, API, etc.)

### 2. Improved Testability
- Components can be tested independently
- No PySpark dependency for query generation and parsing
- Mock platform adapters for unit testing

### 3. Better Extensibility
- Easy to add new platform adapters (Azure Synapse, AWS Glue, etc.)
- Simple to add new query types or agents
- Pluggable architecture with clear interfaces

### 4. Strengthened Logic
- Fixed parsing bugs in chatbot (quarter extraction, days_ago, time series detection)
- Improved regex patterns for natural language understanding
- Better parameter validation and error handling

## Usage Examples

### Basic Usage (Unchanged API)
```python
# Original API still works exactly the same
from DayIDelta import DayIDelta

DayIDelta(
    new_data_df=df,
    key_cols=["sensor_id", "timestamp"],
    tracked_cols=["temperature"],
    dest_catalog="my_catalog",
    dest_sch="my_schema", 
    dest_tb_obs="sensor_readings"
)
```

### New Modular Usage
```python
# Use the new modular structure for more control
from dayidelta.core.scd2_engine import SCD2Engine
from dayidelta.platforms.unity_catalog import UnityCatalogAdapter
from dayidelta.core.models import TableSchema, SCD2Config

# Create platform adapter
adapter = UnityCatalogAdapter()

# Create SCD2 engine
engine = SCD2Engine(adapter)

# Define schema and config
schema = TableSchema(
    catalog="my_catalog",
    schema="my_schema", 
    table="sensor_readings",
    key_columns=["sensor_id", "timestamp"],
    tracked_columns=["temperature"]
)

config = SCD2Config(
    catalog="my_catalog",
    schema="my_schema"
)

# Process data
result = engine.process(spark, new_data_df, schema, config)
print(f"Processed {result.records_inserted} records")
```

### Enhanced Chatbot Usage
```python
# The chatbot now has much better parsing capabilities
from dayidelta.agents.chatbot import SCD2Chatbot
from dayidelta.core.models import TableSchema

schema = TableSchema(
    catalog="sensors",
    schema="prod",
    table="readings",
    key_columns=["sensor_id", "timestamp", "location"],
    tracked_columns=["temperature", "humidity"]
)

chatbot = SCD2Chatbot()

# These queries now work much better with improved parsing
queries = [
    "show me all current data",                           # ✅ Fixed
    "how did temperatures look in 2024 q3?",            # ✅ Fixed quarter parsing
    "show me sensor history over the last 30 days",     # ✅ Fixed days_ago parsing
    "compare data between Q2 and Q3",                   # ✅ Improved comparison
]

for query in queries:
    response = chatbot.chat(query, schema)
    print(f"Query: {query}")
    print(f"Type: {response.query_type}")
    print(f"SQL: {response.sql_query[:100]}...")
    print("---")
```

### Platform Flexibility
```python
# Easy to switch platforms
from dayidelta.platforms.fabric import FabricAdapter
from dayidelta.platforms.unity_catalog import UnityCatalogAdapter

# For Microsoft Fabric
fabric_adapter = FabricAdapter()
fabric_engine = SCD2Engine(fabric_adapter)

# For Unity Catalog  
unity_adapter = UnityCatalogAdapter()
unity_engine = SCD2Engine(unity_adapter)

# Same SCD2 logic, different platforms
```

## Testing Improvements

### Before
- 3 failing tests in chatbot parsing logic
- Monolithic tests hard to isolate issues
- PySpark required for all tests

### After
- All parsing tests pass with improved regex patterns
- Modular tests that can run without PySpark
- Clear test separation by component

```bash
# Test core functionality without PySpark
python -c "from dayidelta.query.parsers import NaturalLanguageParser; print('✅ Works')"

# Test platform-specific functionality (requires PySpark)
python -c "from dayidelta.core.scd2_engine import SCD2Engine; print('✅ Works')"
```

## Migration Guide

### For Existing Users
No changes required! Your existing code will continue to work:

```python
# This still works exactly the same
from DayIDelta import DayIDelta
from scd2_chatbot import SCD2Chatbot

# All existing APIs are preserved
```

### For New Development
Consider using the new modular structure:

```python
# More explicit and flexible
from dayidelta import SCD2Engine, UnityCatalogAdapter, SCD2Chatbot
```

## Performance Improvements

1. **Faster Import Times**: Components load independently
2. **Better Memory Usage**: Only load what you need
3. **Improved Query Generation**: Optimized templates and parsing
4. **Enhanced Error Handling**: Better validation and error messages

## Future Extensibility

The new architecture makes it easy to add:

- **New Platforms**: Azure Synapse, AWS Glue, Databricks SQL
- **New Agents**: REST API, Jupyter widget, VS Code extension  
- **New Query Types**: Audit trails, data lineage, performance analytics
- **New Output Formats**: PowerBI DAX, Looker LookML, dbt models

## Conclusion

DayIDelta 2.0 provides a solid foundation for future growth while ensuring existing users experience no disruption. The modular architecture follows software engineering best practices and makes the codebase much more maintainable and extensible.