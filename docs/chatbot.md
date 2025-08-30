# Chatbot Documentation

High-level usage and selected details (full prior doc condensed).

## Overview
The AI-powered chatbot converts natural language requests (e.g., "compare current data with data 30 days ago") into executable SQL and PySpark code for SCD2 tables managed by DayIDelta.

## Supported Query Classes
1. Current data (active records)
2. Point-in-time snapshot
3. Historical window
4. Comparison between two times
5. Custom / patterned queries (extensible)

## Quick Start
```python
from dayidelta.chatbot import SCD2Chatbot, TableSchema

schema = TableSchema(
    catalog="analytics",
    schema="time_series",
    table="sensor_readings",
    key_columns=["sensor_id","timestamp","location"],
    tracked_columns=["temperature","status"]
)

chatbot = SCD2Chatbot()
response = chatbot.chat("compare data between day_id 120 and 150", schema)
print(response.sql_query)
print(response.python_code)
```

## TableSchema
Defines metadata for generating correct qualified names.

## Query Type Patterns (Illustrative)
Point-in-time:
```sql
SELECT * FROM catalog.schema.table
WHERE start_day_id <= {asof} AND (end_day_id = 0 OR end_day_id > {asof});
```

Comparison (pseudo):
```sql
WITH a AS (... asof A ...),
     b AS (... asof B ...)
SELECT ...
FROM a FULL OUTER JOIN b ON key equality;
```

## Extending
Subclass SCD2Chatbot and override parse_user_input or augment pattern registry.

## Best Practices
- Provide all key columns for correct joins.
- Keep tracked columns limited—affects semantic change detection.
- Review generated SQL for large production queries before running.

## Error Handling
If ambiguity arises, chatbot may produce a CUSTOM template. Refine the wording (e.g., “point-in-time” or “compare …”).

## Further Examples
See examples notebooks and tests.

For advanced pattern reference and extension examples, consult original expanded version (archived if needed).