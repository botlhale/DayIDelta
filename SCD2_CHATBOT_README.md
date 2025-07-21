# SCD2 AI Chatbot Documentation

![DayIDelta logo](dayidelta_logo.png)

## Overview

The SCD2 AI Chatbot is an intelligent assistant designed to help users generate SQL statements and Python code for querying SCD2 (Slowly Changing Dimension Type 2) tables created by DayIDelta. The chatbot understands natural language queries and automatically generates appropriate database queries and PySpark code.

## Features

### 🤖 Natural Language Understanding
- Parses user queries in natural language
- Identifies query intent and patterns
- Extracts parameters like time periods, table names, and filters

### 📊 Query Pattern Support
- **Current Data Queries**: Active records (end_day_id = 0)
- **Point-in-Time Queries**: Historical snapshots at specific dates
- **Time Series History**: Changes over specified periods
- **Data Comparison**: Differences between two time periods
- **Custom Queries**: Template generation for complex needs

### 🔧 Multi-Output Generation
- **SQL Queries**: Ready-to-run SQL statements
- **Python Code**: PySpark code with proper imports and setup
- **Explanations**: Clear descriptions of what each query does

### 🏗️ Platform Support
- Azure Databricks Unity Catalog (3-level naming)
- Microsoft Fabric
- Azure Synapse Analytics
- Other Spark 3.x with Delta Lake environments

## Quick Start

### Installation

The chatbot is included with DayIDelta. No additional installation required.

```python
from scd2_chatbot import SCD2Chatbot, TableSchema, quick_query
```

### Basic Usage

```python
# Define your SCD2 table schema
table_schema = TableSchema(
    catalog="my_catalog",
    schema="time_series", 
    table="sensor_readings",
    key_columns=["sensor_id", "timestamp", "location"],
    tracked_columns=["temperature", "humidity", "status"]
)

# Create chatbot instance
chatbot = SCD2Chatbot()

# Ask natural language questions
response = chatbot.chat("show me all current data", table_schema)

print("SQL Query:")
print(response.sql_query)
print("\nPython Code:")
print(response.python_code)
```

### Quick Query Interface

For simple one-off queries:

```python
response = quick_query(
    user_input="show me current temperature readings",
    catalog="sensors",
    schema="production",
    table="temperature_log",
    key_columns=["sensor_id", "timestamp"],
    tracked_columns=["temperature"]
)
```

## Supported Query Types

### 1. Current Data Queries

Get the latest/active version of all records.

**Example User Inputs:**
- "show me all current data"
- "what is the current state of the data?"
- "give me the latest records"
- "show active data only"

**Generated SQL Pattern:**
```sql
SELECT sensor_id, timestamp, temperature, status
FROM my_catalog.sensors.readings
WHERE end_day_id = 0
ORDER BY sensor_id, timestamp
```

### 2. Point-in-Time Historical Queries

See how data looked at a specific point in time.

**Example User Inputs:**
- "how did the data look in 2024 Q4?"
- "show me data as of day_id 150"
- "what was the state 30 days ago?"
- "point-in-time view for December 2024"

**Generated SQL Pattern:**
```sql
SELECT sensor_id, timestamp, temperature, status
FROM my_catalog.sensors.readings
WHERE start_day_id <= 150 
  AND (end_day_id = 0 OR end_day_id > 150)
ORDER BY sensor_id, timestamp
```

### 3. Time Series History Queries

Track changes and evolution over a specific period.

**Example User Inputs:**
- "show me the history for 2023 Q2"
- "track changes over the last quarter"
- "evolution of sensor data in Q1"
- "time series history from day_id 100 to 200"

**Generated SQL Pattern:**
```sql
SELECT sensor_id, timestamp, temperature, status,
       start_day_id, end_day_id
FROM my_catalog.sensors.readings
WHERE start_day_id <= 200 
  AND (end_day_id = 0 OR end_day_id > 100)
ORDER BY sensor_id, timestamp, start_day_id
```

### 4. Data Comparison Queries

Compare data between two different time periods.

**Example User Inputs:**
- "show differences between today and 7 days ago"
- "compare data between day_id 100 and day_id 200"
- "what changed from last month to this month?"
- "delta between Q3 and Q4 2024"

**Generated SQL Pattern:**
```sql
WITH data_day_100 AS (
    SELECT sensor_id, timestamp, temperature, status
    FROM my_catalog.sensors.readings
    WHERE start_day_id <= 100 AND (end_day_id = 0 OR end_day_id > 100)
),
data_day_200 AS (
    SELECT sensor_id, timestamp, temperature, status  
    FROM my_catalog.sensors.readings
    WHERE start_day_id <= 200 AND (end_day_id = 0 OR end_day_id > 200)
)
SELECT t1.sensor_id, t1.timestamp,
       t1.temperature as temperature_day_100,
       t2.temperature as temperature_day_200,
       CASE WHEN t1.temperature != t2.temperature THEN 'CHANGED' ELSE 'SAME' END as temperature_status
FROM data_day_100 t1
FULL OUTER JOIN data_day_200 t2 ON t1.sensor_id = t2.sensor_id AND t1.timestamp = t2.timestamp
```

## Advanced Usage

### Custom Table Schema

```python
from scd2_chatbot import TableSchema

# Complex schema with multiple column types
schema = TableSchema(
    catalog="production",
    schema="iot_data",
    table="device_metrics",
    key_columns=["device_id", "metric_type", "timestamp", "region"],
    tracked_columns=["value", "quality_score", "alarm_status"],
    other_columns=["metadata", "processing_notes"]
)
```

### Filtering and Customization

```python
# The chatbot can handle additional context
response = chatbot.chat(
    "show me current temperature data for sensor ABC123",
    table_schema
)

# You can also provide filters in the generated code
sql_with_filter = response.sql_query.replace(
    "WHERE end_day_id = 0",
    "WHERE end_day_id = 0 AND sensor_id = 'ABC123'"
)
```

### Integration with DayIDelta

```python
from DayIDelta import DayIDelta, setup_unity_catalog_environment
from scd2_chatbot import SCD2Chatbot, TableSchema

# Setup environment
setup_unity_catalog_environment(spark, "my_catalog", "sensors")

# Process data with DayIDelta
DayIDelta(
    new_data_df=sensor_batch,
    key_cols=["sensor_id", "timestamp"],
    tracked_cols=["temperature"],
    dest_catalog="my_catalog",
    dest_sch="sensors", 
    dest_tb_obs="temperature_readings"
)

# Query the data with chatbot
schema = TableSchema(
    catalog="my_catalog",
    schema="sensors",
    table="temperature_readings",
    key_columns=["sensor_id", "timestamp"],
    tracked_columns=["temperature"]
)

chatbot = SCD2Chatbot()
response = chatbot.chat("show me current readings", schema)

# Execute the generated query
result = spark.sql(response.sql_query)
result.show()
```

## API Reference

### Classes

#### `TableSchema`
Defines the structure of an SCD2 table.

**Parameters:**
- `catalog` (str): Unity Catalog catalog name
- `schema` (str): Schema name within the catalog  
- `table` (str): Table name
- `key_columns` (List[str]): Columns that form the unique key
- `tracked_columns` (List[str]): Columns whose changes trigger SCD2 updates
- `other_columns` (List[str], optional): Additional data columns

**Properties:**
- `full_table_name`: Returns catalog.schema.table
- `all_data_columns`: Returns all data columns (excludes SCD2 system columns)

#### `SCD2Chatbot`
Main chatbot class for generating queries.

**Methods:**
- `chat(user_input: str, table_schema: TableSchema) -> QueryResponse`
- `parse_user_input(user_input: str, table_schema: TableSchema) -> QueryRequest`
- `generate_response(request: QueryRequest) -> QueryResponse`

#### `QueryResponse`
Contains the generated SQL, Python code, and explanation.

**Attributes:**
- `sql_query` (str): Generated SQL statement
- `python_code` (str): Generated PySpark code
- `explanation` (str): Human-readable explanation
- `query_type` (QueryType): Detected query type
- `parameters` (Dict): Extracted parameters

### Functions

#### `quick_query()`
Convenience function for simple queries.

```python
quick_query(
    user_input: str,
    catalog: str,
    schema: str, 
    table: str,
    key_columns: List[str],
    tracked_columns: List[str],
    other_columns: List[str] = None
) -> QueryResponse
```

## Time Period Mapping

The chatbot supports various ways to specify time periods:

### Quarters
- "2024 Q4" → Maps to quarter-based day_id range
- "Q1 2023" → First quarter of 2023
- "fourth quarter" → Current year Q4

### Relative Time
- "30 days ago" → Current day_id minus 30
- "last month" → Previous month's day_id range
- "yesterday" → Previous day_id

### Explicit Day IDs
- "day_id 150" → Exact day_id value
- "from day 100 to day 200" → Specific range

### Current Time
- "today" → Latest day_id
- "current" → Active records (end_day_id = 0)
- "now" → Latest day_id

## Best Practices

### 1. Schema Definition
- Include all relevant key columns for proper uniqueness
- Only specify tracked columns that should trigger SCD2 updates
- Use descriptive column names for better query generation

### 2. Query Optimization
- Review generated SQL before executing on large tables
- Add appropriate filters for your specific use case
- Consider indexing on start_day_id and end_day_id columns

### 3. Error Handling
```python
try:
    response = chatbot.chat(user_input, schema)
    if response.query_type == QueryType.CUSTOM:
        print("Please review the generated template")
    result = spark.sql(response.sql_query)
except Exception as e:
    print(f"Error executing query: {e}")
```

### 4. Performance Considerations
- Use filters to limit data range for large historical queries
- Consider partitioning strategies for your SCD2 tables
- Monitor query performance and optimize as needed

## Integration Examples

### Jupyter Notebook Integration

```python
# Cell 1: Setup
from scd2_chatbot import SCD2Chatbot, TableSchema
from pyspark.sql import SparkSession

spark = SparkSession.builder.getOrCreate()
chatbot = SCD2Chatbot()

# Cell 2: Define Schema
schema = TableSchema(
    catalog="analytics",
    schema="sales",
    table="customer_metrics",
    key_columns=["customer_id", "date"],
    tracked_columns=["revenue", "status", "segment"]
)

# Cell 3: Interactive Query
user_query = input("What would you like to know about the data? ")
response = chatbot.chat(user_query, schema)

print("Generated Query:")
print(response.sql_query)
print("\nExecuting...")
result = spark.sql(response.sql_query)
result.show(20)
```

### Command Line Interface

```python
#!/usr/bin/env python3
import sys
from scd2_chatbot import quick_query

if __name__ == "__main__":
    if len(sys.argv) < 6:
        print("Usage: python query_helper.py <catalog> <schema> <table> <key_cols> <tracked_cols> [query]")
        sys.exit(1)
    
    catalog, schema, table = sys.argv[1:4]
    key_cols = sys.argv[4].split(',')
    tracked_cols = sys.argv[5].split(',')
    
    if len(sys.argv) > 6:
        query = ' '.join(sys.argv[6:])
    else:
        query = input("Enter your query: ")
    
    response = quick_query(query, catalog, schema, table, key_cols, tracked_cols)
    print(f"\n{response.explanation}\n")
    print("SQL:")
    print(response.sql_query)
    print("\nPython:")
    print(response.python_code)
```

### Web API Integration

```python
from flask import Flask, request, jsonify
from scd2_chatbot import SCD2Chatbot, TableSchema

app = Flask(__name__)
chatbot = SCD2Chatbot()

@app.route('/query', methods=['POST'])
def generate_query():
    data = request.json
    
    schema = TableSchema(
        catalog=data['catalog'],
        schema=data['schema'],
        table=data['table'],
        key_columns=data['key_columns'],
        tracked_columns=data['tracked_columns']
    )
    
    response = chatbot.chat(data['user_input'], schema)
    
    return jsonify({
        'sql_query': response.sql_query,
        'python_code': response.python_code,
        'explanation': response.explanation,
        'query_type': response.query_type.value
    })

if __name__ == '__main__':
    app.run(debug=True)
```

## Troubleshooting

### Common Issues

1. **"Could not determine query type"**
   - Solution: Use more specific keywords like "current", "historical", "compare"
   - Example: Change "show data" to "show current data"

2. **"Invalid table name"**
   - Solution: Verify catalog.schema.table exists and is accessible
   - Check Unity Catalog permissions

3. **"Missing time parameters"**
   - Solution: Be more specific about time periods
   - Example: "show data for 2024 Q3" instead of "show old data"

4. **"Generated SQL doesn't work"**
   - Solution: Review the generated SQL and adjust filters/conditions
   - Check if day_id mappings are correct for your environment

### Debug Mode

```python
# Enable verbose output
import logging
logging.basicConfig(level=logging.DEBUG)

response = chatbot.chat("your query", schema)
print(f"Detected type: {response.query_type}")
print(f"Parameters: {response.parameters}")
```

## Extending the Chatbot

### Adding Custom Patterns

```python
class CustomSCD2Chatbot(SCD2Chatbot):
    def parse_user_input(self, user_input, table_schema):
        # Add your custom patterns
        if "weekly report" in user_input.lower():
            # Custom logic for weekly reports
            pass
        
        return super().parse_user_input(user_input, table_schema)
```

### Custom Query Templates

```python
# Extend SCD2QueryPatterns
class CustomQueryPatterns(SCD2QueryPatterns):
    @staticmethod
    def weekly_summary_sql(table_schema, week_start_day_id):
        return f"""
        SELECT DATE_TRUNC('week', dim_day.date) as week,
               COUNT(*) as record_count,
               COUNT(DISTINCT {table_schema.key_columns[0]}) as unique_keys
        FROM {table_schema.full_table_name} scd
        JOIN {table_schema.catalog}.{table_schema.schema}.dim_day dim_day 
            ON scd.start_day_id = dim_day.day_id
        WHERE scd.start_day_id >= {week_start_day_id}
        GROUP BY DATE_TRUNC('week', dim_day.date)
        ORDER BY week
        """
```

## Contributing

We welcome contributions to improve the chatbot! Areas for enhancement:

1. **Natural Language Processing**: Better understanding of complex queries
2. **Time Period Parsing**: More sophisticated date/time recognition
3. **Query Optimization**: Smarter SQL generation for performance
4. **Additional Patterns**: Support for more SCD2 query types
5. **Error Handling**: Better error messages and recovery

Please submit pull requests with tests and documentation.

## License

MIT License - Same as DayIDelta project.

---

For more information about DayIDelta and SCD2 functionality, see the main [README.md](README.md) and [Unity Catalog documentation](README_Unity_Catalog.md).