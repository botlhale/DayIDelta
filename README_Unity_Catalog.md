# DayIDelta: Azure Databricks Unity Catalog Implementation

This document provides specific guidance for using DayIDelta with Azure Databricks Unity Catalog.

## Unity Catalog Overview

Azure Databricks Unity Catalog provides a unified governance solution for data and AI assets. It uses a three-level namespace: `catalog.schema.table`, which differs from the traditional two-level `schema.table` approach.

## Key Differences from Microsoft Fabric

| Feature | Microsoft Fabric | Azure Databricks Unity Catalog |
|---------|------------------|------------------------------|
| Namespace | `schema.table` | `catalog.schema.table` |
| Function Signature | `DayIDelta(..., dest_sch, dest_tb_obs)` | `DayIDelta(..., dest_catalog, dest_sch, dest_tb_obs)` |
| Table Management | Lakehouse attachment | Unity Catalog governance |
| Environment Setup | Fabric Environment | Spark session with Delta extensions |

## Setup Instructions

### 1. Prerequisites

- Azure Databricks workspace with Unity Catalog enabled
- Spark 3.x with Delta Lake extensions
- Python 3.8+
- Appropriate permissions to create/modify tables in Unity Catalog

### 2. Spark Configuration

Ensure your Spark session has Delta Lake extensions enabled:

```python
from pyspark.sql import SparkSession

spark = SparkSession.builder \
    .appName("DayIDelta Unity Catalog") \
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
    .getOrCreate()
```

### 3. Unity Catalog Setup

```python
from DayIDelta import DayIDelta, setup_unity_catalog_environment

# Setup catalog and schema (creates if they don't exist)
catalog_name = "my_catalog"
schema_name = "time_series"

setup_unity_catalog_environment(spark, catalog_name, schema_name)
```

## Usage Examples

### Basic Usage

```python
from DayIDelta import DayIDelta
from pyspark.sql import SparkSession
from datetime import datetime, timedelta

# Initialize Spark session
spark = SparkSession.builder \
    .appName("DayIDelta Example") \
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
    .getOrCreate()

# Example time series data
df = spark.createDataFrame([
    ("SENSOR_001", datetime(2024, 1, 15, 10, 0), "FACTORY_A", 23.5),
    ("SENSOR_002", datetime(2024, 1, 15, 10, 0), "FACTORY_A", 18.2),
    ("SENSOR_001", datetime(2024, 1, 15, 10, 30), "FACTORY_A", 24.1),
], ["sensor_id", "timestamp", "location", "temperature"])

# Process with DayIDelta
DayIDelta(
    new_data_df=df,
    key_cols=["sensor_id", "timestamp", "location"],
    tracked_cols=["temperature"],
    dest_catalog="my_catalog",
    dest_sch="time_series",
    dest_tb_obs="sensor_readings"
)

# Query results
spark.table("my_catalog.time_series.sensor_readings").show()
```

### Advanced Multi-Source Example

```python
# Multi-source data with different processing batches
source_a_data = spark.createDataFrame([
    ("TS1", datetime.now(), "SOURCE_A", 1.11),
    ("TS2", datetime.now(), "SOURCE_A", 2.22),
], ["time_series_name", "datetime", "data_source", "value"])

source_b_data = spark.createDataFrame([
    ("TS1", datetime.now(), "SOURCE_B", 3.33),
    ("TS3", datetime.now(), "SOURCE_B", 4.44),
], ["time_series_name", "datetime", "data_source", "value"])

# Process each source separately
DayIDelta(
    new_data_df=source_a_data,
    key_cols=["time_series_name", "datetime", "data_source"],
    tracked_cols=["value"],
    dest_catalog="analytics",
    dest_sch="staging",
    dest_tb_obs="multi_source_obs"
)

DayIDelta(
    new_data_df=source_b_data,
    key_cols=["time_series_name", "datetime", "data_source"],
    tracked_cols=["value"],
    dest_catalog="analytics",
    dest_sch="staging", 
    dest_tb_obs="multi_source_obs"
)
```

## Function Reference

### DayIDelta Function

```python
DayIDelta(
    new_data_df,        # pyspark.sql.DataFrame: New batch data
    key_cols,           # list: Columns forming unique key
    tracked_cols,       # list: Columns whose changes trigger SCD2
    dest_catalog,       # str: Unity Catalog catalog name
    dest_sch,           # str: Schema name
    dest_tb_obs,        # str: Observation table name
    dim_day_table=None  # str, optional: Day dimension table name
)
```

### Utility Functions

```python
# Setup Unity Catalog environment
setup_unity_catalog_environment(spark, catalog_name, schema_name)

# Create catalog if not exists
create_unity_catalog_if_not_exists(spark, catalog_name)

# Create schema if not exists  
create_schema_if_not_exists(spark, catalog_name, schema_name)
```

## Querying SCD2 Data

### Active Records Only
```sql
SELECT * FROM my_catalog.time_series.sensor_readings 
WHERE end_day_id = 0
```

### Point-in-Time Query
```sql
SELECT * FROM my_catalog.time_series.sensor_readings
WHERE start_day_id <= 3 AND (end_day_id > 3 OR end_day_id = 0)
```

### Full History for Specific Key
```sql
SELECT *, 
       CASE WHEN end_day_id = 0 THEN 'ACTIVE' ELSE 'CLOSED' END as status
FROM my_catalog.time_series.sensor_readings
WHERE sensor_id = 'SENSOR_001'
ORDER BY timestamp, start_day_id
```

### Changes by Day
```sql
SELECT 
    d.day_id,
    d.date,
    COUNT(o.start_day_id) as records_created
FROM my_catalog.time_series.dim_day d
LEFT JOIN my_catalog.time_series.sensor_readings o ON d.day_id = o.start_day_id
GROUP BY d.day_id, d.date
ORDER BY d.day_id
```

## Best Practices

### 1. Catalog and Schema Organization
- Use meaningful catalog names that reflect your organization structure
- Group related tables in the same schema
- Follow consistent naming conventions

### 2. Performance Optimization
- Consider partitioning large tables by `start_day_id` or key columns
- Use `OPTIMIZE` and `VACUUM` commands for Delta Lake maintenance
- Monitor table statistics and file sizes

### 3. Security and Governance
- Set appropriate permissions at catalog, schema, and table levels
- Use Unity Catalog lineage features to track data dependencies
- Document table schemas and business logic

### 4. Monitoring and Maintenance
```python
# Optimize Delta tables periodically
spark.sql("OPTIMIZE my_catalog.time_series.sensor_readings")

# Vacuum old files (after ensuring no time travel queries needed)
spark.sql("VACUUM my_catalog.time_series.sensor_readings RETAIN 168 HOURS")
```

## Migration from Microsoft Fabric

If migrating from Microsoft Fabric implementation:

1. **Update function calls**: Add `dest_catalog` parameter
2. **Update table references**: Change from `schema.table` to `catalog.schema.table`  
3. **Update Spark configuration**: Ensure Delta Lake extensions are enabled
4. **Test thoroughly**: Validate SCD2 behavior matches expectations

### Example Migration

**Before (Fabric):**
```python
DayIDelta(
    new_data_df=df,
    key_cols=["TIME_SERIES_NAME", "DATETIME", "DATA_SOURCE"],
    tracked_cols=["VALUE"],
    dest_sch="dbo",
    dest_tb_obs="dayidelta_obs"
)
```

**After (Unity Catalog):**
```python
DayIDelta(
    new_data_df=df,
    key_cols=["TIME_SERIES_NAME", "DATETIME", "DATA_SOURCE"],
    tracked_cols=["VALUE"],
    dest_catalog="lakehouse",
    dest_sch="dbo", 
    dest_tb_obs="dayidelta_obs"
)
```

## Troubleshooting

### Common Issues

1. **Permission Errors**: Ensure proper Unity Catalog permissions
2. **Table Not Found**: Verify catalog.schema.table naming
3. **Schema Mismatch**: Ensure new data matches existing table schema
4. **Performance Issues**: Consider table optimization and partitioning

### Debug Mode
Enable detailed logging:
```python
import logging
logging.basicConfig(level=logging.INFO)
```

## Testing

Run the comprehensive test suite:
```python
python unity_catalog_test.py
```

Or use the interactive example notebook:
```python
python unity_catalog_example.py
```