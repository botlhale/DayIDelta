# Unity Catalog Guide

This document contains guidance specific to Azure Databricks Unity Catalog usage with DayIDelta.

(Original content migrated from README_Unity_Catalog.md and streamlined.)

## Key Differences vs Fabric
| Feature | Fabric | Unity Catalog |
|---------|--------|---------------|
| Namespace | schema.table | catalog.schema.table |
| Function Signature | dest_sch, dest_tb_obs | dest_catalog, dest_sch, dest_tb_obs |
| Governance | Lakehouse attach | Unity Catalog |
| Setup | Fabric environment | Spark session config |

## Spark Session Configuration
```python
from pyspark.sql import SparkSession

spark = (SparkSession.builder
         .appName("DayIDelta Unity Catalog")
         .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
         .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
         .getOrCreate())
```

## Environment Setup
```python
from dayidelta import setup_unity_catalog_environment
setup_unity_catalog_environment(spark, "my_catalog", "time_series")
```

## Basic Example
```python
from dayidelta import DayIDelta
from datetime import datetime

df = spark.createDataFrame([
    ("SENSOR_001", datetime(2024, 1, 15, 10, 0), "FACTORY_A", 23.5),
    ("SENSOR_001", datetime(2024, 1, 15, 10, 30), "FACTORY_A", 24.1),
], ["sensor_id", "timestamp", "location", "temperature"])

DayIDelta(
    new_data_df=df,
    key_cols=["sensor_id", "timestamp", "location"],
    tracked_cols=["temperature"],
    dest_catalog="my_catalog",
    dest_sch="time_series",
    dest_tb_obs="sensor_readings"
)
```

## Multi-Source Example
```python
source_a = spark.createDataFrame([...], ["id","timestamp","source","value"])
source_b = spark.createDataFrame([...], ["id","timestamp","source","value"])

for frame in [source_a, source_b]:
    DayIDelta(
        new_data_df=frame,
        key_cols=["id","timestamp","source"],
        tracked_cols=["value"],
        dest_catalog="analytics",
        dest_sch="staging",
        dest_tb_obs="multi_source_obs"
    )
```

## Querying
Current records:
```sql
SELECT * FROM my_catalog.time_series.sensor_readings WHERE end_day_id = 0;
```

Point-in-time:
```sql
SELECT * FROM my_catalog.time_series.sensor_readings
WHERE start_day_id <= :asof AND (end_day_id = 0 OR end_day_id > :asof);
```

Change counts:
```sql
SELECT d.day_id, COUNT(o.start_day_id) AS created
FROM my_catalog.time_series.dim_day d
LEFT JOIN my_catalog.time_series.sensor_readings o
  ON d.day_id = o.start_day_id
GROUP BY d.day_id;
```

## Performance Tips
- Consider partitioning large tables (e.g., by a high-cardinality key or original event date if query patterns justify).
- Periodically OPTIMIZE + VACUUM according to retention policies.

## Migration from Fabric
Add `dest_catalog` parameter and qualify table references with catalog prefix.

## Troubleshooting
- Permission errors: verify Unity Catalog grants.
- Schema mismatch: new batch must align with existing Delta schema.
- Performance: inspect Delta transaction log size; consider compaction.