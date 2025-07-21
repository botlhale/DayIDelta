"""
DayIDelta Unity Catalog Example

Example usage of DayIDelta for Azure Databricks Unity Catalog.
This notebook demonstrates the SCD2 functionality with Unity Catalog 3-level naming.
"""

# Databricks notebook source
# MAGIC %md
# MAGIC # DayIDelta: SCD2 for Time Series Data in Unity Catalog
# MAGIC 
# MAGIC This notebook demonstrates how to use DayIDelta for maintaining slowly-changing dimension (SCD2) history 
# MAGIC for time series data in Azure Databricks Unity Catalog.
# MAGIC 
# MAGIC ## Key Features for Unity Catalog:
# MAGIC - **Three-level namespace**: Uses catalog.schema.table naming convention
# MAGIC - **Delta Lake integration**: Fully compatible with Unity Catalog's Delta Lake tables
# MAGIC - **Batch-based SCD2**: Tracks changes using start_day_id and end_day_id
# MAGIC - **Multi-source support**: Handle data from multiple sources independently

# COMMAND ----------

# MAGIC %md
# MAGIC ## Setup and Configuration

# COMMAND ----------

from pyspark.sql import SparkSession
from datetime import datetime, timedelta
from DayIDelta import DayIDelta, setup_unity_catalog_environment

# Unity Catalog configuration
CATALOG_NAME = "main"  # Change to your catalog
SCHEMA_NAME = "default"  # Change to your schema
TABLE_NAME = "dayidelta_demo"

print(f"Using Unity Catalog: {CATALOG_NAME}.{SCHEMA_NAME}.{TABLE_NAME}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Initialize Unity Catalog Environment

# COMMAND ----------

# Setup the Unity Catalog environment (creates catalog/schema if needed)
setup_unity_catalog_environment(spark, CATALOG_NAME, SCHEMA_NAME)

# Clean up any existing demo table
spark.sql(f"DROP TABLE IF EXISTS {CATALOG_NAME}.{SCHEMA_NAME}.{TABLE_NAME}")
spark.sql(f"DROP TABLE IF EXISTS {CATALOG_NAME}.{SCHEMA_NAME}.dim_day")

print("Unity Catalog environment ready")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Example 1: Initial Load

# COMMAND ----------

# Create initial batch of time series data
base_time = datetime(2024, 1, 15, 10, 0, 0)

initial_data = [
    ("SENSOR_001", base_time, "FACTORY_A", 23.5),
    ("SENSOR_002", base_time, "FACTORY_A", 18.2),
    ("SENSOR_001", base_time + timedelta(minutes=30), "FACTORY_A", 24.1),
    ("SENSOR_003", base_time, "FACTORY_B", 31.7),
]

df_initial = spark.createDataFrame(initial_data, 
    ["sensor_id", "timestamp", "location", "temperature"])

print("Initial data:")
df_initial.show()

# Process initial load
DayIDelta(
    new_data_df=df_initial,
    key_cols=["sensor_id", "timestamp", "location"],
    tracked_cols=["temperature"],
    dest_catalog=CATALOG_NAME,
    dest_sch=SCHEMA_NAME,
    dest_tb_obs=TABLE_NAME
)

print("After initial load:")
spark.table(f"{CATALOG_NAME}.{SCHEMA_NAME}.{TABLE_NAME}").show()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Example 2: Update Values (SCD2 Behavior)

# COMMAND ----------

# Second batch with some updated temperatures
update_data = [
    ("SENSOR_001", base_time, "FACTORY_A", 25.0),  # Temperature changed
    ("SENSOR_002", base_time, "FACTORY_A", 18.2),  # No change
    ("SENSOR_001", base_time + timedelta(minutes=30), "FACTORY_A", 24.1),  # No change
    ("SENSOR_003", base_time, "FACTORY_B", 31.7),  # No change
    ("SENSOR_004", base_time, "FACTORY_B", 29.3),  # New sensor
]

df_update = spark.createDataFrame(update_data, 
    ["sensor_id", "timestamp", "location", "temperature"])

print("Update batch:")
df_update.show()

# Process update
DayIDelta(
    new_data_df=df_update,
    key_cols=["sensor_id", "timestamp", "location"],
    tracked_cols=["temperature"],
    dest_catalog=CATALOG_NAME,
    dest_sch=SCHEMA_NAME,
    dest_tb_obs=TABLE_NAME
)

print("After update (notice SCD2 history):")
result_df = spark.table(f"{CATALOG_NAME}.{SCHEMA_NAME}.{TABLE_NAME}")
result_df.orderBy("sensor_id", "timestamp", "start_day_id").show()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Example 3: Logical Deletes

# COMMAND ----------

# Third batch with some sensors missing (logical delete)
partial_data = [
    ("SENSOR_002", base_time, "FACTORY_A", 18.2),
    ("SENSOR_004", base_time, "FACTORY_B", 29.3),
    # SENSOR_001 records are missing - will be logically deleted
    # SENSOR_003 is missing - will be logically deleted
]

df_partial = spark.createDataFrame(partial_data, 
    ["sensor_id", "timestamp", "location", "temperature"])

print("Partial batch (some sensors missing):")
df_partial.show()

# Process partial batch
DayIDelta(
    new_data_df=df_partial,
    key_cols=["sensor_id", "timestamp", "location"],
    tracked_cols=["temperature"],
    dest_catalog=CATALOG_NAME,
    dest_sch=SCHEMA_NAME,
    dest_tb_obs=TABLE_NAME
)

print("After logical deletes (missing sensors are closed):")
result_df = spark.table(f"{CATALOG_NAME}.{SCHEMA_NAME}.{TABLE_NAME}")
result_df.orderBy("sensor_id", "timestamp", "start_day_id").show()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Example 4: Multi-Source Data

# COMMAND ----------

# Add data from a different location (multi-source scenario)
multi_source_data = [
    ("SENSOR_001", base_time, "FACTORY_C", 26.8),  # Same sensor, different location
    ("SENSOR_002", base_time, "FACTORY_C", 19.5),
    ("SENSOR_005", base_time, "FACTORY_C", 22.1),  # New sensor in new location
]

df_multi = spark.createDataFrame(multi_source_data, 
    ["sensor_id", "timestamp", "location", "temperature"])

print("Multi-source batch (FACTORY_C):")
df_multi.show()

# Process multi-source batch
DayIDelta(
    new_data_df=df_multi,
    key_cols=["sensor_id", "timestamp", "location"],
    tracked_cols=["temperature"],
    dest_catalog=CATALOG_NAME,
    dest_sch=SCHEMA_NAME,
    dest_tb_obs=TABLE_NAME
)

print("After adding multi-source data:")
result_df = spark.table(f"{CATALOG_NAME}.{SCHEMA_NAME}.{TABLE_NAME}")
result_df.orderBy("sensor_id", "timestamp", "location", "start_day_id").show()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Example 5: Querying SCD2 History

# COMMAND ----------

# Query active records only
print("=== ACTIVE RECORDS ONLY ===")
active_df = spark.sql(f"""
    SELECT sensor_id, timestamp, location, temperature, start_day_id
    FROM {CATALOG_NAME}.{SCHEMA_NAME}.{TABLE_NAME}
    WHERE end_day_id = 0
    ORDER BY sensor_id, timestamp, location
""")
active_df.show()

# Query full history for a specific sensor
print("=== FULL HISTORY FOR SENSOR_001 ===")
history_df = spark.sql(f"""
    SELECT sensor_id, timestamp, location, temperature, 
           start_day_id, end_day_id,
           CASE WHEN end_day_id = 0 THEN 'ACTIVE' ELSE 'CLOSED' END as status
    FROM {CATALOG_NAME}.{SCHEMA_NAME}.{TABLE_NAME}
    WHERE sensor_id = 'SENSOR_001'
    ORDER BY timestamp, location, start_day_id
""")
history_df.show()

# Query by time range using day dimension
print("=== DAY DIMENSION TABLE ===")
dim_day_df = spark.table(f"{CATALOG_NAME}.{SCHEMA_NAME}.dim_day")
dim_day_df.orderBy("day_id").show()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Example 6: Advanced Querying

# COMMAND ----------

# Point-in-time query: What was active at day_id = 2?
print("=== POINT-IN-TIME QUERY (day_id = 2) ===")
pit_df = spark.sql(f"""
    SELECT sensor_id, timestamp, location, temperature
    FROM {CATALOG_NAME}.{SCHEMA_NAME}.{TABLE_NAME}
    WHERE start_day_id <= 2 AND (end_day_id > 2 OR end_day_id = 0)
    ORDER BY sensor_id, timestamp, location
""")
pit_df.show()

# Changes by day
print("=== CHANGES BY DAY ===")
changes_df = spark.sql(f"""
    SELECT 
        start_day_id as day_id,
        COUNT(*) as records_created,
        COUNT(CASE WHEN end_day_id > 0 THEN 1 END) as records_closed
    FROM {CATALOG_NAME}.{SCHEMA_NAME}.{TABLE_NAME}
    GROUP BY start_day_id
    ORDER BY start_day_id
""")
changes_df.show()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Cleanup

# COMMAND ----------

# Uncomment to clean up demo tables
# spark.sql(f"DROP TABLE IF EXISTS {CATALOG_NAME}.{SCHEMA_NAME}.{TABLE_NAME}")
# spark.sql(f"DROP TABLE IF EXISTS {CATALOG_NAME}.{SCHEMA_NAME}.dim_day")
# print("Demo tables cleaned up")

print("Demo completed successfully!")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Summary
# MAGIC 
# MAGIC This notebook demonstrated:
# MAGIC 
# MAGIC 1. **Initial Load**: Creating SCD2 table with start_day_id/end_day_id
# MAGIC 2. **Updates**: How value changes create new records and close old ones
# MAGIC 3. **Logical Deletes**: Missing records from batch are automatically closed
# MAGIC 4. **Multi-Source**: Different data sources are handled independently
# MAGIC 5. **Querying**: Various ways to query SCD2 history data
# MAGIC 
# MAGIC ### Key Unity Catalog Benefits:
# MAGIC - **Governance**: Full data lineage and access control
# MAGIC - **Performance**: Optimized Delta Lake storage with Unity Catalog
# MAGIC - **Scalability**: Handles large time series datasets efficiently
# MAGIC - **Multi-workspace**: Tables accessible across Databricks workspaces