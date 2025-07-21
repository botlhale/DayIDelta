![DayIDelta logo](dayidelta_logo.png)

# DayIDelta: SCD2 for Time Series Data in Delta Lake

DayIDelta is a robust and production-ready Python function for maintaining **slowly-changing dimension (SCD2) history** for time series observation tables in Delta Lake. It is designed for Spark/PySpark workloads (including Databricks, Azure Synapse, and Microsoft Fabric) and tracks changes to time series records with proper batch-based SCD2 semantics, using a surrogate `start_day_id` and `end_day_id` for each record.

## Features

- **Handles SCD2 for event-based time series:** Each new batch of data gets a unique `start_day_id` for all new or changed records.
- **Batch-based tracking:** `start_day_id` and `end_day_id` represent when the record became active or was closed, based on the batch load (not the event timestamp).
- **Supports logical deletes:** Records missing from a batch are expired with the current `end_day_id`.
- **Multi-source support:** Expire records only for the data sources present in the incoming batch.
- **No schema merge errors:** Uses robust SQL insert logic for dimension table management.
- **No event-time leakage:** `day_id` from event time is only used for dimension management, not for SCD2 tracking.
- **No unnecessary columns:** Only user columns and `start_day_id`/`end_day_id` are present in the output.

---

## How It Works

- **Day Dimension (`dim_day`):** Maintains a surrogate key (`day_id`) for each batch processing day.
- **SCD2 Observation Table:** Each ingestion batch is assigned a new `day_id`, used as `start_day_id` for new/changed records and as `end_day_id` when closing old records.
- **Never merges on event time:** Records are closed and opened only based on the keys and tracked columns, and always using the batch's `day_id`.

---

## Usage

DayIDelta supports multiple Spark platforms. Choose the appropriate setup for your environment:

### Supported Platforms
- **Microsoft Fabric** (Original implementation)
- **Azure Databricks Unity Catalog** (New implementation)
- **Azure Synapse Analytics**
- **Other Spark 3.x with Delta Lake**

### 1. Prerequisites

- Spark 3.x with Delta Lake enabled
- Python 3.x
- The `DayIDelta.py` script in your working directory or accessible in your environment

### 2. Platform-Specific Setup

#### Microsoft Fabric Setup

If you are running in **Microsoft Fabric**:

1. **Create a Fabric Environment** (e.g., `env_dayidelta`).
2. Under **Resources**, add the file `DayIDelta.py` to the environment.
3. **Attach the Fabric Environment** to your Notebook or Test.
4. **Attach your Lakehouse** to the Notebook or Test.
5. **Import the function** as follows:

    ```python
    from env.DayIDelta import DayIDelta
    ```

#### Azure Databricks Unity Catalog Setup

If you are running on **Azure Databricks with Unity Catalog**:

1. **Upload `DayIDelta.py`** to your Databricks workspace or cluster libraries.
2. **Ensure Unity Catalog is enabled** and you have appropriate permissions.
3. **Configure your Spark session** with Delta Lake extensions:

    ```python
    from pyspark.sql import SparkSession
    
    spark = SparkSession.builder \
        .appName("DayIDelta Unity Catalog") \
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
        .getOrCreate()
    ```

4. **Import and setup** as follows:

    ```python
    from DayIDelta import DayIDelta, setup_unity_catalog_environment
    
    # Setup catalog and schema
    setup_unity_catalog_environment(spark, "my_catalog", "my_schema")
    ```

📖 **For detailed Unity Catalog instructions, see [README_Unity_Catalog.md](README_Unity_Catalog.md)**

---

### 3. Table Setup

- **Schema:** The SCD2 observation table (e.g., `dbo.dayidelta_obs`) should have your data columns plus `start_day_id`, `end_day_id` (both `IntegerType`).
- **Day dimension:** The script manages the `dim_day` table automatically (table name: `dim_day`).

---

### 4. Example Usage

#### Microsoft Fabric Example

```python
from env.DayIDelta import DayIDelta
from pyspark.sql import SparkSession
from datetime import datetime, timedelta

# Ensure your Lakehouse is attached!

spark = SparkSession.builder.getOrCreate()

# Example batch 1
df1 = spark.createDataFrame([
    ("TS1", datetime(2025, 6, 23, 0, 0), "SRC1", 1.11),
    ("TS2", datetime(2025, 6, 23, 4, 0), "SRC1", 2.22),
], ["TIME_SERIES_NAME", "DATETIME", "DATA_SOURCE", "VALUE"])

DayIDelta(
    new_data_df=df1,
    key_cols=["TIME_SERIES_NAME", "DATETIME", "DATA_SOURCE"],
    tracked_cols=["VALUE"],
    dest_sch="dbo",
    dest_tb_obs="dayidelta_obs"
)

# View results
spark.table("dbo.dayidelta_obs").show()
```

#### Unity Catalog Example

```python
from DayIDelta import DayIDelta, setup_unity_catalog_environment
from pyspark.sql import SparkSession
from datetime import datetime, timedelta

# Configure Spark for Unity Catalog
spark = SparkSession.builder \
    .appName("DayIDelta Unity Catalog") \
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
    .getOrCreate()

# Setup Unity Catalog environment
setup_unity_catalog_environment(spark, "my_catalog", "time_series")

# Example batch 1
df1 = spark.createDataFrame([
    ("TS1", datetime(2025, 6, 23, 0, 0), "SRC1", 1.11),
    ("TS2", datetime(2025, 6, 23, 4, 0), "SRC1", 2.22),
], ["TIME_SERIES_NAME", "DATETIME", "DATA_SOURCE", "VALUE"])

DayIDelta(
    new_data_df=df1,
    key_cols=["TIME_SERIES_NAME", "DATETIME", "DATA_SOURCE"],
    tracked_cols=["VALUE"],
    dest_catalog="my_catalog",
    dest_sch="time_series",
    dest_tb_obs="dayidelta_obs"
)

# View results
spark.table("my_catalog.time_series.dayidelta_obs").show()
```

---

## Best Practices

- **Call DayIDelta once per batch:** Each batch should represent a logical load or update.
- **Tracked columns:** Only specify the columns whose value changes should result in closing and opening SCD2 records.
- **Key columns:** Specify all columns that uniquely identify a row (typically including all primary keys and timestamp).
- **Do not include `day_id` as a data column:** Only `start_day_id` and `end_day_id` are tracked.

---

## FAQ

**Q: Why is there a separate `dim_day` table?**  
A: To ensure robust surrogate key management for SCD2, decoupled from event time, and to allow future extensibility (e.g., partitioning by day).

**Q: Why not use event time as `start_day_id`?**  
A: SCD2 should reflect batch load windows, not event timestamps. Event time may repeat or be out of order; batch date is always unique and increasing.

**Q: Can I use this for multi-source or multi-tenant data?**  
A: Yes! Just include `DATA_SOURCE` or similar in your key columns.

---

## Contributing

Pull requests and improvements are welcome! Please ensure changes include updated tests.

---

## License

MIT License

Copyright (c) 13668754 Canada Inc

Permission is hereby granted, free of charge, to any person obtaining a copy...