![DayIDelta logo](dayidelta_logo.png)

# DayIDelta
Production-grade SCD2 for time series data in Delta Lake and Spark platforms (Microsoft Fabric, Azure Databricks Unity Catalog, Synapse, generic Spark 3.x)—with an AI assistant for point-in-time and comparative querying.

## Why DayIDelta?
DayIDelta simplifies maintaining Slowly Changing Dimension Type 2 (SCD2) history for event-based time series observation tables. Traditional SCD2 patterns often:
- Depend on fragile MERGE statements
- Confuse event-time with batch-time lineage
- Expire records too broadly during partial-source loads
- Introduce unnecessary technical columns

DayIDelta solves these with a lean, deterministic approach:
- Batch-scoped surrogate day dimension (dim_day) decouples lineage from event timestamps
- Selective multi-source expiration (only closes rows for sources present in the incoming batch)
- Minimal footprint: only your key + tracked columns plus start_day_id/end_day_id
- No event-time leakage: event timestamps never drive SCD2 open/close logic
- AI-powered chatbot generates SQL + PySpark for point-in-time & comparison analytics

## Key Differentiators
- Deterministic insert/close logic (no MERGE)
- Cross-platform (Fabric, Unity Catalog, Synapse, generic Spark)
- Point-in-time, “current”, historical span & comparison query generation via built-in chatbot
- Extensible: pluggable environment setup helpers
- Clear auditability: batch surrogate key ensures reproducible lineage

## Install
(Planned if publishing to PyPI)
```bash
pip install dayidelta
```
Until published, copy `DayIDelta.py` (or `src/dayidelta/`) into your environment.

## Quickstart
```python
from DayIDelta import DayIDelta
from pyspark.sql import SparkSession
from datetime import datetime

spark = SparkSession.builder.getOrCreate()

batch1 = spark.createDataFrame([
    ("TS1", datetime(2025, 6, 23, 0, 0), "SRC1", 1.11),
    ("TS2", datetime(2025, 6, 23, 4, 0), "SRC1", 2.22),
], ["TIME_SERIES_NAME", "DATETIME", "DATA_SOURCE", "VALUE"])

DayIDelta(
    new_data_df=batch1,
    key_cols=["TIME_SERIES_NAME", "DATETIME", "DATA_SOURCE"],
    tracked_cols=["VALUE"],
    dest_sch="dbo",
    dest_tb_obs="dayidelta_obs"
)

spark.table("dbo.dayidelta_obs").show()
```

## Platform Guides
- Microsoft Fabric (env attach) – see docs/unity_catalog.md (Fabric section)
- Azure Databricks Unity Catalog – environment & catalog setup
- Synapse / Generic Spark – standard Spark 3.x with Delta

Example Unity Catalog setup:
```python
from DayIDelta import DayIDelta, setup_unity_catalog_environment
from pyspark.sql import SparkSession
spark = (SparkSession.builder
         .appName("DayIDelta UC")
         .config("spark.sql.extensions","io.delta.sql.DeltaSparkSessionExtension")
         .config("spark.sql.catalog.spark_catalog","org.apache.spark.sql.delta.catalog.DeltaCatalog")
         .getOrCreate())

setup_unity_catalog_environment(spark, "my_catalog", "time_series")
```

## Core Concepts
- dim_day: Holds monotonically increasing surrogate day_id per batch load date.
- start_day_id / end_day_id: Define active window in batch (not event-time) space.
- Selective expiration: Only closes records for sources represented in the incoming batch.
- Tracked columns: Only specified columns trigger a new version on value change.

## AI Chatbot (SCD2 Query Assistant) 🤖
Generate point-in-time, current, historical range, and comparison queries in both SQL & PySpark.
```python
from scd2_chatbot import SCD2Chatbot, TableSchema
schema = TableSchema(
    catalog="my_catalog",
    schema="time_series",
    table="sensor_readings",
    key_columns=["sensor_id","timestamp","location"],
    tracked_columns=["temperature","status"]
)
chatbot = SCD2Chatbot()
resp = chatbot.chat("compare current data with data 7 days ago", schema)
print(resp.sql_query)
print(resp.python_code)
```
See docs/chatbot.md for advanced patterns.

## Best Practices
- Call once per logical batch
- Include all natural key columns (including event timestamp if part of uniqueness)
- Keep tracked_cols tight to reduce unnecessary version churn
- Exclude day_id from source data schema (only start_day_id/end_day_id maintained)

## Performance & Design Rationale
| Concern | Approach | Benefit |
|---------|----------|---------|
| Merge complexity | Insert + close set logic | Fewer schema merge pitfalls |
| Event-time disorder | Surrogate batch day_id | Deterministic lineage |
| Multi-source loads | Source-filtered expiration | No accidental global closes |
| Query ergonomics | Chatbot-generated templates | Faster analytics adoption |

## FAQ (Extended FAQ in docs/faq.md)
Q: Why separate dim_day?  
A: Ensures monotonically increasing surrogate keys independent of event-time irregularities.

Q: Can I skip multi-source logic?  
A: Yes—omit source column from keys if unneeded; expiration becomes global for batch.

## Contributing
We welcome issues & PRs.
1. Fork & create a branch.
2. Install dev deps: `pip install -e .[dev]`
3. Run tests: `pytest -q`
4. Lint & format: `ruff check . && black .`
5. Submit PR with concise description.

See docs/contributing.md for full guidelines.

## Roadmap (Ideas)
- Packaging to PyPI
- Incremental streaming micro-batch variant
- Native point-in-time query helper functions
- Additional chatbot model providers

## License
MIT © 13668754 Canada Inc – see LICENSE for details.


Permission is hereby granted, free of charge, to any person obtaining a copy...
