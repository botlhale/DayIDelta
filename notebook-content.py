# Fabric notebook source

# METADATA ********************

# META {
# META   "kernel_info": {
# META     "name": "synapse_pyspark"
# META   },
# META   "dependencies": {
# META     "lakehouse": {
# META       "default_lakehouse": "d16865c1-7082-488b-9e99-1fa57d2bd7dd",
# META       "default_lakehouse_name": "lh_dayidelta",
# META       "default_lakehouse_workspace_id": "8d9aebdf-e3ac-4093-a522-de6175af2b77",
# META       "known_lakehouses": [
# META         {
# META           "id": "d16865c1-7082-488b-9e99-1fa57d2bd7dd"
# META         }
# META       ]
# META     },
# META     "environment": {
# META       "environmentId": "ce439d53-84de-8c0b-415b-033bd7bfc891",
# META       "workspaceId": "00000000-0000-0000-0000-000000000000"
# META     }
# META   }
# META }

# MARKDOWN ********************

# # DayIDelta Fabric Notebook Test
# 
# Test initial load functionality for DayIDelta in Microsoft Fabric.

# CELL ********************

dest_cata="lh_dayidelta"
dest_sch="dbo"
dest_tb_obs="dayidelta_obs"

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, TimestampType, DoubleType
from datetime import datetime, timedelta

def create_dim_day_table(spark, dest_sch):
    # Only day_id=0 initially, 9999-12-31
    dim_day_data = [(0, datetime(9999, 12, 31, 0, 0, 0))]
    dim_day_df = spark.createDataFrame(dim_day_data, ["day_id", "date"])
    dim_day_df.write.format("delta").mode("overwrite").saveAsTable(f"{dest_sch}.dim_day")

def cleanup_tables(spark, dest_sch, dest_tb_obs):
    spark.sql(f"DROP TABLE IF EXISTS {dest_sch}.dim_day")
    spark.sql(f"DROP TABLE IF EXISTS {dest_sch}.{dest_tb_obs}")

def print_table(spark, table_name, msg):
    print(f"\n=== {msg} ({table_name}) ===")
    df = spark.table(table_name)
    df.show(truncate=False)
    return df

def run_dayidelta_tests(DayIDelta):
    spark = SparkSession.builder.getOrCreate()
    cleanup_tables(spark, dest_sch, dest_tb_obs)
    create_dim_day_table(spark, dest_sch)

    # 1. Initial load: 2 rows, with different timestamps on the same day
    now1 = datetime.now()
    df1 = spark.createDataFrame([
        ("TS1", now1, "SRC1", 1.11),
        ("TS2", now1 + timedelta(hours=4), "SRC1", 2.22),
    ], ["TIME_SERIES_NAME", "DATETIME", "DATA_SOURCE", "VALUE"])
    DayIDelta(
        new_data_df=df1,
        key_cols=["TIME_SERIES_NAME", "DATETIME", "DATA_SOURCE"],
        tracked_cols=["VALUE"],
        dest_sch=dest_sch,
        dest_tb_obs=dest_tb_obs
    )
    df = print_table(spark, f"{dest_sch}.{dest_tb_obs}", "After initial timestamped load")
    assert df.count() == 2
    assert set(df.columns) >= {"start_day_id", "end_day_id"}
    assert "day_id" not in df.columns

    # 2. Insert a record for TS1 with a new DATETIME (should NOT affect old record, just insert a new one)
    now2 = datetime.now()  # should be a few seconds later
    df2 = spark.createDataFrame([
        ("TS1", now2, "SRC1", 1.99),  # new datetime, treated as new record
        ("TS2", now1 + timedelta(hours=4), "SRC1", 2.22),  # unchanged
    ], ["TIME_SERIES_NAME", "DATETIME", "DATA_SOURCE", "VALUE"])
    DayIDelta(
        new_data_df=df2,
        key_cols=["TIME_SERIES_NAME", "DATETIME", "DATA_SOURCE"],
        tracked_cols=["VALUE"],
        dest_sch=dest_sch,
        dest_tb_obs=dest_tb_obs
    )
    df = print_table(spark, f"{dest_sch}.{dest_tb_obs}", "After inserting TS1 with new DATETIME")
    # Should have 3 records: TS1 old (end_day_id set), TS1 new (end_day_id=0), TS2 (untouched)
    assert df.count() == 3
    ts1_old = df.filter(
        (df.TIME_SERIES_NAME == "TS1") & (df.DATETIME == now1)
    ).collect()
    ts1_new = df.filter(
        (df.TIME_SERIES_NAME == "TS1") & (df.DATETIME == now2)
    ).collect()
    ts2 = df.filter(
        (df.TIME_SERIES_NAME == "TS2")
    ).collect()
    assert len(ts1_old) == 1 and ts1_old[0]["end_day_id"] > 0
    assert len(ts1_new) == 1 and ts1_new[0]["end_day_id"] == 0
    assert len(ts2) == 1 and ts2[0]["end_day_id"] == 0

    # 3. Update a VALUE for TS1 at an existing DATETIME (should close old and insert new)
    df3 = spark.createDataFrame([
        ("TS1", now2, "SRC1", 2.22), # same key as previous, value changes
        ("TS2", now1 + timedelta(hours=4), "SRC1", 2.22),
        ("TS3", now1 + timedelta(days=1), "SRC1", 3.33),
    ], ["TIME_SERIES_NAME", "DATETIME", "DATA_SOURCE", "VALUE"])
    DayIDelta(
        new_data_df=df3,
        key_cols=["TIME_SERIES_NAME", "DATETIME", "DATA_SOURCE"],
        tracked_cols=["VALUE"],
        dest_sch=dest_sch,
        dest_tb_obs=dest_tb_obs
    )
    df = print_table(spark, f"{dest_sch}.{dest_tb_obs}", "After updating TS1 VALUE at existing DATETIME and new TS3")
    # The old TS1 with datetime=now2 should be closed, new one open
    closed = df.filter((df.TIME_SERIES_NAME == 'TS1') & (df.DATETIME == now2) & (df.end_day_id > 0)).collect()
    new_open = df.filter((df.TIME_SERIES_NAME == 'TS1') & (df.DATETIME == now2) & (df.end_day_id == 0)).collect()
    assert len(closed) == 1
    assert float(closed[0]["VALUE"]) == 1.99
    assert len(new_open) == 1
    assert float(new_open[0]["VALUE"]) == 2.22

    # 4. Logical delete TS2 (should close old record for TS2 only)
    df4 = spark.createDataFrame([
        ("TS1", now2, "SRC1", 2.22),
        ("TS3", now1 + timedelta(days=1), "SRC1", 3.33),
    ], ["TIME_SERIES_NAME", "DATETIME", "DATA_SOURCE", "VALUE"])
    DayIDelta(
        new_data_df=df4,
        key_cols=["TIME_SERIES_NAME", "DATETIME", "DATA_SOURCE"],
        tracked_cols=["VALUE"],
        dest_sch=dest_sch,
        dest_tb_obs=dest_tb_obs
    )
    df = print_table(spark, f"{dest_sch}.{dest_tb_obs}", "After logical delete of TS2")
    assert df.filter("end_day_id=0").count() == 2
    assert df.filter("TIME_SERIES_NAME = 'TS2' and end_day_id=0").count() == 0
    assert df.filter("TIME_SERIES_NAME = 'TS2' and end_day_id>0").count() > 0

    # 5. All NULL in tracked column: must specify schema!
    schema = StructType([
        StructField("TIME_SERIES_NAME", StringType(), True),
        StructField("DATETIME", TimestampType(), True),
        StructField("DATA_SOURCE", StringType(), True),
        StructField("VALUE", DoubleType(), True),
    ])
    df5 = spark.createDataFrame([
        ("TS1", now2, "SRC1", None),
        ("TS3", now1 + timedelta(days=1), "SRC1", None),
    ], schema=schema)
    DayIDelta(
        new_data_df=df5,
        key_cols=["TIME_SERIES_NAME", "DATETIME", "DATA_SOURCE"],
        tracked_cols=["VALUE"],
        dest_sch=dest_sch,
        dest_tb_obs=dest_tb_obs
    )
    df = print_table(spark, f"{dest_sch}.{dest_tb_obs}", "After input batch with all VALUE NULLs")
    active = df.filter("end_day_id=0")
    assert active.count() == 2
    assert all([row["VALUE"] is None for row in active.collect()])

    # 6. Multi-source: Only expire records for batch's DATA_SOURCE
    df6 = spark.createDataFrame([
        ("TS1", now2, "SRC2", 10.1),
        ("TS3", now1 + timedelta(days=1), "SRC2", 10.3),
    ], ["TIME_SERIES_NAME", "DATETIME", "DATA_SOURCE", "VALUE"])
    DayIDelta(
        new_data_df=df6,
        key_cols=["TIME_SERIES_NAME", "DATETIME", "DATA_SOURCE"],
        tracked_cols=["VALUE"],
        dest_sch=dest_sch,
        dest_tb_obs=dest_tb_obs
    )
    df = print_table(spark, f"{dest_sch}.{dest_tb_obs}", "After adding SRC2 keys (timestamped)")
    # Remove only TS1 from SRC2
    df7 = spark.createDataFrame([
        ("TS3", now1 + timedelta(days=1), "SRC2", 10.3),
    ], ["TIME_SERIES_NAME", "DATETIME", "DATA_SOURCE", "VALUE"])
    DayIDelta(
        new_data_df=df7,
        key_cols=["TIME_SERIES_NAME", "DATETIME", "DATA_SOURCE"],
        tracked_cols=["VALUE"],
        dest_sch=dest_sch,
        dest_tb_obs=dest_tb_obs
    )
    df = print_table(spark, f"{dest_sch}.{dest_tb_obs}", "After logical delete of TS1 from SRC2 (timestamped)")
    active = df.filter("end_day_id=0")
    assert active.filter("TIME_SERIES_NAME='TS1' and DATA_SOURCE='SRC2'").count() == 0
    assert active.filter("TIME_SERIES_NAME='TS3' and DATA_SOURCE='SRC2'").count() == 1
    assert active.filter("DATA_SOURCE='SRC1'").count() == 2

    cleanup_tables(spark, dest_sch, dest_tb_obs)
    print("\nAll DayIDelta time series SCD2 tests passed successfully.")

if __name__ == "__main__":
    # Fabric: import from environment resource
    from env.DayIDelta import DayIDelta
    run_dayidelta_tests(DayIDelta)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
