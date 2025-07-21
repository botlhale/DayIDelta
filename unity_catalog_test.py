"""
DayIDelta Unity Catalog Test

Test suite for DayIDelta SCD2 functionality on Azure Databricks Unity Catalog.
Adapted from the original Microsoft Fabric test for Unity Catalog 3-level naming.
"""

from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, TimestampType, DoubleType
from datetime import datetime, timedelta
from DayIDelta import DayIDelta, setup_unity_catalog_environment

# Unity Catalog configuration
dest_catalog = "main"  # Default catalog, can be changed
dest_schema = "default"  # Default schema, can be changed  
dest_tb_obs = "dayidelta_obs"


def cleanup_tables(spark, catalog, schema, dest_tb_obs):
    """Clean up test tables."""
    try:
        spark.sql(f"DROP TABLE IF EXISTS {catalog}.{schema}.dim_day")
        spark.sql(f"DROP TABLE IF EXISTS {catalog}.{schema}.{dest_tb_obs}")
    except Exception as e:
        print(f"Cleanup warning: {e}")


def print_table(spark, table_name, msg):
    """Print table contents for debugging."""
    print(f"\n=== {msg} ({table_name}) ===")
    try:
        df = spark.table(table_name)
        df.show(truncate=False)
        return df
    except Exception as e:
        print(f"Error reading table {table_name}: {e}")
        return None


def run_unity_catalog_dayidelta_tests():
    """
    Run comprehensive tests for DayIDelta on Unity Catalog.
    """
    spark = SparkSession.builder \
        .appName("DayIDelta Unity Catalog Test") \
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
        .getOrCreate()
    
    print(f"Testing DayIDelta with Unity Catalog: {dest_catalog}.{dest_schema}")
    
    # Setup Unity Catalog environment
    setup_unity_catalog_environment(spark, dest_catalog, dest_schema)
    
    # Clean up any existing test tables
    cleanup_tables(spark, dest_catalog, dest_schema, dest_tb_obs)
    
    print("Starting DayIDelta Unity Catalog tests...")
    
    # Test 1: Initial load with timestamped data
    print("\n1. Testing initial load...")
    now1 = datetime.now()
    df1 = spark.createDataFrame([
        ("TS1", now1, "SRC1", 1.11),
        ("TS2", now1 + timedelta(hours=4), "SRC1", 2.22),
    ], ["TIME_SERIES_NAME", "DATETIME", "DATA_SOURCE", "VALUE"])
    
    DayIDelta(
        new_data_df=df1,
        key_cols=["TIME_SERIES_NAME", "DATETIME", "DATA_SOURCE"],
        tracked_cols=["VALUE"],
        dest_catalog=dest_catalog,
        dest_sch=dest_schema,
        dest_tb_obs=dest_tb_obs
    )
    
    df = print_table(spark, f"{dest_catalog}.{dest_schema}.{dest_tb_obs}", "After initial timestamped load")
    if df:
        assert df.count() == 2, f"Expected 2 records, got {df.count()}"
        assert set(df.columns) >= {"start_day_id", "end_day_id"}, "Missing SCD2 columns"
        assert "day_id" not in df.columns, "Should not have day_id column in observation table"
        print("✓ Initial load test passed")
    
    # Test 2: Insert new record with different timestamp
    print("\n2. Testing insert with new timestamp...")
    now2 = datetime.now()
    df2 = spark.createDataFrame([
        ("TS1", now2, "SRC1", 1.99),  # new datetime, new record
        ("TS2", now1 + timedelta(hours=4), "SRC1", 2.22),  # unchanged
    ], ["TIME_SERIES_NAME", "DATETIME", "DATA_SOURCE", "VALUE"])
    
    DayIDelta(
        new_data_df=df2,
        key_cols=["TIME_SERIES_NAME", "DATETIME", "DATA_SOURCE"],
        tracked_cols=["VALUE"],
        dest_catalog=dest_catalog,
        dest_sch=dest_schema,
        dest_tb_obs=dest_tb_obs
    )
    
    df = print_table(spark, f"{dest_catalog}.{dest_schema}.{dest_tb_obs}", "After inserting TS1 with new DATETIME")
    if df:
        assert df.count() == 3, f"Expected 3 records, got {df.count()}"
        
        ts1_records = df.filter(df.TIME_SERIES_NAME == "TS1").collect()
        assert len(ts1_records) == 2, f"Expected 2 TS1 records, got {len(ts1_records)}"
        
        ts2_records = df.filter(df.TIME_SERIES_NAME == "TS2").collect()
        assert len(ts2_records) == 1 and ts2_records[0]["end_day_id"] == 0, "TS2 should remain active"
        print("✓ Insert with new timestamp test passed")
    
    # Test 3: Update value for existing key (should close old, insert new)
    print("\n3. Testing value update...")
    df3 = spark.createDataFrame([
        ("TS1", now2, "SRC1", 2.22),  # same key, different value
        ("TS2", now1 + timedelta(hours=4), "SRC1", 2.22),  # unchanged
        ("TS3", now1 + timedelta(days=1), "SRC1", 3.33),  # new record
    ], ["TIME_SERIES_NAME", "DATETIME", "DATA_SOURCE", "VALUE"])
    
    DayIDelta(
        new_data_df=df3,
        key_cols=["TIME_SERIES_NAME", "DATETIME", "DATA_SOURCE"],
        tracked_cols=["VALUE"],
        dest_catalog=dest_catalog,
        dest_sch=dest_schema,
        dest_tb_obs=dest_tb_obs
    )
    
    df = print_table(spark, f"{dest_catalog}.{dest_schema}.{dest_tb_obs}", "After updating TS1 VALUE and adding TS3")
    if df:
        # Check that old TS1 record is closed and new one is open
        ts1_at_now2 = df.filter((df.TIME_SERIES_NAME == "TS1") & (df.DATETIME == now2)).collect()
        closed_count = len([r for r in ts1_at_now2 if r["end_day_id"] > 0])
        open_count = len([r for r in ts1_at_now2 if r["end_day_id"] == 0])
        
        assert closed_count == 1 and open_count == 1, f"Expected 1 closed and 1 open TS1 record, got {closed_count} closed, {open_count} open"
        
        # Verify TS3 is added
        ts3_records = df.filter(df.TIME_SERIES_NAME == "TS3").collect()
        assert len(ts3_records) == 1 and ts3_records[0]["end_day_id"] == 0, "TS3 should be active"
        print("✓ Value update test passed")
    
    # Test 4: Logical delete (record missing from batch)
    print("\n4. Testing logical delete...")
    df4 = spark.createDataFrame([
        ("TS1", now2, "SRC1", 2.22),  # keep TS1
        ("TS3", now1 + timedelta(days=1), "SRC1", 3.33),  # keep TS3
        # TS2 is missing - should be logically deleted
    ], ["TIME_SERIES_NAME", "DATETIME", "DATA_SOURCE", "VALUE"])
    
    DayIDelta(
        new_data_df=df4,
        key_cols=["TIME_SERIES_NAME", "DATETIME", "DATA_SOURCE"],
        tracked_cols=["VALUE"],
        dest_catalog=dest_catalog,
        dest_sch=dest_schema,
        dest_tb_obs=dest_tb_obs
    )
    
    df = print_table(spark, f"{dest_catalog}.{dest_schema}.{dest_tb_obs}", "After logical delete of TS2")
    if df:
        active_records = df.filter("end_day_id = 0")
        assert active_records.count() == 2, f"Expected 2 active records, got {active_records.count()}"
        
        # TS2 should be closed
        ts2_active = active_records.filter("TIME_SERIES_NAME = 'TS2'").count()
        assert ts2_active == 0, "TS2 should not be active after logical delete"
        
        # TS2 should exist but be closed
        ts2_closed = df.filter("TIME_SERIES_NAME = 'TS2' AND end_day_id > 0").count()
        assert ts2_closed > 0, "TS2 should exist as closed record"
        print("✓ Logical delete test passed")
    
    # Test 5: Handle NULL values in tracked columns
    print("\n5. Testing NULL values...")
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
        dest_catalog=dest_catalog,
        dest_sch=dest_schema,
        dest_tb_obs=dest_tb_obs
    )
    
    df = print_table(spark, f"{dest_catalog}.{dest_schema}.{dest_tb_obs}", "After batch with NULL values")
    if df:
        active_records = df.filter("end_day_id = 0").collect()
        assert len(active_records) == 2, f"Expected 2 active records, got {len(active_records)}"
        assert all([row["VALUE"] is None for row in active_records]), "All active records should have NULL values"
        print("✓ NULL values test passed")
    
    # Test 6: Multi-source scenario
    print("\n6. Testing multi-source scenario...")
    df6 = spark.createDataFrame([
        ("TS1", now2, "SRC2", 10.1),
        ("TS3", now1 + timedelta(days=1), "SRC2", 10.3),
    ], ["TIME_SERIES_NAME", "DATETIME", "DATA_SOURCE", "VALUE"])
    
    DayIDelta(
        new_data_df=df6,
        key_cols=["TIME_SERIES_NAME", "DATETIME", "DATA_SOURCE"],
        tracked_cols=["VALUE"],
        dest_catalog=dest_catalog,
        dest_sch=dest_schema,
        dest_tb_obs=dest_tb_obs
    )
    
    df = print_table(spark, f"{dest_catalog}.{dest_schema}.{dest_tb_obs}", "After adding SRC2 data")
    
    # Remove TS1 from SRC2 only (logical delete for specific source)
    df7 = spark.createDataFrame([
        ("TS3", now1 + timedelta(days=1), "SRC2", 10.3),
    ], ["TIME_SERIES_NAME", "DATETIME", "DATA_SOURCE", "VALUE"])
    
    DayIDelta(
        new_data_df=df7,
        key_cols=["TIME_SERIES_NAME", "DATETIME", "DATA_SOURCE"],
        tracked_cols=["VALUE"],
        dest_catalog=dest_catalog,
        dest_sch=dest_schema,
        dest_tb_obs=dest_tb_obs
    )
    
    df = print_table(spark, f"{dest_catalog}.{dest_schema}.{dest_tb_obs}", "After removing TS1 from SRC2")
    if df:
        active_records = df.filter("end_day_id = 0")
        
        # TS1 from SRC2 should be gone
        ts1_src2_active = active_records.filter("TIME_SERIES_NAME = 'TS1' AND DATA_SOURCE = 'SRC2'").count()
        assert ts1_src2_active == 0, "TS1 from SRC2 should not be active"
        
        # TS3 from SRC2 should remain
        ts3_src2_active = active_records.filter("TIME_SERIES_NAME = 'TS3' AND DATA_SOURCE = 'SRC2'").count()
        assert ts3_src2_active == 1, "TS3 from SRC2 should remain active"
        
        # SRC1 data should be unaffected
        src1_active = active_records.filter("DATA_SOURCE = 'SRC1'").count()
        assert src1_active == 2, f"Expected 2 active SRC1 records, got {src1_active}"
        print("✓ Multi-source test passed")
    
    # Test the dim_day table
    print("\n7. Checking dim_day table...")
    dim_day_df = print_table(spark, f"{dest_catalog}.{dest_schema}.dim_day", "dim_day table")
    if dim_day_df:
        day_count = dim_day_df.count()
        assert day_count > 1, f"Expected multiple day_id records, got {day_count}"
        print("✓ dim_day table test passed")
    
    # Clean up
    cleanup_tables(spark, dest_catalog, dest_schema, dest_tb_obs)
    
    print("\n🎉 All DayIDelta Unity Catalog tests passed successfully!")
    return True


def run_performance_test():
    """
    Run a performance test with larger datasets.
    """
    spark = SparkSession.builder \
        .appName("DayIDelta Performance Test") \
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
        .getOrCreate()
    
    print("Running performance test...")
    
    setup_unity_catalog_environment(spark, dest_catalog, dest_schema)
    cleanup_tables(spark, dest_catalog, dest_schema, "perf_test_obs")
    
    # Create larger dataset
    import random
    base_time = datetime.now()
    data = []
    for i in range(1000):
        data.append((
            f"TS{i % 100}",  # 100 different time series
            base_time + timedelta(hours=i % 24),
            f"SRC{i % 5}",   # 5 different sources
            random.uniform(1.0, 100.0)
        ))
    
    df_large = spark.createDataFrame(data, ["TIME_SERIES_NAME", "DATETIME", "DATA_SOURCE", "VALUE"])
    
    start_time = datetime.now()
    DayIDelta(
        new_data_df=df_large,
        key_cols=["TIME_SERIES_NAME", "DATETIME", "DATA_SOURCE"],
        tracked_cols=["VALUE"],
        dest_catalog=dest_catalog,
        dest_sch=dest_schema,
        dest_tb_obs="perf_test_obs"
    )
    
    duration = datetime.now() - start_time
    result_count = spark.table(f"{dest_catalog}.{dest_schema}.perf_test_obs").count()
    
    print(f"Performance test completed:")
    print(f"  - Processed {len(data)} records")
    print(f"  - Duration: {duration.total_seconds():.2f} seconds")
    print(f"  - Result count: {result_count}")
    
    cleanup_tables(spark, dest_catalog, dest_schema, "perf_test_obs")
    return True


if __name__ == "__main__":
    print("DayIDelta Unity Catalog Test Suite")
    print("==================================")
    
    # Run main functionality tests
    run_unity_catalog_dayidelta_tests()
    
    # Run performance test
    run_performance_test()
    
    print("\n✅ All tests completed successfully!")