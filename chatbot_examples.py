#!/usr/bin/env python3
"""
SCD2 Chatbot Examples

Interactive examples demonstrating the SCD2 AI Chatbot functionality
for generating SQL and Python code to query SCD2 tables.
"""

# Use modular imports - this provides better maintainability and functionality
try:
    from dayidelta.agents.chatbot import SCD2Chatbot, quick_query
    from dayidelta.core.models import TableSchema, QueryType
except ImportError:
    # Fallback to legacy imports if modular structure not available
    from scd2_chatbot import SCD2Chatbot, TableSchema, quick_query, QueryType

def print_separator(title):
    """Print a formatted separator."""
    print(f"\n{'='*80}")
    print(f"  {title}")
    print('='*80)

def print_response(response, show_code=True):
    """Pretty print a chatbot response."""
    print(f"\n🤖 Query Type: {response.query_type.value.replace('_', ' ').title()}")
    if response.parameters:
        print(f"📊 Parameters: {response.parameters}")
    
    print(f"\n💡 Explanation:")
    print(response.explanation)
    
    if show_code:
        print(f"\n📄 SQL Query:")
        print("-" * 40)
        print(response.sql_query)
        
        print(f"\n🐍 Python Code:")
        print("-" * 40)
        print(response.python_code)

def example_1_basic_usage():
    """Example 1: Basic chatbot usage with different query types."""
    print_separator("Example 1: Basic Usage - Different Query Types")
    
    # Define a sample SCD2 table schema
    table_schema = TableSchema(
        catalog="cata",
        schema="sch",
        table="tabl1",
        key_columns=["TIME_SERIES_NAME", "DATETIME", "DATA_SOURCE"],
        tracked_columns=["VALUE"],
        other_columns=["LOCATION", "UNIT"]
    )
    
    print(f"📋 Table Schema:")
    print(f"   Full Name: {table_schema.full_table_name}")
    print(f"   Key Columns: {table_schema.key_columns}")
    print(f"   Tracked Columns: {table_schema.tracked_columns}")
    print(f"   Other Columns: {table_schema.other_columns}")
    
    # Create chatbot instance
    chatbot = SCD2Chatbot()
    
    # Test queries from the problem statement
    test_queries = [
        "can you show me all current data",
        "Can you show me how the data looked in 2024 q4", 
        "can you show me the history of time series x for 2023 q2",
        "Show me the differences in data between today and 5 days ago"
    ]
    
    for i, query in enumerate(test_queries, 1):
        print(f"\n\n🔍 Test Query {i}: '{query}'")
        print("-" * 60)
        
        response = chatbot.chat(query, table_schema)
        print_response(response, show_code=False)  # Hide code for brevity
        
        # Show just the SQL for this example
        print(f"\n📄 Generated SQL (preview):")
        sql_lines = response.sql_query.split('\n')
        for line in sql_lines[:8]:  # Show first 8 lines
            print(f"   {line}")
        if len(sql_lines) > 8:
            print(f"   ... ({len(sql_lines) - 8} more lines)")

def example_2_sensor_data():
    """Example 2: Realistic sensor data scenario."""
    print_separator("Example 2: IoT Sensor Data Analysis")
    
    # Realistic IoT sensor table
    sensor_schema = TableSchema(
        catalog="iot_platform",
        schema="production",
        table="sensor_readings",
        key_columns=["sensor_id", "timestamp", "location"],
        tracked_columns=["temperature", "humidity", "status"],
        other_columns=["device_type", "firmware_version"]
    )
    
    print(f"🌡️  IoT Sensor Table: {sensor_schema.full_table_name}")
    print(f"   Tracking: {', '.join(sensor_schema.tracked_columns)}")
    
    chatbot = SCD2Chatbot()
    
    # Realistic sensor queries
    sensor_queries = [
        "show me the current temperature readings",
        "how did sensor status look in Q3 2024?",
        "track temperature changes for sensor ABC123 over the last month",
        "compare humidity levels between last week and this week"
    ]
    
    for query in sensor_queries:
        print(f"\n\n❓ Sensor Query: '{query}'")
        print("-" * 50)
        
        response = chatbot.chat(query, sensor_schema)
        print(f"🎯 Detected Intent: {response.query_type.value}")
        print(f"\n💡 What this query does:")
        print(f"   {response.explanation.split('.')[0]}.")
        
        # Show key parts of the SQL
        print(f"\n🔧 Key SQL Components:")
        if "WHERE" in response.sql_query:
            where_part = response.sql_query.split("WHERE")[1].split("ORDER")[0].strip()
            print(f"   WHERE {where_part}")

def example_3_financial_data():
    """Example 3: Financial metrics tracking."""
    print_separator("Example 3: Financial Metrics SCD2 Analysis") 
    
    # Financial metrics table
    finance_schema = TableSchema(
        catalog="finance",
        schema="reporting", 
        table="customer_metrics",
        key_columns=["customer_id", "metric_date", "product_line"],
        tracked_columns=["revenue", "profit_margin", "risk_score"],
        other_columns=["region", "account_manager"]
    )
    
    print(f"💰 Financial Table: {finance_schema.full_table_name}")
    
    chatbot = SCD2Chatbot()
    
    # Financial analysis queries
    finance_queries = [
        "show current customer revenue data",
        "how did profit margins look in Q4 2024?", 
        "track risk score changes for high-value customers in Q1",
        "compare revenue between Q3 and Q4 for all customers"
    ]
    
    for query in finance_queries:
        print(f"\n\n💼 Finance Query: '{query}'")
        print("-" * 50)
        
        response = chatbot.chat(query, finance_schema)
        
        # Show the full SQL for financial examples
        print(f"📊 Generated SQL Query:")
        print(response.sql_query)

def example_4_quick_query_interface():
    """Example 4: Using the quick_query interface."""
    print_separator("Example 4: Quick Query Interface")
    
    print("🚀 Using quick_query() for fast one-off queries")
    
    # Quick query examples
    print(f"\n1️⃣  Quick current data query:")
    response1 = quick_query(
        user_input="show me all current data",
        catalog="retail",
        schema="sales",
        table="product_metrics",
        key_columns=["product_id", "date"],
        tracked_columns=["price", "inventory_level"]
    )
    print(f"   Query Type: {response1.query_type.value}")
    print(f"   Generated for: retail.sales.product_metrics")
    
    print(f"\n2️⃣  Quick historical query:")
    response2 = quick_query(
        user_input="how did the data look in 2024 Q2?",
        catalog="manufacturing",
        schema="operations", 
        table="machine_status",
        key_columns=["machine_id", "shift_start"],
        tracked_columns=["efficiency_score", "maintenance_status"]
    )
    print(f"   Query Type: {response2.query_type.value}")
    print(f"   Generated for: manufacturing.operations.machine_status")
    
    # Show one complete response
    print(f"\n📋 Complete Response for Query 2:")
    print_response(response2)

def example_5_advanced_patterns():
    """Example 5: Advanced query patterns and edge cases."""
    print_separator("Example 5: Advanced Patterns & Edge Cases")
    
    # Complex table with many columns
    complex_schema = TableSchema(
        catalog="enterprise",
        schema="data_warehouse",
        table="customer_360",
        key_columns=["customer_id", "snapshot_date", "channel"],
        tracked_columns=["clv", "satisfaction_score", "churn_risk", "segment"],
        other_columns=["first_name", "last_name", "email", "phone", "address", "created_date"]
    )
    
    chatbot = SCD2Chatbot()
    
    # Test edge cases and advanced patterns
    advanced_queries = [
        "current active customer segments",
        "point-in-time view for day_id 500",
        "evolution of churn risk over time",
        "differences between day_id 100 and day_id 200",
        "show me something complex",  # This should trigger custom response
        "historical snapshot as of December 2024"
    ]
    
    for query in advanced_queries:
        print(f"\n\n🔬 Advanced Query: '{query}'")
        print("-" * 50)
        
        response = chatbot.chat(query, complex_schema)
        print(f"   🎯 Detected Type: {response.query_type.value}")
        
        if response.query_type == QueryType.CUSTOM:
            print(f"   ⚠️  Custom template generated - requires manual adjustment")
        
        if response.parameters:
            print(f"   📈 Extracted Parameters: {response.parameters}")

def example_6_integration_demo():
    """Example 6: Integration with DayIDelta workflow."""
    print_separator("Example 6: Integration with DayIDelta")
    
    print("""
🔄 Complete Workflow Example:

1️⃣  Data Processing with DayIDelta:
   
from DayIDelta import DayIDelta, setup_unity_catalog_environment

# Setup environment  
setup_unity_catalog_environment(spark, "analytics", "timeseries")

# Process new sensor data
DayIDelta(
    new_data_df=new_sensor_batch,
    key_cols=["sensor_id", "timestamp", "location"], 
    tracked_cols=["temperature", "status"],
    dest_catalog="analytics",
    dest_sch="timeseries",
    dest_tb_obs="sensor_readings"
)

2️⃣  Querying with SCD2 Chatbot:
""")
    
    # Demo the integration
    schema = TableSchema(
        catalog="analytics",
        schema="timeseries", 
        table="sensor_readings",
        key_columns=["sensor_id", "timestamp", "location"],
        tracked_columns=["temperature", "status"]
    )
    
    chatbot = SCD2Chatbot()
    
    integration_queries = [
        "show current sensor readings after the batch load",
        "compare temperature data before and after the update"
    ]
    
    for query in integration_queries:
        print(f"\n📊 Post-Processing Query: '{query}'")
        response = chatbot.chat(query, schema)
        print(f"   🎯 Type: {response.query_type.value}")
        print(f"   💡 Purpose: {response.explanation.split('.')[0]}")
        
    print(f"""
3️⃣  Execution in Spark:

# Execute the generated query
result = spark.sql(response.sql_query)
result.show()

# Or use the Python code directly  
exec(response.python_code)
""")

def run_interactive_demo():
    """Run an interactive demo."""
    print_separator("Interactive Demo")
    
    print("🎮 Interactive SCD2 Chatbot Demo")
    print("\nEnter your table details and then ask questions!")
    
    # Get table schema from user
    print(f"\n📋 Define your SCD2 table:")
    catalog = input("Catalog name (e.g., 'my_catalog'): ").strip() or "my_catalog"
    schema = input("Schema name (e.g., 'my_schema'): ").strip() or "my_schema"
    table = input("Table name (e.g., 'my_table'): ").strip() or "my_table"
    
    key_cols_str = input("Key columns (comma-separated, e.g., 'id,timestamp'): ").strip()
    key_cols = [col.strip() for col in key_cols_str.split(',')] if key_cols_str else ["id", "timestamp"]
    
    tracked_cols_str = input("Tracked columns (comma-separated, e.g., 'value,status'): ").strip()
    tracked_cols = [col.strip() for col in tracked_cols_str.split(',')] if tracked_cols_str else ["value"]
    
    table_schema = TableSchema(
        catalog=catalog,
        schema=schema,
        table=table,
        key_columns=key_cols,
        tracked_columns=tracked_cols
    )
    
    print(f"\n✅ Created schema for: {table_schema.full_table_name}")
    print(f"   Key columns: {table_schema.key_columns}")
    print(f"   Tracked columns: {table_schema.tracked_columns}")
    
    chatbot = SCD2Chatbot()
    
    print(f"\n💬 Now ask questions about your data!")
    print("Examples:")
    print("  - 'show me all current data'")
    print("  - 'how did it look in Q3 2024?'")
    print("  - 'compare data between today and last week'")
    print("  - Type 'quit' to exit")
    
    while True:
        query = input(f"\n❓ Your question: ").strip()
        if query.lower() in ['quit', 'exit', 'q']:
            break
            
        if not query:
            continue
            
        response = chatbot.chat(query, table_schema)
        print(f"\n🤖 Response:")
        print_response(response, show_code=True)

def main():
    """Run all examples."""
    print("🚀 SCD2 AI Chatbot Examples")
    print("=" * 80)
    print("This script demonstrates the SCD2 AI Chatbot capabilities")
    print("for generating SQL and Python code to query SCD2 tables.")
    print("\nChoose an example to run:")
    print("1. Basic Usage - Different Query Types")
    print("2. IoT Sensor Data Analysis")
    print("3. Financial Metrics Analysis")
    print("4. Quick Query Interface")
    print("5. Advanced Patterns & Edge Cases")
    print("6. Integration with DayIDelta")
    print("7. Interactive Demo")
    print("8. Run All Examples")
    
    try:
        choice = input("\nEnter choice (1-8): ").strip()
        
        if choice == '1':
            example_1_basic_usage()
        elif choice == '2':
            example_2_sensor_data()
        elif choice == '3':
            example_3_financial_data()
        elif choice == '4':
            example_4_quick_query_interface()
        elif choice == '5':
            example_5_advanced_patterns()
        elif choice == '6':
            example_6_integration_demo()
        elif choice == '7':
            run_interactive_demo()
        elif choice == '8':
            example_1_basic_usage()
            example_2_sensor_data()
            example_3_financial_data()
            example_4_quick_query_interface()
            example_5_advanced_patterns()
            example_6_integration_demo()
        else:
            print("Invalid choice. Running basic example...")
            example_1_basic_usage()
            
    except KeyboardInterrupt:
        print("\n\n👋 Goodbye!")
    except Exception as e:
        print(f"\n❌ Error: {e}")

if __name__ == "__main__":
    main()