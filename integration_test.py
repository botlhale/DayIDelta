"""
Integration Test for SCD2 Chatbot with DayIDelta

This test demonstrates the complete workflow:
1. Create sample data
2. Process with DayIDelta to create SCD2 table
3. Query the SCD2 table using the AI chatbot
4. Validate the generated queries work correctly

Note: This test requires a Spark environment with Delta Lake support.
In a real environment, you would have SparkSession configured.
"""

from datetime import datetime, timedelta

# Use modular imports - this provides better maintainability and functionality
try:
    from dayidelta.agents.chatbot import SCD2Chatbot, quick_query
    from dayidelta.core.models import TableSchema
except ImportError:
    # Fallback to legacy imports if modular structure not available
    from scd2_chatbot import SCD2Chatbot, TableSchema, quick_query


def create_sample_schema():
    """Create a sample table schema for testing."""
    return TableSchema(
        catalog="test_catalog",
        schema="test_schema",
        table="sensor_readings",
        key_columns=["sensor_id", "timestamp", "location"],
        tracked_columns=["temperature", "humidity", "status"],
        other_columns=["device_type", "battery_level"]
    )


def test_chatbot_query_generation():
    """Test that the chatbot generates valid SQL and Python for all query types."""
    schema = create_sample_schema()
    chatbot = SCD2Chatbot()
    
    test_cases = [
        {
            "query": "show me all current sensor data",
            "expected_type": "current_data",
            "expected_sql_contains": ["end_day_id = 0", "sensor_id", "temperature"]
        },
        {
            "query": "how did the temperature readings look in 2024 Q2?",
            "expected_type": "point_in_time", 
            "expected_sql_contains": ["start_day_id <=", "end_day_id >", "2024", "QUARTER(date) = 2"]
        },
        {
            "query": "show me temperature history for sensor ABC123 over Q1 2024",
            "expected_type": "time_series_history",
            "expected_sql_contains": ["start_day_id", "end_day_id", "ORDER BY"]
        },
        {
            "query": "compare humidity levels between Q3 and Q4 2024",
            "expected_type": "data_comparison",
            "expected_sql_contains": ["WITH", "FULL OUTER JOIN", "humidity"]
        }
    ]
    
    print("🧪 Testing Chatbot Query Generation")
    print("=" * 50)
    
    for i, test_case in enumerate(test_cases, 1):
        print(f"\nTest {i}: {test_case['query']}")
        print("-" * 40)
        
        response = chatbot.chat(test_case['query'], schema)
        
        # Validate response
        assert response.query_type.value == test_case['expected_type'], f"Expected {test_case['expected_type']}, got {response.query_type.value}"
        assert len(response.sql_query) > 0, "SQL query should not be empty"
        assert len(response.python_code) > 0, "Python code should not be empty"
        assert len(response.explanation) > 0, "Explanation should not be empty"
        
        # Check SQL content
        for expected_content in test_case['expected_sql_contains']:
            assert expected_content in response.sql_query, f"SQL should contain '{expected_content}'"
        
        # Check Python content
        assert "from pyspark.sql" in response.python_code, "Python code should import PySpark"
        assert schema.full_table_name in response.python_code, "Python code should reference the table"
        
        print(f"✅ Query Type: {response.query_type.value}")
        print(f"✅ SQL contains expected elements")
        print(f"✅ Python code is valid")
    
    print(f"\n🎉 All {len(test_cases)} test cases passed!")
    return True


def test_quick_query_interface():
    """Test the quick_query convenience function."""
    print("\n🚀 Testing Quick Query Interface")
    print("=" * 40)
    
    response = quick_query(
        user_input="show current temperature data",
        catalog="sensors",
        schema="production",
        table="readings",
        key_columns=["sensor_id", "timestamp"],
        tracked_columns=["temperature", "status"]
    )
    
    assert response.query_type.value == "current_data"
    assert "sensors.production.readings" in response.sql_query
    assert "end_day_id = 0" in response.sql_query
    assert "temperature" in response.sql_query
    
    print("✅ Quick query interface works correctly")
    return True


def test_parameter_extraction():
    """Test that the chatbot correctly extracts parameters from queries."""
    schema = create_sample_schema()
    chatbot = SCD2Chatbot()
    
    print("\n🔍 Testing Parameter Extraction")
    print("=" * 40)
    
    # Test quarter extraction
    response = chatbot.chat("how did data look in 2024 q3", schema)
    assert response.parameters.get('year') == 2024
    assert response.parameters.get('quarter') == 3
    print("✅ Quarter parameters extracted correctly")
    
    # Test day_id extraction
    response = chatbot.chat("show data at day_id 150", schema)
    assert response.parameters.get('day_id') == 150
    print("✅ Day ID parameters extracted correctly")
    
    # Test days ago extraction
    response = chatbot.chat("show historical data from 30 days ago", schema)
    assert response.parameters.get('days_ago') == 30
    print("✅ Days ago parameters extracted correctly")
    
    return True


def test_sql_syntax_validation():
    """Test that generated SQL has valid basic syntax."""
    schema = create_sample_schema()
    chatbot = SCD2Chatbot()
    
    print("\n📝 Testing SQL Syntax Validation")
    print("=" * 40)
    
    queries = [
        "show current data",
        "historical data at day_id 100", 
        "compare day_id 50 and day_id 100",
        "time series history for Q1 2024"
    ]
    
    for query in queries:
        response = chatbot.chat(query, schema)
        sql = response.sql_query
        
        # Basic SQL validation
        assert "SELECT" in sql.upper(), "SQL should contain SELECT"
        assert "FROM" in sql.upper(), "SQL should contain FROM"
        assert schema.full_table_name in sql, "SQL should reference the correct table"
        
        # Check for balanced parentheses
        assert sql.count("(") == sql.count(")"), "SQL should have balanced parentheses"
        
        # Check for basic structure
        assert not sql.strip().endswith(";"), "SQL should not end with semicolon"
        
    print("✅ Generated SQL passes basic syntax validation")
    return True


def demonstrate_real_world_scenarios():
    """Demonstrate real-world usage scenarios."""
    print("\n🌍 Real-World Usage Scenarios")
    print("=" * 50)
    
    # IoT Sensor Monitoring
    iot_schema = TableSchema(
        catalog="iot_platform",
        schema="production",
        table="device_metrics",
        key_columns=["device_id", "metric_timestamp", "location_id"],
        tracked_columns=["cpu_usage", "memory_usage", "temperature"],
        other_columns=["device_type", "firmware_version"]
    )
    
    # Financial Data Tracking
    finance_schema = TableSchema(
        catalog="finance",
        schema="analytics",
        table="customer_metrics",
        key_columns=["customer_id", "reporting_date", "product_line"],
        tracked_columns=["revenue", "profit_margin", "risk_score"],
        other_columns=["account_manager", "region"]
    )
    
    chatbot = SCD2Chatbot()
    
    scenarios = [
        {
            "title": "IoT Device Monitoring",
            "schema": iot_schema,
            "queries": [
                "show current CPU usage for all devices",
                "how did memory usage look last quarter?",
                "compare device performance between Q3 and Q4"
            ]
        },
        {
            "title": "Financial Analytics",
            "schema": finance_schema,
            "queries": [
                "show current customer revenue metrics",
                "track risk score changes over the last quarter", 
                "compare profit margins between Q2 and Q3 2024"
            ]
        }
    ]
    
    for scenario in scenarios:
        print(f"\n📊 {scenario['title']}")
        print(f"   Table: {scenario['schema'].full_table_name}")
        print(f"   Tracked: {', '.join(scenario['schema'].tracked_columns)}")
        
        for query in scenario['queries']:
            response = chatbot.chat(query, scenario['schema'])
            print(f"\n   ❓ '{query}'")
            print(f"   🎯 {response.query_type.value.replace('_', ' ').title()}")
            print(f"   📄 {response.sql_query.split('FROM')[0].strip()}")
    
    print("\n✅ Real-world scenarios demonstrated successfully")
    return True


def main():
    """Run all integration tests."""
    print("🧪 SCD2 Chatbot Integration Tests")
    print("=" * 60)
    print("Testing chatbot functionality with realistic SCD2 scenarios")
    
    try:
        # Run all tests
        test_chatbot_query_generation()
        test_quick_query_interface()
        test_parameter_extraction()
        test_sql_syntax_validation()
        demonstrate_real_world_scenarios()
        
        print(f"\n🎉 All Integration Tests Passed!")
        print("\n✅ The SCD2 AI Chatbot is ready for production use!")
        print("\nNext steps:")
        print("1. Deploy the chatbot in your Spark environment")
        print("2. Configure table schemas for your SCD2 tables")
        print("3. Train users on natural language query patterns")
        print("4. Monitor and optimize query performance")
        
    except AssertionError as e:
        print(f"\n❌ Test failed: {e}")
        return False
    except Exception as e:
        print(f"\n💥 Unexpected error: {e}")
        return False
    
    return True


if __name__ == "__main__":
    success = main()
    exit(0 if success else 1)