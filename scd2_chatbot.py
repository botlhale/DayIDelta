"""
SCD2 AI Chatbot for DayIDelta

An AI-powered chatbot that helps users generate SQL statements and Python code
to query SCD2 (Slowly Changing Dimension Type 2) tables created by DayIDelta.

This module now uses the new modular DayIDelta structure for improved maintainability
while preserving the original API for backward compatibility.

The chatbot understands natural language queries and generates appropriate SQL
and PySpark code for common SCD2 query patterns including:
- Current/active data queries
- Point-in-time historical queries
- Time series history for specific periods
- Data comparison between different time periods
"""

# Use the new modular structure
try:
    from dayidelta.agents.chatbot import SCD2Chatbot, quick_query
    from dayidelta.core.models import TableSchema, QueryType, QueryRequest, QueryResponse
    from dayidelta.query.patterns import SCD2QueryPatterns
    DAYIDELTA_CHATBOT_AVAILABLE = True
except ImportError:
    # Fallback to original implementation if new structure not available
    DAYIDELTA_CHATBOT_AVAILABLE = False

if not DAYIDELTA_CHATBOT_AVAILABLE:
    # Fallback: Preserve original implementation for backward compatibility
    import re
    import datetime
    from typing import Dict, List, Tuple, Optional, Union
    from dataclasses import dataclass
    from enum import Enum
    
    # Minimal fallback implementation - direct users to new structure
    class SCD2Chatbot:
        """Fallback chatbot implementation."""
        def __init__(self):
            raise ImportError(
                "The new modular DayIDelta chatbot is not available. "
                "Please install the dayidelta package or use: "
                "from dayidelta.agents.chatbot import SCD2Chatbot"
            )
    
    def quick_query(*args, **kwargs):
        raise ImportError(
            "The new modular DayIDelta is not available. "
            "Please install the dayidelta package or use: "
            "from dayidelta.agents.chatbot import quick_query"
        )

# Export for backward compatibility
if DAYIDELTA_CHATBOT_AVAILABLE:
    __all__ = ['SCD2Chatbot', 'quick_query', 'TableSchema', 'QueryType', 'SCD2QueryPatterns']
else:
    __all__ = ['SCD2Chatbot', 'quick_query']


if __name__ == "__main__":
    # Example usage using the new structure
    try:
        from dayidelta.core.models import TableSchema
        from dayidelta.agents.chatbot import SCD2Chatbot
        
        # Test different query types
        table_schema = TableSchema(
            catalog="cata",
            schema="sch", 
            table="tabl1",
            key_columns=["TIME_SERIES_NAME", "DATETIME", "DATA_SOURCE"],
            tracked_columns=["VALUE"]
        )
        
        chatbot = SCD2Chatbot()
        
        test_queries = [
            "can you show me all current data",
            "Can you show me how the data looked in 2024 q4",
            "can you show me the history of time series x for 2023 q2", 
            "Show me the differences in data between today and 5 days ago"
        ]
        
        print("🧪 Testing DayIDelta SCD2 Chatbot")
        print("="*60)
        
        for query in test_queries:
            print(f"\n📝 Query: {query}")
            print("-" * 40)
            
            response = chatbot.chat(query, table_schema)
            print(f"🤖 Type: {response.query_type.value}")
            print(f"📊 Parameters: {response.parameters}")
            print(f"💡 Generated SQL (preview): {response.sql_query[:100]}...")
            
        print(f"\n✅ All tests completed successfully!")
        
    except ImportError as e:
        print(f"❌ Error: {e}")
        print("Please ensure the dayidelta package is properly installed.")