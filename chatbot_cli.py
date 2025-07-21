#!/usr/bin/env python3
"""
SCD2 Chatbot CLI

Command-line interface for the SCD2 AI Chatbot.
Allows quick generation of SQL and Python code for SCD2 queries.

Usage:
    python chatbot_cli.py <catalog> <schema> <table> <key_cols> <tracked_cols> [query]
    
Examples:
    python chatbot_cli.py cata sch tabl1 "id,timestamp" "value" "show current data"
    python chatbot_cli.py sensors prod readings "sensor_id,timestamp" "temperature" 
"""

import sys
import argparse
from scd2_chatbot import SCD2Chatbot, TableSchema, quick_query


def parse_args():
    """Parse command line arguments."""
    parser = argparse.ArgumentParser(
        description="SCD2 AI Chatbot - Generate SQL and Python code for SCD2 queries",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
    # Interactive mode
    python chatbot_cli.py cata sch tabl1 "id,timestamp" "value"
    
    # Direct query
    python chatbot_cli.py cata sch tabl1 "id,timestamp" "value" "show current data"
    
    # Complex table with multiple columns
    python chatbot_cli.py sensors prod readings "sensor_id,timestamp,location" "temperature,status" "compare Q3 and Q4 data"
        """
    )
    
    parser.add_argument("catalog", help="Unity Catalog catalog name")
    parser.add_argument("schema", help="Schema name")
    parser.add_argument("table", help="Table name")
    parser.add_argument("key_columns", help="Key columns (comma-separated)")
    parser.add_argument("tracked_columns", help="Tracked columns (comma-separated)")
    parser.add_argument("query", nargs="?", help="Optional query string")
    parser.add_argument("--other-columns", help="Other data columns (comma-separated)")
    parser.add_argument("--sql-only", action="store_true", help="Show only SQL query")
    parser.add_argument("--python-only", action="store_true", help="Show only Python code")
    parser.add_argument("--explain", action="store_true", help="Show explanation only")
    
    return parser.parse_args()


def format_columns(columns_str):
    """Parse comma-separated column string."""
    return [col.strip() for col in columns_str.split(",") if col.strip()]


def print_response(response, args):
    """Print the chatbot response based on arguments."""
    print(f"\n🤖 Query Type: {response.query_type.value.replace('_', ' ').title()}")
    
    if response.parameters:
        print(f"📊 Parameters: {response.parameters}")
    
    if args.explain or (not args.sql_only and not args.python_only):
        print(f"\n💡 Explanation:")
        print(response.explanation)
    
    if args.sql_only or (not args.python_only and not args.explain):
        print(f"\n📄 SQL Query:")
        print("-" * 50)
        print(response.sql_query)
    
    if args.python_only or (not args.sql_only and not args.explain):
        print(f"\n🐍 Python Code:")
        print("-" * 50)
        print(response.python_code)


def interactive_mode(schema, args):
    """Run in interactive mode."""
    chatbot = SCD2Chatbot()
    
    print(f"🎮 SCD2 Chatbot Interactive Mode")
    print(f"📋 Table: {schema.full_table_name}")
    print(f"🔑 Key Columns: {', '.join(schema.key_columns)}")
    print(f"📈 Tracked Columns: {', '.join(schema.tracked_columns)}")
    
    if schema.other_columns:
        print(f"📦 Other Columns: {', '.join(schema.other_columns)}")
    
    print(f"\n💬 Ask questions about your SCD2 data!")
    print("Examples:")
    print("  • 'show me all current data'")
    print("  • 'how did the data look in 2024 Q3?'")
    print("  • 'compare data between today and last week'")
    print("  • 'time series history for 2023'")
    print("\nType 'quit', 'exit', or 'q' to exit\n")
    
    while True:
        try:
            query = input("❓ Your question: ").strip()
            
            if query.lower() in ['quit', 'exit', 'q', '']:
                print("👋 Goodbye!")
                break
            
            response = chatbot.chat(query, schema)
            print_response(response, args)
            print("\n" + "="*60)
            
        except KeyboardInterrupt:
            print("\n👋 Goodbye!")
            break
        except Exception as e:
            print(f"❌ Error: {e}")


def main():
    """Main CLI function."""
    args = parse_args()
    
    # Parse columns
    key_columns = format_columns(args.key_columns)
    tracked_columns = format_columns(args.tracked_columns)
    other_columns = format_columns(args.other_columns) if args.other_columns else None
    
    # Validate inputs
    if not key_columns:
        print("❌ Error: At least one key column is required")
        sys.exit(1)
    
    if not tracked_columns:
        print("❌ Error: At least one tracked column is required")
        sys.exit(1)
    
    # Create table schema
    schema = TableSchema(
        catalog=args.catalog,
        schema=args.schema,
        table=args.table,
        key_columns=key_columns,
        tracked_columns=tracked_columns,
        other_columns=other_columns
    )
    
    if args.query:
        # Direct query mode
        if args.sql_only or args.python_only or args.explain:
            # Use main chatbot for more detailed output
            chatbot = SCD2Chatbot()
            response = chatbot.chat(args.query, schema)
        else:
            # Use quick_query for simple output
            response = quick_query(
                user_input=args.query,
                catalog=args.catalog,
                schema=args.schema,
                table=args.table,
                key_columns=key_columns,
                tracked_columns=tracked_columns,
                other_columns=other_columns
            )
        
        print_response(response, args)
    else:
        # Interactive mode
        interactive_mode(schema, args)


if __name__ == "__main__":
    main()