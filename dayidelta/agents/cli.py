"""
Command Line Interface agent for SCD2 operations.

Provides both interactive and direct query modes for the SCD2 chatbot.
"""

import sys
import argparse
import logging
from typing import List, Optional
from ..core.interfaces import Agent
from ..core.models import TableSchema, QueryResponse
from .chatbot import SCD2Chatbot

logger = logging.getLogger(__name__)


class SCD2CLI(Agent):
    """
    Command-line interface agent for SCD2 operations.
    
    Provides:
    - Interactive chat mode
    - Direct query execution
    - Formatted output options
    """
    
    def __init__(self, chatbot: Optional[SCD2Chatbot] = None):
        """
        Initialize CLI agent.
        
        Args:
            chatbot: SCD2Chatbot instance (optional, creates default if None)
        """
        self.chatbot = chatbot or SCD2Chatbot()
    
    def process_request(self, user_input: str, schema: TableSchema) -> QueryResponse:
        """Process a user request using the chatbot."""
        return self.chatbot.process_request(user_input, schema)
    
    def format_response(self, response: QueryResponse) -> str:
        """Format response for CLI display."""
        return self.chatbot.format_response(response)
    
    def run_interactive(self, schema: TableSchema) -> None:
        """Run in interactive mode."""
        self._print_welcome(schema)
        
        while True:
            try:
                query = input("\n❓ Your question: ").strip()
                
                if query.lower() in ['quit', 'exit', 'q', '']:
                    print("👋 Goodbye!")
                    break
                
                response = self.process_request(query, schema)
                print(self.format_response(response))
                print("\n" + "="*60)
                
            except KeyboardInterrupt:
                print("\n👋 Goodbye!")
                break
            except Exception as e:
                print(f"❌ Error: {e}")
    
    def run_direct_query(self, query: str, schema: TableSchema, 
                        sql_only: bool = False, python_only: bool = False,
                        explain_only: bool = False) -> None:
        """Run a direct query and print results."""
        try:
            response = self.process_request(query, schema)
            
            print(f"\n🤖 Query Type: {response.query_type.value.replace('_', ' ').title()}")
            
            if response.parameters:
                print(f"📊 Parameters: {response.parameters}")
            
            if explain_only or (not sql_only and not python_only):
                print(f"\n💡 Explanation:")
                print(response.explanation)
            
            if sql_only or (not python_only and not explain_only):
                print(f"\n📄 SQL Query:")
                print("-" * 50)
                print(response.sql_query)
            
            if python_only or (not sql_only and not explain_only):
                print(f"\n🐍 Python Code:")
                print("-" * 50)
                print(response.python_code)
            
        except Exception as e:
            print(f"❌ Error: {e}")
            sys.exit(1)
    
    def _print_welcome(self, schema: TableSchema) -> None:
        """Print welcome message for interactive mode."""
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
        print("\nType 'quit', 'exit', or 'q' to exit")


def create_cli_parser() -> argparse.ArgumentParser:
    """Create command line argument parser."""
    parser = argparse.ArgumentParser(
        description="SCD2 AI Chatbot - Generate SQL and Python code for SCD2 queries",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
    # Interactive mode
    python -m dayidelta.agents.cli cata sch tabl1 "id,timestamp" "value"
    
    # Direct query
    python -m dayidelta.agents.cli cata sch tabl1 "id,timestamp" "value" "show current data"
    
    # Complex table with multiple columns
    python -m dayidelta.agents.cli sensors prod readings "sensor_id,timestamp,location" "temperature,status" "compare Q3 and Q4 data"
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
    
    return parser


def format_columns(columns_str: str) -> List[str]:
    """Parse comma-separated column string."""
    return [col.strip() for col in columns_str.split(",") if col.strip()]


def main() -> None:
    """Main CLI function."""
    parser = create_cli_parser()
    args = parser.parse_args()
    
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
    
    # Create CLI agent
    cli = SCD2CLI()
    
    if args.query:
        # Direct query mode
        cli.run_direct_query(
            query=args.query,
            schema=schema,
            sql_only=args.sql_only,
            python_only=args.python_only,
            explain_only=args.explain
        )
    else:
        # Interactive mode
        cli.run_interactive(schema)


if __name__ == "__main__":
    main()