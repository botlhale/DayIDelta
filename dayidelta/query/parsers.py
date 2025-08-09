"""
Natural language query parsers for SCD2 queries.

Handles parsing of natural language input to extract query types and parameters.
"""

import re
from typing import Dict, Optional, Any
from ..core.interfaces import QueryParser
from ..core.models import TableSchema, QueryRequest, QueryType


class NaturalLanguageParser(QueryParser):
    """
    Parser for natural language queries about SCD2 data.
    
    Understands various patterns for current data, historical queries,
    time series analysis, and data comparison requests.
    """
    
    def parse_query(self, user_input: str, schema: TableSchema) -> QueryRequest:
        """Parse user input to determine query type and extract parameters."""
        user_input_lower = user_input.lower().strip()
        
        # Determine query type using pattern matching
        query_type = self._identify_query_type(user_input_lower)
        
        # Extract parameters based on query type
        parameters = self.extract_parameters(user_input_lower, schema)
        
        return QueryRequest(
            user_input=user_input,
            table_schema=schema,
            query_type=query_type,
            parameters=parameters
        )
    
    def extract_parameters(self, user_input: str, schema: TableSchema) -> Dict[str, Any]:
        """Extract parameters from natural language input."""
        parameters = {}
        
        # Extract time-related parameters
        time_params = self._extract_time_parameters(user_input, schema)
        parameters.update(time_params)
        
        # Extract comparison parameters
        comparison_params = self._extract_comparison_parameters(user_input, schema)
        parameters.update(comparison_params)
        
        # Extract filter parameters
        filter_params = self._extract_filter_parameters(user_input, schema)
        parameters.update(filter_params)
        
        return parameters
    
    def _identify_query_type(self, user_input: str) -> QueryType:
        """Identify the query type from user input."""
        
        # Current data patterns - improved regex patterns
        current_patterns = [
            r'\b(current|active|latest|present)\s+(?:data|records|values|information)',
            r'\bshow\s+(?:me\s+)?(?:all\s+)?current',
            r'\bwhat\s+(?:is|are)\s+(?:the\s+)?current',
            r'\bget\s+(?:all\s+)?(?:current|active|latest)',
            r'\bactive\s+(?:records|data|entries)',
            r'\blatest\s+(?:data|information|values)'
        ]
        
        # Historical/point-in-time patterns - improved
        historical_patterns = [
            r'\bhow\s+(?:did\s+)?(?:the\s+)?(?:data|values|records)\s+(?:look|appear)',
            r'\bdata\s+(?:as\s+of|at|for|in)',
            r'\bpoint[\s-]?in[\s-]?time',
            r'\bhistorical\s+(?:data|values|snapshot)',
            r'\bview\s+(?:for|at|as\s+of)',
            r'\bat\s+day[_\s]?id',
            r'\bin\s+(?:q\d|quarter)',
            r'\back\s+in',
            r'\bhow\s+.*(?:look|appear).*(?:in|at|for)'
        ]
        
        # Time series history patterns - improved  
        time_series_patterns = [
            r'\bhistory\s+(?:of|for|over)',
            r'\btime\s+series.*(?:for|of|over)',
            r'\bchanges\s+over\s+(?:time|period)',
            r'\bevolution\s+(?:of|for)',
            r'\btrack\s+changes',
            r'\bshow.*history',
            r'\bover\s+(?:q\d|quarter|period|time)',
            r'\bthroughout\s+(?:q\d|quarter|period)',
            r'\b(?:temperature|sensor|data)\s+history'
        ]
        
        # Comparison patterns - improved
        comparison_patterns = [
            r'\b(?:difference|compare|comparison)\s+between',
            r'\bcompare\s+.*\s+(?:and|with|to)',
            r'\bchanges\s+from\s+.*\s+to',
            r'\bwhat\s+changed\s+(?:between|from)',
            r'\bdelta\s+between',
            r'\b(?:vs|versus)\b',
            r'\bdifferences?\s+(?:between|from)'
        ]
        
        # Check patterns in order of specificity
        if any(re.search(pattern, user_input) for pattern in comparison_patterns):
            return QueryType.DATA_COMPARISON
        elif any(re.search(pattern, user_input) for pattern in time_series_patterns):
            return QueryType.TIME_SERIES_HISTORY
        elif any(re.search(pattern, user_input) for pattern in historical_patterns):
            return QueryType.POINT_IN_TIME
        elif any(re.search(pattern, user_input) for pattern in current_patterns):
            return QueryType.CURRENT_DATA
        else:
            return QueryType.CUSTOM
    
    def _extract_time_parameters(self, user_input: str, schema: Optional[TableSchema] = None) -> Dict[str, Any]:
        """Extract time-related parameters from user input."""
        parameters = {}
        
        # Look for quarters with better patterns - fixed regex
        quarter_patterns = [
            r'\b(\d{4})\s+q(\d)\b',  # "2024 q4"
            r'\bq(\d)\s+(\d{4})\b',  # "q4 2024"  
            r'\bquarter\s+(\d)\s+(?:of\s+)?(\d{4})\b',  # "quarter 4 of 2024"
            r'\b(\d{4})\s+quarter\s+(\d)\b'  # "2024 quarter 4"
        ]
        
        for pattern in quarter_patterns:
            match = re.search(pattern, user_input)
            if match:
                if pattern.startswith(r'\bq(\d)'):  # q4 2024 format
                    quarter, year = match.groups()
                    parameters['quarter'] = int(quarter)
                    parameters['year'] = int(year)
                elif 'quarter' in pattern and pattern.endswith(r'(\d{4})\b'):  # quarter 4 of 2024
                    quarter, year = match.groups()
                    parameters['quarter'] = int(quarter)
                    parameters['year'] = int(year)
                else:  # 2024 q4 or 2024 quarter 4 format
                    year, quarter = match.groups()
                    parameters['year'] = int(year)
                    parameters['quarter'] = int(quarter)
                break
        
        # Look for specific day_id mentions
        day_id_match = re.search(r'\bday[_\s]?id[_\s]?(\d+)\b', user_input)
        if day_id_match:
            parameters['day_id'] = int(day_id_match.group(1))
        
        # Look for "x days ago" - fixed regex
        days_ago_match = re.search(r'\b(\d+)\s+days?\s+ago\b', user_input)
        if days_ago_match:
            days_ago = int(days_ago_match.group(1))
            parameters['days_ago'] = days_ago
            if schema:
                parameters['estimated_day_id'] = f"(SELECT MAX(day_id) - {days_ago} FROM {schema.catalog}.{schema.schema}.dim_day)"
        
        # Look for month names and years
        month_year_match = re.search(r'\b(january|february|march|april|may|june|july|august|september|october|november|december)\s+(\d{4})\b', user_input)
        if month_year_match:
            month_name, year = month_year_match.groups()
            parameters['month'] = month_name.lower()
            parameters['year'] = int(year)
        
        return parameters
    
    def _extract_comparison_parameters(self, user_input: str, schema: Optional[TableSchema] = None) -> Dict[str, Any]:
        """Extract comparison parameters from user input."""
        parameters = {}
        
        # Look for "between X and Y" patterns - improved
        between_patterns = [
            r'\bbetween\s+(.+?)\s+and\s+(.+?)(?:\s|$|\.)',
            r'\bcompare\s+(.+?)\s+(?:with|to|and)\s+(.+?)(?:\s|$|\.)',
            r'\bfrom\s+(.+?)\s+to\s+(.+?)(?:\s|$|\.)'
        ]
        
        for pattern in between_patterns:
            match = re.search(pattern, user_input)
            if match:
                param1 = match.group(1).strip()
                param2 = match.group(2).strip()
                
                # Try to extract day_ids or time periods
                day_id_1 = self._extract_day_id_from_text(param1)
                day_id_2 = self._extract_day_id_from_text(param2)
                
                if day_id_1:
                    parameters['day_id_1'] = day_id_1
                if day_id_2:
                    parameters['day_id_2'] = day_id_2
                break
        
        return parameters
    
    def _extract_filter_parameters(self, user_input: str, schema: Optional[TableSchema] = None) -> Dict[str, Any]:
        """Extract filter parameters from user input."""
        parameters = {}
        
        # Look for specific entity mentions that could be filters
        if schema:
            # Look for key column values that might be mentioned
            for key_col in schema.key_columns:
                if 'sensor' in key_col.lower() and 'sensor' in user_input:
                    # Extract sensor ID patterns
                    sensor_match = re.search(r'\bsensor\s+([a-zA-Z0-9_]+)\b', user_input)
                    if sensor_match:
                        parameters[f'{key_col}_filter'] = sensor_match.group(1)
        
        return parameters
    
    def _extract_day_id_from_text(self, text: str) -> Optional[Any]:
        """Extract day_id from text fragment."""
        # Look for explicit day_id
        day_id_match = re.search(r'\bday[_\s]?id[_\s]?(\d+)\b', text)
        if day_id_match:
            return int(day_id_match.group(1))
        
        # Look for "today"
        if re.search(r'\btoday\b', text):
            return "CURRENT_DAY_ID"  # Placeholder
        
        # Look for "X days ago"
        days_ago_match = re.search(r'\b(\d+)\s+days?\s+ago\b', text)
        if days_ago_match:
            return f"CURRENT_DAY_ID - {days_ago_match.group(1)}"
        
        # Look for quarters
        quarter_match = re.search(r'\b(\d{4})\s+q(\d)\b', text)
        if quarter_match:
            year, quarter = quarter_match.groups()
            return f"Q{quarter}_{year}_DAY_ID"
        
        return None