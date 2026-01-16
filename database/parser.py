"""
Simple SQL-like query parser.
"""

import re
from typing import Dict, List, Any, Optional, Tuple
from enum import Enum


class QueryType(Enum):
    """Types of SQL queries we support."""
    CREATE_TABLE = "CREATE_TABLE"
    INSERT = "INSERT"
    SELECT = "SELECT"
    UPDATE = "UPDATE"
    DELETE = "DELETE"
    UNKNOWN = "UNKNOWN"


class QueryParser:
    """Parses SQL-like queries into structured dictionaries."""

    # Regular expressions for different query types
    CREATE_TABLE_PATTERN = re.compile(
        r'CREATE TABLE (\w+)\s*\((.*)\)',
        re.IGNORECASE | re.DOTALL
    )

    INSERT_PATTERN = re.compile(
        r'INSERT INTO (\w+)\s*(?:\((.*?)\))?\s*VALUES\s*\((.*)\)',
        re.IGNORECASE | re.DOTALL
    )

    SELECT_PATTERN = re.compile(
        r'SELECT (.*?) FROM (\w+)(?:\s+WHERE\s+(.*?))?(?:\s+INNER JOIN\s+(\w+)\s+ON\s+(.*))?$',
        re.IGNORECASE
    )

    UPDATE_PATTERN = re.compile(
        r'UPDATE (\w+)\s+SET\s+(.*?)(?:\s+WHERE\s+(.*))?$',
        re.IGNORECASE
    )

    DELETE_PATTERN = re.compile(
        r'DELETE FROM (\w+)(?:\s+WHERE\s+(.*))?$',
        re.IGNORECASE
    )

    def parse(self, query: str) -> Dict[str, Any]:
        """Parse a SQL-like query into a structured dictionary."""
        query = query.strip().rstrip(';')

        if query.upper().startswith("CREATE TABLE"):
            return self._parse_create_table(query)
        elif query.upper().startswith("INSERT INTO"):
            return self._parse_insert(query)
        elif query.upper().startswith("SELECT"):
            return self._parse_select(query)
        elif query.upper().startswith("UPDATE"):
            return self._parse_update(query)
        elif query.upper().startswith("DELETE FROM"):
            return self._parse_delete(query)
        else:
            raise SyntaxError(f"Unsupported query type: {query}")

    def _parse_create_table(self, query: str) -> Dict[str, Any]:
        """Parse CREATE TABLE query."""
        match = self.CREATE_TABLE_PATTERN.match(query)
        if not match:
            raise SyntaxError("Invalid CREATE TABLE syntax")

        table_name = match.group(1)
        columns_str = match.group(2)

        columns = []
        for col_def in self._split_by_commas(columns_str):
            col_def = col_def.strip()
            if not col_def:
                continue

            parts = col_def.split()
            col_name = parts[0]
            col_type = parts[1].upper()

            constraints = []
            if len(parts) > 2:
                constraints = [c.upper() for c in parts[2:]]

            columns.append({
                'name': col_name,
                'type': col_type,
                'constraints': constraints
            })

        return {
            'type': QueryType.CREATE_TABLE,
            'table_name': table_name,
            'columns': columns
        }

    def _parse_insert(self, query: str) -> Dict[str, Any]:
        """Parse INSERT INTO query."""
        match = self.INSERT_PATTERN.match(query)
        if not match:
            raise SyntaxError("Invalid INSERT syntax")

        table_name = match.group(1)
        columns_str = match.group(2)
        values_str = match.group(3)

        # Parse column names if specified
        if columns_str:
            columns = [col.strip() for col in columns_str.split(',')]
        else:
            columns = None

        # Parse values
        values = self._parse_values(values_str)

        return {
            'type': QueryType.INSERT,
            'table_name': table_name,
            'columns': columns,
            'values': values
        }

    def _parse_select(self, query: str) -> Dict[str, Any]:
        """Parse SELECT query."""
        match = self.SELECT_PATTERN.match(query)
        if not match:
            raise SyntaxError("Invalid SELECT syntax")

        columns_str = match.group(1).strip()
        table_name = match.group(2).strip()
        where_clause = match.group(3)
        join_table = match.group(4)
        join_condition = match.group(5)

        # Parse columns
        if columns_str == "*":
            columns = ["*"]
        else:
            columns = [col.strip() for col in columns_str.split(',')]

        # Parse WHERE conditions
        conditions = self._parse_where_clause(where_clause) if where_clause else None

        # Parse JOIN
        join = None
        if join_table and join_condition:
            join_parts = join_condition.split('=')
            if len(join_parts) != 2:
                raise SyntaxError("Invalid JOIN condition")

            left_table, left_col = join_parts[0].strip().split('.')
            right_table, right_col = join_parts[1].strip().split('.')

            join = {
                'table': join_table.strip(),
                'left_column': left_col,
                'right_column': right_col
            }

        return {
            'type': QueryType.SELECT,
            'table_name': table_name,
            'columns': columns,
            'where': conditions,
            'join': join
        }

    def _parse_update(self, query: str) -> Dict[str, Any]:
        """Parse UPDATE query."""
        match = self.UPDATE_PATTERN.match(query)
        if not match:
            raise SyntaxError("Invalid UPDATE syntax")

        table_name = match.group(1)
        set_clause = match.group(2)
        where_clause = match.group(3)

        # Parse SET assignments
        assignments = {}
        for assign in self._split_by_commas(set_clause):
            col, value = assign.split('=', 1)
            assignments[col.strip()] = self._parse_value(value.strip())

        # Parse WHERE conditions
        conditions = self._parse_where_clause(where_clause) if where_clause else None

        return {
            'type': QueryType.UPDATE,
            'table_name': table_name,
            'set': assignments,
            'where': conditions
        }

    def _parse_delete(self, query: str) -> Dict[str, Any]:
        """Parse DELETE query."""
        match = self.DELETE_PATTERN.match(query)
        if not match:
            raise SyntaxError("Invalid DELETE syntax")

        table_name = match.group(1)
        where_clause = match.group(2)

        conditions = self._parse_where_clause(where_clause) if where_clause else None

        return {
            'type': QueryType.DELETE,
            'table_name': table_name,
            'where': conditions
        }

    def _parse_where_clause(self, where_str: str) -> List[Dict[str, Any]]:
        """Parse WHERE clause into list of conditions."""
        conditions = []
        for cond in self._split_by_commas(where_str):
            if '=' in cond:
                left, right = cond.split('=', 1)
                conditions.append({
                    'column': left.strip(),
                    'operator': '=',
                    'value': self._parse_value(right.strip())
                })
            elif '!=' in cond:
                left, right = cond.split('!=', 1)
                conditions.append({
                    'column': left.strip(),
                    'operator': '!=',
                    'value': self._parse_value(right.strip())
                })
        return conditions

    def _parse_value(self, value_str: str) -> Any:
        """Parse a SQL value into Python type."""
        value_str = value_str.strip()

        if value_str.upper() == 'NULL':
            return None
        elif value_str.upper() == 'TRUE':
            return True
        elif value_str.upper() == 'FALSE':
            return False
        elif value_str.startswith("'") and value_str.endswith("'"):
            return value_str[1:-1]
        elif value_str.startswith('"') and value_str.endswith('"'):
            return value_str[1:-1]
        elif value_str.isdigit() or (value_str.startswith('-') and value_str[1:].isdigit()):
            return int(value_str)
        else:
            # Try to parse as number
            try:
                return float(value_str)
            except ValueError:
                return value_str

    def _parse_values(self, values_str: str) -> List[Any]:
        """Parse a list of values."""
        values = []
        for val in self._split_by_commas(values_str):
            values.append(self._parse_value(val.strip()))
        return values

    def _split_by_commas(self, s: str) -> List[str]:
        """Split string by commas, handling nested parentheses."""
        result = []
        current = ""
        paren_depth = 0

        for char in s:
            if char == '(':
                paren_depth += 1
            elif char == ')':
                paren_depth -= 1
            elif char == ',' and paren_depth == 0:
                result.append(current.strip())
                current = ""
                continue
            current += char

        if current:
            result.append(current.strip())
        return result