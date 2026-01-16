"""
Main database engine class.
"""

from typing import Any, Dict
from .parser import QueryParser
from .executor import QueryExecutor
from .storage import Storage


class DatabaseEngine:
    """Main database engine interface."""

    def __init__(self, data_dir: str = "data"):
        self.storage = Storage(data_dir)
        self.parser = QueryParser()
        self.executor = QueryExecutor(self.storage)

    def execute(self, query: str) -> Any:
        """
        Execute a SQL-like query.

        Args:
            query: SQL-like query string

        Returns:
            Query result as dictionary

        Raises:
            SyntaxError: If query syntax is invalid
            ValueError: If query violates constraints
        """
        # Parse the query
        parsed_query = self.parser.parse(query)

        # Execute the query
        result = self.executor.execute(parsed_query)

        return result

    def list_tables(self) -> list:
        """List all tables in the database."""
        return self.storage.list_tables()

    def get_table_info(self, table_name: str) -> Dict[str, Any]:
        """Get information about a table."""
        if table_name not in self.executor.tables:
            raise ValueError(f"Table '{table_name}' does not exist")

        table = self.executor.tables[table_name]
        return {
            'schema': [
                {
                    'name': col.name,
                    'type': col.dtype.value,
                    'is_primary': col.is_primary,
                    'is_unique': col.is_unique
                }
                for col in table['schema']
            ],
            'row_count': len(table['data']['rows'])
        }