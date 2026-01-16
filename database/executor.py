"""
Query executor that processes parsed queries.
"""

from typing import Dict, List, Any, Optional, Set, Tuple
from .types import DataType, Column, Index
from .storage import Storage
import json


class QueryExecutor:
    """Executes parsed queries against the database."""

    def __init__(self, storage: Storage):
        self.storage = storage
        self.tables = {}  # table_name -> {'schema': [], 'data': {}, 'indexes': {}}
        self._load_existing_tables()

    def _load_existing_tables(self):
        """Load all existing tables from storage."""
        tables = self.storage.list_tables()
        for table_name in tables:
            self._load_table(table_name)

    def _load_table(self, table_name: str):
        """Load a single table from storage."""
        schema_data = self.storage.load_table_schema(table_name)
        if not schema_data:
            return

        # Convert schema data to Column objects
        columns = []
        indexes = {}

        for col_data in schema_data:
            col = Column(
                name=col_data['name'],
                dtype=DataType(col_data['type']),
                is_primary='PRIMARY KEY' in col_data['constraints'],
                is_unique='UNIQUE' in col_data['constraints'] or 'PRIMARY KEY' in col_data['constraints']
            )
            columns.append(col)

            # Create indexes for primary key and unique columns
            if col.is_primary or col.is_unique:
                indexes[col.name] = Index(col.name, is_unique=col.is_primary or col.is_unique)

        # Load table data
        table_data = self.storage.load_table_data(table_name) or {}

        # Build indexes from existing data
        next_row_id = 1
        rows = table_data.get('rows', {})

        for row_id_str, row in rows.items():
            row_id = int(row_id_str)
            next_row_id = max(next_row_id, row_id + 1)

            for col in columns:
                if col.name in indexes:
                    value = row.get(col.name)
                    indexes[col.name].insert(value, row_id)

        self.tables[table_name] = {
            'schema': columns,
            'data': table_data,
            'indexes': indexes,
            'next_row_id': next_row_id
        }

    def execute(self, parsed_query: Dict[str, Any]) -> Any:
        """Execute a parsed query."""
        query_type = parsed_query['type']

        if query_type.name == 'CREATE_TABLE':
            return self._execute_create_table(parsed_query)
        elif query_type.name == 'INSERT':
            return self._execute_insert(parsed_query)
        elif query_type.name == 'SELECT':
            return self._execute_select(parsed_query)
        elif query_type.name == 'UPDATE':
            return self._execute_update(parsed_query)
        elif query_type.name == 'DELETE':
            return self._execute_delete(parsed_query)
        else:
            raise ValueError(f"Unsupported query type: {query_type}")

    def _execute_create_table(self, query: Dict[str, Any]) -> Dict[str, Any]:
        """Execute CREATE TABLE query."""
        table_name = query['table_name']

        if table_name in self.tables:
            raise ValueError(f"Table '{table_name}' already exists")

        columns = []
        schema_data = []

        for col_def in query['columns']:
            col = Column(
                name=col_def['name'],
                dtype=DataType(col_def['type']),
                is_primary='PRIMARY KEY' in col_def['constraints'],
                is_unique='UNIQUE' in col_def['constraints'] or 'PRIMARY KEY' in col_def['constraints']
            )
            columns.append(col)

            schema_data.append({
                'name': col.name,
                'type': col.dtype.value,
                'constraints': col_def['constraints']
            })

        # Create indexes for primary key and unique columns
        indexes = {}
        for col in columns:
            if col.is_primary or col.is_unique:
                indexes[col.name] = Index(col.name, is_unique=col.is_primary or col.is_unique)

        self.tables[table_name] = {
            'schema': columns,
            'data': {'rows': {}},
            'indexes': indexes,
            'next_row_id': 1
        }

        # Save to disk
        self.storage.save_table_schema(table_name, schema_data)
        self.storage.save_table_data(table_name, self.tables[table_name]['data'])

        return {'status': 'OK', 'message': f"Table '{table_name}' created successfully"}

    def _execute_insert(self, query: Dict[str, Any]) -> Dict[str, Any]:
        """Execute INSERT query."""
        table_name = query['table_name']

        if table_name not in self.tables:
            raise ValueError(f"Table '{table_name}' does not exist")

        table = self.tables[table_name]
        columns = table['schema']
        rows = table['data']['rows']
        indexes = table['indexes']

        # Get column order
        if query['columns']:
            col_names = query['columns']
        else:
            col_names = [col.name for col in columns]

        values = query['values']

        if len(col_names) != len(values):
            raise ValueError(f"Column count ({len(col_names)}) doesn't match value count ({len(values)})")

        # Create row dictionary
        row_data = {}
        for col_name, value in zip(col_names, values):
            # Find column definition
            col_def = next((col for col in columns if col.name == col_name), None)
            if not col_def:
                raise ValueError(f"Column '{col_name}' does not exist")

            # Validate data type
            if not col_def.validate_value(value):
                raise ValueError(f"Invalid value type for column '{col_name}'. Expected {col_def.dtype.value}")

            row_data[col_name] = value

        # Fill missing columns with NULL
        for col in columns:
            if col.name not in row_data:
                row_data[col.name] = None

        # Check constraints
        self._check_constraints(table_name, row_data, None)

        # Insert row
        row_id = table['next_row_id']
        rows[str(row_id)] = row_data
        table['next_row_id'] += 1

        # Update indexes
        for col_name, index in indexes.items():
            value = row_data.get(col_name)
            if not index.insert(value, row_id):
                # Rollback
                del rows[str(row_id)]
                table['next_row_id'] -= 1
                raise ValueError(f"Unique constraint violation on column '{col_name}'")

        # Save to disk
        self.storage.save_table_data(table_name, table['data'])

        return {'status': 'OK', 'row_id': row_id}

    def _execute_select(self, query: Dict[str, Any]) -> Dict[str, Any]:
        """Execute SELECT query."""
        table_name = query['table_name']

        if table_name not in self.tables:
            raise ValueError(f"Table '{table_name}' does not exist")

        table = self.tables[table_name]
        rows = table['data']['rows']
        schema = table['schema']

        # Get requested columns
        if query['columns'] == ["*"]:
            selected_columns = [col.name for col in schema]
        else:
            selected_columns = query['columns']

        # Apply WHERE clause
        filtered_rows = self._apply_where_clause(table_name, rows, query.get('where'))

        # Handle JOIN if specified
        if query.get('join'):
            join_result = self._execute_join(table_name, filtered_rows, query['join'])
            return {
                'status': 'OK',
                'columns': selected_columns,
                'rows': join_result
            }

        # Select columns
        result = []
        for row_id, row in filtered_rows.items():
            result_row = {}
            for col_name in selected_columns:
                if col_name in row:
                    result_row[col_name] = row[col_name]
                else:
                    raise ValueError(f"Column '{col_name}' does not exist")
            result.append(result_row)

        return {
            'status': 'OK',
            'columns': selected_columns,
            'rows': result
        }

    def _execute_update(self, query: Dict[str, Any]) -> Dict[str, Any]:
        """Execute UPDATE query."""
        table_name = query['table_name']

        if table_name not in self.tables:
            raise ValueError(f"Table '{table_name}' does not exist")

        table = self.tables[table_name]
        rows = table['data']['rows']
        indexes = table['indexes']

        # Apply WHERE clause
        filtered_rows = self._apply_where_clause(table_name, rows, query.get('where'))

        updated_count = 0

        for row_id_str in filtered_rows:
            row_id = int(row_id_str)
            row = rows[row_id_str]

            # Create updated row
            updated_row = row.copy()
            for col_name, new_value in query['set'].items():
                # Validate column exists
                col_def = next((col for col in table['schema'] if col.name == col_name), None)
                if not col_def:
                    raise ValueError(f"Column '{col_name}' does not exist")

                # Validate data type
                if not col_def.validate_value(new_value):
                    raise ValueError(f"Invalid value type for column '{col_name}'. Expected {col_def.dtype.value}")

                updated_row[col_name] = new_value

            # Check constraints
            self._check_constraints(table_name, updated_row, row_id)

            # Update indexes
            for col_name, index in indexes.items():
                old_value = row.get(col_name)
                new_value = updated_row.get(col_name)

                if old_value != new_value:
                    if not index.update(old_value, new_value, row_id):
                        raise ValueError(f"Unique constraint violation on column '{col_name}'")

            # Update row
            rows[row_id_str] = updated_row
            updated_count += 1

        # Save to disk
        if updated_count > 0:
            self.storage.save_table_data(table_name, table['data'])

        return {'status': 'OK', 'updated_count': updated_count}

    def _execute_delete(self, query: Dict[str, Any]) -> Dict[str, Any]:
        """Execute DELETE query."""
        table_name = query['table_name']

        if table_name not in self.tables:
            raise ValueError(f"Table '{table_name}' does not exist")

        table = self.tables[table_name]
        rows = table['data']['rows']
        indexes = table['indexes']

        # Apply WHERE clause
        filtered_rows = self._apply_where_clause(table_name, rows, query.get('where'))

        deleted_count = 0

        for row_id_str in list(filtered_rows.keys()):
            row_id = int(row_id_str)
            row = rows[row_id_str]

            # Remove from indexes
            for col_name, index in indexes.items():
                value = row.get(col_name)
                index.delete(value, row_id)

            # Delete row
            del rows[row_id_str]
            deleted_count += 1

        # Save to disk
        if deleted_count > 0:
            self.storage.save_table_data(table_name, table['data'])

        return {'status': 'OK', 'deleted_count': deleted_count}

    def _apply_where_clause(self, table_name: str, rows: Dict[str, Dict], conditions: Optional[List[Dict]]) -> Dict[
        str, Dict]:
        """Apply WHERE clause to filter rows."""
        if not conditions:
            return rows.copy()

        table = self.tables[table_name]
        indexes = table['indexes']

        filtered_rows = {}

        # Try to use indexes for equality conditions
        for condition in conditions:
            if condition['operator'] == '=' and condition['column'] in indexes:
                index = indexes[condition['column']]
                matching_row_ids = index.search(condition['value'])

                for row_id in matching_row_ids:
                    row_id_str = str(row_id)
                    if row_id_str in rows:
                        filtered_rows[row_id_str] = rows[row_id_str]
                return filtered_rows

        # Fallback to full scan
        for row_id, row in rows.items():
            match_all = True
            for condition in conditions:
                col_value = row.get(condition['column'])
                if condition['operator'] == '=':
                    if col_value != condition['value']:
                        match_all = False
                        break
                elif condition['operator'] == '!=':
                    if col_value == condition['value']:
                        match_all = False
                        break

            if match_all:
                filtered_rows[row_id] = row

        return filtered_rows

    def _execute_join(self, left_table_name: str, left_rows: Dict[str, Dict], join_info: Dict) -> List[Dict]:
        """Execute INNER JOIN between two tables."""
        right_table_name = join_info['table']

        if right_table_name not in self.tables:
            raise ValueError(f"Table '{right_table_name}' does not exist")

        right_table = self.tables[right_table_name]
        right_rows = right_table['data']['rows']
        right_indexes = right_table['indexes']

        left_col = join_info['left_column']
        right_col = join_info['right_column']

        result = []

        # Try to use index on right table
        if right_col in right_indexes:
            index = right_indexes[right_col]

            for left_row_id, left_row in left_rows.items():
                left_value = left_row.get(left_col)
                if left_value is None:
                    continue

                matching_right_row_ids = index.search(left_value)

                for right_row_id in matching_right_row_ids:
                    right_row = right_rows.get(str(right_row_id))
                    if right_row:
                        # Merge rows
                        merged_row = {f"{left_table_name}.{k}": v for k, v in left_row.items()}
                        merged_row.update({f"{right_table_name}.{k}": v for k, v in right_row.items()})
                        result.append(merged_row)

        else:
            # Nested loop join
            for left_row_id, left_row in left_rows.items():
                left_value = left_row.get(left_col)
                if left_value is None:
                    continue

                for right_row_id, right_row in right_rows.items():
                    right_value = right_row.get(right_col)
                    if right_value == left_value:
                        # Merge rows
                        merged_row = {f"{left_table_name}.{k}": v for k, v in left_row.items()}
                        merged_row.update({f"{right_table_name}.{k}": v for k, v in right_row.items()})
                        result.append(merged_row)

        return result

    def _check_constraints(self, table_name: str, row: Dict, exclude_row_id: Optional[int] = None):
        """Check constraints for a row."""
        table = self.tables[table_name]
        columns = table['schema']
        rows = table['data']['rows']
        indexes = table['indexes']

        for col in columns:
            value = row.get(col.name)

            # Check NOT NULL constraint for primary keys
            if col.is_primary and value is None:
                raise ValueError(f"Primary key column '{col.name}' cannot be NULL")

            # Check unique constraints
            if (col.is_primary or col.is_unique) and value is not None:
                index = indexes[col.name]

                # Check if value already exists (excluding current row)
                if index.has_value(value):
                    existing_rows = index.search(value)

                    # If exclude_row_id is provided and it's the only existing row, that's OK (for updates)
                    if exclude_row_id is not None:
                        existing_rows = existing_rows - {exclude_row_id}

                    if existing_rows:
                        raise ValueError(f"Duplicate value '{value}' for unique column '{col.name}'")