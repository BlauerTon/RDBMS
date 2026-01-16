"""
Interactive REPL for the database.
"""

import sys
from .engine import DatabaseEngine


class DatabaseREPL:
    """Command-line REPL for interacting with the database."""

    def __init__(self, data_dir: str = "data"):
        self.engine = DatabaseEngine(data_dir)
        self.running = False

    def run(self):
        """Run the REPL."""
        self.running = True
        print("Simple RDBMS REPL")
        print("Type 'exit' or 'quit' to exit")
        print("Type 'help' for help\n")

        while self.running:
            try:
                # Get input
                line = input("db> ").strip()

                # Handle special commands
                if line.lower() in ('exit', 'quit'):
                    break
                elif line.lower() == 'help':
                    self._print_help()
                    continue
                elif line.lower() == 'tables':
                    self._list_tables()
                    continue
                elif line.lower().startswith('.tables'):
                    self._list_tables()
                    continue

                # Handle multi-line input
                query = line
                while not query.endswith(';') and query:
                    next_line = input("... ").strip()
                    if next_line.lower() in ('exit', 'quit'):
                        self.running = False
                        break
                    query += " " + next_line

                if not query or query.lower() in ('exit', 'quit'):
                    break

                # Remove trailing semicolon if present
                if query.endswith(';'):
                    query = query[:-1]

                # Execute query
                result = self.engine.execute(query)

                # Display result
                self._display_result(result)

            except (SyntaxError, ValueError) as e:
                print(f"Error: {e}")
            except KeyboardInterrupt:
                print("\nInterrupted")
                break
            except EOFError:
                print()
                break
            except Exception as e:
                print(f"Unexpected error: {e}")

    def _print_help(self):
        """Print help information."""
        help_text = """
Available commands:
  exit, quit           - Exit the REPL
  help                 - Show this help
  tables, .tables      - List all tables

SQL-like queries:
  CREATE TABLE         - Create a new table
  INSERT INTO          - Insert data into a table
  SELECT               - Query data from tables
  UPDATE               - Update data in a table
  DELETE FROM          - Delete data from a table

Examples:
  CREATE TABLE users (id INT PRIMARY KEY, name TEXT, age INT)
  INSERT INTO users VALUES (1, 'Alice', 30)
  SELECT * FROM users
  SELECT name, age FROM users WHERE age > 25
  UPDATE users SET age = 31 WHERE name = 'Alice'
  DELETE FROM users WHERE id = 1
        """
        print(help_text)

    def _list_tables(self):
        """List all tables."""
        tables = self.engine.list_tables()
        if tables:
            print("Tables:")
            for table in tables:
                info = self.engine.get_table_info(table)
                print(f"  {table} ({info['row_count']} rows)")
                for col in info['schema']:
                    constraints = []
                    if col['is_primary']:
                        constraints.append("PRIMARY KEY")
                    if col['is_unique']:
                        constraints.append("UNIQUE")
                    constraint_str = f" ({', '.join(constraints)})" if constraints else ""
                    print(f"    {col['name']} {col['type']}{constraint_str}")
        else:
            print("No tables in database.")

    def _display_result(self, result: dict):
        """Display query result in a readable format."""
        if result['status'] != 'OK':
            return

        if 'message' in result:
            print(result['message'])

        if 'row_id' in result:
            print(f"Inserted row with ID: {result['row_id']}")

        if 'updated_count' in result:
            print(f"Updated {result['updated_count']} row(s)")

        if 'deleted_count' in result:
            print(f"Deleted {result['deleted_count']} row(s)")

        if 'rows' in result:
            rows = result['rows']
            if not rows:
                print("No rows returned")
                return

            columns = result['columns']

            # Calculate column widths
            col_widths = {}
            for col in columns:
                col_widths[col] = len(str(col))

            for row in rows:
                for col in columns:
                    if col in row:
                        col_widths[col] = max(col_widths[col], len(str(row[col])))

            # Print header
            header = " | ".join(f"{col:<{col_widths[col]}}" for col in columns)
            print(header)
            print("-" * len(header))

            # Print rows
            for row in rows:
                row_str = " | ".join(f"{str(row.get(col, 'NULL')):<{col_widths[col]}}" for col in columns)
                print(row_str)

            print(f"\n{len(rows)} row(s) returned")


def main():
    """Main entry point for the REPL."""
    import argparse

    parser = argparse.ArgumentParser(description="Simple RDBMS REPL")
    parser.add_argument("--data-dir", default="data", help="Directory for database files")

    args = parser.parse_args()

    repl = DatabaseREPL(args.data_dir)
    repl.run()


if __name__ == "__main__":
    main()