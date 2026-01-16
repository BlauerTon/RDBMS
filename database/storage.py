"""
File-based storage for tables and schemas.
"""

import json
import os
from pathlib import Path
from typing import Dict, List, Any, Optional
import pickle


class Storage:
    """Handles disk persistence for tables and schemas."""

    def __init__(self, data_dir: str = "data"):
        self.data_dir = Path(data_dir)
        self.data_dir.mkdir(exist_ok=True)

    def save_table_schema(self, table_name: str, columns: List[Dict]) -> None:
        """Save table schema as JSON."""
        schema_file = self.data_dir / f"{table_name}_schema.json"
        with open(schema_file, 'w') as f:
            json.dump(columns, f, indent=2)

    def load_table_schema(self, table_name: str) -> Optional[List[Dict]]:
        """Load table schema from JSON."""
        schema_file = self.data_dir / f"{table_name}_schema.json"
        if not schema_file.exists():
            return None
        with open(schema_file, 'r') as f:
            return json.load(f)

    def save_table_data(self, table_name: str, data: Dict[str, Any]) -> None:
        """Save table data using pickle for simplicity."""
        data_file = self.data_dir / f"{table_name}_data.pkl"
        with open(data_file, 'wb') as f:
            pickle.dump(data, f)

    def load_table_data(self, table_name: str) -> Optional[Dict[str, Any]]:
        """Load table data from pickle."""
        data_file = self.data_dir / f"{table_name}_data.pkl"
        if not data_file.exists():
            return None
        with open(data_file, 'rb') as f:
            return pickle.load(f)

    def table_exists(self, table_name: str) -> bool:
        """Check if a table exists on disk."""
        schema_file = self.data_dir / f"{table_name}_schema.json"
        return schema_file.exists()

    def list_tables(self) -> List[str]:
        """List all tables in the database."""
        tables = []
        for file in self.data_dir.glob("*_schema.json"):
            tables.append(file.name.replace("_schema.json", ""))
        return tables