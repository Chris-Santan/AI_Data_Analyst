# First add these imports at the very top of the file
import os
import sys

# Go up three directory levels (database -> unit -> tests -> project root)
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '../../../')))
# Import the setup script
import setup_path

import unittest
from unittest.mock import patch, MagicMock, PropertyMock

import sqlalchemy as sa
from sqlalchemy.engine import Engine

from database.schema_retriever import SchemaRetriever
from database.connection import DatabaseConnection
from core.exceptions.custom_exceptions import DatabaseConnectionError


class TestSchemaRetriever(unittest.TestCase):
    """Test cases for the SchemaRetriever class."""

    def setUp(self):
        """Set up test environment before each test."""
        # Mock DatabaseConnection
        self.mock_connection = MagicMock(spec=DatabaseConnection)

        # Create mock engine
        self.mock_engine = MagicMock(spec=Engine)

        # Set up connection to return the engine
        type(self.mock_connection)._engine = PropertyMock(return_value=self.mock_engine)

        # Create SchemaRetriever
        self.schema_retriever = SchemaRetriever(self.mock_connection)

    def test_get_engine_success(self):
        """Test getting the engine successfully."""
        # Get engine
        engine = self.schema_retriever._get_engine()

        # Assert
        self.assertEqual(engine, self.mock_engine)

    def test_get_engine_failure(self):
        """Test handling missing engine."""
        # Set engine to None
        type(self.mock_connection)._engine = PropertyMock(return_value=None)

        # Try to get engine
        with self.assertRaises(DatabaseConnectionError) as context:
            self.schema_retriever._get_engine()

        # Assert error message
        self.assertIn("No active database engine", str(context.exception))

    # Instead of trying to patch inspect, let's directly test class methods with mocked dependencies
    def test_get_all_tables(self):
        """Test getting all tables."""
        # Mock the _get_engine method and inspect
        with patch.object(SchemaRetriever, '_get_engine') as mock_get_engine:
            with patch('database.schema_retriever.inspect') as mock_inspect:
                # Setup mocks
                mock_inspector = MagicMock()
                mock_inspect.return_value = mock_inspector
                mock_get_engine.return_value = self.mock_engine
                mock_inspector.get_table_names.return_value = ["users", "products", "orders"]

                # Call the method
                tables = self.schema_retriever.get_all_tables()

                # Assertions
                self.assertEqual(tables, ["users", "products", "orders"])
                mock_get_engine.assert_called_once()
                mock_inspect.assert_called_once_with(self.mock_engine)
                mock_inspector.get_table_names.assert_called_once()

    def test_get_table_schema(self):
        """Test getting schema for a specific table."""
        # Mock the _get_engine method and inspect
        with patch.object(SchemaRetriever, '_get_engine') as mock_get_engine:
            with patch('database.schema_retriever.inspect') as mock_inspect:
                # Setup mocks
                mock_inspector = MagicMock()
                mock_inspect.return_value = mock_inspector
                mock_get_engine.return_value = self.mock_engine

                # Set up inspector mock return values
                mock_inspector.get_columns.return_value = [
                    {"name": "id", "type": sa.Integer(), "nullable": False},
                    {"name": "name", "type": sa.String(), "nullable": True}
                ]
                mock_inspector.get_pk_constraint.return_value = {
                    "name": "pk_users",
                    "constrained_columns": ["id"]
                }
                mock_inspector.get_foreign_keys.return_value = []
                mock_inspector.get_indexes.return_value = []
                mock_inspector.get_unique_constraints.return_value = []

                # Call the method
                schema = self.schema_retriever.get_table_schema("users")

                # Assertions
                self.assertEqual(schema["table_name"], "users")
                self.assertEqual(len(schema["columns"]), 2)
                self.assertEqual(schema["columns"][0]["name"], "id")
                self.assertEqual(schema["primary_key"]["constrained_columns"], ["id"])

                # Verify methods were called
                mock_get_engine.assert_called_once()
                mock_inspect.assert_called_once_with(self.mock_engine)
                mock_inspector.get_columns.assert_called_once_with("users")
                mock_inspector.get_pk_constraint.assert_called_once_with("users")
                mock_inspector.get_foreign_keys.assert_called_once_with("users")
                mock_inspector.get_indexes.assert_called_once_with("users")
                mock_inspector.get_unique_constraints.assert_called_once_with("users")

    def test_get_column_metadata(self):
        """Test getting column metadata."""
        # Mock the _get_engine method and inspect
        with patch.object(SchemaRetriever, '_get_engine') as mock_get_engine:
            with patch('database.schema_retriever.inspect') as mock_inspect:
                # Setup mocks
                mock_inspector = MagicMock()
                mock_inspect.return_value = mock_inspector
                mock_get_engine.return_value = self.mock_engine

                # Set up inspector mock return values
                mock_inspector.get_columns.return_value = [
                    {
                        "name": "id",
                        "type": sa.Integer(),
                        "nullable": False,
                        "default": sa.text("nextval('users_id_seq'::regclass)"),
                        "comment": "Primary key"
                    },
                    {
                        "name": "name",
                        "type": sa.String(),
                        "nullable": True,
                        "default": None,
                        "comment": "User's full name"
                    }
                ]

                # Call the method
                metadata = self.schema_retriever.get_column_metadata("users")

                # Assertions
                self.assertEqual(len(metadata), 2)
                self.assertEqual(metadata[0]["name"], "id")
                self.assertEqual(metadata[0]["type"], str(sa.Integer()))
                self.assertEqual(metadata[0]["nullable"], False)

                # Verify methods were called
                mock_get_engine.assert_called_once()
                mock_inspect.assert_called_once_with(self.mock_engine)
                mock_inspector.get_columns.assert_called_once_with("users")

    def test_get_table_relationships(self):
        """Test getting table relationships."""
        # Mock the _get_engine method, get_all_tables, and inspect
        with patch.object(SchemaRetriever, '_get_engine') as mock_get_engine:
            with patch('database.schema_retriever.inspect') as mock_inspect:
                with patch.object(SchemaRetriever, 'get_all_tables') as mock_get_tables:
                    # Setup mocks
                    mock_inspector = MagicMock()
                    mock_inspect.return_value = mock_inspector
                    mock_get_engine.return_value = self.mock_engine
                    mock_get_tables.return_value = ["users", "orders", "products"]

                    # Set up outgoing foreign keys
                    outgoing_fks = []

                    # Set up incoming foreign keys
                    incoming_fks = [
                        {
                            "name": "fk_orders_user",
                            "referred_table": "users",
                            "referred_columns": ["id"],
                            "constrained_columns": ["user_id"]
                        }
                    ]

                    # Set up mock returns
                    def get_foreign_keys_side_effect(table_name):
                        if table_name == "users":
                            return outgoing_fks
                        elif table_name == "orders":
                            return incoming_fks
                        else:
                            return []

                    mock_inspector.get_foreign_keys.side_effect = get_foreign_keys_side_effect

                    # Call the method
                    relationships = self.schema_retriever.get_table_relationships("users")

                    # Assertions
                    self.assertEqual(len(relationships["outgoing"]), 0)
                    self.assertEqual(len(relationships["incoming"]), 1)
                    self.assertEqual(relationships["incoming"][0]["referred_table"], "users")
                    self.assertEqual(relationships["incoming"][0]["constrained_columns"], ["user_id"])
                    self.assertEqual(relationships["incoming"][0]["table_name"], "orders")

                    # Verify methods were called
                    mock_get_engine.assert_called_once()
                    mock_inspect.assert_called_once_with(self.mock_engine)
                    mock_get_tables.assert_called_once()
                    # At least two calls: once for users and once for orders
                    self.assertGreaterEqual(mock_inspector.get_foreign_keys.call_count, 2)

    def test_get_database_schema(self):
        """Test getting schema for the entire database."""
        # Mock the get_all_tables and get_table_schema methods
        with patch.object(SchemaRetriever, 'get_all_tables') as mock_get_tables:
            with patch.object(SchemaRetriever, 'get_table_schema') as mock_get_schema:
                # Setup mocks
                mock_get_tables.return_value = ["users", "orders"]
                mock_get_schema.side_effect = [
                    {"table_name": "users", "columns": [{"name": "id"}, {"name": "name"}]},
                    {"table_name": "orders", "columns": [{"name": "id"}, {"name": "user_id"}]}
                ]

                # Call the method
                schema = self.schema_retriever.get_database_schema()

                # Assertions
                self.assertIn("tables", schema)
                self.assertEqual(len(schema["tables"]), 2)
                self.assertIn("users", schema["tables"])
                self.assertIn("orders", schema["tables"])

                # Verify methods were called
                mock_get_tables.assert_called_once()
                self.assertEqual(mock_get_schema.call_count, 2)
                mock_get_schema.assert_any_call("users")
                mock_get_schema.assert_any_call("orders")

    def test_get_schema_summary(self):
        """Test getting a summarized schema."""
        # Mock the get_all_tables and get_column_metadata methods
        with patch.object(SchemaRetriever, 'get_all_tables') as mock_get_tables:
            with patch.object(SchemaRetriever, 'get_column_metadata') as mock_get_columns:
                # Setup mocks
                mock_get_tables.return_value = ["users", "orders"]
                mock_get_columns.side_effect = [
                    [
                        {"name": "id", "type": "INTEGER"},
                        {"name": "name", "type": "VARCHAR"}
                    ],
                    [
                        {"name": "id", "type": "INTEGER"},
                        {"name": "user_id", "type": "INTEGER"},
                        {"name": "total", "type": "NUMERIC"}
                    ]
                ]

                # Call the method
                summary = self.schema_retriever.get_schema_summary()

                # Assertions
                self.assertEqual(summary["table_count"], 2)
                self.assertEqual(len(summary["tables"]), 2)
                self.assertIn("users", summary["tables"])
                self.assertIn("orders", summary["tables"])
                self.assertEqual(summary["tables"]["users"]["column_count"], 2)
                self.assertEqual(summary["tables"]["users"]["columns"], ["id", "name"])

                # Verify methods were called
                mock_get_tables.assert_called_once()
                self.assertEqual(mock_get_columns.call_count, 2)
                mock_get_columns.assert_any_call("users")
                mock_get_columns.assert_any_call("orders")


if __name__ == "__main__":
    unittest.main()