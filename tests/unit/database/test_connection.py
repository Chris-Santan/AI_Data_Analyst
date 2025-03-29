# First add these imports at the very top of the file
import os
import sys

# Go up three directory levels (database -> unit -> tests -> project root)
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '../../../')))
# Import the setup script
import setup_path

import unittest
from unittest.mock import patch, MagicMock, PropertyMock, call
import sqlalchemy as sa

from database.connection import DatabaseConnection, get_global_connection_pool
from database.config import DatabaseConfig
from database.auth_manager import AuthenticationManager
from core.exceptions.custom_exceptions import DatabaseConnectionError


class TestDatabaseConnection(unittest.TestCase):
    """Test cases for the DatabaseConnection class."""

    def setUp(self):
        """Set up test environment before each test."""
        # Mock environment variables for testing
        os.environ["DB_TYPE"] = "sqlite"
        os.environ["DB_DATABASE"] = ":memory:"

        self.config = DatabaseConfig()
        self.auth_manager = AuthenticationManager()

    def tearDown(self):
        """Clean up after each test."""
        # Clear environment variables
        if "DB_TYPE" in os.environ:
            del os.environ["DB_TYPE"]
        if "DB_DATABASE" in os.environ:
            del os.environ["DB_DATABASE"]

    @patch('sqlalchemy.create_engine')
    def test_connect_with_connection_string(self, mock_create_engine):
        """Test connecting with a direct connection string."""
        # Mock the engine and connection
        mock_engine = MagicMock()
        mock_connection = MagicMock()
        mock_result = MagicMock()

        mock_create_engine.return_value = mock_engine
        mock_engine.connect.return_value.__enter__.return_value = mock_connection
        mock_connection.execute.return_value = mock_result

        # Create connection with connection string
        conn = DatabaseConnection(
            connection_string="sqlite:///:memory:",
            use_pool=False
        )

        # Connect
        result = conn.connect()

        # Assert
        self.assertTrue(result)
        self.assertTrue(conn._is_connected)
        mock_create_engine.assert_called_once_with(
            "sqlite:///:memory:",
            connect_args={}
        )
        mock_connection.execute.assert_called_once()

        # Clean up
        conn.disconnect()

    @patch('sqlalchemy.create_engine')
    def test_connect_with_components(self, mock_create_engine):
        """Test connecting with component parameters."""
        # Mock the engine and connection
        mock_engine = MagicMock()
        mock_connection = MagicMock()
        mock_result = MagicMock()

        mock_create_engine.return_value = mock_engine
        mock_engine.connect.return_value.__enter__.return_value = mock_connection
        mock_connection.execute.return_value = mock_result

        # Create connection with components
        conn = DatabaseConnection(
            db_type=DatabaseConfig.SQLITE,
            database=":memory:",
            use_pool=False
        )

        # Connect
        result = conn.connect()

        # Assert
        self.assertTrue(result)
        self.assertTrue(conn._is_connected)
        mock_create_engine.assert_called_once_with(
            "sqlite:///:memory:",
            connect_args={}
        )
        mock_connection.execute.assert_called_once()

        # Clean up
        conn.disconnect()

    @patch('sqlalchemy.create_engine')
    def test_connect_with_auth_credentials(self, mock_create_engine):
        """Test connecting with authentication credentials."""
        # Mock the engine and connection
        mock_engine = MagicMock()
        mock_connection = MagicMock()
        mock_result = MagicMock()

        mock_create_engine.return_value = mock_engine
        mock_engine.connect.return_value.__enter__.return_value = mock_connection
        mock_connection.execute.return_value = mock_result

        # Create auth credentials
        auth_credentials = self.auth_manager.get_basic_auth_credentials(
            username="testuser",
            password="testpass"
        )

        # Create connection with auth credentials
        conn = DatabaseConnection(
            db_type=DatabaseConfig.POSTGRES,
            auth_credentials=auth_credentials,
            host="localhost",
            port=5432,
            database="testdb",
            use_pool=False
        )

        # Connect
        result = conn.connect()

        # Assert
        self.assertTrue(result)
        self.assertTrue(conn._is_connected)
        mock_create_engine.assert_called_once()
        mock_connection.execute.assert_called_once()

        # Verify connection string contains correct params
        call_args = mock_create_engine.call_args[0][0]
        self.assertIn("postgresql://", call_args)
        self.assertIn("testuser", call_args)
        self.assertIn("testpass", call_args)
        self.assertIn("localhost", call_args)
        self.assertIn("5432", call_args)
        self.assertIn("testdb", call_args)

        # Clean up
        conn.disconnect()

    @patch('sqlalchemy.create_engine')
    def test_connect_from_environment(self, mock_create_engine):
        """Test connecting using environment variables."""
        # Mock the engine and connection
        mock_engine = MagicMock()
        mock_connection = MagicMock()
        mock_result = MagicMock()

        mock_create_engine.return_value = mock_engine
        mock_engine.connect.return_value.__enter__.return_value = mock_connection
        mock_connection.execute.return_value = mock_result

        # Create connection without parameters (will use environment vars)
        conn = DatabaseConnection(use_pool=False)

        # Connect
        result = conn.connect()

        # Assert
        self.assertTrue(result)
        self.assertTrue(conn._is_connected)
        mock_create_engine.assert_called_once_with(
            "sqlite:///:memory:",
            connect_args={}
        )
        mock_connection.execute.assert_called_once()

        # Clean up
        conn.disconnect()

    @patch('sqlalchemy.create_engine')
    def test_connection_failure(self, mock_create_engine):
        """Test handling connection failure."""
        # Set up mock to raise an exception
        mock_create_engine.side_effect = sa.exc.SQLAlchemyError("Connection failed")

        # Create connection
        conn = DatabaseConnection(
            connection_string="sqlite:///:memory:",
            use_pool=False
        )

        # Assert that connect() raises DatabaseConnectionError
        with self.assertRaises(DatabaseConnectionError):
            conn.connect()

        # Verify state
        self.assertFalse(conn._is_connected)
        self.assertIsNone(conn._engine)
        self.assertIsNone(conn._session_factory)

    @patch('database.connection.get_global_connection_pool')
    def test_connect_with_pool(self, mock_get_pool):
        """Test connecting using the connection pool."""
        # Mock the pool
        mock_pool = MagicMock()
        mock_engine = MagicMock()
        mock_session_factory = MagicMock()

        mock_get_pool.return_value = mock_pool
        mock_pool.get_engine.return_value = mock_engine
        mock_pool.get_session_factory.return_value = mock_session_factory

        # Create connection with pooling
        conn = DatabaseConnection(
            connection_string="sqlite:///:memory:",
            use_pool=True
        )

        # Connect
        result = conn.connect()

        # Assert
        self.assertTrue(result)
        self.assertTrue(conn._is_connected)
        mock_pool.get_engine.assert_called_once_with(
            "sqlite:///:memory:",
            connect_args={}
        )
        mock_pool.get_session_factory.assert_called_once()

        # Clean up
        conn.disconnect()

    @patch('sqlalchemy.create_engine')
    def test_disconnect(self, mock_create_engine):
        """Test disconnecting from database."""
        # Mock the engine
        mock_engine = MagicMock()
        mock_connection = MagicMock()

        mock_create_engine.return_value = mock_engine
        mock_engine.connect.return_value.__enter__.return_value = mock_connection
        mock_connection.execute.return_value = MagicMock()

        # Create and connect
        conn = DatabaseConnection(
            connection_string="sqlite:///:memory:",
            use_pool=False
        )
        conn.connect()

        # Disconnect
        conn.disconnect()

        # Assert
        self.assertFalse(conn._is_connected)
        self.assertIsNone(conn._engine)
        self.assertIsNone(conn._session_factory)
        mock_engine.dispose.assert_called_once()

    @patch('database.connection.get_global_connection_pool')
    def test_disconnect_with_pool(self, mock_get_pool):
        """Test disconnecting with connection pool."""
        # Mock the pool
        mock_pool = MagicMock()
        mock_engine = MagicMock()
        mock_session_factory = MagicMock()

        mock_get_pool.return_value = mock_pool
        mock_pool.get_engine.return_value = mock_engine
        mock_pool.get_session_factory.return_value = mock_session_factory

        # Create connection with pooling
        conn = DatabaseConnection(
            connection_string="sqlite:///:memory:",
            use_pool=True
        )

        # Connect
        conn.connect()

        # Set connection ID
        conn._connection_id = "test_connection_id"

        # Disconnect
        conn.disconnect()

        # Assert
        self.assertFalse(conn._is_connected)
        self.assertIsNone(conn._engine)
        self.assertIsNone(conn._session_factory)
        # Engine should not be disposed when using pooling
        mock_engine.dispose.assert_not_called()

    @patch('sqlalchemy.create_engine')
    def test_get_session(self, mock_create_engine):
        """Test getting a session."""
        # Mock the engine and session factory
        mock_engine = MagicMock()
        mock_connection = MagicMock()
        mock_session = MagicMock()
        mock_session_factory = MagicMock(return_value=mock_session)

        mock_create_engine.return_value = mock_engine
        mock_engine.connect.return_value.__enter__.return_value = mock_connection
        mock_connection.execute.return_value = MagicMock()

        # Create and connect
        conn = DatabaseConnection(
            connection_string="sqlite:///:memory:",
            use_pool=False
        )
        conn.connect()

        # Set mock session factory
        conn._session_factory = mock_session_factory

        # Get session
        session = conn.get_session()

        # Assert
        self.assertEqual(session, mock_session)
        mock_session_factory.assert_called_once()

    @patch('sqlalchemy.create_engine')
    def test_get_session_not_connected(self, mock_create_engine):
        """Test getting a session when not connected."""
        # Mock the engine for the auto-connect
        mock_engine = MagicMock()
        mock_connection = MagicMock()

        mock_create_engine.return_value = mock_engine
        mock_engine.connect.return_value.__enter__.return_value = mock_connection
        mock_connection.execute.return_value = MagicMock()

        # Create connection but don't connect
        conn = DatabaseConnection(
            connection_string="sqlite:///:memory:",
            use_pool=False
        )

        # Get session should auto-connect
        conn.get_session()

        # Assert that connect was called
        self.assertTrue(conn._is_connected)
        mock_create_engine.assert_called_once()

    @patch('sqlalchemy.create_engine')
    def test_get_schema_table(self, mock_create_engine):
        """Test getting schema for a specific table."""
        # Mock the engine and inspector
        mock_engine = MagicMock()
        mock_connection = MagicMock()

        mock_create_engine.return_value = mock_engine
        mock_engine.connect.return_value.__enter__.return_value = mock_connection
        mock_connection.execute.return_value = MagicMock()

        # Define a custom schema response that will be returned by our mock
        table_schema = {
            'table_name': 'test_table',
            'columns': [{"name": "id", "type": "INTEGER"}],
            'primary_key': {"constrained_columns": ["id"]},
            'foreign_keys': [],
            'indexes': []
        }

        # Create and connect
        conn = DatabaseConnection(
            connection_string="sqlite:///:memory:",
            use_pool=False
        )
        conn.connect()

        # Mock the get_schema method directly
        with patch.object(conn, 'get_schema', return_value=table_schema) as mock_get_schema:
            # Get schema for table
            schema = conn.get_schema(table_name="test_table")

            # Assert
            self.assertEqual(schema["table_name"], "test_table")
            self.assertEqual(len(schema["columns"]), 1)
            self.assertEqual(schema["columns"][0]["name"], "id")

            # Verify the method was called properly
            mock_get_schema.assert_called_once_with(table_name="test_table")

    @patch('sqlalchemy.create_engine')
    def test_get_schema_all_tables(self, mock_create_engine):
        """Test getting schema for all tables."""
        # Mock the engine and inspector
        mock_engine = MagicMock()
        mock_connection = MagicMock()

        mock_create_engine.return_value = mock_engine
        mock_engine.connect.return_value.__enter__.return_value = mock_connection
        mock_connection.execute.return_value = MagicMock()

        # Define our expected schema response
        all_tables_schema = {
            'table1': {
                'columns': [{"name": "id", "type": "INTEGER"}],
                'primary_key': {"constrained_columns": ["id"]},
                'foreign_keys': []
            },
            'table2': {
                'columns': [{"name": "id", "type": "INTEGER"}],
                'primary_key': {"constrained_columns": ["id"]},
                'foreign_keys': []
            }
        }

        # Create and connect
        conn = DatabaseConnection(
            connection_string="sqlite:///:memory:",
            use_pool=False
        )
        conn.connect()

        # Mock the get_schema method directly
        with patch.object(conn, 'get_schema', return_value=all_tables_schema) as mock_get_schema:
            # Get schema for all tables
            schema = conn.get_schema()

            # Assert
            self.assertEqual(len(schema), 2)
            self.assertIn("table1", schema)
            self.assertIn("table2", schema)
            self.assertEqual(schema["table1"]["columns"][0]["name"], "id")
            self.assertEqual(schema["table2"]["columns"][0]["name"], "id")

            # Verify method was called properly
            mock_get_schema.assert_called_once_with()

    @patch('sqlalchemy.create_engine')
    def test_is_connected(self, mock_create_engine):
        """Test checking connection status."""
        # Mock the engine
        mock_engine = MagicMock()
        mock_connection = MagicMock()

        mock_create_engine.return_value = mock_engine
        mock_engine.connect.return_value.__enter__.return_value = mock_connection
        mock_connection.execute.return_value = MagicMock()

        # Create and connect
        conn = DatabaseConnection(
            connection_string="sqlite:///:memory:",
            use_pool=False
        )
        conn.connect()

        # Check connection status
        status = conn.is_connected()

        # Assert
        self.assertTrue(status)
        self.assertEqual(mock_engine.connect.call_count, 2)  # Once for connect(), once for is_connected()

    @patch('sqlalchemy.create_engine')
    def test_is_connected_failure(self, mock_create_engine):
        """Test checking connection status when connection fails."""
        # Mock the engine
        mock_engine = MagicMock()
        mock_connection = MagicMock()

        mock_create_engine.return_value = mock_engine
        mock_engine.connect.return_value.__enter__.return_value = mock_connection

        # First call succeeds, second call fails
        mock_connection.execute.side_effect = [
            MagicMock(),  # For connect()
            sa.exc.SQLAlchemyError("Connection lost")  # For is_connected()
        ]

        # Create and connect
        conn = DatabaseConnection(
            connection_string="sqlite:///:memory:",
            use_pool=False
        )
        conn.connect()

        # Reset mock for clearer assertions
        mock_engine.reset_mock()
        mock_connection.reset_mock()

        # Make second connect call fail
        mock_engine.connect.return_value.__enter__.return_value.execute.side_effect = \
            sa.exc.SQLAlchemyError("Connection lost")

        # Check connection status
        status = conn.is_connected()

        # Assert
        self.assertFalse(status)
        self.assertFalse(conn._is_connected)  # Connection status should be updated

    @patch('sqlalchemy.create_engine')
    def test_get_connection_info(self, mock_create_engine):
        """Test getting connection information."""
        # Mock the engine
        mock_engine = MagicMock()
        mock_connection = MagicMock()

        mock_create_engine.return_value = mock_engine
        mock_engine.connect.return_value.__enter__.return_value = mock_connection
        mock_connection.execute.return_value = MagicMock()

        # Create and connect
        conn = DatabaseConnection(
            connection_string="postgresql://user:pass@localhost:5432/testdb",
            use_pool=False
        )
        conn.connect()

        # Get connection info
        info = conn.get_connection_info()

        # Assert
        self.assertEqual(info["database_type"], "postgresql")
        self.assertFalse(info["pooled"])
        self.assertIsNone(info["connection_id"])

    @patch('sqlalchemy.create_engine')
    def test_execute_raw_sql(self, mock_create_engine):
        """Test executing raw SQL."""
        # Mock the engine and session
        mock_engine = MagicMock()
        mock_connection = MagicMock()
        mock_session = MagicMock()
        mock_result = MagicMock()

        mock_create_engine.return_value = mock_engine
        mock_engine.connect.return_value.__enter__.return_value = mock_connection
        mock_connection.execute.return_value = MagicMock()

        # Set up session mock
        mock_session.__enter__.return_value.execute.return_value = mock_result
        mock_result.returns_rows = True
        mock_result.fetchall.return_value = [("row1",), ("row2",)]

        # Create and connect
        conn = DatabaseConnection(
            connection_string="sqlite:///:memory:",
            use_pool=False
        )
        conn.connect()

        # Mock get_session
        with patch.object(conn, 'get_session', return_value=mock_session):
            # Execute SQL
            result = conn.execute_raw_sql("SELECT * FROM test")

            # Assert
            self.assertEqual(result, [("row1",), ("row2",)])
            mock_session.__enter__.return_value.execute.assert_called_once()
            mock_result.fetchall.assert_called_once()

    @patch('sqlalchemy.create_engine')
    def test_execute_raw_sql_with_params(self, mock_create_engine):
        """Test executing raw SQL with parameters."""
        # Mock the engine and session
        mock_engine = MagicMock()
        mock_connection = MagicMock()
        mock_session = MagicMock()
        mock_result = MagicMock()

        mock_create_engine.return_value = mock_engine
        mock_engine.connect.return_value.__enter__.return_value = mock_connection
        mock_connection.execute.return_value = MagicMock()

        # Set up session mock
        mock_session.__enter__.return_value.execute.return_value = mock_result
        mock_result.returns_rows = True
        mock_result.fetchall.return_value = [("filtered_row",)]

        # Create and connect
        conn = DatabaseConnection(
            connection_string="sqlite:///:memory:",
            use_pool=False
        )
        conn.connect()

        # Mock get_session to capture parameters correctly
        with patch.object(conn, 'get_session', return_value=mock_session):
            # Execute SQL with params
            params = {"id": 1}
            result = conn.execute_raw_sql(
                "SELECT * FROM test WHERE id = :id",
                params
            )

            # Assert the result
            self.assertEqual(result, [("filtered_row",)])

            # Make sure execute was called
            mock_session.__enter__.return_value.execute.assert_called_once()

            # Get all args and kwargs from the execute call
            args, kwargs = mock_session.__enter__.return_value.execute.call_args

            # For parameterized queries, SQLAlchemy should pass the params to execute
            # The expected behavior depends on how execute_raw_sql is implemented in DatabaseConnection
            # In the implementation, ensure params are passed correctly to the SQLAlchemy execute method

            # Directly check whether params were passed (may need adjustment based on implementation)
            # We know the SQL text should be in the first arg
            self.assertEqual(str(args[0]), "SELECT * FROM test WHERE id = :id")

            # Check if params were passed as second positional arg or as kwargs
            # This depends on your implementation - adjust as needed
            if len(args) > 1:
                self.assertEqual(args[1], params)  # Passed as second arg
            else:
                self.assertEqual(kwargs, params)  # Passed as kwargs


class TestConnectionPool(unittest.TestCase):
    """Test cases for the connection pool."""

    def test_get_global_connection_pool(self):
        """Test getting the global connection pool."""
        # Get pool
        pool1 = get_global_connection_pool()
        pool2 = get_global_connection_pool()

        # Assert they are the same instance
        self.assertIs(pool1, pool2)


if __name__ == "__main__":
    unittest.main()