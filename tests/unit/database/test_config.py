# First add these imports at the very top of the file
import os
import sys
# Go up three directory levels (database -> unit -> tests -> project root)
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '../../../')))
# Import the setup script

import unittest
from unittest.mock import patch, MagicMock
from pathlib import Path

from data_analytics_platform.database.config import DatabaseConfig
from data_analytics_platform.core.exceptions.custom_exceptions import DatabaseConnectionError


class TestDatabaseConfig(unittest.TestCase):
    """Test cases for the DatabaseConfig class."""

    def setUp(self):
        """Set up test environment before each test."""
        self.config = DatabaseConfig()

    def tearDown(self):
        """Clean up after each test."""
        # Clear environment variables that might affect tests
        env_vars = [
            "DB_TYPE", "DB_USERNAME", "DB_PASSWORD",
            "DB_HOST", "DB_PORT", "DB_DATABASE"
        ]
        for var in env_vars:
            if var in os.environ:
                del os.environ[var]

    def test_get_connection_string_sqlite(self):
        """Test getting SQLite connection string."""
        conn_string = self.config.get_connection_string(
            db_type=DatabaseConfig.SQLITE,
            database="test.db"
        )
        self.assertEqual(conn_string, "sqlite:///test.db")

    def test_get_connection_string_postgres(self):
        """Test getting PostgreSQL connection string."""
        conn_string = self.config.get_connection_string(
            db_type=DatabaseConfig.POSTGRES,
            username="testuser",
            password="testpass",
            host="localhost",
            port=5432,
            database="testdb"
        )
        self.assertEqual(
            conn_string,
            "postgresql://testuser:testpass@localhost:5432/testdb"
        )

    def test_get_connection_string_mysql(self):
        """Test getting MySQL connection string."""
        conn_string = self.config.get_connection_string(
            db_type=DatabaseConfig.MYSQL,
            username="testuser",
            password="testpass",
            host="localhost",
            port=3306,
            database="testdb"
        )
        self.assertEqual(
            conn_string,
            "mysql+pymysql://testuser:testpass@localhost:3306/testdb"
        )

    def test_get_connection_string_missing_params(self):
        """Test handling missing parameters for connection string."""
        # SQLite only needs database
        self.config.get_connection_string(
            db_type=DatabaseConfig.SQLITE,
            database="test.db"
        )

        # PostgreSQL needs all parameters
        with self.assertRaises(ValueError):
            self.config.get_connection_string(
                db_type=DatabaseConfig.POSTGRES,
                database="testdb"  # Missing username, password, host
            )

    def test_get_connection_string_unsupported_db(self):
        """Test handling unsupported database type."""
        with self.assertRaises(ValueError):
            self.config.get_connection_string(
                db_type="unsupported",
                database="test.db"
            )

    def test_get_connection_string_from_env_sqlite(self):
        """Test getting SQLite connection string from environment variables."""
        # Set environment variables
        os.environ["DB_TYPE"] = "sqlite"
        os.environ["DB_DATABASE"] = "test.db"

        conn_string = self.config.get_connection_string_from_env()
        self.assertEqual(conn_string, "sqlite:///test.db")

    def test_get_connection_string_from_env_postgres(self):
        """Test getting PostgreSQL connection string from environment variables."""
        # Set environment variables
        os.environ["DB_TYPE"] = "postgresql"
        os.environ["DB_USERNAME"] = "testuser"
        os.environ["DB_PASSWORD"] = "testpass"
        os.environ["DB_HOST"] = "localhost"
        os.environ["DB_PORT"] = "5432"
        os.environ["DB_DATABASE"] = "testdb"

        conn_string = self.config.get_connection_string_from_env()
        self.assertEqual(
            conn_string,
            "postgresql://testuser:testpass@localhost:5432/testdb"
        )

    def test_get_connection_string_from_env_missing_vars(self):
        """Test handling missing environment variables."""
        # Missing DB_TYPE
        with self.assertRaises(DatabaseConnectionError):
            self.config.get_connection_string_from_env()

        # Set DB_TYPE but missing other required vars for PostgreSQL
        os.environ["DB_TYPE"] = "postgresql"
        os.environ["DB_DATABASE"] = "testdb"
        with self.assertRaises(DatabaseConnectionError):
            self.config.get_connection_string_from_env()

    def test_get_connection_string_from_env_invalid_port(self):
        """Test handling invalid port in environment variables."""
        # Set environment variables with invalid port
        os.environ["DB_TYPE"] = "postgresql"
        os.environ["DB_USERNAME"] = "testuser"
        os.environ["DB_PASSWORD"] = "testpass"
        os.environ["DB_HOST"] = "localhost"
        os.environ["DB_PORT"] = "invalid"  # Invalid port
        os.environ["DB_DATABASE"] = "testdb"

        with self.assertRaises(DatabaseConnectionError):
            self.config.get_connection_string_from_env()

    def test_set_config_file(self):
        """Test setting configuration file path."""
        # Mock file existence check
        with patch.object(Path, 'exists', return_value=True):
            self.config.set_config_file("/path/to/config.ini")
            self.assertEqual(self.config._config_file_path, Path("/path/to/config.ini"))

    def test_set_config_file_nonexistent(self):
        """Test setting non-existent configuration file path."""
        with patch.object(Path, 'exists', return_value=False):
            with self.assertRaises(ValueError):
                self.config.set_config_file("/path/to/nonexistent.ini")

    def test_set_env_prefix(self):
        """Test setting environment variable prefix."""
        self.config.set_env_prefix("TEST_")
        self.assertEqual(self.config._env_prefix, "TEST_")

    def test_get_connection_params(self):
        """Test getting connection parameters."""
        # Mock auth_manager
        auth_manager_mock = MagicMock()
        auth_manager_mock.get_auth_params.return_value = {
            "username": "testuser",
            "password": "testpass"
        }
        self.config.auth_manager = auth_manager_mock

        # Get connection params
        params = self.config.get_connection_params(
            db_type=DatabaseConfig.POSTGRES,
            auth_credentials={"auth_type": "basic"},
            host="localhost",
            port=5432,
            database="testdb"
        )

        # Assert
        self.assertEqual(params["db_type"], DatabaseConfig.POSTGRES)
        self.assertEqual(params["username"], "testuser")
        self.assertEqual(params["password"], "testpass")
        self.assertEqual(params["host"], "localhost")
        self.assertEqual(params["port"], 5432)
        self.assertEqual(params["database"], "testdb")

    def test_get_connection_params_missing_host(self):
        """Test handling missing host in connection parameters."""
        # Mock auth_manager
        auth_manager_mock = MagicMock()
        auth_manager_mock.get_auth_params.return_value = {
            "username": "testuser",
            "password": "testpass"
        }
        self.config.auth_manager = auth_manager_mock

        # Missing host for non-SQLite database
        with self.assertRaises(ValueError):
            self.config.get_connection_params(
                db_type=DatabaseConfig.POSTGRES,
                auth_credentials={"auth_type": "basic"},
                database="testdb"  # Missing host
            )

    def test_get_connection_params_sqlite(self):
        """Test getting connection parameters for SQLite."""
        # Mock auth_manager to avoid the actual validation
        auth_manager_mock = MagicMock()
        auth_manager_mock.get_auth_params.return_value = {}
        self.config.auth_manager = auth_manager_mock

        # SQLite doesn't require host or auth, but we need a valid auth_type
        # Use BASIC_AUTH which is a valid type
        params = self.config.get_connection_params(
            db_type=DatabaseConfig.SQLITE,
            auth_credentials={"auth_type": "basic"},  # Use a valid auth type
            database="test.db"
        )

        # Assert
        self.assertEqual(params["db_type"], DatabaseConfig.SQLITE)
        self.assertEqual(params["database"], "test.db")

        # Verify auth_manager was called with the correct credentials
        auth_manager_mock.get_auth_params.assert_called_once_with({"auth_type": "basic"})

    def test_get_connection_pool_args(self):
        """Test getting connection pool arguments."""
        pool_args = self.config.get_connection_pool_args(
            pool_size=10,
            max_overflow=15,
            pool_timeout=60,
            pool_recycle=3600
        )

        # Assert
        self.assertEqual(pool_args["pool_size"], 10)
        self.assertEqual(pool_args["max_overflow"], 15)
        self.assertEqual(pool_args["pool_timeout"], 60)
        self.assertEqual(pool_args["pool_recycle"], 3600)

    def test_get_connection_pool_args_default(self):
        """Test getting default connection pool arguments."""
        pool_args = self.config.get_connection_pool_args()

        # Assert default values
        self.assertEqual(pool_args["pool_size"], 5)
        self.assertEqual(pool_args["max_overflow"], 10)
        self.assertEqual(pool_args["pool_timeout"], 30)
        self.assertEqual(pool_args["pool_recycle"], 1800)


if __name__ == "__main__":
    unittest.main()