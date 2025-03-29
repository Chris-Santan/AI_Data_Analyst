import os
import sys
# Go up three directory levels (database -> unit -> tests -> project root)
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '../../../')))
# Import the setup script

import unittest
from unittest.mock import patch, MagicMock
from datetime import datetime
import datetime
from decimal import Decimal

from sqlalchemy.exc import SQLAlchemyError

from data_analytics_platform.database import QueryExecutor
from data_analytics_platform.database.connection import DatabaseConnection
from data_analytics_platform.core.exceptions.custom_exceptions import QueryExecutionError


class TestQueryExecutor(unittest.TestCase):
    """Test cases for the QueryExecutor class."""

    def setUp(self):
        """Set up test environment before each test."""
        # Mock DatabaseConnection
        self.mock_connection = MagicMock(spec=DatabaseConnection)
        self.query_executor = QueryExecutor(self.mock_connection)

    def test_validate_query_valid(self):
        """Test validating valid SQL queries."""
        # Test various valid query types
        valid_queries = [
            "SELECT * FROM users",
            "SELECT id, name FROM products WHERE price > 10",
            "SELECT * FROM orders;",
            "WITH temp AS (SELECT * FROM users) SELECT * FROM temp",
            "SHOW TABLES",
            "DESCRIBE users",
            "EXPLAIN SELECT * FROM users"
        ]

        for query in valid_queries:
            self.assertTrue(
                self.query_executor.validate_query(query),
                f"Query should be valid: {query}"
            )

    def test_validate_query_invalid(self):
        """Test validating invalid SQL queries."""
        # Test various invalid query types
        invalid_queries = [
            "",  # Empty query
            "   ",  # Whitespace only
            "INSERT INTO users VALUES (1, 'test')",  # Non-SELECT/SHOW/DESCRIBE
            "UPDATE users SET name = 'test'",  # Non-SELECT/SHOW/DESCRIBE
            "DELETE FROM users",  # Non-SELECT/SHOW/DESCRIBE
            "DROP TABLE users",  # Non-SELECT/SHOW/DESCRIBE
            "SELECT * FROM users; DELETE FROM users",  # Multiple statements
        ]

        for query in invalid_queries:
            self.assertFalse(
                self.query_executor.validate_query(query),
                f"Query should be invalid: {query}"
            )

    def test_sanitize_query(self):
        """Test query sanitization."""
        # Test sanitizing queries with potential SQL injection
        original = "SELECT * FROM users; DROP TABLE users; --comment"
        sanitized = self.query_executor.sanitize_query(original)

        # Should remove everything after the first semicolon and comments
        self.assertEqual(sanitized, "SELECT * FROM users;")

        # Test with comment
        original = "SELECT * FROM users -- get all users"
        sanitized = self.query_executor.sanitize_query(original)

        # Should remove comments
        self.assertEqual(sanitized, "SELECT * FROM users")

    def test_execute_query_success(self):
        """Test executing a query successfully."""
        # Mock session
        mock_session = MagicMock()
        mock_result = MagicMock()
        mock_result.returns_rows = True
        mock_result.keys.return_value = ["id", "name"]
        mock_result.__iter__.return_value = [(1, "test"), (2, "test2")]

        # Set up the mock session and connection
        self.mock_connection.get_session.return_value = mock_session
        mock_session.__enter__.return_value.execute.return_value = mock_result

        # Execute query
        result = self.query_executor.execute_query("SELECT * FROM users")

        # Assert
        self.assertEqual(len(result), 2)
        self.assertEqual(result[0]["id"], 1)
        self.assertEqual(result[0]["name"], "test")
        self.assertEqual(result[1]["id"], 2)
        self.assertEqual(result[1]["name"], "test2")

        # Verify execute was called with the right query
        mock_session.__enter__.return_value.execute.assert_called_once()
        args, _ = mock_session.__enter__.return_value.execute.call_args
        self.assertEqual(str(args[0]), "SELECT * FROM users")

    def test_execute_query_no_rows(self):
        """Test executing a query that doesn't return rows."""
        # Mock session
        mock_session = MagicMock()
        mock_result = MagicMock()
        mock_result.returns_rows = False

        # Set up the mock session and connection
        self.mock_connection.get_session.return_value = mock_session
        mock_session.__enter__.return_value.execute.return_value = mock_result

        # Execute query
        result = self.query_executor.execute_query("EXPLAIN SELECT * FROM users")

        # Assert
        self.assertEqual(len(result), 1)
        self.assertEqual(result[0]["message"], "Query executed successfully. No rows returned.")

    def test_execute_query_invalid(self):
        """Test executing an invalid query."""
        # Try to execute an invalid query
        with self.assertRaises(QueryExecutionError) as context:
            self.query_executor.execute_query("INSERT INTO users VALUES (1, 'test')")

        # Assert error message
        self.assertIn("Invalid query structure or syntax", str(context.exception))

    def test_execute_query_db_error(self):
        """Test handling database errors during query execution."""
        # Mock session to raise an error
        mock_session = MagicMock()
        mock_error = SQLAlchemyError("Database error")

        # Set up the mock session and connection
        self.mock_connection.get_session.return_value = mock_session
        mock_session.__enter__.return_value.execute.side_effect = mock_error

        # Execute query
        with self.assertRaises(QueryExecutionError) as context:
            self.query_executor.execute_query("SELECT * FROM users")

        # Assert error message
        self.assertIn("Database error", str(context.exception))

    def test_execute_select_count(self):
        """Test executing a COUNT query."""
        # Mock the execute_query method
        with patch.object(self.query_executor, 'execute_query') as mock_execute:
            # Set up mock return value
            mock_execute.return_value = [{"count": 10}]

            # Execute count query
            result = self.query_executor.execute_select_count("users")

            # Assert
            self.assertEqual(result, 10)
            mock_execute.assert_called_once_with("SELECT COUNT(*) FROM users")

    def test_execute_select_count_error(self):
        """Test handling errors during COUNT query execution."""
        # Mock the execute_query method to raise an error
        with patch.object(self.query_executor, 'execute_query') as mock_execute:
            mock_execute.side_effect = QueryExecutionError(
                query="SELECT COUNT(*) FROM users",
                error_message="Table not found"
            )

            # Execute count query
            with self.assertRaises(QueryExecutionError) as context:
                self.query_executor.execute_select_count("users")

            # Assert error message
            self.assertIn("Failed to count rows in users", str(context.exception))

    def test_execute_query_with_parameters(self):
        """Test executing a parameterized query."""
        # Mock session
        mock_session = MagicMock()
        mock_result = MagicMock()
        mock_result.returns_rows = True
        mock_result.keys.return_value = ["id", "name"]
        mock_result.__iter__.return_value = [(1, "test")]

        # Set up the mock session and connection
        self.mock_connection.get_session.return_value = mock_session

        # Create a custom mock for session.execute to capture parameters properly
        execute_mock = MagicMock(return_value=mock_result)
        mock_session.__enter__.return_value.execute = execute_mock

        # Execute parameterized query
        params = {"user_id": 1}
        result = self.query_executor.execute_query_with_parameters(
            "SELECT * FROM users WHERE id = :user_id",
            params
        )

        # Assert
        self.assertEqual(len(result), 1)
        self.assertEqual(result[0]["id"], 1)
        self.assertEqual(result[0]["name"], "test")

        # Verify session.execute was called
        execute_mock.assert_called_once()

        # Get the call arguments
        args, kwargs = execute_mock.call_args

        # Check the SQL statement
        self.assertEqual(str(args[0]), "SELECT * FROM users WHERE id = :user_id")

        # The parameters could be passed in different ways depending on the SQLAlchemy version
        # and QueryExecutor implementation
        if len(args) > 1:
            # Parameters were passed as positional argument
            self.assertEqual(args[1], params)
        elif kwargs:
            # Parameters were passed as keyword arguments
            self.assertEqual(kwargs, params)
        else:
            # Parameters were passed in a different way than expected
            # Compare the stringified SQL to see if parameters were bound
            # This is a bit of a hack but can help when normal assertions fail
            self.fail("Parameters were not properly passed to session.execute")

    def test_execute_query_with_parameters_invalid(self):
        """Test executing an invalid parameterized query."""
        # Try to execute an invalid query
        with self.assertRaises(QueryExecutionError) as context:
            self.query_executor.execute_query_with_parameters(
                "INSERT INTO users VALUES (:id, :name)",
                {"id": 1, "name": "test"}
            )

        # Assert error message
        self.assertIn("Invalid query structure or syntax", str(context.exception))

    def test_execute_query_with_parameters_db_error(self):
        """Test handling database errors during parameterized query execution."""
        # Mock session to raise an error
        mock_session = MagicMock()
        mock_error = SQLAlchemyError("Database error")

        # Set up the mock session and connection
        self.mock_connection.get_session.return_value = mock_session
        mock_session.__enter__.return_value.execute.side_effect = mock_error

        # Execute query
        with self.assertRaises(QueryExecutionError) as context:
            self.query_executor.execute_query_with_parameters(
                "SELECT * FROM users WHERE id = :user_id",
                {"user_id": 1}
            )

        # Assert error message
        self.assertIn("Database error", str(context.exception))

    # The following tests can be added to your existing test_query_executor.py file

    def test_execute_query_large_result_set(self):
        """Test handling of large result sets."""
        # Mock session
        mock_session = MagicMock()
        mock_result = MagicMock()
        mock_result.returns_rows = True
        mock_result.keys.return_value = ["id", "name"]

        # Create a large result set (1000 rows)
        large_data = [(i, f"test{i}") for i in range(1000)]
        mock_result.__iter__.return_value = large_data

        # Set up the mock session and connection
        self.mock_connection.get_session.return_value = mock_session
        mock_session.__enter__.return_value.execute.return_value = mock_result

        # Execute query
        result = self.query_executor.execute_query("SELECT * FROM large_table")

        # Assert
        self.assertEqual(len(result), 1000)
        self.assertEqual(result[0]["id"], 0)
        self.assertEqual(result[999]["id"], 999)

    def test_execute_query_empty_result(self):
        """Test handling of empty result sets."""
        # Mock session
        mock_session = MagicMock()
        mock_result = MagicMock()
        mock_result.returns_rows = True
        mock_result.keys.return_value = ["id", "name"]
        mock_result.__iter__.return_value = []  # Empty result set

        # Set up the mock session and connection
        self.mock_connection.get_session.return_value = mock_session
        mock_session.__enter__.return_value.execute.return_value = mock_result

        # Execute query
        result = self.query_executor.execute_query("SELECT * FROM users WHERE 1=0")

        # Assert
        self.assertEqual(len(result), 0)
        self.assertEqual(result, [])

    def test_execute_query_with_null_values(self):
        """Test handling of NULL values in query results."""
        # Mock session
        mock_session = MagicMock()
        mock_result = MagicMock()
        mock_result.returns_rows = True
        mock_result.keys.return_value = ["id", "name", "email"]
        mock_result.__iter__.return_value = [
            (1, "test", None),  # NULL email
            (2, None, "test@example.com"),  # NULL name
            (3, "test3", "test3@example.com")  # No NULLs
        ]

        # Set up the mock session and connection
        self.mock_connection.get_session.return_value = mock_session
        mock_session.__enter__.return_value.execute.return_value = mock_result

        # Execute query
        result = self.query_executor.execute_query("SELECT * FROM users")

        # Assert
        self.assertEqual(len(result), 3)
        self.assertIsNone(result[0]["email"])
        self.assertIsNone(result[1]["name"])
        self.assertIsNotNone(result[2]["name"])
        self.assertIsNotNone(result[2]["email"])

    @patch("data_analytics_platform.database.error_handler.DatabaseErrorHandler.execute_with_retry")
    def test_retry_mechanism(self, mock_retry):
        """Test that the retry mechanism is used during query execution."""
        # Set up mock return value
        mock_retry.return_value = [{"id": 1, "name": "test"}]

        # Execute query
        result = self.query_executor.execute_query("SELECT * FROM users")

        # Assert retry was called
        mock_retry.assert_called_once()
        self.assertEqual(len(result), 1)

    def test_query_with_special_characters(self):
        """Test query with special characters and Unicode."""
        # Mock session
        mock_session = MagicMock()
        mock_result = MagicMock()
        mock_result.returns_rows = True
        mock_result.keys.return_value = ["id", "name"]
        mock_result.__iter__.return_value = [(1, "José"), (2, "你好")]

        # Set up the mock session and connection
        self.mock_connection.get_session.return_value = mock_session
        mock_session.__enter__.return_value.execute.return_value = mock_result

        # Execute query with Unicode characters
        query = "SELECT * FROM users WHERE name LIKE '%ö%'"
        result = self.query_executor.execute_query(query)

        # Assert
        self.assertEqual(len(result), 2)
        self.assertEqual(result[0]["name"], "José")
        self.assertEqual(result[1]["name"], "你好")

        # Verify execute was called with the right query
        mock_session.__enter__.return_value.execute.assert_called_once()
        args, _ = mock_session.__enter__.return_value.execute.call_args
        self.assertEqual(str(args[0]), query)

    def test_execute_query_with_complex_parameters(self):
        """Test executing a query with complex parameter types."""
        # Mock session
        mock_session = MagicMock()
        mock_result = MagicMock()
        mock_result.returns_rows = True
        mock_result.keys.return_value = ["id", "name"]
        mock_result.__iter__.return_value = [(1, "test"), (2, "test2")]

        # Set up the mock session and connection
        self.mock_connection.get_session.return_value = mock_session
        mock_session.__enter__.return_value.execute.return_value = mock_result

        # Execute parameterized query with complex parameters
        params = {
            "ids": [1, 2, 3],  # List
            "start_date": datetime.date(2023, 1, 1),  # Date object
            "active": True,  # Boolean
            "amount": Decimal("123.45")  # Decimal
        }

        result = self.query_executor.execute_query_with_parameters(
            "SELECT * FROM users WHERE id IN :ids AND created_at > :start_date AND active = :active AND balance > :amount",
            params
        )

        # Assert
        self.assertEqual(len(result), 2)

        # Verify execute was called with the parameters
        mock_session.__enter__.return_value.execute.assert_called_once()


if __name__ == "__main__":
    unittest.main()