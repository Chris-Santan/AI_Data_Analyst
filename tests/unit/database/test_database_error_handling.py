# tests/unit/database/test_database_error_handling.py
import os
import sys
# Go up three directory levels (database -> unit -> tests -> project root)
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '../../../')))
# Import the setup script

import unittest
import logging
import sqlite3

# Import custom exceptions
from data_analytics_platform.core.exceptions.custom_exceptions import QueryExecutionError, DatabaseConnectionError

# Set up basic logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)


class TestDatabaseErrorHandling(unittest.TestCase):
    """Test cases for database error handling functionality."""

    def setUp(self):
        """Set up an in-memory SQLite database for testing."""
        # Create a direct SQLite connection for simplicity
        self.conn = sqlite3.connect(':memory:')
        self.cursor = self.conn.cursor()

        # Create a table for testing
        self.cursor.execute('''
            CREATE TABLE test_table (
                id INTEGER PRIMARY KEY,
                name TEXT UNIQUE,
                value INTEGER
            )
        ''')

        # Insert test data
        test_data = [
            ('test1', 100),
            ('test2', 200),
            ('test3', 300)
        ]
        self.cursor.executemany(
            'INSERT INTO test_table (name, value) VALUES (?, ?)',
            test_data
        )
        self.conn.commit()

    def tearDown(self):
        """Clean up database resources."""
        if hasattr(self, 'conn') and self.conn:
            self.conn.close()

    def test_successful_query(self):
        """Test that a simple query works correctly."""
        self.cursor.execute("SELECT * FROM test_table")
        results = self.cursor.fetchall()

        # Check that we got 3 rows
        self.assertEqual(len(results), 3, "Should return 3 rows of data")

        # Check values
        names = [row[1] for row in results]  # name is the second column
        self.assertIn('test1', names)
        self.assertIn('test2', names)
        self.assertIn('test3', names)

    def test_syntax_error_handling(self):
        """Test that SQL syntax errors are properly caught."""
        with self.assertRaises(sqlite3.OperationalError) as context:
            self.cursor.execute("SELECT * FORM test_table")

        # Check the error message
        error_message = str(context.exception)
        self.assertIn("syntax error", error_message.lower(),
                      f"Error should mention syntax error. Got: {error_message}")

    def test_constraint_violation(self):
        """Test that constraint violations are properly caught."""
        with self.assertRaises(sqlite3.IntegrityError) as context:
            # Try to insert a duplicate name, which should violate the UNIQUE constraint
            self.cursor.execute("INSERT INTO test_table (name, value) VALUES ('test1', 400)")

        # Check the error message
        error_message = str(context.exception)
        self.assertTrue("unique" in error_message.lower() or "constraint" in error_message.lower(),
                        f"Error should mention constraint violation. Got: {error_message}")

    def test_exception_creation(self):
        """Test that our custom exceptions can wrap SQLite errors appropriately."""
        # Create a QueryExecutionError from a syntax error
        try:
            self.cursor.execute("SELECT * FORM test_table")
        except sqlite3.OperationalError as e:
            error = QueryExecutionError(
                query="SELECT * FORM test_table",
                error_message=str(e)
            )

            # Test the error message
            self.assertIn("SELECT * FORM test_table", str(error))
            self.assertIn(str(e), str(error))

        # Create a DatabaseConnectionError from a constraint violation
        try:
            self.cursor.execute("INSERT INTO test_table (name, value) VALUES ('test1', 400)")
        except sqlite3.IntegrityError as e:
            error = DatabaseConnectionError(
                message=f"Database constraint violation: {str(e)}",
                error_code="DB_CONSTRAINT_ERROR"
            )

            # Test error code and message
            self.assertEqual(error.error_code, "DB_CONSTRAINT_ERROR",
                             "Error code should be set correctly")
            self.assertIn("Database constraint violation", str(error))


if __name__ == "__main__":
    unittest.main()