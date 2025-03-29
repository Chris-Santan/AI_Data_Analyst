# tests/unit/database/test_query_service.py
import unittest
from unittest.mock import Mock, patch, MagicMock
import pandas as pd
import time
from decimal import Decimal
from datetime import datetime, date

from data_analytics_platform.database.query_service import QueryService, QueryResult
from data_analytics_platform.database.query_executor import QueryExecutor
from data_analytics_platform.database.connection import DatabaseConnection
from data_analytics_platform.database.error_handler import DatabaseErrorHandler
from data_analytics_platform.core.exceptions.custom_exceptions import QueryExecutionError, DatabaseConnectionError


class TestQueryResult(unittest.TestCase):
    """Test cases for the QueryResult class."""

    def setUp(self):
        """Set up test fixtures."""
        # Sample data for testing
        self.rows = [
            {"id": 1, "name": "Alice", "age": 30},
            {"id": 2, "name": "Bob", "age": 25},
            {"id": 3, "name": "Charlie", "age": 35}
        ]
        self.query = "SELECT id, name, age FROM users"
        self.execution_time = 0.1
        self.column_names = ["id", "name", "age"]

        # Create QueryResult instance
        self.result = QueryResult(
            rows=self.rows,
            query=self.query,
            execution_time=self.execution_time,
            row_count=len(self.rows),
            column_names=self.column_names
        )

    def test_to_dataframe(self):
        """Test conversion to pandas DataFrame."""
        df = self.result.to_dataframe()

        # Verify the DataFrame
        self.assertIsInstance(df, pd.DataFrame)
        self.assertEqual(len(df), 3)
        self.assertListEqual(list(df.columns), self.column_names)
        self.assertEqual(df.iloc[0]["name"], "Alice")

    def test_to_dict(self):
        """Test conversion to dictionary."""
        result_dict = self.result.to_dict()

        # Verify the dictionary
        self.assertIsInstance(result_dict, dict)
        self.assertEqual(result_dict["rows"], self.rows)
        self.assertEqual(result_dict["query"], self.query)
        self.assertEqual(result_dict["execution_time"], self.execution_time)
        self.assertEqual(result_dict["row_count"], 3)
        self.assertEqual(result_dict["column_names"], self.column_names)

    def test_get_column_types(self):
        """Test getting column types."""
        column_types = self.result.get_column_types()

        # Verify column types
        self.assertIsInstance(column_types, dict)
        self.assertEqual(len(column_types), 3)
        self.assertIn("id", column_types)
        self.assertIn("name", column_types)
        self.assertIn("age", column_types)

    def test_first(self):
        """Test getting the first row."""
        first_row = self.result.first()

        # Verify first row
        self.assertEqual(first_row, self.rows[0])

        # Test with empty result
        empty_result = QueryResult([], self.query, self.execution_time, 0, self.column_names)
        self.assertIsNone(empty_result.first())

    def test_value(self):
        """Test getting the first value."""
        value = self.result.value()

        # Verify first value
        self.assertEqual(value, 1)  # First value is id=1

        # Test with empty result
        empty_result = QueryResult([], self.query, self.execution_time, 0, self.column_names)
        self.assertIsNone(empty_result.value())

        # Test with empty row
        empty_row_result = QueryResult([{}], self.query, self.execution_time, 1, [])
        self.assertIsNone(empty_row_result.value())

    def test_len(self):
        """Test len() functionality."""
        self.assertEqual(len(self.result), 3)

        # Test with empty result
        empty_result = QueryResult([], self.query, self.execution_time, 0, self.column_names)
        self.assertEqual(len(empty_result), 0)

    def test_bool(self):
        """Test boolean evaluation."""
        self.assertTrue(bool(self.result))

        # Test with empty result
        empty_result = QueryResult([], self.query, self.execution_time, 0, self.column_names)
        self.assertFalse(bool(empty_result))


class TestQueryService(unittest.TestCase):
    """Test cases for the QueryService class."""

    def setUp(self):
        """Set up test fixtures."""
        # Create mock connection
        self.mock_connection = Mock(spec=DatabaseConnection)

        # Create mock executor
        self.mock_executor = Mock(spec=QueryExecutor)

        # Create mock error handler
        self.mock_error_handler = Mock(spec=DatabaseErrorHandler)

        # Create query service with mocks
        self.query_service = QueryService(
            connection=self.mock_connection,
            executor=self.mock_executor,
            error_handler=self.mock_error_handler,
            max_results=1000,
            default_timeout=10
        )

        # Sample data for testing
        self.sample_rows = [
            {"id": 1, "name": "Alice", "age": 30},
            {"id": 2, "name": "Bob", "age": 25}
        ]

    def test_execute_query(self):
        """Test basic query execution."""
        # Mock the executor validate_query and execute_query methods
        self.mock_executor.validate_query.return_value = True
        self.mock_executor.execute_query.return_value = self.sample_rows

        # Execute a test query
        result = self.query_service.execute_query("SELECT * FROM users")

        # Verify the result
        self.assertIsInstance(result, QueryResult)
        self.assertEqual(result.rows, self.sample_rows)
        self.assertEqual(result.row_count, 2)
        self.assertEqual(result.query, "SELECT * FROM users")

        # Verify that methods were called correctly
        self.mock_executor.validate_query.assert_called_once_with("SELECT * FROM users")
        self.mock_executor.execute_query.assert_called_once_with("SELECT * FROM users")

    def test_execute_query_with_limit(self):
        """Test query execution with a limit."""
        # Mock the executor methods
        self.mock_executor.validate_query.return_value = True
        self.mock_executor.execute_query.return_value = self.sample_rows[:1]

        # Execute a test query with limit
        result = self.query_service.execute_query("SELECT * FROM users", limit=1)

        # Verify the query had a LIMIT clause
        self.mock_executor.execute_query.assert_called_once()
        called_query = self.mock_executor.execute_query.call_args[0][0]
        self.assertIn("LIMIT 1", called_query)

    def test_execute_query_with_parameters(self):
        """Test query execution with parameters."""
        # Mock the executor methods
        self.mock_executor.validate_query.return_value = True
        self.mock_executor.execute_query_with_parameters.return_value = [self.sample_rows[0]]

        # Execute a test query with parameters
        params = {"user_id": 1}
        result = self.query_service.execute_query(
            "SELECT * FROM users WHERE id = :user_id",
            parameters=params
        )

        # Verify the result
        self.assertEqual(result.row_count, 1)

        # Verify that the executor was called with parameters
        self.mock_executor.execute_query_with_parameters.assert_called_once()
        call_args = self.mock_executor.execute_query_with_parameters.call_args
        self.assertEqual(call_args[0][1], params)

    def test_execute_query_invalid(self):
        """Test query execution with invalid query."""
        # Mock the executor validate_query method to return False
        self.mock_executor.validate_query.return_value = False

        # Execute an invalid query and expect an exception
        with self.assertRaises(QueryExecutionError):
            self.query_service.execute_query("DELETE FROM users")

    def test_execute_query_executor_error(self):
        """Test handling of executor errors."""
        # Mock the executor to validate but then fail on execution
        self.mock_executor.validate_query.return_value = True
        self.mock_executor.execute_query.side_effect = QueryExecutionError(
            query="SELECT * FROM nonexistent",
            error_message="Table not found"
        )

        # Execute a query and expect an exception
        with self.assertRaises(QueryExecutionError):
            self.query_service.execute_query("SELECT * FROM nonexistent")

    def test_execute_and_fetch_dataframe(self):
        """Test execution and conversion to DataFrame."""
        # Set up a sample QueryResult
        sample_result = QueryResult(
            rows=self.sample_rows,
            query="SELECT * FROM users",
            execution_time=0.1,
            row_count=2,
            column_names=["id", "name", "age"]
        )

        # Mock execute_query to return our sample result
        self.query_service.execute_query = Mock(return_value=sample_result)

        # Execute and fetch DataFrame
        df = self.query_service.execute_and_fetch_dataframe("SELECT * FROM users")

        # Verify the DataFrame
        self.assertIsInstance(df, pd.DataFrame)
        self.assertEqual(len(df), 2)
        self.assertEqual(list(df.columns), ["id", "name", "age"])

    def test_execute_scalar(self):
        """Test execution of scalar queries."""
        # Set up a sample QueryResult with a scalar value
        sample_result = QueryResult(
            rows=[{"count": 10}],
            query="SELECT COUNT(*) AS count FROM users",
            execution_time=0.1,
            row_count=1,
            column_names=["count"]
        )

        # Mock execute_query to return our sample result
        self.query_service.execute_query = Mock(return_value=sample_result)

        # Execute scalar query
        scalar = self.query_service.execute_scalar("SELECT COUNT(*) AS count FROM users")

        # Verify the scalar result
        self.assertEqual(scalar, 10)

        # Verify execute_query was called with limit=1
        self.query_service.execute_query.assert_called_once()
        call_args, call_kwargs = self.query_service.execute_query.call_args
        self.assertEqual(call_kwargs["limit"], 1)

    def test_execute_script(self):
        """Test execution of multi-statement scripts."""
        # Mock the executor validate_query method
        self.mock_executor.validate_query.return_value = True

        # Set up two sample QueryResults
        result1 = QueryResult(
            rows=[{"result": "Table created"}],
            query="CREATE TABLE test (id INT)",
            execution_time=0.1,
            row_count=1,
            column_names=["result"]
        )

        result2 = QueryResult(
            rows=[{"result": "Data inserted"}],
            query="INSERT INTO test VALUES (1)",
            execution_time=0.1,
            row_count=1,
            column_names=["result"]
        )

        # Mock execute_query to return our sample results
        self.query_service.execute_query = Mock(side_effect=[result1, result2])

        # Execute script
        results = self.query_service.execute_script("""
            CREATE TABLE test (id INT);
            INSERT INTO test VALUES (1);
        """)

        # Verify the results
        self.assertEqual(len(results), 2)
        self.assertIs(results[0], result1)
        self.assertIs(results[1], result2)

    def test_paginate_query(self):
        """Test query pagination."""
        # Mock execute_query and execute_scalar
        result_rows = [{"id": i, "name": f"User {i}"} for i in range(1, 6)]

        sample_result = QueryResult(
            rows=result_rows[:2],  # Return first 2 rows for page 1
            query="SELECT * FROM users LIMIT 2 OFFSET 0",
            execution_time=0.1,
            row_count=2,
            column_names=["id", "name"]
        )

        self.query_service.execute_query = Mock(return_value=sample_result)
        self.query_service.execute_scalar = Mock(return_value=5)  # Total of 5 rows

        # Execute paginated query
        pagination = self.query_service.paginate_query(
            "SELECT * FROM users",
            page=1,
            page_size=2
        )

        # Verify the pagination results
        self.assertEqual(len(pagination["data"]), 2)
        self.assertEqual(pagination["pagination"]["page"], 1)
        self.assertEqual(pagination["pagination"]["page_size"], 2)
        self.assertEqual(pagination["pagination"]["total"], 5)
        self.assertEqual(pagination["pagination"]["total_pages"], 3)
        self.assertTrue(pagination["pagination"]["has_next"])
        self.assertFalse(pagination["pagination"]["has_prev"])

        # Verify execute_query was called with the correct pagination
        self.query_service.execute_query.assert_called_once()
        call_args = self.query_service.execute_query.call_args[0]
        self.assertIn("LIMIT 2 OFFSET 0", call_args[0])

    def test_paginate_query_last_page(self):
        """Test pagination on the last page."""
        # Mock execute_query and execute_scalar
        result_rows = [{"id": 5, "name": "User 5"}]

        sample_result = QueryResult(
            rows=result_rows,  # Return just one row for the last page
            query="SELECT * FROM users LIMIT 2 OFFSET 4",
            execution_time=0.1,
            row_count=1,
            column_names=["id", "name"]
        )

        self.query_service.execute_query = Mock(return_value=sample_result)
        self.query_service.execute_scalar = Mock(return_value=5)  # Total of 5 rows

        # Execute paginated query for the last page
        pagination = self.query_service.paginate_query(
            "SELECT * FROM users",
            page=3,
            page_size=2
        )

        # Verify the pagination results
        self.assertEqual(len(pagination["data"]), 1)
        self.assertEqual(pagination["pagination"]["page"], 3)
        self.assertEqual(pagination["pagination"]["total_pages"], 3)
        self.assertFalse(pagination["pagination"]["has_next"])
        self.assertTrue(pagination["pagination"]["has_prev"])

    def test_paginate_query_invalid_page(self):
        """Test pagination with invalid page number."""
        with self.assertRaises(ValueError):
            self.query_service.paginate_query("SELECT * FROM users", page=0)

        with self.assertRaises(ValueError):
            self.query_service.paginate_query("SELECT * FROM users", page_size=0)

    def test_describe_query_results(self):
        """Test analysis of query results."""
        # Create a sample result with numeric and non-numeric columns
        sample_rows = [
            {"id": 1, "name": "Alice", "age": 30, "dept": "HR"},
            {"id": 2, "name": "Bob", "age": 25, "dept": "IT"},
            {"id": 3, "name": "Charlie", "age": 35, "dept": "HR"},
            {"id": 4, "name": "Dave", "age": 40, "dept": "IT"}
        ]

        sample_result = QueryResult(
            rows=sample_rows,
            query="SELECT * FROM users",
            execution_time=0.1,
            row_count=4,
            column_names=["id", "name", "age", "dept"]
        )

        # Get statistics
        stats = self.query_service.describe_query_results(sample_result)

        # Verify the statistics
        self.assertIn("info", stats)
        self.assertEqual(stats["info"]["row_count"], 4)
        self.assertEqual(stats["info"]["column_count"], 4)

        # Check numeric stats
        self.assertIn("numeric", stats)
        self.assertIn("id", stats["numeric"])
        self.assertIn("age", stats["numeric"])

        # Check non-numeric stats
        self.assertIn("non_numeric", stats)
        self.assertIn("name", stats["non_numeric"])
        self.assertIn("dept", stats["non_numeric"])
        self.assertEqual(stats["non_numeric"]["dept"]["unique"], 2)

    def test_describe_query_results_empty(self):
        """Test analysis of empty query results."""
        # Create an empty sample result
        sample_result = QueryResult(
            rows=[],
            query="SELECT * FROM users WHERE 1=0",
            execution_time=0.1,
            row_count=0,
            column_names=["id", "name", "age"]
        )

        # Get statistics
        stats = self.query_service.describe_query_results(sample_result)

        # Verify the statistics
        self.assertIn("message", stats)
        self.assertEqual(stats["message"], "No data to analyze")

    def test_query_history(self):
        """Test query history tracking."""
        # Clear any existing history
        self.query_service.clear_history()

        # Create sample results
        sample_result1 = QueryResult(
            rows=[{"id": 1, "name": "Alice"}],
            query="SELECT * FROM users",
            execution_time=0.1,
            row_count=1,
            column_names=["id", "name"]
        )

        sample_result2 = QueryResult(
            rows=[{"count": 5}],
            query="SELECT COUNT(*) FROM orders",
            execution_time=0.05,
            row_count=1,
            column_names=["count"]
        )

        # Manually add to history
        self.query_service._add_to_history(sample_result1)
        self.query_service._add_to_history(sample_result2)

        # Get history
        history = self.query_service.get_query_history()

        # Verify history
        self.assertEqual(len(history), 2)
        self.assertEqual(history[0]["query"], "SELECT * FROM users")
        self.assertEqual(history[1]["query"], "SELECT COUNT(*) FROM orders")

        # Clear history
        self.query_service.clear_history()
        self.assertEqual(len(self.query_service.get_query_history()), 0)

    def test_execute_query_with_timeout(self):
        """Test query execution with custom timeout."""
        # Mock the executor methods
        self.mock_executor.validate_query.return_value = True
        self.mock_executor.execute_query.return_value = self.sample_rows

        # Execute a test query with timeout
        self.query_service.execute_query(
            "SELECT * FROM users",
            timeout=30  # Custom timeout
        )

        # Verify the timeout was stored in metadata
        self.mock_executor.execute_query.assert_called_once()

        # We can't directly check the timeout parameter as it's stored in metadata
        # and not directly accessible, but we can verify execute_query was called correctly
        call_args = self.mock_executor.execute_query.call_args[0]
        self.assertEqual(call_args[0], "SELECT * FROM users")

    def test_execute_query_with_complex_parameters(self):
        """Test query execution with complex parameter types."""
        # Mock the executor methods
        self.mock_executor.validate_query.return_value = True
        self.mock_executor.execute_query_with_parameters.return_value = self.sample_rows

        # Create complex parameters
        params = {
            "ids": [1, 2, 3],
            "date": date(2023, 1, 1),
            "amount": Decimal("123.45"),
            "active": True
        }

        # Execute a test query with parameters
        result = self.query_service.execute_query(
            "SELECT * FROM users WHERE id IN :ids AND created_at > :date AND balance > :amount AND active = :active",
            parameters=params
        )

        # Verify the result
        self.assertEqual(result.row_count, 2)

        # Verify that the executor was called with parameters
        self.mock_executor.execute_query_with_parameters.assert_called_once()
        call_args = self.mock_executor.execute_query_with_parameters.call_args
        self.assertEqual(call_args[0][1], params)

    def test_history_limit(self):
        """Test that history size is limited."""
        # Create a query service with a small history limit
        query_service_small_history = QueryService(
            connection=self.mock_connection,
            executor=self.mock_executor,
            error_handler=self.mock_error_handler,
            max_results=1000,
            default_timeout=10
        )

        # Overwrite the _query_history attribute to test size limit
        query_service_small_history._query_history = []

        # Add many entries to history (more than max_history which is 100 by default)
        for i in range(110):
            query_service_small_history._add_to_history(
                QueryResult(
                    rows=[],
                    query=f"SELECT {i}",
                    execution_time=0.1,
                    row_count=0,
                    column_names=[]
                )
            )

        # Verify history is limited to 100 entries
        history = query_service_small_history.get_query_history()
        self.assertEqual(len(history), 100)

        # Verify the oldest entries were removed
        self.assertEqual(history[0]["query"], "SELECT 10")  # First 10 entries were removed


if __name__ == "__main__":
    unittest.main()