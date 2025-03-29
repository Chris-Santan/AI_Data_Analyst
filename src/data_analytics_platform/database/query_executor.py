# src/database/query_executor.py
from typing import Dict, Any, List, Optional
import re
import sqlalchemy as sa

from data_analytics_platform.core.interfaces.query_interface import QueryExecutionInterface
from data_analytics_platform.core.exceptions.custom_exceptions import QueryExecutionError, DatabaseConnectionError
from data_analytics_platform.database.connection import DatabaseConnection
from data_analytics_platform.database.error_handler import DatabaseErrorHandler

class QueryExecutor(QueryExecutionInterface):
    """
    Executes SQL queries against a database connection.
    Provides validation, execution, and result formatting.
    """

    def __init__(self, connection: DatabaseConnection, error_handler: Optional[DatabaseErrorHandler] = None):
        """
        Initialize with a database connection.

        Args:
            connection (DatabaseConnection): An established database connection
            error_handler (Optional[DatabaseErrorHandler]): Error handler for database operations
        """
        self._connection = connection
        self._error_handler = error_handler or DatabaseErrorHandler()

    def validate_query(self, query: str) -> bool:
        """
        Validate the structure and syntax of a SQL query.
        Performs basic validation without executing the query.

        Args:
            query (str): SQL query to validate

        Returns:
            bool: True if query appears valid, False otherwise
        """
        # Check if query is empty or None
        if not query or not query.strip():
            return False

        # Basic SQL injection protection - check for multiple statements
        if ";" in query[:-1]:  # Allow semicolon at the end
            return False

        # Check for valid SQL statement type
        valid_start_patterns = [
            r'^SELECT\s+',
            r'^WITH\s+',
            r'^SHOW\s+',
            r'^DESCRIBE\s+',
            r'^EXPLAIN\s+'
        ]

        # Check if the query starts with any valid pattern (case insensitive)
        if not any(re.match(pattern, query.strip(), re.IGNORECASE) for pattern in valid_start_patterns):
            return False

        # Basic check passed
        return True

    def sanitize_query(self, query: str) -> str:
        """
        Perform basic query sanitization to prevent SQL injection.

        Args:
            query (str): SQL query to sanitize

        Returns:
            str: Sanitized query
        """
        # Remove multiple statements
        sanitized = re.sub(r';.*', ';', query)

        # Remove potentially dangerous SQL comments
        sanitized = re.sub(r'--.*$', '', sanitized, flags=re.MULTILINE)

        return sanitized.strip()

    def execute_query(self, query: str) -> List[Dict[str, Any]]:
        """
        Execute a SQL query with error handling and retries.

        Args:
            query (str): SQL query to execute

        Returns:
            List[Dict[str, Any]]: Query results as list of dictionaries

        Raises:
            QueryExecutionError: If query execution fails
        """
        if not self.validate_query(query):
            raise QueryExecutionError(
                query=query,
                error_message="Invalid query structure or syntax"
            )

        # Sanitize query before execution
        sanitized_query = self.sanitize_query(query)

        # Define the function to execute with retry
        def _execute_query():
            # Get a session from the connection
            session = self._connection.get_session()

            # Execute query
            with session as s:
                result = s.execute(sa.text(sanitized_query))

                # If query returns results
                if result.returns_rows:
                    # Get column names
                    columns = result.keys()

                    # Convert results to list of dictionaries
                    rows = []
                    for row in result:
                        row_dict = {}
                        for i, column in enumerate(columns):
                            row_dict[column] = row[i]
                        rows.append(row_dict)

                    return rows
                else:
                    # For queries that don't return rows (e.g. EXPLAIN)
                    return [{"message": "Query executed successfully. No rows returned."}]

        try:
            # Execute the query with retry logic
            return self._error_handler.execute_with_retry(
                _execute_query,
                operation_name=f"execution of query: {sanitized_query[:50]}..."
            )
        except (QueryExecutionError, DatabaseConnectionError) as e:
            # These are already our custom exceptions, so just re-raise
            raise
        except Exception as e:
            # For any other exceptions, handle them and convert to our custom exceptions
            context = {"query": sanitized_query}
            exception = self._error_handler.handle_error(e, "query execution", context)
            raise exception

    def execute_select_count(self, table_name: str) -> int:
        """
        Execute a simple COUNT query on a table.

        Args:
            table_name (str): Name of the table to count

        Returns:
            int: Number of rows in the table

        Raises:
            QueryExecutionError: If query execution fails
        """
        query = f"SELECT COUNT(*) FROM {table_name}"
        try:
            result = self.execute_query(query)
            # The result should be a list with one dictionary containing one key
            if result and isinstance(result[0], dict):
                # Get the first (and only) value in the first dictionary
                return list(result[0].values())[0]
            return 0
        except QueryExecutionError as e:
            raise QueryExecutionError(
                query=query,
                error_message=f"Failed to count rows in {table_name}: {e.error_message}"
            ) from e

    def execute_query_with_parameters(self, query: str, parameters: Dict[str, Any]) -> List[Dict[str, Any]]:
        """
        Execute a parameterized SQL query.

        Args:
            query (str): SQL query with parameter placeholders
            parameters (Dict[str, Any]): Parameters to bind to the query

        Returns:
            List[Dict[str, Any]]: Query results

        Raises:
            QueryExecutionError: If query execution fails
        """
        if not self.validate_query(query):
            raise QueryExecutionError(
                query=query,
                error_message="Invalid query structure or syntax"
            )

        # Define the function to execute with retry
        def _execute_query_with_params():
            # Get a session from the connection
            session = self._connection.get_session()

            # Execute parameterized query
            with session as s:
                result = s.execute(sa.text(query), parameters)

                # If query returns results
                if result.returns_rows:
                    # Get column names
                    columns = result.keys()

                    # Convert results to list of dictionaries
                    rows = []
                    for row in result:
                        row_dict = {}
                        for i, column in enumerate(columns):
                            row_dict[column] = row[i]
                        rows.append(row_dict)

                    return rows
                else:
                    # For queries that don't return rows
                    return [{"message": "Query executed successfully. No rows returned."}]

        try:
            # Execute the query with retry logic
            return self._error_handler.execute_with_retry(
                _execute_query_with_params,
                operation_name=f"execution of parameterized query: {query[:50]}..."
            )
        except (QueryExecutionError, DatabaseConnectionError) as e:
            # These are already our custom exceptions, so just re-raise
            raise
        except Exception as e:
            # For any other exceptions, handle them and convert to our custom exceptions
            context = {"query": query, "parameters": parameters}
            exception = self._error_handler.handle_error(e, "parameterized query execution", context)
            raise exception