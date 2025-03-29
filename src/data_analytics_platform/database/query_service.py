# src/database/query_service.py
import time
import logging
from typing import Dict, Any, List, Optional
import pandas as pd

from data_analytics_platform.core.exceptions.custom_exceptions import QueryExecutionError, DatabaseConnectionError
from data_analytics_platform.database.connection import DatabaseConnection
from data_analytics_platform.database.query_executor import QueryExecutor
from data_analytics_platform.database.error_handler import DatabaseErrorHandler

# Set up logging
logger = logging.getLogger(__name__)


class QueryResult:
    """
    Container for query execution results with metadata and utility methods.
    """

    def __init__(self,
                 rows: List[Dict[str, Any]],
                 query: str,
                 execution_time: float,
                 row_count: int,
                 column_names: List[str],
                 metadata: Optional[Dict[str, Any]] = None):
        """
        Initialize a query result.

        Args:
            rows (List[Dict[str, Any]]): Query result rows
            query (str): The executed query
            execution_time (float): Query execution time in seconds
            row_count (int): Number of rows returned
            column_names (List[str]): Column names in the result
            metadata (Optional[Dict[str, Any]]): Additional metadata
        """
        self.rows = rows
        self.query = query
        self.execution_time = execution_time
        self.row_count = row_count
        self.column_names = column_names
        self.metadata = metadata or {}

    def to_dataframe(self) -> pd.DataFrame:
        """
        Convert results to a pandas DataFrame.

        Returns:
            pd.DataFrame: DataFrame with query results
        """
        return pd.DataFrame(self.rows)

    def to_dict(self) -> Dict[str, Any]:
        """
        Convert the result to a dictionary suitable for serialization.

        Returns:
            Dict[str, Any]: Dictionary representation of the result
        """
        return {
            'rows': self.rows,
            'query': self.query,
            'execution_time': self.execution_time,
            'row_count': self.row_count,
            'column_names': self.column_names,
            'metadata': self.metadata
        }

    def get_column_types(self) -> Dict[str, str]:
        """
        Get the inferred data types of each column.

        Returns:
            Dict[str, str]: Column name to data type mapping
        """
        if not self.rows:
            return {col: 'unknown' for col in self.column_names}

        df = self.to_dataframe()
        return {col: str(dtype) for col, dtype in df.dtypes.items()}

    def first(self) -> Optional[Dict[str, Any]]:
        """
        Get the first row of results or None if no results.

        Returns:
            Optional[Dict[str, Any]]: First row or None
        """
        return self.rows[0] if self.rows else None

    def value(self) -> Any:
        """
        Get the first value from the first row or None if no results.
        Useful for COUNT(*) and similar queries that return a single value.

        Returns:
            Any: First value or None
        """
        if not self.rows:
            return None

        first_row = self.rows[0]
        if not first_row:
            return None

        return next(iter(first_row.values()), None)

    def __len__(self) -> int:
        """Get the number of rows."""
        return self.row_count

    def __bool__(self) -> bool:
        """Check if the result contains any rows."""
        return self.row_count > 0


class QueryService:
    """
    Service for executing database queries with advanced features:
    - Query validation and sanitization
    - Performance tracking
    - Result formatting (JSON, DataFrames)
    - Parameterized queries
    - Pagination and limiting
    - Query history and caching
    """

    def __init__(self,
                 connection: DatabaseConnection,
                 executor: Optional[QueryExecutor] = None,
                 error_handler: Optional[DatabaseErrorHandler] = None,
                 max_results: int = 10000,
                 default_timeout: int = 30):
        """
        Initialize the query service.

        Args:
            connection (DatabaseConnection): Database connection
            executor (Optional[QueryExecutor]): Query executor
            error_handler (Optional[DatabaseErrorHandler]): Error handler
            max_results (int): Maximum number of results to return
            default_timeout (int): Default query timeout in seconds
        """
        self._connection = connection
        self._executor = executor or QueryExecutor(connection)
        self._error_handler = error_handler or DatabaseErrorHandler()
        self._max_results = max_results
        self._default_timeout = default_timeout
        self._query_history = []

    def execute_query(self,
                      query: str,
                      parameters: Optional[Dict[str, Any]] = None,
                      timeout: Optional[int] = None,
                      limit: Optional[int] = None) -> QueryResult:
        """
        Execute a SQL query with parameters and return a structured result.

        Args:
            query (str): SQL query to execute
            parameters (Optional[Dict[str, Any]]): Query parameters
            timeout (Optional[int]): Query timeout in seconds
            limit (Optional[int]): Maximum number of rows to return

        Returns:
            QueryResult: Structured query result

        Raises:
            QueryExecutionError: If query execution fails
        """
        # Validate the query
        if not self._executor.validate_query(query):
            raise QueryExecutionError(
                query=query,
                error_message="Invalid query structure or syntax"
            )

        # Apply limit if specified
        limited_query = query
        if limit is not None:
            # Simple approach - may need more sophistication for complex queries
            if "LIMIT" not in query.upper():
                limited_query = f"{query} LIMIT {limit}"

        # Track execution time
        start_time = time.time()

        try:
            # Execute the query
            if parameters:
                rows = self._executor.execute_query_with_parameters(limited_query, parameters)
            else:
                rows = self._executor.execute_query(limited_query)

            execution_time = time.time() - start_time

            # Get column names
            column_names = []
            if rows:
                column_names = list(rows[0].keys())

            # Create result object
            result = QueryResult(
                rows=rows,
                query=query,
                execution_time=execution_time,
                row_count=len(rows),
                column_names=column_names,
                metadata={
                    'parameters': parameters,
                    'timeout': timeout or self._default_timeout,
                    'limit': limit,
                    'timestamp': time.time()
                }
            )

            # Add to query history
            self._add_to_history(result)

            return result

        except (QueryExecutionError, DatabaseConnectionError) as e:
            # Log the error
            logger.error(f"Query execution failed: {str(e)}", exc_info=True)

            # These are already our custom exceptions, so just re-raise
            raise

        except Exception as e:
            # For any other exceptions, handle them and convert to our custom exceptions
            logger.error(f"Unexpected error during query execution: {str(e)}", exc_info=True)

            context = {"query": query, "parameters": parameters}
            exception = self._error_handler.handle_error(e, "query execution", context)
            raise exception

    def execute_and_fetch_dataframe(self,
                                    query: str,
                                    parameters: Optional[Dict[str, Any]] = None,
                                    timeout: Optional[int] = None,
                                    limit: Optional[int] = None) -> pd.DataFrame:
        """
        Execute a query and return results as a pandas DataFrame.

        Args:
            query (str): SQL query to execute
            parameters (Optional[Dict[str, Any]]): Query parameters
            timeout (Optional[int]): Query timeout in seconds
            limit (Optional[int]): Maximum number of rows to return

        Returns:
            pd.DataFrame: Query results as DataFrame

        Raises:
            QueryExecutionError: If query execution fails
        """
        result = self.execute_query(query, parameters, timeout, limit)
        return result.to_dataframe()

    def execute_scalar(self,
                       query: str,
                       parameters: Optional[Dict[str, Any]] = None) -> Any:
        """
        Execute a query and return a single scalar value.

        Args:
            query (str): SQL query to execute
            parameters (Optional[Dict[str, Any]]): Query parameters

        Returns:
            Any: Scalar result

        Raises:
            QueryExecutionError: If query execution fails
        """
        result = self.execute_query(query, parameters, limit=1)
        return result.value()

    def execute_script(self,
                       script: str,
                       parameters: Optional[Dict[str, Any]] = None) -> List[QueryResult]:
        """
        Execute a multi-statement SQL script.

        Args:
            script (str): SQL script with multiple statements
            parameters (Optional[Dict[str, Any]]): Script parameters

        Returns:
            List[QueryResult]: List of query results

        Raises:
            QueryExecutionError: If script execution fails
        """
        # Split script into individual statements
        statements = [stmt.strip() for stmt in script.split(';') if stmt.strip()]

        results = []
        for statement in statements:
            if self._executor.validate_query(statement):
                result = self.execute_query(statement, parameters)
                results.append(result)

        return results

    def paginate_query(self,
                       query: str,
                       page: int = 1,
                       page_size: int = 100,
                       parameters: Optional[Dict[str, Any]] = None) -> Dict[str, Any]:
        """
        Execute a query with pagination.

        Args:
            query (str): SQL query to execute
            page (int): Page number (starting from 1)
            page_size (int): Number of rows per page
            parameters (Optional[Dict[str, Any]]): Query parameters

        Returns:
            Dict[str, Any]: Pagination result with data, total, and page info

        Raises:
            QueryExecutionError: If query execution fails
            ValueError: If page or page_size is invalid
        """
        if page < 1:
            raise ValueError("Page must be a positive integer")

        if page_size < 1:
            raise ValueError("Page size must be a positive integer")

        # Apply limit and offset
        offset = (page - 1) * page_size
        paginated_query = f"SELECT * FROM ({query}) as subquery LIMIT {page_size} OFFSET {offset}"

        # Execute the query
        result = self.execute_query(paginated_query, parameters)

        # Get total count
        count_query = f"SELECT COUNT(*) FROM ({query}) as subquery"
        total_result = self.execute_scalar(count_query, parameters)
        total = int(total_result) if total_result is not None else 0

        # Calculate pagination info
        total_pages = (total + page_size - 1) // page_size
        has_next = page < total_pages
        has_prev = page > 1

        return {
            "data": result.rows,
            "pagination": {
                "page": page,
                "page_size": page_size,
                "total": total,
                "total_pages": total_pages,
                "has_next": has_next,
                "has_prev": has_prev
            },
            "metadata": {
                "execution_time": result.execution_time,
                "column_names": result.column_names
            }
        }

    def describe_query_results(self, result: QueryResult) -> Dict[str, Any]:
        """
        Generate descriptive statistics for query results.

        Args:
            result (QueryResult): Query result to analyze

        Returns:
            Dict[str, Any]: Descriptive statistics
        """
        if not result.rows:
            return {"message": "No data to analyze"}

        df = result.to_dataframe()

        # Get numeric columns
        numeric_cols = df.select_dtypes(include=['number']).columns.tolist()

        # Basic statistics
        stats = {}

        if numeric_cols:
            # Numeric statistics
            numeric_stats = df[numeric_cols].describe().to_dict()
            stats["numeric"] = numeric_stats

        # Non-numeric columns
        non_numeric_cols = [col for col in df.columns if col not in numeric_cols]
        if non_numeric_cols:
            non_numeric_stats = {}
            for col in non_numeric_cols:
                # For non-numeric columns, return count, unique values, top value and frequency
                value_counts = df[col].value_counts().to_dict()
                top_values = sorted(value_counts.items(), key=lambda x: x[1], reverse=True)[:5]

                non_numeric_stats[col] = {
                    "count": df[col].count(),
                    "unique": df[col].nunique(),
                    "top_values": dict(top_values)
                }

            stats["non_numeric"] = non_numeric_stats

        # General info
        stats["info"] = {
            "row_count": len(df),
            "column_count": len(df.columns),
            "memory_usage": df.memory_usage(deep=True).sum(),
            "column_types": {col: str(dtype) for col, dtype in df.dtypes.items()}
        }

        return stats

    def _add_to_history(self, result: QueryResult) -> None:
        """
        Add a query result to the history.

        Args:
            result (QueryResult): Query result to add
        """
        # Store simplified history to avoid memory issues
        history_entry = {
            'query': result.query,
            'timestamp': time.time(),
            'execution_time': result.execution_time,
            'row_count': result.row_count
        }

        self._query_history.append(history_entry)

        # Keep history at a reasonable size
        max_history = 100
        if len(self._query_history) > max_history:
            self._query_history = self._query_history[-max_history:]

    def get_query_history(self) -> List[Dict[str, Any]]:
        """
        Get the query execution history.

        Returns:
            List[Dict[str, Any]]: Query history
        """
        return self._query_history.copy()

    def clear_history(self) -> None:
        """Clear the query history."""
        self._query_history = []