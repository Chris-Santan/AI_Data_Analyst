# src/database/error_handler.py
import logging
import time
from typing import Callable, Any, Optional, Dict
from sqlalchemy.exc import SQLAlchemyError, OperationalError, IntegrityError, ProgrammingError

# Import using relative imports from src directory
from data_analytics_platform.core.exceptions.custom_exceptions import (
    DatabaseConnectionError,
    QueryExecutionError,
    DataAnalyticsPlatformError
)

logger = logging.getLogger(__name__)


class DatabaseErrorHandler:
    """
    Centralized error handler for database operations.
    Provides standardized error handling, retries for transient errors,
    and detailed logging.
    """

    # Classification of SQLAlchemy errors
    TRANSIENT_ERRORS = (
        OperationalError,  # Connection errors, timeouts, etc.
    )

    QUERY_ERRORS = (
        ProgrammingError,  # SQL syntax errors
        IntegrityError,  # Constraint violations
    )

    def __init__(
            self,
            max_retries: int = 3,
            retry_delay: float = 1.0,
            exponential_backoff: bool = True,
            log_level: int = logging.ERROR
    ):
        """
        Initialize the database error handler.

        Args:
            max_retries: Maximum number of retry attempts for transient errors
            retry_delay: Initial delay between retries in seconds
            exponential_backoff: Whether to use exponential backoff for retries
            log_level: Logging level for database errors
        """
        self.max_retries = max_retries
        self.retry_delay = retry_delay
        self.exponential_backoff = exponential_backoff
        self.log_level = log_level

    def handle_error(
            self,
            error: Exception,
            operation: str,
            context: Optional[Dict[str, Any]] = None
    ) -> DataAnalyticsPlatformError:
        """
        Handle a database error.

        Args:
            error: The original exception
            operation: Description of the operation being performed
            context: Additional context information

        Returns:
            DataAnalyticsPlatformError: Appropriate custom exception
        """
        context = context or {}

        # Log the error
        self._log_error(error, operation, context)

        # Convert to appropriate custom exception
        if isinstance(error, SQLAlchemyError):
            return self._handle_sqlalchemy_error(error, operation, context)

        # For unknown errors, wrap in a generic exception
        return DatabaseConnectionError(
            message=f"Unexpected error during {operation}: {str(error)}",
            error_code="DB_UNKNOWN_ERROR"
        )

    def _handle_sqlalchemy_error(
            self,
            error: SQLAlchemyError,
            operation: str,
            context: Dict[str, Any]
    ) -> DataAnalyticsPlatformError:
        """
        Handle SQLAlchemy-specific errors.

        Args:
            error: The SQLAlchemy exception
            operation: Description of the operation being performed
            context: Additional context information

        Returns:
            DataAnalyticsPlatformError: Appropriate custom exception
        """
        error_str = str(error)

        # Handle connection errors
        if isinstance(error, OperationalError):
            if "connection" in error_str.lower() or "timeout" in error_str.lower():
                return DatabaseConnectionError(
                    message=f"Database connection error during {operation}: {error_str}",
                    error_code="DB_CONNECTION_ERROR"
                )

        # Handle integrity errors
        if isinstance(error, IntegrityError):
            if "unique constraint" in error_str.lower():
                return QueryExecutionError(
                    query=context.get("query", "Unknown query"),
                    error_message=f"Unique constraint violation: {error_str}"
                )
            if "foreign key constraint" in error_str.lower():
                return QueryExecutionError(
                    query=context.get("query", "Unknown query"),
                    error_message=f"Foreign key constraint violation: {error_str}"
                )

        # Handle programming errors
        if isinstance(error, ProgrammingError):
            return QueryExecutionError(
                query=context.get("query", "Unknown query"),
                error_message=f"SQL syntax error: {error_str}"
            )

        # Default case: general query execution error
        return QueryExecutionError(
            query=context.get("query", "Unknown query"),
            error_message=f"Database error: {error_str}"
        )

    def _log_error(
            self,
            error: Exception,
            operation: str,
            context: Dict[str, Any]
    ) -> None:
        """
        Log a database error with appropriate level and context.

        Args:
            error: The original exception
            operation: Description of the operation being performed
            context: Additional context information
        """
        # Build error message
        message = f"Database error during {operation}: {str(error)}"

        # Include relevant context, but exclude sensitive information
        safe_context = {k: v for k, v in context.items()
                        if not any(sensitive in k.lower()
                                   for sensitive in ['password', 'token', 'key', 'secret'])}

        # Log with appropriate level
        logger.log(self.log_level, message, exc_info=True, extra={"context": safe_context})

    def execute_with_retry(
            self,
            func: Callable[..., Any],
            *args,
            operation_name: str = "database operation",
            **kwargs
    ) -> Any:
        """
        Execute a database operation with automatic retry for transient errors.

        Args:
            func: Function to execute
            *args: Arguments to pass to the function
            operation_name: Name of the operation for error messages
            **kwargs: Keyword arguments to pass to the function

        Returns:
            Any: Result of the function

        Raises:
            DataAnalyticsPlatformError: If the operation fails after retries
        """
        attempt = 0
        last_error = None

        while attempt <= self.max_retries:
            try:
                return func(*args, **kwargs)
            except Exception as e:
                last_error = e

                # Only retry for transient errors
                if not isinstance(e, self.TRANSIENT_ERRORS):
                    break

                attempt += 1
                if attempt > self.max_retries:
                    break

                # Calculate delay with exponential backoff if enabled
                delay = self.retry_delay
                if self.exponential_backoff:
                    delay = self.retry_delay * (2 ** (attempt - 1))

                logger.warning(
                    f"Transient error during {operation_name} (attempt {attempt}/{self.max_retries}). "
                    f"Retrying in {delay:.2f} seconds. Error: {str(e)}"
                )

                time.sleep(delay)

        # If we get here, all retries failed or the error wasn't transient
        context = {"args": args, "kwargs": kwargs, "attempts": attempt}

        # Use the handle_error method to convert to appropriate exception
        exception = self.handle_error(last_error, operation_name, context)
        raise exception