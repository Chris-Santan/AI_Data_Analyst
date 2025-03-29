# src/database/error_handling_examples.py
import logging
from sqlalchemy.exc import OperationalError, ProgrammingError, IntegrityError
import time
import sys
import os

# Add parent directory to path to help with imports
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '../..')))

# Import from project structure
from src.database.config import DatabaseConfig
from src.database.connection import DatabaseConnection
from src.database.query_executor import QueryExecutor
from src.database.error_handler import DatabaseErrorHandler
from src.core.exceptions.custom_exceptions import DatabaseConnectionError, QueryExecutionError

# Set up logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[logging.StreamHandler(sys.stdout)]
)
logger = logging.getLogger(__name__)


def demonstrate_connection_error_handling():
    """Demonstrate handling of connection errors."""
    print("\n=== Connection Error Handling Example ===")

    # Create an error handler with custom settings
    error_handler = DatabaseErrorHandler(
        max_retries=2,
        retry_delay=0.5,
        exponential_backoff=True
    )

    # Create a connection with an invalid host to trigger an error
    try:
        connection = DatabaseConnection(
            db_type=DatabaseConfig.POSTGRES,
            host="nonexistent-host",
            port=5432,
            database="test_db",
            auth_credentials={
                "auth_type": "basic",
                "username": "user",
                "password": "password"
            },
            error_handler=error_handler
        )

        # This will fail but will retry
        connection.connect()
    except DatabaseConnectionError as e:
        print(f"✓ Caught expected connection error: {e}")
        print(f"  Error code: {e.error_code}")
        print(f"  User message: {e.get_user_message()}")
        print(f"  Recovery suggestions: {e.get_recovery_suggestions()}")


def demonstrate_query_error_handling():
    """Demonstrate handling of query execution errors."""
    print("\n=== Query Error Handling Example ===")

    # Create an in-memory SQLite database
    connection = DatabaseConnection(
        db_type=DatabaseConfig.SQLITE,
        database=":memory:"
    )
    connection.connect()

    # Create a table for testing
    connection.execute_raw_sql("""
        CREATE TABLE test_table (
            id INTEGER PRIMARY KEY,
            name TEXT NOT NULL UNIQUE,
            value INTEGER
        )
    """)

    # Insert some test data
    connection.execute_raw_sql("""
        INSERT INTO test_table (name, value) VALUES
        ('item1', 100),
        ('item2', 200)
    """)

    # Create query executor with error handler
    query_executor = QueryExecutor(connection)

    # 1. Demonstrate SQL syntax error
    try:
        print("\n1. Testing SQL syntax error:")
        query_executor.execute_query("SELECT * FORM test_table")
    except QueryExecutionError as e:
        print(f"✓ Caught expected syntax error: {e}")
        print(f"  User message: {e.get_user_message()}")
        print(f"  Recovery suggestions: {e.get_recovery_suggestions()}")

    # 2. Demonstrate constraint violation
    try:
        print("\n2. Testing unique constraint violation:")
        # This will fail because 'item1' already exists
        query_executor.execute_query("INSERT INTO test_table (name, value) VALUES ('item1', 300)")
    except QueryExecutionError as e:
        print(f"✓ Caught expected constraint violation: {e}")
        print(f"  User message: {e.get_user_message()}")
        print(f"  Recovery suggestions: {e.get_recovery_suggestions()}")

    # 3. Demonstrate successful query after errors
    try:
        print("\n3. Testing successful query after errors:")
        results = query_executor.execute_query("SELECT * FROM test_table")
        print(f"✓ Query executed successfully. Results: {results}")
    except QueryExecutionError as e:
        print(f"✗ Unexpected error: {e}")

    # Clean up
    connection.disconnect()


def demonstrate_retry_logic():
    """Demonstrate retry logic for transient errors."""
    print("\n=== Retry Logic Example ===")

    # Create a mock function that fails a few times then succeeds
    failure_count = [0]

    def flaky_operation():
        if failure_count[0] < 2:
            failure_count[0] += 1
            print(f"  Operation failed (attempt {failure_count[0]})")
            raise OperationalError("statement", {}, "Simulated transient error")
        print("  Operation succeeded!")
        return "Success"

    # Create error handler with retry logic
    error_handler = DatabaseErrorHandler(
        max_retries=3,
        retry_delay=0.2,
        exponential_backoff=True
    )

    try:
        # This should fail twice then succeed
        result = error_handler.execute_with_retry(
            flaky_operation,
            operation_name="flaky test operation"
        )
        print(f"✓ Operation eventually succeeded: {result}")
    except Exception as e:
        print(f"✗ Unexpected failure: {e}")


def main():
    print("=== Database Error Handling Examples ===")

    try:
        demonstrate_retry_logic()
        demonstrate_query_error_handling()
        demonstrate_connection_error_handling()

        print("\n=== All examples completed ===")
    except Exception as e:
        print(f"Unexpected error in examples: {e}")


if __name__ == "__main__":
    main()