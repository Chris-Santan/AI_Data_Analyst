from core.exceptions.custom_exceptions import DatabaseConnectionError
from database.config import DatabaseConfig
from database.connection import DatabaseConnection

def example_postgres_connection():
    """Example of connecting to a PostgreSQL database."""
    try:
        # Generate connection string
        connection_string = DatabaseConfig.get_connection_string(
            db_type=DatabaseConfig.POSTGRES,
            username="myuser",
            password="mypassword",
            host="localhost",
            port=5432,
            database="mydatabase"
        )

        # Create connection
        db_connection = DatabaseConnection(connection_string)

        # Connect
        if db_connection.connect():
            # Get a session
            session = db_connection.get_session()

            # Use the session for database operations
            # ... your database operations here ...

            # Close connection
            db_connection.disconnect()

    except DatabaseConnectionError as e:
        print(f"Connection failed: {e}")