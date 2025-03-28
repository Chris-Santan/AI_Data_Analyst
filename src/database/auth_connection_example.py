import os
import time
from dotenv import load_dotenv
from typing import Dict, Any, List

from database.config import DatabaseConfig
from database.auth_manager import AuthenticationManager
from database.connection import DatabaseConnection, get_global_connection_pool
from database.schema_retriever import SchemaRetriever
from database.query_executor import QueryExecutor
from core.exceptions.custom_exceptions import DatabaseConnectionError


def example_authentication_methods():
    """Example demonstrating different authentication methods."""
    print("=== Authentication Methods Example ===\n")

    auth_manager = AuthenticationManager()

    # 1. Basic username/password authentication
    print("1. Basic Authentication")
    basic_auth = auth_manager.get_basic_auth_credentials(
        username="user123",
        password="securepass"
    )
    print(f"   ✓ Basic auth params: {auth_manager.get_auth_params(basic_auth)}")

    # 2. Environment variable authentication
    print("\n2. Environment Variable Authentication")
    try:
        # Set environment variables for testing
        os.environ["DB_USERNAME"] = "env_user"
        os.environ["DB_PASSWORD"] = "env_pass"

        env_auth = auth_manager.get_env_credentials(
            username_var="DB_USERNAME",
            password_var="DB_PASSWORD"
        )
        print(f"   ✓ Environment auth params: {auth_manager.get_auth_params(env_auth)}")
    except DatabaseConnectionError as e:
        print(f"   ✗ Environment auth error: {str(e)}")

    # 3. SSL certificate authentication
    print("\n3. SSL Certificate Authentication")
    try:
        # This would require actual certificate files
        ssl_auth = auth_manager.get_ssl_credentials(
            cert_path="dummy_cert.pem",
            key_path="dummy_key.pem"
        )
        print(f"   ✓ SSL auth params: {auth_manager.get_auth_params(ssl_auth)}")
    except DatabaseConnectionError as e:
        print(f"   ✗ SSL auth error: {str(e)}")

    # 4. Token-based authentication
    print("\n4. Token-based Authentication")
    token_auth = auth_manager.get_token_auth_credentials(
        token="eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.example"
    )
    print(f"   ✓ Token auth params: {auth_manager.get_auth_params(token_auth)}")

    # 5. Credential encryption
    print("\n5. Credential Encryption")
    # Generate encryption key
    key = auth_manager._encryption_key or auth_manager.encrypt_credentials({})[:44]

    # Encrypt credentials
    encrypted = auth_manager.encrypt_credentials(basic_auth, key)
    print(f"   ✓ Encrypted credentials: {encrypted[:20]}...")

    # Decrypt credentials
    decrypted = auth_manager.decrypt_credentials(encrypted, key)
    print(f"   ✓ Decrypted credentials: {decrypted}")

    print("\n=== End of Authentication Methods Example ===")


def example_connection_pooling():
    """Example demonstrating connection pooling."""
    print("\n=== Connection Pooling Example ===\n")

    # Get the global connection pool
    pool = get_global_connection_pool()

    # Create multiple database connections using the pool
    connections = []
    config = DatabaseConfig()

    try:
        # Create SQLite connection string
        sqlite_conn_string = config.get_connection_string(
            db_type=config.SQLITE,
            database=":memory:"  # In-memory SQLite database
        )

        print(f"Connection string: {sqlite_conn_string}")
        print("Creating 3 connections to the same database...")

        # Create multiple connections
        for i in range(3):
            conn = DatabaseConnection(
                connection_string=sqlite_conn_string,
                use_pool=True  # Use connection pooling
            )
            conn.connect()
            connections.append(conn)
            print(f"   ✓ Connection {i + 1} established: {conn.get_connection_info()}")

        # Get pool statistics
        stats = pool.get_stats()
        print(f"\nPool statistics: {stats}")

        # Use the first connection to create a table
        print("\nCreating a table using the first connection...")
        connections[0].execute_raw_sql("""
            CREATE TABLE example (
                id INTEGER PRIMARY KEY,
                name TEXT,
                value REAL
            )
        """)

        # Use the second connection to insert data
        print("Inserting data using the second connection...")
        connections[1].execute_raw_sql("""
            INSERT INTO example (name, value) VALUES
            ('item1', 10.5),
            ('item2', 20.7),
            ('item3', 30.9)
        """)

        # Use the third connection to query data
        print("Querying data using the third connection...")
        results = connections[2].execute_raw_sql("SELECT * FROM example")
        print(f"Query results: {results}")

        # Disconnect one connection
        print("\nDisconnecting the first connection...")
        connections[0].disconnect()

        # Updated pool statistics
        print(f"Updated pool statistics: {pool.get_stats()}")

    except Exception as e:
        print(f"Error: {str(e)}")
    finally:
        # Disconnect all connections
        print("\nDisconnecting all connections...")
        for conn in connections:
            try:
                conn.disconnect()
            except:
                pass

        # Final pool statistics
        print(f"Final pool statistics: {pool.get_stats()}")

    print("\n=== End of Connection Pooling Example ===")


def example_connection_with_auth():
    """Example demonstrating connection with different authentication methods."""
    print("\n=== Connection with Authentication Example ===\n")

    config = DatabaseConfig()
    auth_manager = AuthenticationManager()

    try:
        # Example 1: SQLite connection (no authentication needed)
        print("1. SQLite Connection (No Authentication)")

        sqlite_conn = DatabaseConnection(
            db_type=config.SQLITE,
            database=":memory:"  # In-memory SQLite database
        )
        sqlite_conn.connect()
        print(f"   ✓ Connected: {sqlite_conn.get_connection_info()}")

        # Create and query a table
        sqlite_conn.execute_raw_sql("""
            CREATE TABLE example (id INTEGER PRIMARY KEY, name TEXT);
            INSERT INTO example (name) VALUES ('test1'), ('test2');
        """)

        results = sqlite_conn.execute_raw_sql("SELECT * FROM example")
        print(f"   ✓ Query results: {results}")

        # Example 2: Connection with credentials
        print("\n2. Connection with Basic Authentication")

        # Get basic authentication credentials
        basic_auth = auth_manager.get_basic_auth_credentials(
            username="test_user",
            password="test_pass"
        )

        # This would normally connect to a real database
        # Here we're just demonstrating the setup
        try:
            postgres_conn = DatabaseConnection(
                db_type=config.POSTGRES,
                auth_credentials=basic_auth,
                host="localhost",
                port=5432,
                database="test_db"
            )
            # Note: this will fail since we don't have a real PostgreSQL server
            postgres_conn.connect()
        except DatabaseConnectionError as e:
            print(f"   ✗ Connection failed (expected): {str(e)}")

        # Example 3: Connection with environment variables
        print("\n3. Connection from Environment Variables")

        # Set environment variables for testing
        os.environ["DB_TYPE"] = "sqlite"
        os.environ["DB_DATABASE"] = ":memory:"

        env_conn = DatabaseConnection()  # Will use environment variables
        env_conn.connect()
        print(f"   ✓ Connected from environment: {env_conn.get_connection_info()}")

        # Clean up
        sqlite_conn.disconnect()
        env_conn.disconnect()

    except Exception as e:
        print(f"Error: {str(e)}")

    print("\n=== End of Connection with Authentication Example ===")


if __name__ == "__main__":
    # Load environment variables if .env file exists
    load_dotenv()

    # Run examples
    example_authentication_methods()
    example_connection_pooling()
    example_connection_with_auth()