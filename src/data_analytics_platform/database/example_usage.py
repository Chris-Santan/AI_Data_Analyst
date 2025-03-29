from data_analytics_platform.core.exceptions.custom_exceptions import DatabaseConnectionError, QueryExecutionError
from data_analytics_platform.database.config import DatabaseConfig
from data_analytics_platform.database.connection import DatabaseConnection
from data_analytics_platform.database.schema_retriever import SchemaRetriever
from data_analytics_platform.database.query_executor import QueryExecutor


def example_database_operations():
    """Example demonstrating database connection, schema retrieval, and query execution."""
    try:
        # Generate connection string for a PostgreSQL database
        connection_string = DatabaseConfig.get_connection_string(
            db_type=DatabaseConfig.POSTGRES,
            username="myuser",
            password="mypassword",
            host="localhost",
            port=5432,
            database="mydatabase"
        )

        # Create and establish database connection
        db_connection = DatabaseConnection(connection_string)
        db_connection.connect()

        # Print success message
        print("✓ Connected to database successfully")

        # Create schema retriever and get database schema
        schema_retriever = SchemaRetriever(db_connection)

        # Get all table names
        tables = schema_retriever.get_all_tables()
        print(f"✓ Found {len(tables)} tables in the database: {', '.join(tables)}")

        # Get schema for a specific table
        if tables:
            example_table = tables[0]
            table_schema = schema_retriever.get_table_schema(example_table)
            print(f"✓ Schema for table '{example_table}':")
            print(f"  - {len(table_schema['columns'])} columns")
            print(f"  - Primary key: {table_schema['primary_key']}")

            # Get relationships for the table
            relationships = schema_retriever.get_table_relationships(example_table)
            print(f"✓ Relationships for table '{example_table}':")
            print(f"  - {len(relationships['outgoing'])} outgoing foreign keys")
            print(f"  - {len(relationships['incoming'])} incoming foreign keys")

        # Create query executor
        query_executor = QueryExecutor(db_connection)

        # Execute a simple query
        if tables:
            example_table = tables[0]
            query = f"SELECT * FROM {example_table} LIMIT 5"

            # Validate query
            is_valid = query_executor.validate_query(query)
            print(f"✓ Query validation: {'Valid' if is_valid else 'Invalid'}")

            # Execute query
            results = query_executor.execute_query(query)
            print(f"✓ Query executed successfully")
            print(f"  - Retrieved {len(results)} rows")

            # Print the first row as an example
            if results:
                print("  - Example row:")
                for key, value in results[0].items():
                    print(f"    {key}: {value}")

        # Close the database connection
        db_connection.disconnect()
        print("✓ Database connection closed")

    except DatabaseConnectionError as e:
        print(f"✗ Connection error: {e}")
    except QueryExecutionError as e:
        print(f"✗ Query error: {e}")


def example_sqlite_connection():
    """Example demonstrating connection to an SQLite database."""
    try:
        # Generate connection string for an SQLite database
        connection_string = DatabaseConfig.get_connection_string(
            db_type=DatabaseConfig.SQLITE,
            database="example.db"  # Path to SQLite file
        )

        # Create and establish database connection
        db_connection = DatabaseConnection(connection_string)
        db_connection.connect()

        # Print success message
        print("✓ Connected to SQLite database successfully")

        # Create schema retriever
        schema_retriever = SchemaRetriever(db_connection)

        # Get a summary of the database schema
        schema_summary = schema_retriever.get_schema_summary()
        print(f"✓ Database summary:")
        print(f"  - {schema_summary['table_count']} tables")

        # Print table information
        for table_name, table_info in schema_summary['tables'].items():
            print(f"  - Table '{table_name}': {table_info['column_count']} columns")

        # Close the database connection
        db_connection.disconnect()
        print("✓ SQLite database connection closed")

    except DatabaseConnectionError as e:
        print(f"✗ Connection error: {e}")


if __name__ == "__main__":
    print("=== PostgreSQL Database Operations Example ===")
    example_database_operations()

    print("\n=== SQLite Database Operations Example ===")
    example_sqlite_connection()