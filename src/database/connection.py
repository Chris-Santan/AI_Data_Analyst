from typing import Dict, Any, Optional, List
import sqlalchemy as sa
from sqlalchemy.engine import Engine
from sqlalchemy.orm import sessionmaker, Session
from sqlalchemy.exc import SQLAlchemyError
from sqlalchemy import inspect
import logging
import threading

from core.interfaces.database_interface import DatabaseConnectionInterface
from core.exceptions.custom_exceptions import DatabaseConnectionError
from database.config import DatabaseConfig
from database.auth_manager import AuthenticationManager
from database.connection_pool import ConnectionPool

logger = logging.getLogger(__name__)

# Global connection pool
_GLOBAL_POOL = None
_POOL_LOCK = threading.Lock()


def get_global_connection_pool() -> ConnectionPool:
    """
    Get or create a global connection pool.

    Returns:
        ConnectionPool: Global connection pool
    """
    global _GLOBAL_POOL

    with _POOL_LOCK:
        if _GLOBAL_POOL is None:
            config = DatabaseConfig()
            _GLOBAL_POOL = ConnectionPool(config)
            _GLOBAL_POOL.start_monitoring()

    return _GLOBAL_POOL


class DatabaseConnection(DatabaseConnectionInterface):
    """
    Manages database connections using SQLAlchemy.
    Supports multiple database types, authentication methods, and connection pooling.
    """

    def __init__(
            self,
            connection_string: Optional[str] = None,
            db_type: Optional[str] = None,
            auth_credentials: Optional[Dict[str, Any]] = None,
            host: Optional[str] = None,
            port: Optional[int] = None,
            database: Optional[str] = None,
            connect_args: Optional[Dict[str, Any]] = None,
            use_pool: bool = True,
            config: Optional[DatabaseConfig] = None,
            auth_manager: Optional[AuthenticationManager] = None
    ):
        """
        Initialize database connection with flexible configuration options.

        Args:
            connection_string (Optional[str]): Direct connection string (overrides other params)
            db_type (Optional[str]): Database type (postgres, mysql, sqlite, etc.)
            auth_credentials (Optional[Dict[str, Any]]): Authentication credentials
            host (Optional[str]): Database host
            port (Optional[int]): Database port
            database (Optional[str]): Database name
            connect_args (Optional[Dict[str, Any]]): Additional connection arguments
            use_pool (bool): Whether to use connection pooling
            config (Optional[DatabaseConfig]): Database configuration
            auth_manager (Optional[AuthenticationManager]): Authentication manager
        """
        self._connection_string = connection_string
        self._db_type = db_type
        self._auth_credentials = auth_credentials
        self._host = host
        self._port = port
        self._database = database
        self._connect_args = connect_args or {}
        self._use_pool = use_pool
        self._config = config or DatabaseConfig()
        self._auth_manager = auth_manager or AuthenticationManager()

        # Connection state
        self._engine: Optional[Engine] = None
        self._session_factory = None
        self._is_connected = False
        self._connection_id = None

        # Connection pooling
        self._pool = get_global_connection_pool() if use_pool else None

    def connect(self) -> bool:
        """
        Establish a database connection using the provided configuration.

        Returns:
            bool: True if connection is successful

        Raises:
            DatabaseConnectionError: If connection fails
        """
        try:
            # If already connected, return True
            if self._is_connected and self._engine:
                return True

            # Get connection string if not provided
            if not self._connection_string:
                if not self._db_type:
                    # Try to get from environment
                    self._connection_string = self._config.get_connection_string_from_env()
                else:
                    # Build from components
                    if self._auth_credentials:
                        # Get auth parameters
                        auth_params = self._auth_manager.get_auth_params(self._auth_credentials)

                        # Extract username and password
                        username = auth_params.get('username')
                        password = auth_params.get('password')

                        # Update connect_args if needed
                        if 'connect_args' in auth_params:
                            self._connect_args.update(auth_params['connect_args'])

                        # Generate connection string
                        self._connection_string = self._config.get_connection_string(
                            db_type=self._db_type,
                            username=username,
                            password=password,
                            host=self._host,
                            port=self._port,
                            database=self._database
                        )
                    else:
                        # Database without authentication (e.g., SQLite)
                        self._connection_string = self._config.get_connection_string(
                            db_type=self._db_type,
                            database=self._database
                        )

            logger.debug(f"Connecting to database with connection string: {self._connection_string}")

            # Use connection pool if enabled
            if self._use_pool and self._pool:
                self._engine = self._pool.get_engine(
                    self._connection_string,
                    connect_args=self._connect_args
                )
                self._session_factory = self._pool.get_session_factory(
                    self._connection_string,
                    connect_args=self._connect_args
                )
                self._connection_id = f"{self._connection_string}:{hash(str(self._connect_args))}"
            else:
                # Create engine with connection string and additional arguments
                self._engine = sa.create_engine(
                    self._connection_string,
                    connect_args=self._connect_args
                )

                # Test the connection
                with self._engine.connect() as connection:
                    connection.execute(sa.text("SELECT 1"))

                # Create session factory
                self._session_factory = sessionmaker(bind=self._engine)

            self._is_connected = True
            logger.info(f"Successfully connected to database: {self._db_type or self._connection_string}")

            return True
        except SQLAlchemyError as e:
            self._is_connected = False
            self._engine = None
            self._session_factory = None

            error_message = f"Failed to connect to database: {str(e)}"
            logger.error(error_message)

            raise DatabaseConnectionError(error_message) from e

    def disconnect(self) -> None:
        """
        Close the database connection.
        For pooled connections, returns the connection to the pool rather than closing.
        """
        if not self._is_connected:
            return

        if self._use_pool and self._pool and self._connection_id:
            # For pooled connections, just mark as not connected
            # The pool will manage actual connection lifecycle
            self._is_connected = False
            self._engine = None
            self._session_factory = None
            logger.debug(f"Released connection {self._connection_id} back to pool")
        else:
            # For non-pooled connections, dispose the engine
            if self._engine:
                self._engine.dispose()

            self._engine = None
            self._session_factory = None
            self._is_connected = False
            logger.info("Database connection closed")

    def get_session(self) -> Session:
        """
        Get a database session.

        Returns:
            Session: SQLAlchemy database session

        Raises:
            DatabaseConnectionError: If no connection is established
        """
        if not self._is_connected or not self._session_factory:
            # Try to connect if not already connected
            if not self._is_connected:
                self.connect()

            if not self._session_factory:
                raise DatabaseConnectionError("No active database connection")

        return self._session_factory()

    def get_schema(self, table_name: Optional[str] = None) -> Dict[str, Any]:
        """
        Retrieve database or table schema.

        Args:
            table_name (Optional[str]): Specific table to retrieve schema for.

        Returns:
            Dict[str, Any]: Schema information.

        Raises:
            DatabaseConnectionError: If no connection is established
        """
        if not self._is_connected or not self._engine:
            raise DatabaseConnectionError("No active database connection")

        inspector = inspect(self._engine)

        # If table_name is provided, return schema for that specific table
        if table_name:
            try:
                # Get column information
                columns = inspector.get_columns(table_name)

                # Get primary key information
                pk_constraint = inspector.get_pk_constraint(table_name)

                # Get foreign key information
                foreign_keys = inspector.get_foreign_keys(table_name)

                # Get index information
                indexes = inspector.get_indexes(table_name)

                return {
                    'table_name': table_name,
                    'columns': columns,
                    'primary_key': pk_constraint,
                    'foreign_keys': foreign_keys,
                    'indexes': indexes
                }
            except SQLAlchemyError as e:
                raise DatabaseConnectionError(
                    f"Failed to retrieve schema for table {table_name}: {str(e)}"
                ) from e

        # If no table_name is provided, return all tables schema
        try:
            tables = inspector.get_table_names()
            schema = {}

            for table in tables:
                columns = inspector.get_columns(table)
                pk_constraint = inspector.get_pk_constraint(table)
                foreign_keys = inspector.get_foreign_keys(table)

                schema[table] = {
                    'columns': columns,
                    'primary_key': pk_constraint,
                    'foreign_keys': foreign_keys
                }

            return schema
        except SQLAlchemyError as e:
            raise DatabaseConnectionError(
                f"Failed to retrieve database schema: {str(e)}"
            ) from e

    def is_connected(self) -> bool:
        """
        Check if the database connection is active.

        Returns:
            bool: True if connected, False otherwise
        """
        if not self._is_connected or not self._engine:
            return False

        try:
            # Test the connection
            with self._engine.connect() as connection:
                connection.execute(sa.text("SELECT 1"))
            return True
        except SQLAlchemyError:
            self._is_connected = False
            return False

    def get_connection_info(self) -> Dict[str, Any]:
        """
        Get information about the current connection.

        Returns:
            Dict[str, Any]: Connection information

        Raises:
            DatabaseConnectionError: If no connection is established
        """
        if not self._is_connected:
            raise DatabaseConnectionError("No active database connection")

        db_type = self._db_type
        if not db_type and self._connection_string:
            # Try to extract DB type from connection string
            if "postgres" in self._connection_string:
                db_type = "postgresql"
            elif "mysql" in self._connection_string:
                db_type = "mysql"
            elif "sqlite" in self._connection_string:
                db_type = "sqlite"
            elif "mssql" in self._connection_string:
                db_type = "mssql"
            elif "oracle" in self._connection_string:
                db_type = "oracle"

        return {
            "database_type": db_type,
            "host": self._host,
            "database": self._database,
            "pooled": self._use_pool,
            "connection_id": self._connection_id if self._use_pool else None
        }

    def execute_raw_sql(self, sql: str, params: Optional[Dict[str, Any]] = None) -> Any:
        """
        Execute raw SQL directly.

        Args:
            sql (str): SQL statement to execute
            params (Optional[Dict[str, Any]]): Parameters for the SQL statement

        Returns:
            Any: Result of the SQL execution

        Raises:
            DatabaseConnectionError: If execution fails
        """
        if not self._is_connected:
            raise DatabaseConnectionError("No active database connection")

        try:
            session = self.get_session()
            with session as s:
                if params:
                    result = s.execute(sa.text(sql), params)
                else:
                    result = s.execute(sa.text(sql))

                if result.returns_rows:
                    return result.fetchall()
                return None
        except SQLAlchemyError as e:
            raise DatabaseConnectionError(f"Failed to execute SQL: {str(e)}") from e