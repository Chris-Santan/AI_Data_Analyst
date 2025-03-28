from typing import Dict, Any, Optional, List
import sqlalchemy as sa
from sqlalchemy.engine import Engine
from sqlalchemy.orm import sessionmaker
from sqlalchemy.exc import SQLAlchemyError
from sqlalchemy import inspect

from core.interfaces.database_interface import DatabaseConnectionInterface
from core.exceptions.custom_exceptions import DatabaseConnectionError


class DatabaseConnection(DatabaseConnectionInterface):
    """
    Manages database connections using SQLAlchemy.
    Supports multiple database types through connection strings.
    """

    def __init__(
            self,
            connection_string: str,
            connect_args: Optional[Dict[str, Any]] = None
    ):
        """
        Initialize database connection.

        Args:
            connection_string (str): SQLAlchemy-compatible database connection string
            connect_args (Optional[Dict[str, Any]]): Additional connection arguments
        """
        self._connection_string = connection_string
        self._connect_args = connect_args or {}
        self._engine: Optional[Engine] = None
        self._session_factory = None

    def connect(self) -> bool:
        """
        Establish a database connection.

        Returns:
            bool: True if connection is successful

        Raises:
            DatabaseConnectionError: If connection fails
        """
        try:
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

            return True
        except SQLAlchemyError as e:
            raise DatabaseConnectionError(
                f"Failed to connect to database: {str(e)}"
            ) from e

    def disconnect(self) -> None:
        """
        Close the database connection.
        """
        if self._engine:
            self._engine.dispose()
            self._engine = None
            self._session_factory = None

    def get_session(self):
        """
        Get a database session.

        Returns:
            Session: SQLAlchemy database session

        Raises:
            DatabaseConnectionError: If no connection is established
        """
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
        if not self._engine:
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