from typing import Dict, Any, Optional
import sqlalchemy as sa
from sqlalchemy.engine import Engine
from sqlalchemy.orm import sessionmaker
from sqlalchemy.exc import SQLAlchemyError

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