import os
from dataclasses import dataclass
from typing import Dict, Any, List, Optional, Union
from pathlib import Path

from database.auth_manager import AuthenticationManager
from core.exceptions.custom_exceptions import DatabaseConnectionError


@dataclass
class DatabaseConfig:
    """
    Centralized configuration for database connections.
    Supports multiple database types, authentication methods, and connection options.
    """

    # Connection string types
    POSTGRES = "postgresql"
    MYSQL = "mysql"
    SQLITE = "sqlite"
    MSSQL = "mssql"
    ORACLE = "oracle"

    def __init__(self):
        """Initialize database configuration."""
        self.auth_manager = AuthenticationManager()
        self._config_file_path = None
        self._env_prefix = "DB_"

    def set_config_file(self, file_path: Union[str, Path]) -> None:
        """
        Set path to a configuration file.

        Args:
            file_path (Union[str, Path]): Path to configuration file

        Raises:
            ValueError: If file doesn't exist
        """
        path = Path(file_path) if isinstance(file_path, str) else file_path
        if not path.exists():
            raise ValueError(f"Configuration file not found: {path}")
        self._config_file_path = path

    def set_env_prefix(self, prefix: str) -> None:
        """
        Set prefix for environment variables.

        Args:
            prefix (str): Prefix for environment variables
        """
        self._env_prefix = prefix

    @classmethod
    def get_connection_string(
            cls,
            db_type: str,
            username: Optional[str] = None,
            password: Optional[str] = None,
            host: Optional[str] = None,
            port: Optional[int] = None,
            database: str = "",
            **kwargs
    ) -> str:
        """
        Generate a connection string based on database type.

        Args:
            db_type (str): Type of database (postgres, mysql, sqlite)
            username (Optional[str]): Database username
            password (Optional[str]): Database password
            host (Optional[str]): Database host
            port (Optional[int]): Database port
            database (str): Database name
            **kwargs: Additional connection parameters

        Returns:
            str: Formatted connection string
        """
        if db_type == cls.SQLITE:
            # For SQLite, typically a file path
            return f"sqlite:///{database}"

        if db_type == cls.POSTGRES:
            if not all([username, password, host, database]):
                raise ValueError("Missing required connection parameters")

            return (
                f"postgresql://{username}:{password}@"
                f"{host}:{port or 5432}/{database}"
            )

        if db_type == cls.MYSQL:
            if not all([username, password, host, database]):
                raise ValueError("Missing required connection parameters")

            return (
                f"mysql+pymysql://{username}:{password}@"
                f"{host}:{port or 3306}/{database}"
            )

        if db_type == cls.MSSQL:
            if not all([username, password, host, database]):
                raise ValueError("Missing required connection parameters")

            return (
                f"mssql+pyodbc://{username}:{password}@"
                f"{host}:{port or 1433}/{database}"
                f"?driver=ODBC+Driver+17+for+SQL+Server"
            )

        if db_type == cls.ORACLE:
            if not all([username, password, host, database]):
                raise ValueError("Missing required connection parameters")

            return (
                f"oracle+cx_oracle://{username}:{password}@"
                f"{host}:{port or 1521}/{database}"
            )

        raise ValueError(f"Unsupported database type: {db_type}")

    def get_connection_string_from_env(self, db_type: Optional[str] = None) -> str:
        """
        Generate a connection string from environment variables.

        Args:
            db_type (Optional[str]): Database type, reads from env if not provided

        Returns:
            str: Connection string

        Raises:
            DatabaseConnectionError: If required environment variables are missing
        """
        prefix = self._env_prefix

        # Get database type from environment if not provided
        if not db_type:
            db_type = os.getenv(f"{prefix}TYPE")
            if not db_type:
                raise DatabaseConnectionError(f"Missing {prefix}TYPE environment variable")

        # For SQLite, we only need the database file path
        if db_type.lower() in ["sqlite", self.SQLITE]:
            database = os.getenv(f"{prefix}DATABASE")
            if not database:
                raise DatabaseConnectionError(f"Missing {prefix}DATABASE environment variable")
            return f"sqlite:///{database}"

        # For other database types, we need more parameters
        username = os.getenv(f"{prefix}USERNAME")
        password = os.getenv(f"{prefix}PASSWORD")
        host = os.getenv(f"{prefix}HOST")
        port = os.getenv(f"{prefix}PORT")
        database = os.getenv(f"{prefix}DATABASE")

        # Check for required parameters
        if not all([username, password, host, database]):
            missing = []
            if not username:
                missing.append(f"{prefix}USERNAME")
            if not password:
                missing.append(f"{prefix}PASSWORD")
            if not host:
                missing.append(f"{prefix}HOST")
            if not database:
                missing.append(f"{prefix}DATABASE")

            raise DatabaseConnectionError(f"Missing environment variables: {', '.join(missing)}")

        # Convert port to integer if provided
        if port:
            try:
                port = int(port)
            except ValueError:
                raise DatabaseConnectionError(f"Invalid port number: {port}")

        # Generate connection string
        return self.get_connection_string(
            db_type=db_type,
            username=username,
            password=password,
            host=host,
            port=port,
            database=database
        )

    def get_connection_params(self,
                              db_type: str,
                              auth_credentials: Dict[str, Any],
                              host: Optional[str] = None,
                              port: Optional[int] = None,
                              database: str = "",
                              **kwargs) -> Dict[str, Any]:
        """
        Get connection parameters for SQLAlchemy.

        Args:
            db_type (str): Database type
            auth_credentials (Dict[str, Any]): Authentication credentials
            host (Optional[str]): Database host
            port (Optional[int]): Database port
            database (str): Database name
            **kwargs: Additional connection parameters

        Returns:
            Dict[str, Any]: Connection parameters
        """
        # Get authentication parameters
        auth_params = self.auth_manager.get_auth_params(auth_credentials)

        # Build base connection parameters
        connection_params = {
            "db_type": db_type,
            "database": database,
            **auth_params
        }

        # Add host and port if provided (not needed for SQLite)
        if db_type != self.SQLITE:
            if not host:
                raise ValueError("Host is required for non-SQLite databases")

            connection_params["host"] = host

            # Set default port based on database type
            if not port:
                if db_type == self.POSTGRES:
                    port = 5432
                elif db_type == self.MYSQL:
                    port = 3306
                elif db_type == self.MSSQL:
                    port = 1433
                elif db_type == self.ORACLE:
                    port = 1521

            connection_params["port"] = port

        # Add any additional parameters
        for key, value in kwargs.items():
            connection_params[key] = value

        return connection_params

    def get_connection_pool_args(self,
                                 pool_size: int = 5,
                                 max_overflow: int = 10,
                                 pool_timeout: int = 30,
                                 pool_recycle: int = 1800) -> Dict[str, Any]:
        """
        Get connection pooling arguments for SQLAlchemy.

        Args:
            pool_size (int): The size of the pool to be maintained
            max_overflow (int): The maximum overflow size of the pool
            pool_timeout (int): Seconds to wait before giving up on getting a connection
            pool_recycle (int): Seconds after which a connection is recycled

        Returns:
            Dict[str, Any]: Connection pooling arguments
        """
        return {
            "pool_size": pool_size,
            "max_overflow": max_overflow,
            "pool_timeout": pool_timeout,
            "pool_recycle": pool_recycle
        }