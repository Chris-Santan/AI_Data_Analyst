import os
from dataclasses import dataclass
from typing import Dict, Any, List, Optional


@dataclass
class DatabaseConfig:
    """
    Centralized configuration for database connections.
    Supports multiple database types and connection methods.
    """

    # Connection string types
    POSTGRES = "postgresql"
    MYSQL = "mysql"
    SQLITE = "sqlite"

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

        Returns:
            str: Formatted connection string
        """
        if db_type == cls.SQLITE:
            # For SQLite, typically a file path
            return f"sqlite:///{database}"

        if not all([username, password, host, database]):
            raise ValueError("Missing required connection parameters")

        if db_type == cls.POSTGRES:
            return (
                f"postgresql://{username}:{password}@"
                f"{host}:{port or 5432}/{database}"
            )

        if db_type == cls.MYSQL:
            return (
                f"mysql+pymysql://{username}:{password}@"
                f"{host}:{port or 3306}/{database}"
            )

        raise ValueError(f"Unsupported database type: {db_type}")