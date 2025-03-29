from abc import ABC, abstractmethod
from typing import Dict, Any, List, Optional


class DatabaseConnectionInterface(ABC):
    """Abstract base class for database connections."""

    @abstractmethod
    def connect(self) -> bool:
        """Establish a database connection.

        Returns:
            bool: True if connection is successful, False otherwise.
        """
        pass

    @abstractmethod
    def disconnect(self) -> None:
        """Close the database connection."""
        pass

    @abstractmethod
    def get_schema(self, table_name: Optional[str] = None) -> Dict[str, Any]:
        """Retrieve database or table schema.

        Args:
            table_name (Optional[str]): Specific table to retrieve schema for.

        Returns:
            Dict[str, Any]: Schema information.
        """
        pass