from abc import ABC, abstractmethod
from typing import Dict, Any, List, Optional

class QueryExecutionInterface(ABC):
    """Abstract base class for query execution."""

    @abstractmethod
    def execute_query(self, query: str) -> List[Dict[str, Any]]:
        """Execute a SQL query.

        Args:
            query (str): SQL query to execute.

        Returns:
            List[Dict[str, Any]]: Query results.
        """
        pass

    @abstractmethod
    def validate_query(self, query: str) -> bool:
        """Validate the structure and syntax of a SQL query.

        Args:
            query (str): SQL query to validate.

        Returns:
            bool: True if query is valid, False otherwise.
        """
        pass