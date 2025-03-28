from abc import ABC, abstractmethod
from typing import Dict, Any, List, Optional

class AIQueryGeneratorInterface(ABC):
    """Abstract base class for AI-powered query generation."""

    @abstractmethod
    def generate_sql_query(self, natural_language_query: str) -> str:
        """Convert natural language to SQL query.

        Args:
            natural_language_query (str): User's natural language query.

        Returns:
            str: Generated SQL query.
        """
        pass

    @abstractmethod
    def suggest_statistical_test(self, data_schema: Dict[str, Any]) -> str:
        """Suggest appropriate statistical tests based on data schema.

        Args:
            data_schema (Dict[str, Any]): Schema of the data.

        Returns:
            str: Recommended statistical test.
        """
        pass