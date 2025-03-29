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


class DataConversationInterface(ABC):
    """Abstract base class for AI conversational analysis of data."""

    @abstractmethod
    def analyze_query(self, user_query: str) -> Dict[str, Any]:
        """
        Analyze a user's natural language query about data.

        Args:
            user_query (str): User's natural language query or question

        Returns:
            Dict[str, Any]: Analysis of the query, including:
                - query_type: Type of question (descriptive, analytical, statistical, etc.)
                - entities: Data entities mentioned (tables, columns, etc.)
                - operations: Operations requested (aggregation, filtering, etc.)
                - parameters: Any parameters for the operations
        """
        pass

    @abstractmethod
    def generate_response(self,
                          user_query: str,
                          analysis_results: Optional[Dict[str, Any]] = None) -> str:
        """
        Generate a natural language response to a user query.

        Args:
            user_query (str): User's natural language query
            analysis_results (Optional[Dict[str, Any]]): Results of data analysis or queries

        Returns:
            str: Natural language response explaining the results
        """
        pass

    @abstractmethod
    def suggest_followup_questions(self,
                                   user_query: str,
                                   current_context: Dict[str, Any]) -> List[str]:
        """
        Suggest follow-up questions based on the current conversation context.

        Args:
            user_query (str): User's most recent query
            current_context (Dict[str, Any]): Current conversation context

        Returns:
            List[str]: Suggested follow-up questions
        """
        pass

    @abstractmethod
    def determine_required_analysis(self, user_query: str) -> Dict[str, Any]:
        """
        Determine what type of analysis is needed to answer a user query.

        Args:
            user_query (str): User's natural language query

        Returns:
            Dict[str, Any]: Description of required analysis including:
                - analysis_type: Type of analysis needed (query, statistical test, etc.)
                - parameters: Parameters needed for the analysis
                - data_requirements: Description of data needed
        """
        pass