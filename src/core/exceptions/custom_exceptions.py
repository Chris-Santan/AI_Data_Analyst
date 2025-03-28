from abc import ABC, abstractmethod
from typing import Dict, Any, List, Optional

class DataAnalyticsPlatformError(Exception):
    """Base exception for the Data Analytics Platform."""
    pass

class DatabaseConnectionError(DataAnalyticsPlatformError):
    """Raised when there's an issue with database connection."""
    def __init__(self, message: str, error_code: Optional[str] = None):
        self.message = message
        self.error_code = error_code
        super().__init__(self.message)

class QueryExecutionError(DataAnalyticsPlatformError):
    """Raised when there's an error executing a database query."""
    def __init__(self, query: str, error_message: str):
        self.query = query
        self.error_message = error_message
        super().__init__(f"Error executing query: {query}. Details: {error_message}")

class StatisticalTestError(DataAnalyticsPlatformError):
    """Raised when there's an issue with statistical test execution."""
    def __init__(self, test_name: str, reason: str):
        self.test_name = test_name
        self.reason = reason
        super().__init__(f"Error in {test_name} test: {reason}")

class AIGenerationError(DataAnalyticsPlatformError):
    """Raised when AI fails to generate a query or suggestion."""
    def __init__(self, input_text: str, error_details: str):
        self.input_text = input_text
        self.error_details = error_details
        super().__init__(f"AI generation failed for input: {input_text}. Details: {error_details}")