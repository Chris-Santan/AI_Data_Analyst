# src/core/exceptions/custom_exceptions.py
from typing import Dict, Any, List, Optional


class DataAnalyticsPlatformError(Exception):
    """Base exception for the Data Analytics Platform."""

    def __init__(self, message: str):
        self.message = message
        super().__init__(self.message)

    def get_user_message(self) -> str:
        """Get a user-friendly message."""
        return self.message

    def get_recovery_suggestions(self) -> List[str]:
        """Get suggestions for recovering from this error."""
        return ["Please try again later."]


class DatabaseConnectionError(DataAnalyticsPlatformError):
    """Raised when there's an issue with database connection."""

    def __init__(self, message: str, error_code: Optional[str] = None):
        self.error_code = error_code
        super().__init__(message)

    def get_user_message(self) -> str:
        """Get a user-friendly message about the connection error."""
        return f"Unable to connect to database. {self.message}"

    def get_recovery_suggestions(self) -> List[str]:
        """Get suggestions for recovering from connection errors."""
        suggestions = [
            "Check that the database server is running.",
            "Verify that network connectivity to the database server is available.",
            "Ensure database credentials are correct."
        ]

        if "timeout" in self.message.lower():
            suggestions.append("Database server may be under heavy load. Try again later.")

        return suggestions


class QueryExecutionError(DataAnalyticsPlatformError):
    """Raised when there's an error executing a database query."""

    def __init__(self, query: str, error_message: str):
        self.query = query
        self.error_message = error_message
        super().__init__(f"Error executing query: {query}. Details: {error_message}")

    def get_user_message(self) -> str:
        """Get a user-friendly message for query errors."""
        # Determine error type
        if "syntax" in self.error_message.lower():
            return "The query syntax is incorrect."
        elif "permission" in self.error_message.lower():
            return "You don't have permission to execute this query."
        elif "constraint" in self.error_message.lower():
            return "The query violates database constraints."
        else:
            return "An error occurred while executing the query."

    def get_recovery_suggestions(self) -> List[str]:
        """Get suggestions for recovering from query errors."""
        suggestions = ["Review the query for errors."]

        if "syntax" in self.error_message.lower():
            suggestions.append("Check for syntax errors in the SQL statement.")
        elif "permission" in self.error_message.lower():
            suggestions.append("Contact your database administrator for appropriate permissions.")
        elif "unique constraint" in self.error_message.lower():
            suggestions.append("The data you're trying to insert already exists.")
        elif "foreign key constraint" in self.error_message.lower():
            suggestions.append("The referenced data doesn't exist in the parent table.")

        return suggestions


class StatisticalTestError(DataAnalyticsPlatformError):
    """Raised when there's an issue with statistical test execution."""

    def __init__(self, test_name: str, reason: str):
        self.test_name = test_name
        self.reason = reason
        super().__init__(f"Error in {test_name} test: {reason}")

    def get_user_message(self) -> str:
        """Get a user-friendly message about the statistical test error."""
        return f"Could not perform {self.test_name} test. {self.reason}"

    def get_recovery_suggestions(self) -> List[str]:
        """Get suggestions for recovering from statistical test errors."""
        suggestions = ["Try a different statistical test that better fits your data."]

        if "sample size" in self.reason.lower():
            suggestions.append("Increase your sample size for more reliable results.")
        elif "assumption" in self.reason.lower():
            suggestions.append("Check if your data meets the test assumptions.")
        elif "missing values" in self.reason.lower():
            suggestions.append("Handle missing values in your dataset before analysis.")

        return suggestions


class AIGenerationError(DataAnalyticsPlatformError):
    """Raised when AI fails to generate a query or suggestion."""

    def __init__(self, input_text: str, error_details: str):
        self.input_text = input_text
        self.error_details = error_details
        super().__init__(f"AI generation failed for input: {input_text}. Details: {error_details}")

    def get_user_message(self) -> str:
        """Get a user-friendly message about the AI generation error."""
        return "The AI couldn't generate a response based on your input."

    def get_recovery_suggestions(self) -> List[str]:
        """Get suggestions for recovering from AI generation errors."""
        return [
            "Try rephrasing your query.",
            "Be more specific in your request.",
            "Break down complex requests into simpler ones."
        ]