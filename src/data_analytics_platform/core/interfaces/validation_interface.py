# src/data_analytics_platform/core/interfaces/validation_interface.py
from abc import ABC, abstractmethod
from typing import Dict, Any, List, Optional, Union, Generic, TypeVar
import pandas as pd

T = TypeVar('T')
ValidationResult = Dict[str, Any]


class DataValidationInterface(ABC, Generic[T]):
    """Abstract base class for data validation."""

    @abstractmethod
    def validate(self, data: T) -> ValidationResult:
        """
        Validate data and return validation results.

        Args:
            data (T): Data to validate

        Returns:
            ValidationResult: Dictionary containing validation results
        """
        pass

    @abstractmethod
    def is_valid(self, data: T) -> bool:
        """
        Check if data is valid.

        Args:
            data (T): Data to check

        Returns:
            bool: True if data is valid, False otherwise
        """
        pass


class DataFrameValidationInterface(DataValidationInterface[pd.DataFrame]):
    """Interface for validating pandas DataFrames."""

    @abstractmethod
    def validate_column(self, data: pd.DataFrame, column: str) -> ValidationResult:
        """
        Validate a specific column in the DataFrame.

        Args:
            data (pd.DataFrame): DataFrame to validate
            column (str): Column name to validate

        Returns:
            ValidationResult: Dictionary containing validation results for the column
        """
        pass

    @abstractmethod
    def validate_schema(self, data: pd.DataFrame) -> ValidationResult:
        """
        Validate DataFrame against a schema.

        Args:
            data (pd.DataFrame): DataFrame to validate

        Returns:
            ValidationResult: Dictionary containing schema validation results
        """
        pass


class ValidationPipelineInterface(ABC):
    """Interface for creating validation pipelines."""

    @abstractmethod
    def add_validator(self, validator: DataValidationInterface) -> None:
        """
        Add a validator to the pipeline.

        Args:
            validator (DataValidationInterface): Validator to add
        """
        pass

    @abstractmethod
    def validate(self, data: Any) -> List[ValidationResult]:
        """
        Run all validators on the data.

        Args:
            data (Any): Data to validate

        Returns:
            List[ValidationResult]: List of validation results from all validators
        """
        pass

    @abstractmethod
    def is_valid(self, data: Any) -> bool:
        """
        Check if data passes all validations.

        Args:
            data (Any): Data to check

        Returns:
            bool: True if data passes all validations, False otherwise
        """
        pass


class ValidationReportInterface(ABC):
    """Interface for validation reporting."""

    @abstractmethod
    def generate_report(self, validation_results: List[ValidationResult]) -> Dict[str, Any]:
        """
        Generate a comprehensive validation report.

        Args:
            validation_results (List[ValidationResult]): List of validation results

        Returns:
            Dict[str, Any]: Structured validation report
        """
        pass

    @abstractmethod
    def summarize(self, validation_results: List[ValidationResult]) -> str:
        """
        Generate a summary of validation results.

        Args:
            validation_results (List[ValidationResult]): List of validation results

        Returns:
            str: Summary string
        """
        pass