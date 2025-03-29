from typing import Dict, Any, List, Optional, Union, Set, Callable
import pandas as pd
import numpy as np
import json
import logging
from datetime import datetime
import os

from data_analytics_platform.core.exceptions.validation_exceptions import ValidationError, SchemaValidationError, OutlierValidationError
from data_analytics_platform.core.exceptions.custom_exceptions import DataAnalyticsPlatformError
from data_analytics_platform.preprocessing.validation.schema_validator import SchemaValidator, DataFrameSchema, ColumnSchema
from data_analytics_platform.preprocessing.validation.outlier_validator import OutlierValidator, OutlierMethod, OutlierConfig
from data_analytics_platform.preprocessing.validation.validation_pipeline import ValidationPipeline
from data_analytics_platform.preprocessing.validation.validation_report import ValidationReportGenerator


class DataValidator:
    """
    Main data validation class that provides a simplified interface
    for validating data against various criteria.
    """

    def __init__(self, name: str = "data_validator"):
        """
        Initialize data validator.

        Args:
            name (str): Name for this validator
        """
        self.name = name
        self.logger = logging.getLogger(__name__)
        self.report_generator = ValidationReportGenerator(report_name=f"{name}_report")
        self.pipeline = ValidationPipeline(name=f"{name}_pipeline")
        self.validation_results = []

    def validate_schema(self,
                        data: pd.DataFrame,
                        schema: Optional[Union[DataFrameSchema, Dict[str, Any]]] = None,
                        infer_if_none: bool = True) -> Dict[str, Any]:
        """
        Validate DataFrame against a schema.

        Args:
            data (pd.DataFrame): DataFrame to validate
            schema (Optional[Union[DataFrameSchema, Dict[str, Any]]]): Schema to validate against
            infer_if_none (bool): Whether to infer schema if not provided

        Returns:
            Dict[str, Any]: Validation results
        """
        if not isinstance(data, pd.DataFrame):
            raise ValueError("Input must be a pandas DataFrame")

        # Handle schema
        if schema is None and infer_if_none:
            self.logger.info("No schema provided, inferring from data")
            schema = DataFrameSchema.infer_from_dataframe(data)
        elif isinstance(schema, dict):
            schema = SchemaValidator.from_dict(schema).schema
        elif not isinstance(schema, DataFrameSchema):
            raise ValueError("Schema must be a DataFrameSchema, dictionary, or None")

        # Create validator
        validator = SchemaValidator(schema)

        # Validate data
        results = validator.validate(data)
        self.validation_results = [results]

        return results

    def validate_outliers(self,
                          data: pd.DataFrame,
                          columns: Optional[List[str]] = None,
                          method: str = OutlierMethod.IQR,
                          threshold: float = 1.5,
                          params: Optional[Dict[str, Any]] = None) -> Dict[str, Any]:
        """
        Validate DataFrame for outliers.

        Args:
            data (pd.DataFrame): DataFrame to validate
            columns (Optional[List[str]]): Columns to check
            method (str): Outlier detection method
            threshold (float): Threshold for outlier detection
            params (Optional[Dict[str, Any]]): Additional parameters

        Returns:
            Dict[str, Any]: Validation results
        """
        if not isinstance(data, pd.DataFrame):
            raise ValueError("Input must be a pandas DataFrame")

        # Get columns to check
        if columns is None:
            columns = data.select_dtypes(include=['number']).columns.tolist()

        # Create validator
        validator = OutlierValidator()

        # Configure validator
        for column in columns:
            if column in data.columns:
                validator.add_column(
                    column=column,
                    method=method,
                    threshold=threshold,
                    params=params
                )

        # Validate data
        results = validator.validate(data)
        self.validation_results = [results]

        return results

    def build_pipeline(self):
        """
        Build a new validation pipeline.
        """
        self.pipeline = ValidationPipeline(name=f"{self.name}_pipeline")
        return self

    def add_schema_validation(self,
                              schema: Optional[Union[DataFrameSchema, Dict[str, Any]]] = None,
                              infer_from: Optional[pd.DataFrame] = None,
                              description: str = "Schema Validation") -> 'DataValidator':
        """
        Add schema validation to the pipeline.

        Args:
            schema (Optional[Union[DataFrameSchema, Dict[str, Any]]]): Schema to validate against
            infer_from (Optional[pd.DataFrame]): DataFrame to infer schema from
            description (str): Validator description

        Returns:
            DataValidator: Self for chaining
        """
        if schema is None and infer_from is not None:
            schema = DataFrameSchema.infer_from_dataframe(infer_from)
        elif isinstance(schema, dict):
            schema = SchemaValidator.from_dict(schema).schema

        if schema is None:
            raise ValueError("Either schema or infer_from must be provided")

        validator = SchemaValidator(schema)
        self.pipeline.add_validator(validator, description)

        return self

    def add_outlier_validation(self,
                               columns: Optional[List[str]] = None,
                               method: str = OutlierMethod.IQR,
                               threshold: float = 1.5,
                               params: Optional[Dict[str, Any]] = None,
                               auto_config_from: Optional[pd.DataFrame] = None,
                               sensitivity: str = 'medium',
                               description: str = "Outlier Validation") -> 'DataValidator':
        """
        Add outlier validation to the pipeline.

        Args:
            columns (Optional[List[str]]): Columns to check
            method (str): Outlier detection method
            threshold (float): Threshold for outlier detection
            params (Optional[Dict[str, Any]]): Additional parameters
            auto_config_from (Optional[pd.DataFrame]): DataFrame to auto-configure from
            sensitivity (str): Sensitivity level for auto-configuration
            description (str): Validator description

        Returns:
            DataValidator: Self for chaining
        """
        if auto_config_from is not None:
            validator = OutlierValidator.auto_config(
                auto_config_from,
                method=method,
                threshold=threshold,
                sensitivity=sensitivity
            )
        else:
            validator = OutlierValidator()

            if columns is not None:
                for column in columns:
                    validator.add_column(
                        column=column,
                        method=method,
                        threshold=threshold,
                        params=params
                    )

        self.pipeline.add_validator(validator, description)

        return self

    def run_pipeline(self, data: pd.DataFrame) -> List[Dict[str, Any]]:
        """
        Run the validation pipeline on data.

        Args:
            data (pd.DataFrame): DataFrame to validate

        Returns:
            List[Dict[str, Any]]: Validation results
        """
        if not isinstance(data, pd.DataFrame):
            raise ValueError("Input must be a pandas DataFrame")

        if not self.pipeline.validators:
            raise ValueError("No validators in the pipeline. Add validators first.")

        # Run pipeline
        self.validation_results = self.pipeline.validate(data)

        return self.validation_results

    def is_valid(self, data: Optional[pd.DataFrame] = None) -> bool:
        """
        Check if data is valid.

        Args:
            data (Optional[pd.DataFrame]): Data to check (uses last results if None)

        Returns:
            bool: True if data is valid, False otherwise
        """
        if data is not None:
            # Run validation on new data
            results = self.run_pipeline(data)
        else:
            # Use last results
            results = self.validation_results

        if not results:
            raise ValueError("No validation results available")

        return all(result.get('valid', False) for result in results)

    def generate_report(self, data: Optional[pd.DataFrame] = None) -> Dict[str, Any]:
        """
        Generate a validation report.

        Args:
            data (Optional[pd.DataFrame]): Data to validate (uses last results if None)

        Returns:
            Dict[str, Any]: Validation report
        """
        if data is not None:
            # Run validation on new data
            self.run_pipeline(data)

        if not self.validation_results:
            raise ValueError("No validation results available")

        # Generate report
        return self.report_generator.generate_report(self.validation_results)

    def summarize_validation(self, data: Optional[pd.DataFrame] = None) -> str:
        """
        Generate a human-readable summary of validation results.

        Args:
            data (Optional[pd.DataFrame]): Data to validate (uses last results if None)

        Returns:
            str: Validation summary
        """
        if data is not None:
            # Run validation on new data
            self.run_pipeline(data)

        if not self.validation_results:
            raise ValueError("No validation results available")

        # Generate summary
        return self.report_generator.summarize(self.validation_results)

    def save_report(self,
                    filepath: str,
                    format: str = 'json',
                    data: Optional[pd.DataFrame] = None) -> str:
        """
        Save validation report to a file.

        Args:
            filepath (str): Path to save the report
            format (str): Report format ('json' or 'txt')
            data (Optional[pd.DataFrame]): Data to validate (uses last results if None)

        Returns:
            str: Path to saved report file
        """
        # Generate report
        report = self.generate_report(data)

        # Save report
        return self.report_generator.save_report(report, filepath, format)

    @classmethod
    def quick_validate(cls,
                       data: pd.DataFrame,
                       schema: Optional[Union[DataFrameSchema, Dict[str, Any]]] = None,
                       check_outliers: bool = True,
                       outlier_method: str = OutlierMethod.IQR,
                       outlier_threshold: float = 1.5) -> Dict[str, Any]:
        """
        Quickly validate a DataFrame with default settings.

        Args:
            data (pd.DataFrame): DataFrame to validate
            schema (Optional[Union[DataFrameSchema, Dict[str, Any]]]): Schema to validate against
            check_outliers (bool): Whether to check for outliers
            outlier_method (str): Outlier detection method
            outlier_threshold (float): Threshold for outlier detection

        Returns:
            Dict[str, Any]: Validation report
        """
        validator = cls(name="quick_validator")

        # Build pipeline
        validator.build_pipeline()

        # Add schema validation
        if schema is not None:
            validator.add_schema_validation(schema=schema)
        else:
            validator.add_schema_validation(infer_from=data)

        # Add outlier validation
        if check_outliers:
            validator.add_outlier_validation(
                auto_config_from=data,
                method=outlier_method,
                threshold=outlier_threshold
            )

        # Run pipeline
        validator.run_pipeline(data)

        # Generate report
        return validator.generate_report()