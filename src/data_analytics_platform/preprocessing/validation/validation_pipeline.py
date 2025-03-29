from typing import Dict, Any, List, Optional, Union, Callable
import pandas as pd
import json
import time
from datetime import datetime

from data_analytics_platform.core.interfaces.validation_interface import (
    DataValidationInterface,
    ValidationPipelineInterface,
    ValidationResult
)
from data_analytics_platform.core.exceptions.validation_exceptions import ValidationError
from data_analytics_platform.core.exceptions.custom_exceptions import DataAnalyticsPlatformError
from data_analytics_platform.preprocessing.validation.schema_validator import SchemaValidator, DataFrameSchema
from data_analytics_platform.preprocessing.validation.outlier_validator import OutlierValidator


class ValidationPipeline(ValidationPipelineInterface):
    """
    A pipeline for running multiple validators in sequence.
    """

    def __init__(self, name: str = "validation_pipeline", fail_fast: bool = False):
        """
        Initialize validation pipeline.

        Args:
            name (str): Name of the pipeline
            fail_fast (bool): Whether to stop on first validation failure
        """
        self.name = name
        self.fail_fast = fail_fast
        self.validators: List[DataValidationInterface] = []
        self.descriptions: List[str] = []

    def add_validator(self, validator: DataValidationInterface, description: str = "") -> None:
        """
        Add a validator to the pipeline.

        Args:
            validator (DataValidationInterface): Validator to add
            description (str): Description of the validator
        """
        self.validators.append(validator)
        self.descriptions.append(description or f"Validator {len(self.validators)}")

    def validate(self, data: Any) -> List[ValidationResult]:
        """
        Run all validators on the data.

        Args:
            data (Any): Data to validate

        Returns:
            List[ValidationResult]: List of validation results from all validators
        """
        results = []

        for i, validator in enumerate(self.validators):
            start_time = time.time()

            try:
                result = validator.validate(data)

                # Add metadata to result
                result.update({
                    'validator_index': i,
                    'validator_type': validator.__class__.__name__,
                    'validator_description': self.descriptions[i],
                    'execution_time': time.time() - start_time
                })

                results.append(result)

                # Stop on first failure if fail_fast is True
                if self.fail_fast and not result.get('valid', True):
                    break

            except Exception as e:
                error_result = {
                    'valid': False,
                    'validator_index': i,
                    'validator_type': validator.__class__.__name__,
                    'validator_description': self.descriptions[i],
                    'error': str(e),
                    'execution_time': time.time() - start_time
                }
                results.append(error_result)

                if self.fail_fast:
                    break

        return results

    def is_valid(self, data: Any) -> bool:
        """
        Check if data passes all validations.

        Args:
            data (Any): Data to check

        Returns:
            bool: True if data passes all validations, False otherwise
        """
        results = self.validate(data)
        return all(result.get('valid', False) for result in results)

    def generate_report(self, data: Any) -> Dict[str, Any]:
        """
        Generate a comprehensive validation report.

        Args:
            data (Any): Data to validate

        Returns:
            Dict[str, Any]: Validation report
        """
        results = self.validate(data)

        report = {
            'pipeline_name': self.name,
            'timestamp': datetime.now().isoformat(),
            'data_type': type(data).__name__,
            'overall_valid': all(result.get('valid', False) for result in results),
            'validator_count': len(self.validators),
            'results': results,
            'summary': self._generate_summary(results)
        }

        if isinstance(data, pd.DataFrame):
            report['data_info'] = {
                'shape': data.shape,
                'columns': list(data.columns),
                'missing_values': int(data.isna().sum().sum()),
                'missing_ratio': float(data.isna().sum().sum() / data.size) if data.size > 0 else 0
            }

        return report

    def _generate_summary(self, results: List[ValidationResult]) -> Dict[str, Any]:
        """
        Generate a summary of validation results.

        Args:
            results (List[ValidationResult]): List of validation results

        Returns:
            Dict[str, Any]: Summary of results
        """
        summary = {
            'total_validators': len(results),
            'passed_validators': sum(1 for r in results if r.get('valid', False)),
            'failed_validators': sum(1 for r in results if not r.get('valid', False)),
            'errors': [r.get('error', '') for r in results if 'error' in r],
            'total_execution_time': sum(r.get('execution_time', 0) for r in results)
        }

        # Summarize schema validation results if available
        schema_results = [r for r in results if r.get('validator_type') == 'SchemaValidator']
        if schema_results:
            schema_result = schema_results[0]

            error_counts = {}
            for col, col_result in schema_result.get('column_results', {}).items():
                for error_type, count in col_result.get('error_counts', {}).items():
                    if error_type not in error_counts:
                        error_counts[error_type] = 0
                    error_counts[error_type] += count

            summary['schema_validation'] = {
                'missing_columns': schema_result.get('missing_columns', []),
                'extra_columns': schema_result.get('extra_columns', []),
                'error_counts': error_counts
            }

        # Summarize outlier validation results if available
        outlier_results = [r for r in results if r.get('validator_type') == 'OutlierValidator']
        if outlier_results:
            outlier_result = outlier_results[0]

            summary['outlier_validation'] = {
                'total_outliers': outlier_result.get('total_outliers', 0),
                'outlier_ratio': outlier_result.get('outlier_ratio', 0)
            }

        return summary

    def save_report(self, report: Dict[str, Any], filepath: str) -> None:
        """
        Save validation report to a file.

        Args:
            report (Dict[str, Any]): Validation report
            filepath (str): Path to save the report
        """
        with open(filepath, 'w') as f:
            json.dump(report, f, indent=2, default=str)

    @classmethod
    def default_pipeline(cls, df: pd.DataFrame = None) -> 'ValidationPipeline':
        """
        Create a default validation pipeline with common validators.

        Args:
            df (pd.DataFrame): Optional sample DataFrame to infer schema

        Returns:
            ValidationPipeline: Configured validation pipeline
        """
        pipeline = cls(name="Default Validation Pipeline")

        # Add schema validator if sample data is provided
        if df is not None:
            schema = DataFrameSchema.infer_from_dataframe(df)
            schema_validator = SchemaValidator(schema)
            pipeline.add_validator(schema_validator, "Inferred Schema Validator")

            # Add outlier validator for numeric columns
            outlier_validator = OutlierValidator.auto_config(df)
            pipeline.add_validator(outlier_validator, "Automatic Outlier Validator")

        return pipeline