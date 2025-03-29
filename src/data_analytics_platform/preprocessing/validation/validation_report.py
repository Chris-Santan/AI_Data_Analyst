from typing import Dict, Any, List, Optional, Union, Set
import pandas as pd
import numpy as np
import json
from datetime import datetime
import os
import logging

from data_analytics_platform.core.interfaces.validation_interface import ValidationReportInterface, ValidationResult
from data_analytics_platform.core.exceptions.validation_exceptions import ValidationError
from data_analytics_platform.core.exceptions.custom_exceptions import DataAnalyticsPlatformError


class ValidationReportGenerator(ValidationReportInterface):
    """
    Generates comprehensive validation reports from validation results.
    """

    def __init__(self, report_name: str = "data_validation_report"):
        """
        Initialize validation report generator.

        Args:
            report_name (str): Name for the report
        """
        self.report_name = report_name
        self.logger = logging.getLogger(__name__)

    def generate_report(self, validation_results: List[ValidationResult]) -> Dict[str, Any]:
        """
        Generate a comprehensive validation report.

        Args:
            validation_results (List[ValidationResult]): List of validation results

        Returns:
            Dict[str, Any]: Structured validation report
        """
        if not validation_results:
            return {
                'report_name': self.report_name,
                'timestamp': datetime.now().isoformat(),
                'valid': True,
                'validator_count': 0,
                'message': 'No validation results provided'
            }

        # Check overall validity
        overall_valid = all(result.get('valid', False) for result in validation_results)

        # Calculate statistics
        validators_passed = sum(1 for result in validation_results if result.get('valid', False))
        validators_failed = sum(1 for result in validation_results if not result.get('valid', False))
        execution_times = [result.get('execution_time', 0) for result in validation_results]

        report = {
            'report_name': self.report_name,
            'timestamp': datetime.now().isoformat(),
            'valid': overall_valid,
            'summary': {
                'validator_count': len(validation_results),
                'validators_passed': validators_passed,
                'validators_failed': validators_failed,
                'total_execution_time': sum(execution_times),
                'average_execution_time': sum(execution_times) / len(execution_times) if execution_times else 0
            },
            'validator_results': validation_results,
            'validation_issues': self._extract_validation_issues(validation_results)
        }

        # Add schema validation summary if available
        schema_results = [r for r in validation_results if r.get('validator_type') == 'SchemaValidator']
        if schema_results:
            report['schema_validation'] = self._summarize_schema_validation(schema_results[0])

        # Add outlier validation summary if available
        outlier_results = [r for r in validation_results if r.get('validator_type') == 'OutlierValidator']
        if outlier_results:
            report['outlier_validation'] = self._summarize_outlier_validation(outlier_results[0])

        return report

    def _extract_validation_issues(self, validation_results: List[ValidationResult]) -> List[Dict[str, Any]]:
        """
        Extract validation issues from results.

        Args:
            validation_results (List[ValidationResult]): List of validation results

        Returns:
            List[Dict[str, Any]]: List of validation issues
        """
        issues = []

        for result in validation_results:
            if not result.get('valid', True):
                # General error
                if 'error' in result:
                    issues.append({
                        'validator_type': result.get('validator_type', 'Unknown'),
                        'issue_type': 'error',
                        'message': result['error']
                    })

                # Handle schema validation issues
                if result.get('validator_type') == 'SchemaValidator':
                    # Missing columns
                    for col in result.get('missing_columns', []):
                        issues.append({
                            'validator_type': 'SchemaValidator',
                            'issue_type': 'missing_column',
                            'column': col,
                            'message': f"Required column '{col}' is missing"
                        })

                    # Extra columns
                    for col in result.get('extra_columns', []):
                        issues.append({
                            'validator_type': 'SchemaValidator',
                            'issue_type': 'extra_column',
                            'column': col,
                            'message': f"Unexpected column '{col}' found"
                        })

                    # Column errors
                    for col_name, col_result in result.get('column_results', {}).items():
                        if not col_result.get('valid', True):
                            for error in col_result.get('errors', []):
                                issues.append({
                                    'validator_type': 'SchemaValidator',
                                    'issue_type': 'column_validation',
                                    'column': col_name,
                                    'message': error
                                })

                # Handle outlier validation issues
                if result.get('validator_type') == 'OutlierValidator':
                    for col_name, col_result in result.get('column_results', {}).items():
                        if not col_result.get('valid', True):
                            issues.append({
                                'validator_type': 'OutlierValidator',
                                'issue_type': 'outliers',
                                'column': col_name,
                                'outlier_count': col_result.get('outlier_count', 0),
                                'outlier_ratio': col_result.get('outlier_ratio', 0),
                                'message': f"Column '{col_name}' has {col_result.get('outlier_count', 0)} outliers"
                            })

        return issues

    def _summarize_schema_validation(self, schema_result: ValidationResult) -> Dict[str, Any]:
        """
        Summarize schema validation results.

        Args:
            schema_result (ValidationResult): Schema validation result

        Returns:
            Dict[str, Any]: Schema validation summary
        """
        # Total error counts
        error_counts = {}
        for col, col_result in schema_result.get('column_results', {}).items():
            for error_type, count in col_result.get('error_counts', {}).items():
                if error_type not in error_counts:
                    error_counts[error_type] = 0
                error_counts[error_type] += count

        # Get detailed column errors
        column_errors = {}
        for col, col_result in schema_result.get('column_results', {}).items():
            if not col_result.get('valid', True):
                column_errors[col] = col_result.get('errors', [])

        return {
            'valid': schema_result.get('valid', False),
            'missing_columns': schema_result.get('missing_columns', []),
            'extra_columns': schema_result.get('extra_columns', []),
            'error_counts': error_counts,
            'column_errors': column_errors,
            'error_summary': self._generate_schema_error_summary(schema_result)
        }

    def _generate_schema_error_summary(self, schema_result: ValidationResult) -> str:
        """
        Generate a human-readable summary of schema errors.

        Args:
            schema_result (ValidationResult): Schema validation result

        Returns:
            str: Error summary
        """
        errors = []

        # Missing columns
        missing_columns = schema_result.get('missing_columns', [])
        if missing_columns:
            errors.append(f"Missing columns: {', '.join(missing_columns)}")

        # Extra columns
        extra_columns = schema_result.get('extra_columns', [])
        if extra_columns:
            errors.append(f"Unexpected columns: {', '.join(extra_columns)}")

        # Column errors
        for col, col_result in schema_result.get('column_results', {}).items():
            if not col_result.get('valid', True):
                for error in col_result.get('errors', []):
                    errors.append(f"Column '{col}': {error}")

        if not errors:
            return "No schema errors detected."

        return "Schema validation issues:\n• " + "\n• ".join(errors)

    def _summarize_outlier_validation(self, outlier_result: ValidationResult) -> Dict[str, Any]:
        """
        Summarize outlier validation results.

        Args:
            outlier_result (ValidationResult): Outlier validation result

        Returns:
            Dict[str, Any]: Outlier validation summary
        """
        columns_with_outliers = {}
        for col, col_result in outlier_result.get('column_results', {}).items():
            if col_result.get('outlier_count', 0) > 0:
                columns_with_outliers[col] = {
                    'outlier_count': col_result.get('outlier_count', 0),
                    'outlier_ratio': col_result.get('outlier_ratio', 0),
                    'outlier_min': col_result.get('outlier_min'),
                    'outlier_max': col_result.get('outlier_max'),
                    'outlier_mean': col_result.get('outlier_mean'),
                    'data_mean': col_result.get('data_mean'),
                    'data_std': col_result.get('data_std')
                }

        return {
            'valid': outlier_result.get('valid', False),
            'total_outliers': outlier_result.get('total_outliers', 0),
            'outlier_ratio': outlier_result.get('outlier_ratio', 0),
            'columns_with_outliers': columns_with_outliers,
            'outlier_summary': self._generate_outlier_summary(outlier_result)
        }

    def _generate_outlier_summary(self, outlier_result: ValidationResult) -> str:
        """
        Generate a human-readable summary of outlier detection.

        Args:
            outlier_result (ValidationResult): Outlier validation result

        Returns:
            str: Outlier summary
        """
        if not outlier_result.get('column_results'):
            return "No outlier analysis was performed."

        total_outliers = outlier_result.get('total_outliers', 0)
        if total_outliers == 0:
            return "No outliers detected."

        # Get columns with outliers, sorted by outlier count
        cols_with_outliers = []
        for col, col_result in outlier_result.get('column_results', {}).items():
            outlier_count = col_result.get('outlier_count', 0)
            if outlier_count > 0:
                cols_with_outliers.append((col, outlier_count, col_result.get('outlier_ratio', 0)))

        cols_with_outliers.sort(key=lambda x: x[1], reverse=True)

        outlier_messages = [
            f"Total outliers: {total_outliers} ({outlier_result.get('outlier_ratio', 0):.2%} of data)"
        ]

        for col, count, ratio in cols_with_outliers:
            outlier_messages.append(f"Column '{col}': {count} outliers ({ratio:.2%})")

        return "Outlier detection results:\n• " + "\n• ".join(outlier_messages)

    def summarize(self, validation_results: List[ValidationResult]) -> str:
        """
        Generate a summary of validation results.

        Args:
            validation_results (List[ValidationResult]): List of validation results

        Returns:
            str: Summary string
        """
        if not validation_results:
            return "No validation results available."

        overall_valid = all(result.get('valid', False) for result in validation_results)
        validators_run = len(validation_results)
        validators_passed = sum(1 for result in validation_results if result.get('valid', True))

        summary = [
            f"Validation Report: {'PASSED' if overall_valid else 'FAILED'}",
            f"Timestamp: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}",
            f"Validators: {validators_passed}/{validators_run} passed"
        ]

        if not overall_valid:
            # Add summaries for each validator type
            schema_results = [r for r in validation_results if r.get('validator_type') == 'SchemaValidator']
            if schema_results:
                summary.append("\n" + self._generate_schema_error_summary(schema_results[0]))

            outlier_results = [r for r in validation_results if r.get('validator_type') == 'OutlierValidator']
            if outlier_results:
                summary.append("\n" + self._generate_outlier_summary(outlier_results[0]))

        return "\n".join(summary)

    def save_report(self, report: Dict[str, Any], filepath: str, format: str = 'json') -> str:
        """
        Save validation report to a file.

        Args:
            report (Dict[str, Any]): Validation report
            filepath (str): Path to save the report
            format (str): Report format ('json' or 'txt')

        Returns:
            str: Path to saved report file
        """
        # Ensure directory exists
        os.makedirs(os.path.dirname(os.path.abspath(filepath)), exist_ok=True)

        if format.lower() == 'json':
            with open(filepath, 'w') as f:
                json.dump(report, f, indent=2, default=str)
            self.logger.info(f"Validation report saved as JSON: {filepath}")

        elif format.lower() == 'txt':
            # Generate plain text report
            summary = self.summarize([report] if isinstance(report, dict) else report)

            with open(filepath, 'w') as f:
                f.write(summary)
            self.logger.info(f"Validation report saved as text: {filepath}")

        else:
            raise ValueError(f"Unsupported report format: {format}")

        return filepath

    def generate_dashboard_data(self, validation_results: List[ValidationResult]) -> Dict[str, Any]:
        """
        Generate data suitable for dashboard visualization.

        Args:
            validation_results (List[ValidationResult]): List of validation results

        Returns:
            Dict[str, Any]: Dashboard-friendly data
        """
        if not validation_results:
            return {"error": "No validation results available"}

        # Overall metrics
        validators_run = len(validation_results)
        validators_passed = sum(1 for result in validation_results if result.get('valid', True))
        overall_valid = all(result.get('valid', False) for result in validation_results)

        dashboard_data = {
            "overall_status": "PASSED" if overall_valid else "FAILED",
            "validators": {
                "total": validators_run,
                "passed": validators_passed,
                "failed": validators_run - validators_passed,
                "pass_rate": validators_passed / validators_run if validators_run > 0 else 0
            },
            "issues": []
        }

        # Extract issues for each validator
        validation_issues = self._extract_validation_issues(validation_results)

        # Group issues by type
        issue_counts = {}
        for issue in validation_issues:
            issue_type = issue.get("issue_type", "other")
            if issue_type not in issue_counts:
                issue_counts[issue_type] = 0
            issue_counts[issue_type] += 1

        dashboard_data["issue_summary"] = {
            "total": len(validation_issues),
            "by_type": issue_counts
        }

        # Add top issues (limited to 10)
        dashboard_data["top_issues"] = validation_issues[:10]

        # Extract schema statistics if available
        schema_results = [r for r in validation_results if r.get('validator_type') == 'SchemaValidator']
        if schema_results:
            schema_result = schema_results[0]
            dashboard_data["schema_validation"] = {
                "missing_columns": len(schema_result.get('missing_columns', [])),
                "extra_columns": len(schema_result.get('extra_columns', [])),
                "error_counts": schema_result.get('error_counts', {})
            }

        # Extract outlier statistics if available
        outlier_results = [r for r in validation_results if r.get('validator_type') == 'OutlierValidator']
        if outlier_results:
            outlier_result = outlier_results[0]

            # Get top columns with outliers (up to 5)
            top_outlier_columns = []
            column_results = outlier_result.get('column_results', {})
            if column_results:
                # Sort columns by outlier count
                sorted_columns = sorted(
                    [(col, res.get('outlier_count', 0)) for col, res in column_results.items()],
                    key=lambda x: x[1],
                    reverse=True
                )

                # Take top 5
                top_outlier_columns = sorted_columns[:5]

            dashboard_data["outlier_validation"] = {
                "total_outliers": outlier_result.get('total_outliers', 0),
                "outlier_ratio": outlier_result.get('outlier_ratio', 0),
                "top_outlier_columns": [
                    {"column": col, "count": count} for col, count in top_outlier_columns
                ]
            }

        return dashboard_data