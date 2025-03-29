# src/data_analytics_platform/core/exceptions/validation_exceptions.py
from typing import Dict, Any, List, Optional
from data_analytics_platform.core.exceptions.custom_exceptions import DataAnalyticsPlatformError


class ValidationError(DataAnalyticsPlatformError):
    """Raised when data fails validation."""

    def __init__(self, message: str, validation_results: Optional[Dict[str, Any]] = None):
        """
        Initialize validation error.

        Args:
            message (str): Error message
            validation_results (Optional[Dict[str, Any]]): Validation results that caused the error
        """
        self.validation_results = validation_results
        super().__init__(message)

    def get_user_message(self) -> str:
        """Get a user-friendly message."""
        if self.validation_results and not self.validation_results.get('valid', True):
            if 'schema_validation' in self.validation_results:
                schema_errors = self._extract_schema_errors()
                if schema_errors:
                    return f"Data validation failed: {schema_errors}"

            if 'outlier_validation' in self.validation_results:
                outlier_errors = self._extract_outlier_errors()
                if outlier_errors:
                    return f"Data validation failed: {outlier_errors}"

        return f"Data validation failed: {self.message}"

    def get_recovery_suggestions(self) -> List[str]:
        """Get suggestions for recovering from validation errors."""
        suggestions = ["Check the data quality and ensure it meets the validation criteria."]

        if self.validation_results:
            if not self.validation_results.get('valid', True):
                # Schema validation suggestions
                if 'missing_columns' in self.validation_results and self.validation_results['missing_columns']:
                    suggestions.append(
                        f"Add missing columns: {', '.join(self.validation_results['missing_columns'])}"
                    )

                if 'extra_columns' in self.validation_results and self.validation_results['extra_columns']:
                    suggestions.append(
                        f"Remove unexpected columns: {', '.join(self.validation_results['extra_columns'])}"
                    )

                # Extract column-specific errors
                column_errors = {}
                for col_name, col_result in self.validation_results.get('column_results', {}).items():
                    if not col_result.get('valid', True) and 'errors' in col_result:
                        column_errors[col_name] = col_result['errors']

                if column_errors:
                    for col, errors in column_errors.items():
                        for error in errors:
                            if 'type' in error.lower():
                                suggestions.append(f"Fix data type issue in column '{col}'")
                            elif 'null' in error.lower():
                                suggestions.append(f"Handle missing values in column '{col}'")
                            elif 'unique' in error.lower():
                                suggestions.append(f"Fix duplicate values in column '{col}'")
                            elif 'minimum' in error.lower() or 'maximum' in error.lower():
                                suggestions.append(f"Check value ranges in column '{col}'")

                # Outlier suggestions
                if 'outlier_validation' in self.validation_results:
                    outlier_data = self.validation_results['outlier_validation']
                    if outlier_data.get('total_outliers', 0) > 0:
                        suggestions.append("Consider handling outliers before analysis")

                        # Get top outlier columns
                        columns_with_outliers = outlier_data.get('columns_with_outliers', {})
                        if columns_with_outliers:
                            top_columns = sorted(
                                [(col, info['outlier_count']) for col, info in columns_with_outliers.items()],
                                key=lambda x: x[1],
                                reverse=True
                            )[:3]  # Top 3 columns

                            columns_str = ", ".join([f"'{col}'" for col, _ in top_columns])
                            suggestions.append(f"Check for outliers in columns: {columns_str}")

        return suggestions

    def get_error_summary(self) -> Dict[str, Any]:
        """
        Get a summary of validation errors.

        Returns:
            Dict[str, Any]: Error summary
        """
        if not self.validation_results:
            return {"message": self.message}

        summary = {
            "message": self.message,
            "valid": self.validation_results.get('valid', False)
        }

        # Add validation issues
        if 'validation_issues' in self.validation_results:
            summary['issues'] = self.validation_results['validation_issues']

        return summary

    def _extract_schema_errors(self) -> str:
        """
        Extract schema validation errors as a string.

        Returns:
            str: Schema errors
        """
        errors = []

        # Missing columns
        if 'missing_columns' in self.validation_results and self.validation_results['missing_columns']:
            missing_cols = self.validation_results['missing_columns']
            if len(missing_cols) == 1:
                errors.append(f"Missing required column '{missing_cols[0]}'")
            else:
                errors.append(f"Missing {len(missing_cols)} required columns")

        # Extra columns
        if 'extra_columns' in self.validation_results and self.validation_results['extra_columns']:
            extra_cols = self.validation_results['extra_columns']
            if len(extra_cols) == 1:
                errors.append(f"Unexpected column '{extra_cols[0]}'")
            else:
                errors.append(f"Found {len(extra_cols)} unexpected columns")

        # Column errors
        column_error_count = 0
        for col_name, col_result in self.validation_results.get('column_results', {}).items():
            if not col_result.get('valid', True) and 'errors' in col_result:
                column_error_count += len(col_result['errors'])

        if column_error_count > 0:
            errors.append(f"Found {column_error_count} column validation errors")

        if not errors:
            return "Schema validation failed"

        return "; ".join(errors)

    def _extract_outlier_errors(self) -> str:
        """
        Extract outlier validation errors as a string.

        Returns:
            str: Outlier errors
        """
        if 'outlier_validation' not in self.validation_results:
            return ""

        outlier_data = self.validation_results['outlier_validation']
        total_outliers = outlier_data.get('total_outliers', 0)

        if total_outliers == 0:
            return ""

        outlier_ratio = outlier_data.get('outlier_ratio', 0)
        return f"Found {total_outliers} outliers ({outlier_ratio:.2%} of data)"


class SchemaValidationError(ValidationError):
    """Raised when data fails schema validation."""

    def __init__(self, message: str, validation_results: Optional[Dict[str, Any]] = None):
        """
        Initialize schema validation error.

        Args:
            message (str): Error message
            validation_results (Optional[Dict[str, Any]]): Validation results that caused the error
        """
        super().__init__(f"Schema validation error: {message}", validation_results)


class OutlierValidationError(ValidationError):
    """Raised when data contains too many outliers."""

    def __init__(self, message: str, validation_results: Optional[Dict[str, Any]] = None):
        """
        Initialize outlier validation error.

        Args:
            message (str): Error message
            validation_results (Optional[Dict[str, Any]]): Validation results that caused the error
        """
        super().__init__(f"Outlier validation error: {message}", validation_results)