from typing import Dict, Any, List, Optional, Union, Set, Callable
import pandas as pd
import numpy as np
from datetime import datetime

from data_analytics_platform.core.interfaces.validation_interface import DataFrameValidationInterface, ValidationResult
from data_analytics_platform.core.exceptions.validation_exceptions import ValidationError
from data_analytics_platform.core.exceptions.custom_exceptions import DataAnalyticsPlatformError


class ColumnSchema:
    """Schema definition for a DataFrame column."""

    def __init__(
            self,
            name: str,
            dtype: Optional[type] = None,
            nullable: bool = True,
            unique: bool = False,
            min_value: Optional[Any] = None,
            max_value: Optional[Any] = None,
            allowed_values: Optional[Set[Any]] = None,
            validation_fn: Optional[Callable[[pd.Series], bool]] = None,
            regex_pattern: Optional[str] = None
    ):
        """
        Initialize column schema.

        Args:
            name (str): Column name
            dtype (Optional[type]): Expected data type
            nullable (bool): Whether null values are allowed
            unique (bool): Whether values must be unique
            min_value (Optional[Any]): Minimum allowed value
            max_value (Optional[Any]): Maximum allowed value
            allowed_values (Optional[Set[Any]]): Set of allowed values
            validation_fn (Optional[Callable[[pd.Series], bool]]): Custom validation function
            regex_pattern (Optional[str]): Regex pattern for string validation
        """
        self.name = name
        self.dtype = dtype
        self.nullable = nullable
        self.unique = unique
        self.min_value = min_value
        self.max_value = max_value
        self.allowed_values = allowed_values
        self.validation_fn = validation_fn
        self.regex_pattern = regex_pattern

    def to_dict(self) -> Dict[str, Any]:
        """
        Convert schema to dictionary.

        Returns:
            Dict[str, Any]: Dictionary representation of schema
        """
        return {
            'name': self.name,
            'dtype': str(self.dtype) if self.dtype else None,
            'nullable': self.nullable,
            'unique': self.unique,
            'min_value': self.min_value,
            'max_value': self.max_value,
            'allowed_values': list(self.allowed_values) if self.allowed_values else None,
            'has_validation_fn': self.validation_fn is not None,
            'regex_pattern': self.regex_pattern
        }


class DataFrameSchema:
    """Schema definition for a DataFrame."""

    def __init__(
            self,
            columns: List[ColumnSchema],
            require_all_columns: bool = True,
            allow_extra_columns: bool = False,
            row_validation_fn: Optional[Callable[[pd.DataFrame], pd.Series]] = None
    ):
        """
        Initialize DataFrame schema.

        Args:
            columns (List[ColumnSchema]): List of column schemas
            require_all_columns (bool): Whether all defined columns are required
            allow_extra_columns (bool): Whether extra columns are allowed
            row_validation_fn (Optional[Callable[[pd.DataFrame], pd.Series]]):
                Function that validates rows and returns boolean Series
        """
        self.columns = {col.name: col for col in columns}
        self.require_all_columns = require_all_columns
        self.allow_extra_columns = allow_extra_columns
        self.row_validation_fn = row_validation_fn

    def to_dict(self) -> Dict[str, Any]:
        """
        Convert schema to dictionary.

        Returns:
            Dict[str, Any]: Dictionary representation of schema
        """
        return {
            'columns': {name: col.to_dict() for name, col in self.columns.items()},
            'require_all_columns': self.require_all_columns,
            'allow_extra_columns': self.allow_extra_columns,
            'has_row_validation_fn': self.row_validation_fn is not None
        }

    @classmethod
    def infer_from_dataframe(
            cls,
            df: pd.DataFrame,
            sample_size: Optional[int] = None,
            nullable_threshold: float = 0.05,
            detect_unique: bool = True
    ) -> 'DataFrameSchema':
        """
        Infer schema from DataFrame.

        Args:
            df (pd.DataFrame): DataFrame to infer schema from
            sample_size (Optional[int]): Number of rows to sample
            nullable_threshold (float): Threshold for null values (0.0 to 1.0)
            detect_unique (bool): Whether to detect unique columns

        Returns:
            DataFrameSchema: Inferred schema
        """
        # Sample the dataframe if needed
        if sample_size and len(df) > sample_size:
            sample_df = df.sample(sample_size, random_state=42)
        else:
            sample_df = df

        columns = []

        for col_name in df.columns:
            series = sample_df[col_name]

            # Determine if column has null values
            null_ratio = series.isna().mean()
            nullable = null_ratio > 0

            # Determine if column has unique values
            unique = False
            if detect_unique:
                non_null_values = series.dropna()
                if len(non_null_values) > 0:
                    unique = non_null_values.is_unique

            # Get min and max values for numeric columns
            min_value = None
            max_value = None
            if pd.api.types.is_numeric_dtype(series):
                non_null = series.dropna()
                if len(non_null) > 0:
                    min_value = non_null.min()
                    max_value = non_null.max()

            # Determine appropriate Python type based on pandas dtype
            dtype = None
            if pd.api.types.is_integer_dtype(series):
                dtype = int
            elif pd.api.types.is_float_dtype(series):
                dtype = float
            elif pd.api.types.is_bool_dtype(series):
                dtype = bool
            elif pd.api.types.is_datetime64_dtype(series):
                dtype = datetime
            elif pd.api.types.is_string_dtype(series) or pd.api.types.is_object_dtype(series):
                dtype = str

            # Create column schema
            column_schema = ColumnSchema(
                name=col_name,
                dtype=dtype,  # Use Python type instead of pandas dtype
                nullable=nullable,
                unique=unique,
                min_value=min_value,
                max_value=max_value
            )

            columns.append(column_schema)

        return cls(columns=columns)


class SchemaValidator(DataFrameValidationInterface):
    """Validates DataFrames against a schema."""

    def __init__(self, schema: DataFrameSchema):
        """
        Initialize schema validator.

        Args:
            schema (DataFrameSchema): Schema to validate against
        """
        self.schema = schema

    def validate(self, data: pd.DataFrame) -> ValidationResult:
        """
        Validate DataFrame against schema.

        Args:
            data (pd.DataFrame): DataFrame to validate

        Returns:
            ValidationResult: Validation results
        """
        if not isinstance(data, pd.DataFrame):
            return {
                'valid': False,
                'error': 'Input is not a pandas DataFrame'
            }

        results = {
            'valid': True,
            'schema_name': getattr(self.schema, 'name', 'unnamed_schema'),
            'column_results': {},
            'extra_columns': [],
            'missing_columns': [],
            'row_validation': None,
            'error_counts': {
                'type_errors': 0,
                'null_errors': 0,
                'unique_errors': 0,
                'range_errors': 0,
                'allowed_value_errors': 0,
                'regex_errors': 0,
                'custom_validation_errors': 0
            }
        }

        # Check for extra columns
        extra_columns = set(data.columns) - set(self.schema.columns.keys())
        if extra_columns and not self.schema.allow_extra_columns:
            results['valid'] = False
            results['extra_columns'] = list(extra_columns)

        # Check for missing columns
        missing_columns = set(self.schema.columns.keys()) - set(data.columns)
        if missing_columns and self.schema.require_all_columns:
            results['valid'] = False
            results['missing_columns'] = list(missing_columns)

        # Validate each column against its schema
        for col_name, col_schema in self.schema.columns.items():
            if col_name in data.columns:
                col_result = self.validate_column(data, col_name)
                results['column_results'][col_name] = col_result

                # Update error counts
                for error_type, count in col_result.get('error_counts', {}).items():
                    if error_type in results['error_counts']:
                        results['error_counts'][error_type] += count

                # Update overall validity
                if not col_result.get('valid', True):
                    results['valid'] = False

        # Validate rows if a row validation function is provided
        if self.schema.row_validation_fn:
            try:
                row_valid = self.schema.row_validation_fn(data)
                if not isinstance(row_valid, pd.Series) or row_valid.dtype != bool:
                    row_validation = {
                        'valid': False,
                        'error': 'Row validation function must return boolean Series'
                    }
                else:
                    invalid_rows = (~row_valid).sum()
                    row_validation = {
                        'valid': invalid_rows == 0,
                        'invalid_row_count': int(invalid_rows),
                        'invalid_row_indices': list(data[~row_valid].index) if invalid_rows > 0 else []
                    }

                    if invalid_rows > 0:
                        results['valid'] = False
            except Exception as e:
                row_validation = {
                    'valid': False,
                    'error': f'Row validation function error: {str(e)}'
                }
                results['valid'] = False

            results['row_validation'] = row_validation

        return results

    def validate_column(self, data: pd.DataFrame, column: str) -> ValidationResult:
        """
        Validate a specific column in the DataFrame.

        Args:
            data (pd.DataFrame): DataFrame to validate
            column (str): Column name to validate

        Returns:
            ValidationResult: Validation results for the column
        """
        if column not in self.schema.columns:
            return {
                'valid': not self.schema.require_all_columns,
                'error': 'Column not in schema' if self.schema.require_all_columns else None
            }

        if column not in data.columns:
            return {
                'valid': False,
                'error': 'Column not in data'
            }

        col_schema = self.schema.columns[column]
        series = data[column]

        result = {
            'valid': True,
            'name': column,
            'error_counts': {
                'type_errors': 0,
                'null_errors': 0,
                'unique_errors': 0,
                'range_errors': 0,
                'allowed_value_errors': 0,
                'regex_errors': 0,
                'custom_validation_errors': 0
            },
            'errors': []
        }

        # Check data type
        if col_schema.dtype is not None:
            expected_type = col_schema.dtype
            type_check = False

            # Handle different type checking scenarios
            try:
                # Handle special cases for pandas/numpy types
                if expected_type == int:
                    type_check = pd.api.types.is_integer_dtype(series)
                elif expected_type == float:
                    type_check = pd.api.types.is_float_dtype(series)
                elif expected_type == str:
                    # This is key: check both string and object dtypes for string values
                    type_check = pd.api.types.is_string_dtype(series) or pd.api.types.is_object_dtype(series)
                elif expected_type == bool:
                    type_check = pd.api.types.is_bool_dtype(series)
                elif expected_type == datetime:
                    type_check = pd.api.types.is_datetime64_dtype(series)
                elif isinstance(expected_type, type):
                    # For standard Python types that can be used with isinstance
                    try:
                        type_check = all(isinstance(x, expected_type) for x in series.dropna())
                    except (TypeError, ValueError):
                        type_check = False
                else:
                    # For pandas/numpy dtypes, compare string representations
                    current_dtype_str = str(series.dtype)
                    expected_dtype_str = str(expected_type)
                    type_check = current_dtype_str == expected_dtype_str
            except (TypeError, ValueError):
                # If any errors occur during type checking, consider it a failure
                type_check = False

            if not type_check:
                result['valid'] = False

                # Get type name safely
                if hasattr(expected_type, '__name__'):
                    expected_type_name = expected_type.__name__
                else:
                    expected_type_name = str(expected_type)

                error = f"Expected type {expected_type_name}, got {str(series.dtype)}"
                result['errors'].append(error)
                result['error_counts']['type_errors'] += 1

        # Check for null values
        has_nulls = series.isna().any()
        if has_nulls and not col_schema.nullable:
            result['valid'] = False
            null_count = series.isna().sum()
            error = f"Column contains {null_count} null values but is not nullable"
            result['errors'].append(error)
            result['error_counts']['null_errors'] += 1

        # Check uniqueness
        if col_schema.unique:
            has_duplicates = series.duplicated().any()
            if has_duplicates:
                result['valid'] = False
                dup_count = series.duplicated().sum()
                error = f"Column contains {dup_count} duplicate values but should be unique"
                result['errors'].append(error)
                result['error_counts']['unique_errors'] += 1

        # Check min/max values for numeric data
        if pd.api.types.is_numeric_dtype(series):
            non_null = series.dropna()

            if col_schema.min_value is not None and len(non_null) > 0:
                below_min = (non_null < col_schema.min_value).sum()
                if below_min > 0:
                    result['valid'] = False
                    error = f"{below_min} values below minimum ({col_schema.min_value})"
                    result['errors'].append(error)
                    result['error_counts']['range_errors'] += 1

            if col_schema.max_value is not None and len(non_null) > 0:
                above_max = (non_null > col_schema.max_value).sum()
                if above_max > 0:
                    result['valid'] = False
                    error = f"{above_max} values above maximum ({col_schema.max_value})"
                    result['errors'].append(error)
                    result['error_counts']['range_errors'] += 1

        # Check allowed values
        if col_schema.allowed_values is not None:
            non_null = series.dropna()
            if len(non_null) > 0:
                invalid_values = ~non_null.isin(col_schema.allowed_values)
                invalid_count = invalid_values.sum()
                if invalid_count > 0:
                    result['valid'] = False
                    error = f"{invalid_count} values not in allowed set"
                    result['errors'].append(error)
                    result['error_counts']['allowed_value_errors'] += 1

        # Check regex pattern for string data
        if col_schema.regex_pattern is not None and pd.api.types.is_string_dtype(series):
            non_null = series.dropna()
            if len(non_null) > 0:
                import re
                pattern = re.compile(col_schema.regex_pattern)
                # Apply pattern match to each value
                matches = non_null.map(lambda x: bool(pattern.match(x)))
                invalid_count = (~matches).sum()
                if invalid_count > 0:
                    result['valid'] = False
                    error = f"{invalid_count} values don't match regex pattern"
                    result['errors'].append(error)
                    result['error_counts']['regex_errors'] += 1

        # Apply custom validation function
        if col_schema.validation_fn is not None:
            try:
                is_valid = col_schema.validation_fn(series)
                if not is_valid:
                    result['valid'] = False
                    error = "Failed custom validation function"
                    result['errors'].append(error)
                    result['error_counts']['custom_validation_errors'] += 1
            except Exception as e:
                result['valid'] = False
                error = f"Custom validation error: {str(e)}"
                result['errors'].append(error)
                result['error_counts']['custom_validation_errors'] += 1

        return result

    def validate_schema(self, data: pd.DataFrame) -> ValidationResult:
        """
        Validate DataFrame against schema definition.

        Args:
            data (pd.DataFrame): DataFrame to validate

        Returns:
            ValidationResult: Schema validation results
        """
        return self.validate(data)

    def is_valid(self, data: pd.DataFrame) -> bool:
        """
        Check if data is valid according to schema.

        Args:
            data (pd.DataFrame): Data to check

        Returns:
            bool: True if data is valid, False otherwise
        """
        return self.validate(data)['valid']

    @classmethod
    def from_dict(cls, schema_dict: Dict[str, Any]) -> 'SchemaValidator':
        """
        Create a SchemaValidator from a dictionary.

        Args:
            schema_dict (Dict[str, Any]): Schema dictionary

        Returns:
            SchemaValidator: Schema validator instance
        """
        columns = []

        for col_name, col_def in schema_dict.get('columns', {}).items():
            # Convert string dtype representation to actual type
            dtype_str = col_def.get('dtype')
            dtype = None
            if dtype_str:
                if 'int' in dtype_str:
                    dtype = int
                elif 'float' in dtype_str:
                    dtype = float
                elif 'bool' in dtype_str:
                    dtype = bool
                elif 'str' in dtype_str or 'object' in dtype_str:
                    dtype = str
                elif 'datetime' in dtype_str:
                    dtype = datetime

            column = ColumnSchema(
                name=col_name,
                dtype=dtype,
                nullable=col_def.get('nullable', True),
                unique=col_def.get('unique', False),
                min_value=col_def.get('min_value'),
                max_value=col_def.get('max_value'),
                allowed_values=set(col_def.get('allowed_values', [])) if col_def.get('allowed_values') else None,
                regex_pattern=col_def.get('regex_pattern')
            )

            columns.append(column)

        schema = DataFrameSchema(
            columns=columns,
            require_all_columns=schema_dict.get('require_all_columns', True),
            allow_extra_columns=schema_dict.get('allow_extra_columns', False)
        )

        return cls(schema=schema)