# tests/unit/preprocessing/validation/test_schema_validator.py
import unittest
import pandas as pd
import numpy as np
from datetime import datetime

from data_analytics_platform.preprocessing.validation.schema_validator import (
    SchemaValidator,
    DataFrameSchema,
    ColumnSchema
)


class TestSchemaValidator(unittest.TestCase):
    """Tests for the SchemaValidator class."""

    def setUp(self):
        """Set up test data."""
        # Create a sample DataFrame
        self.data = pd.DataFrame({
            'id': [1, 2, 3, 4, 5],
            'name': ['Alice', 'Bob', 'Charlie', 'David', 'Eve'],
            'age': [25, 30, 35, 40, 45],
            'registered': [True, False, True, True, False],
            'score': [95.5, 80.3, None, 75.0, 90.2],
            'date': pd.to_datetime(['2020-01-01', '2020-02-01', '2020-03-01', '2020-04-01', '2020-05-01'])
        })

        # Create a schema for the DataFrame
        self.schema = DataFrameSchema(
            columns=[
                ColumnSchema(name='id', dtype=int, nullable=False, unique=True),
                ColumnSchema(name='name', dtype=str, nullable=False, unique=True),
                ColumnSchema(name='age', dtype=int, nullable=False, min_value=18, max_value=100),
                ColumnSchema(name='registered', dtype=bool, nullable=False),
                ColumnSchema(name='score', dtype=float, nullable=True, min_value=0, max_value=100),
                ColumnSchema(name='date', dtype=datetime, nullable=False)
            ],
            require_all_columns=True,
            allow_extra_columns=False
        )

        # Create a schema validator
        self.validator = SchemaValidator(self.schema)

    def test_valid_dataframe(self):
        """Test validation of a valid DataFrame."""
        result = self.validator.validate(self.data)
        self.assertTrue(result['valid'])

    def test_is_valid_true(self):
        """Test the is_valid method for a valid DataFrame."""
        self.assertTrue(self.validator.is_valid(self.data))

    def test_missing_column(self):
        """Test validation when a column is missing."""
        # Remove a column
        data_missing_column = self.data.drop('registered', axis=1)

        result = self.validator.validate(data_missing_column)
        self.assertFalse(result['valid'])
        self.assertIn('registered', result['missing_columns'])

    def test_extra_column(self):
        """Test validation when an extra column is present."""
        # Add an extra column
        data_extra_column = self.data.copy()
        data_extra_column['extra'] = ['a', 'b', 'c', 'd', 'e']

        result = self.validator.validate(data_extra_column)
        self.assertFalse(result['valid'])
        self.assertIn('extra', result['extra_columns'])

    def test_null_value_in_non_nullable_column(self):
        """Test validation when a non-nullable column contains null values."""
        # Add a null value to a non-nullable column
        data_with_null = self.data.copy()
        data_with_null.loc[2, 'name'] = None

        result = self.validator.validate(data_with_null)
        self.assertFalse(result['valid'])
        self.assertFalse(result['column_results']['name']['valid'])

    def test_value_below_min(self):
        """Test validation when a value is below the minimum."""
        # Add a value below the minimum
        data_below_min = self.data.copy()
        data_below_min.loc[2, 'age'] = 10  # Below min_value=18

        result = self.validator.validate(data_below_min)
        self.assertFalse(result['valid'])
        self.assertFalse(result['column_results']['age']['valid'])

    def test_value_above_max(self):
        """Test validation when a value is above the maximum."""
        # Add a value above the maximum
        data_above_max = self.data.copy()
        data_above_max.loc[2, 'age'] = 110  # Above max_value=100

        result = self.validator.validate(data_above_max)
        self.assertFalse(result['valid'])
        self.assertFalse(result['column_results']['age']['valid'])

    def test_non_unique_values_in_unique_column(self):
        """Test validation when a unique column contains duplicate values."""
        # Add a duplicate value to a unique column
        data_non_unique = self.data.copy()
        data_non_unique.loc[4, 'name'] = 'Alice'  # Duplicate value

        result = self.validator.validate(data_non_unique)
        self.assertFalse(result['valid'])
        self.assertFalse(result['column_results']['name']['valid'])

    def test_wrong_data_type(self):
        """Test validation when a column has the wrong data type."""
        # Change the data type of a column
        data_wrong_type = self.data.copy()
        data_wrong_type['age'] = data_wrong_type['age'].astype(str)

        result = self.validator.validate(data_wrong_type)
        self.assertFalse(result['valid'])
        self.assertFalse(result['column_results']['age']['valid'])

    def test_infer_schema(self):
        """Test inferring a schema from a DataFrame."""
        # Infer schema from the DataFrame
        inferred_schema = DataFrameSchema.infer_from_dataframe(self.data)

        # Print inferred schema details for debugging
        print("\nInferred Schema Details:")
        for col_name, col_schema in inferred_schema.columns.items():
            print(f"Column: {col_name}, Type: {col_schema.dtype}, Nullable: {col_schema.nullable}")

        # Create a validator with the inferred schema
        inferred_validator = SchemaValidator(inferred_schema)

        # Validate the original DataFrame
        result = inferred_validator.validate(self.data)

        # Print validation results for debugging
        print("\nValidation Results:")
        print(f"Valid: {result['valid']}")
        if not result['valid']:
            for col_name, col_result in result.get('column_results', {}).items():
                if not col_result.get('valid', True):
                    print(f"Column {col_name} validation failed:")
                    for error in col_result.get('errors', []):
                        print(f"  - {error}")

        # This should pass since we're validating against a schema inferred from the same data
        self.assertTrue(result['valid'])

    def test_validate_schema(self):
        """Test the validate_schema method."""
        result = self.validator.validate_schema(self.data)
        self.assertTrue(result['valid'])

    def test_from_dict(self):
        """Test creating a validator from a dictionary."""
        schema_dict = {
            'columns': {
                'id': {
                    'dtype': 'int',
                    'nullable': False,
                    'unique': True
                },
                'name': {
                    'dtype': 'str',
                    'nullable': False,
                    'unique': True
                }
            },
            'require_all_columns': False,
            'allow_extra_columns': True
        }

        # Create validator from dictionary
        dict_validator = SchemaValidator.from_dict(schema_dict)

        # Validate with a subset of columns
        subset_data = pd.DataFrame({
            'id': [1, 2, 3],
            'name': ['Alice', 'Bob', 'Charlie']
        })

        result = dict_validator.validate(subset_data)
        self.assertTrue(result['valid'])


if __name__ == '__main__':
    unittest.main()