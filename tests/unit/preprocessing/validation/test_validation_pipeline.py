# tests/unit/preprocessing/validation/test_validation_pipeline.py
import unittest
import pandas as pd
import numpy as np
import os
import tempfile

from data_analytics_platform.preprocessing.validation.schema_validator import (
    SchemaValidator,
    DataFrameSchema,
    ColumnSchema
)
from data_analytics_platform.preprocessing.validation.outlier_validator import (
    OutlierValidator,
    OutlierMethod
)
from data_analytics_platform.preprocessing.validation.validation_pipeline import ValidationPipeline


class TestValidationPipeline(unittest.TestCase):
    """Tests for the ValidationPipeline class."""

    def setUp(self):
        """Set up test data."""
        # Create a sample DataFrame
        np.random.seed(42)
        self.data = pd.DataFrame({
            'id': range(1, 101),
            'name': [f'Person_{i}' for i in range(1, 101)],
            'age': np.random.randint(20, 60, 100),
            'score': np.random.normal(loc=75, scale=15, size=100)
        })

        # Add some outliers to the score column
        self.data.loc[0, 'score'] = 0
        self.data.loc[1, 'score'] = 150

        # Create a schema for the DataFrame
        self.schema = DataFrameSchema(
            columns=[
                ColumnSchema(name='id', dtype=int, nullable=False, unique=True),
                ColumnSchema(name='name', dtype=str, nullable=False),
                ColumnSchema(name='age', dtype=int, nullable=False, min_value=18, max_value=100),
                ColumnSchema(name='score', dtype=float, nullable=True, min_value=0, max_value=100)
            ]
        )

        # Create validators
        self.schema_validator = SchemaValidator(self.schema)
        self.outlier_validator = OutlierValidator(columns=['score'])

        # Create a pipeline
        self.pipeline = ValidationPipeline(name="test_pipeline")
        self.pipeline.add_validator(self.schema_validator, "Schema Validation")
        self.pipeline.add_validator(self.outlier_validator, "Outlier Validation")

    def test_validate(self):
        """Test running the validation pipeline."""
        results = self.pipeline.validate(self.data)

        # Check that we have results for both validators
        self.assertEqual(len(results), 2)

        # Check validator types
        self.assertEqual(results[0]['validator_type'], 'SchemaValidator')
        self.assertEqual(results[1]['validator_type'], 'OutlierValidator')

        # Check that schema validation detected the problem with the score column
        self.assertFalse(results[0]['valid'])

        # Check that outlier validation detected outliers
        self.assertGreater(results[1]['column_results']['score']['outlier_count'], 0)

    def test_is_valid(self):
        """Test the is_valid method."""
        # The data should fail validation due to schema issues
        self.assertFalse(self.pipeline.is_valid(self.data))

        # Fix the data to pass schema validation
        fixed_data = self.data.copy()
        fixed_data.loc[1, 'score'] = 95  # Within allowed range

        # Create a new pipeline with higher outlier threshold
        pipeline = ValidationPipeline(name="test_pipeline")
        pipeline.add_validator(self.schema_validator, "Schema Validation")

        # This should now pass
        self.assertTrue(pipeline.is_valid(fixed_data))

    def test_fail_fast(self):
        """Test the fail_fast option."""
        # Create a pipeline with fail_fast=True
        fail_fast_pipeline = ValidationPipeline(name="fail_fast_pipeline", fail_fast=True)
        fail_fast_pipeline.add_validator(self.schema_validator, "Schema Validation")
        fail_fast_pipeline.add_validator(self.outlier_validator, "Outlier Validation")

        # Run validation - it should stop after the first validator fails
        results = fail_fast_pipeline.validate(self.data)

        # Should only have results for the first validator
        self.assertEqual(len(results), 1)
        self.assertEqual(results[0]['validator_type'], 'SchemaValidator')

    def test_generate_report(self):
        """Test generating a validation report."""
        # Run the pipeline
        results = self.pipeline.validate(self.data)

        # Generate a report
        report = self.pipeline.generate_report(self.data)

        # Check that the report has the expected sections
        self.assertIn('pipeline_name', report)
        self.assertIn('timestamp', report)
        self.assertIn('overall_valid', report)
        self.assertIn('results', report)
        self.assertIn('summary', report)

        # Check that the overall validity is False
        self.assertFalse(report['overall_valid'])

    def test_save_report(self):
        """Test saving a validation report to a file."""
        # Create a temporary file
        with tempfile.NamedTemporaryFile(suffix='.json', delete=False) as temp_file:
            temp_path = temp_file.name

        try:
            # Run the pipeline and save the report
            report = self.pipeline.generate_report(self.data)
            self.pipeline.save_report(report, temp_path)

            # Check that the file exists and is not empty
            self.assertTrue(os.path.exists(temp_path))
            self.assertGreater(os.path.getsize(temp_path), 0)

            # Load the file and check its content
            import json
            with open(temp_path, 'r') as f:
                loaded_report = json.load(f)

            # Check that the loaded report has the expected sections
            self.assertIn('pipeline_name', loaded_report)
            self.assertIn('overall_valid', loaded_report)
        finally:
            # Clean up
            if os.path.exists(temp_path):
                os.unlink(temp_path)

    def test_default_pipeline(self):
        """Test creating a default validation pipeline."""
        # Create a default pipeline from sample data
        default_pipeline = ValidationPipeline.default_pipeline(self.data)

        # Should have validators
        self.assertGreater(len(default_pipeline.validators), 0)

        # Run the pipeline
        results = default_pipeline.validate(self.data)

        # Check that we have results
        self.assertTrue(len(results) > 0)


if __name__ == '__main__':
    unittest.main()