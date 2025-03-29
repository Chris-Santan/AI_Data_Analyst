# tests/unit/preprocessing/validation/test_data_validator.py
import unittest
import pandas as pd
import numpy as np
import os
import tempfile

from data_analytics_platform.preprocessing.validation.data_validator import DataValidator
from data_analytics_platform.preprocessing.validation.schema_validator import DataFrameSchema, ColumnSchema
from data_analytics_platform.preprocessing.validation.outlier_validator import OutlierMethod


class TestDataValidator(unittest.TestCase):
    """Tests for the DataValidator class."""

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

        # Create a validator
        self.validator = DataValidator(name="test_validator")

    def test_validate_schema(self):
        """Test schema validation."""
        # Validate schema
        results = self.validator.validate_schema(self.data, self.schema)

        # Check that validation failed (due to outlier in score column)
        self.assertFalse(results['valid'])

        # Fix the data to pass schema validation
        fixed_data = self.data.copy()
        fixed_data.loc[1, 'score'] = 95  # Within allowed range

        # Validate again
        results = self.validator.validate_schema(fixed_data, self.schema)

        # Check that validation passed
        self.assertTrue(results['valid'])

    def test_validate_outliers(self):
        """Test outlier validation."""
        # Validate outliers
        results = self.validator.validate_outliers(
            self.data,
            columns=['score'],
            method=OutlierMethod.ZSCORE,
            threshold=3.0
        )

        # Check that outliers were detected
        self.assertGreater(results['column_results']['score']['outlier_count'], 0)

    def test_build_pipeline(self):
        """Test building a validation pipeline."""
        # Build a pipeline
        self.validator.build_pipeline()

        # Add validators to the pipeline
        self.validator.add_schema_validation(schema=self.schema)
        self.validator.add_outlier_validation(
            columns=['score'],
            method=OutlierMethod.ZSCORE,
            threshold=3.0
        )

        # Run the pipeline
        results = self.validator.run_pipeline(self.data)

        # Check that we have results for both validators
        self.assertEqual(len(results), 2)

        # Check validator types
        self.assertEqual(results[0]['validator_type'], 'SchemaValidator')
        self.assertEqual(results[1]['validator_type'], 'OutlierValidator')

    def test_is_valid(self):
        """Test the is_valid method."""
        # Build a pipeline
        self.validator.build_pipeline()
        self.validator.add_schema_validation(schema=self.schema)

        # The data should fail validation
        self.assertFalse(self.validator.is_valid(self.data))

        # Fix the data to pass schema validation
        fixed_data = self.data.copy()
        fixed_data.loc[1, 'score'] = 95  # Within allowed range

        # This should now pass
        self.assertTrue(self.validator.is_valid(fixed_data))

    def test_generate_report(self):
        """Test generating a validation report."""
        # Build a pipeline
        self.validator.build_pipeline()
        self.validator.add_schema_validation(schema=self.schema)
        self.validator.add_outlier_validation(columns=['score'])

        # Run the pipeline
        self.validator.run_pipeline(self.data)

        # Generate a report
        report = self.validator.generate_report()

        # Check that the report has the expected sections
        self.assertIn('report_name', report)
        self.assertIn('timestamp', report)
        self.assertIn('valid', report)
        self.assertIn('validator_results', report)

        # Check that the overall validity is False
        self.assertFalse(report['valid'])

    def test_summarize_validation(self):
        """Test summarizing validation results."""
        # Build a pipeline
        self.validator.build_pipeline()
        self.validator.add_schema_validation(schema=self.schema)

        # Run the pipeline
        self.validator.run_pipeline(self.data)

        # Get summary
        summary = self.validator.summarize_validation()

        # Check that the summary is a non-empty string
        self.assertIsInstance(summary, str)
        self.assertGreater(len(summary), 0)

        # Check that the summary contains the word "FAILED"
        self.assertIn("FAILED", summary)

    def test_save_report(self):
        """Test saving a validation report to a file."""
        # Build a pipeline
        self.validator.build_pipeline()
        self.validator.add_schema_validation(schema=self.schema)

        # Run the pipeline
        self.validator.run_pipeline(self.data)

        # Create a temporary file
        with tempfile.NamedTemporaryFile(suffix='.json', delete=False) as temp_file:
            temp_path = temp_file.name

        try:
            # Save the report
            self.validator.save_report(temp_path)

            # Check that the file exists and is not empty
            self.assertTrue(os.path.exists(temp_path))
            self.assertGreater(os.path.getsize(temp_path), 0)

            # Load the file and check its content
            import json
            with open(temp_path, 'r') as f:
                loaded_report = json.load(f)

            # Check that the loaded report has the expected sections
            self.assertIn('report_name', loaded_report)
            self.assertIn('valid', loaded_report)
        finally:
            # Clean up
            if os.path.exists(temp_path):
                os.unlink(temp_path)

    def test_add_schema_validation_infer(self):
        """Test adding schema validation with schema inference."""
        # Build a pipeline
        self.validator.build_pipeline()

        # Add schema validation with inference
        self.validator.add_schema_validation(infer_from=self.data)

        # Run the pipeline
        results = self.validator.run_pipeline(self.data)

        # Check that validation passed (inferred schema should match the data)
        self.assertTrue(results[0]['valid'])

    def test_add_outlier_validation_auto_config(self):
        """Test adding outlier validation with auto-configuration."""
        # Build a pipeline
        self.validator.build_pipeline()

        # Add outlier validation with auto-configuration
        self.validator.add_outlier_validation(auto_config_from=self.data)

        # Run the pipeline
        results = self.validator.run_pipeline(self.data)

        # Check that validation was performed
        self.assertEqual(results[0]['validator_type'], 'OutlierValidator')

        # Check that outliers were detected in the score column
        self.assertIn('score', results[0]['column_results'])
        self.assertGreater(results[0]['column_results']['score']['outlier_count'], 0)

    def test_quick_validate(self):
        """Test the quick_validate class method."""
        # Run quick validation
        report = DataValidator.quick_validate(
            self.data,
            schema=self.schema,
            check_outliers=True
        )

        # Check that the report has the expected sections
        self.assertIn('report_name', report)
        self.assertIn('valid', report)
        self.assertIn('validator_results', report)

        # Check that validation failed
        self.assertFalse(report['valid'])


if __name__ == '__main__':
    unittest.main()