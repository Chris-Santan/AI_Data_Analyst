import unittest
import pandas as pd
import numpy as np

from data_analytics_platform.preprocessing.validation.outlier_validator import (
    OutlierValidator,
    OutlierMethod,
    OutlierConfig
)


class TestOutlierValidator(unittest.TestCase):
    """Tests for the OutlierValidator class."""

    def setUp(self):
        """Set up test data."""
        # Create a sample DataFrame with outliers
        np.random.seed(42)

        # Create normal values
        normal_values = np.random.normal(loc=100, scale=10, size=100)

        # Add some outliers
        outlier_values = np.array([150, 160, 40, 30])

        # Create a sample of normal values to use
        sample_normal = normal_values[:96]  # Take first 96 values to make room for outliers

        # Combine normal and outlier values to get 100 total values
        all_values = np.concatenate([sample_normal, outlier_values])

        # Create the DataFrame with equal-length arrays
        self.data = pd.DataFrame({
            'normal_col': normal_values,
            'outlier_col': all_values,
            'categorical_col': ['A'] * 50 + ['B'] * 50
        })

    def test_zscore_outlier_detection(self):
        """Test Z-score outlier detection."""
        # Create validator with Z-score method
        validator = OutlierValidator()
        validator.add_column(
            column='outlier_col',
            method=OutlierMethod.ZSCORE,
            threshold=3.0
        )

        # Validate data
        result = validator.validate(self.data)

        # Check that outliers were detected
        self.assertGreater(result['column_results']['outlier_col']['outlier_count'], 0)

        # Should detect at least the extreme outliers
        self.assertGreaterEqual(result['column_results']['outlier_col']['outlier_count'], 4)

    def test_iqr_outlier_detection(self):
        """Test IQR outlier detection."""
        # Create validator with IQR method
        validator = OutlierValidator()
        validator.add_column(
            column='outlier_col',
            method=OutlierMethod.IQR,
            threshold=1.5
        )

        # Validate data
        result = validator.validate(self.data)

        # Check that outliers were detected
        self.assertGreater(result['column_results']['outlier_col']['outlier_count'], 0)

        # Should detect at least the extreme outliers
        self.assertGreaterEqual(result['column_results']['outlier_col']['outlier_count'], 4)

    def test_auto_config(self):
        """Test auto-configuration of outlier validator."""
        # Create auto-configured validator
        validator = OutlierValidator.auto_config(
            self.data,
            sensitivity='medium'
        )

        # Validate data
        result = validator.validate(self.data)

        # Check that outliers were detected in the outlier column
        self.assertIn('outlier_col', result['column_results'])
        self.assertGreater(result['column_results']['outlier_col']['outlier_count'], 0)

    def test_is_valid(self):
        """Test the is_valid method."""
        # Create validator with a high threshold to make it pass
        validator = OutlierValidator()
        validator.add_column(
            column='outlier_col',
            method=OutlierMethod.ZSCORE,
            threshold=5.0,  # Very high threshold to pass validation
            params={'max_outlier_ratio': 0.1}  # Allow up to 10% outliers
        )

        # Validate data
        self.assertTrue(validator.is_valid(self.data))

        # Create validator with a low threshold to make it fail
        validator = OutlierValidator()
        validator.add_column(
            column='outlier_col',
            method=OutlierMethod.ZSCORE,
            threshold=1.0,  # Very low threshold
            params={'max_outlier_ratio': 0.01}  # Allow only 1% outliers
        )

        # Validate data
        self.assertFalse(validator.is_valid(self.data))

    def test_skip_non_numeric_columns(self):
        """Test that non-numeric columns are skipped."""
        # Create validator that includes a non-numeric column
        validator = OutlierValidator(columns=['outlier_col', 'categorical_col'])

        # Validate data
        result = validator.validate(self.data)

        # Check that the categorical column has a warning
        self.assertIn('categorical_col', result['column_results'])
        self.assertIn('warning', result['column_results']['categorical_col'])

    def test_validate_column(self):
        """Test validating a specific column."""
        validator = OutlierValidator()
        result = validator.validate_column(self.data, 'outlier_col')

        # Check that outliers were detected
        self.assertGreater(result['outlier_count'], 0)

    def test_different_outlier_methods(self):
        """Test different outlier detection methods."""
        methods = [
            OutlierMethod.ZSCORE,
            OutlierMethod.IQR,
            # Skip machine learning methods as they require scikit-learn
            # OutlierMethod.ISOLATION_FOREST,
            # OutlierMethod.LOF,
            # OutlierMethod.DBSCAN
        ]

        for method in methods:
            # Create validator with the method
            validator = OutlierValidator()
            validator.add_column(
                column='outlier_col',
                method=method
            )

            # Validate data
            result = validator.validate(self.data)

            # Check that outliers were detected
            self.assertGreater(result['column_results']['outlier_col']['outlier_count'], 0)

    def test_multiple_columns(self):
        """Test validating multiple columns."""
        # Create validator with multiple columns
        validator = OutlierValidator(columns=['normal_col', 'outlier_col'])

        # Validate data
        result = validator.validate(self.data)

        # Check that both columns were validated
        self.assertIn('normal_col', result['column_results'])
        self.assertIn('outlier_col', result['column_results'])

        # Outlier column should have more outliers
        self.assertGreater(
            result['column_results']['outlier_col']['outlier_count'],
            result['column_results']['normal_col']['outlier_count']
        )


if __name__ == "__main__":
    unittest.main()