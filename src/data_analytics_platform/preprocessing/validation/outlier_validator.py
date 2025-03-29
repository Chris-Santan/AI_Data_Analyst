from typing import Dict, Any, List, Optional, Union, Set, Callable
import pandas as pd
import numpy as np
from scipy import stats

from data_analytics_platform.core.interfaces.validation_interface import DataFrameValidationInterface, ValidationResult
from data_analytics_platform.core.exceptions.validation_exceptions import ValidationError
from data_analytics_platform.core.exceptions.custom_exceptions import DataAnalyticsPlatformError


class OutlierMethod:
    """Available methods for outlier detection."""
    ZSCORE = "zscore"
    IQR = "iqr"
    ISOLATION_FOREST = "isolation_forest"
    LOF = "local_outlier_factor"
    DBSCAN = "dbscan"


class OutlierConfig:
    """Configuration for outlier detection."""

    def __init__(
            self,
            column: str,
            method: str = OutlierMethod.ZSCORE,
            threshold: float = 3.0,
            params: Optional[Dict[str, Any]] = None
    ):
        """
        Initialize outlier detection configuration.

        Args:
            column (str): Column name to check for outliers
            method (str): Outlier detection method
            threshold (float): Threshold for outlier detection
            params (Optional[Dict[str, Any]]): Additional parameters for the method
        """
        self.column = column
        self.method = method
        self.threshold = threshold
        self.params = params or {}

    def to_dict(self) -> Dict[str, Any]:
        """
        Convert config to dictionary.

        Returns:
            Dict[str, Any]: Dictionary representation of config
        """
        return {
            'column': self.column,
            'method': self.method,
            'threshold': self.threshold,
            'params': self.params
        }


class OutlierValidator(DataFrameValidationInterface):
    """Validates DataFrames for outliers."""

    def __init__(
            self,
            configs: List[OutlierConfig] = None,
            columns: List[str] = None,
            method: str = OutlierMethod.ZSCORE,
            threshold: float = 3.0,
            params: Optional[Dict[str, Any]] = None
    ):
        """
        Initialize outlier validator.

        Args:
            configs (List[OutlierConfig]): List of outlier configurations
            columns (List[str]): Columns to check (if configs not provided)
            method (str): Default outlier detection method
            threshold (float): Default threshold for outlier detection
            params (Optional[Dict[str, Any]]): Default additional parameters
        """
        if configs is not None:
            self.configs = configs
        elif columns is not None:
            # Create default configs for each column
            self.configs = [
                OutlierConfig(column=col, method=method, threshold=threshold, params=params)
                for col in columns
            ]
        else:
            self.configs = []

    def add_column(
            self,
            column: str,
            method: str = OutlierMethod.ZSCORE,
            threshold: float = 3.0,
            params: Optional[Dict[str, Any]] = None
    ) -> None:
        """
        Add a column to validate.

        Args:
            column (str): Column name
            method (str): Outlier detection method
            threshold (float): Threshold for outlier detection
            params (Optional[Dict[str, Any]]): Additional parameters
        """
        config = OutlierConfig(
            column=column,
            method=method,
            threshold=threshold,
            params=params or {}
        )
        self.configs.append(config)

    def validate_schema(self, data: pd.DataFrame) -> ValidationResult:
        """
        Validate DataFrame against schema definition.

        Args:
            data (pd.DataFrame): DataFrame to validate

        Returns:
            ValidationResult: Schema validation results
        """
        # For OutlierValidator, this just redirects to the validate method
        # since outlier detection doesn't use a traditional schema
        return self.validate(data)

    def validate_column(self, data: pd.DataFrame, column: str) -> ValidationResult:
        """
        Validate a specific column for outliers.

        Args:
            data (pd.DataFrame): DataFrame to validate
            column (str): Column name to validate

        Returns:
            ValidationResult: Validation results for the column
        """
        if column not in data.columns:
            return {
                'valid': False,
                'error': f"Column {column} not found in data"
            }

        # Find the configuration for this column
        config = next((c for c in self.configs if c.column == column), None)
        if config is None:
            # Use default configuration
            config = OutlierConfig(column=column)

        series = data[column]

        # Skip non-numeric columns
        if not pd.api.types.is_numeric_dtype(series):
            return {
                'valid': True,
                'warning': f"Column {column} is not numeric, skipping outlier detection"
            }

        # Drop missing values
        non_null = series.dropna()
        if len(non_null) == 0:
            return {
                'valid': True,
                'warning': f"Column {column} has no non-null values"
            }

        outliers = self._detect_outliers(non_null, config)

        outlier_count = outliers.sum()
        outlier_ratio = outlier_count / len(non_null)

        threshold_ratio = config.params.get('max_outlier_ratio', 0.05)

        result = {
            'valid': outlier_ratio <= threshold_ratio,
            'column': column,
            'method': config.method,
            'threshold': config.threshold,
            'outlier_count': int(outlier_count),
            'outlier_ratio': float(outlier_ratio),
            'outlier_indices': list(data.index[outliers]) if outlier_count > 0 else []
        }

        if outlier_count > 0:
            # Get some statistics on outliers
            outlier_values = non_null[outliers]
            result.update({
                'outlier_min': float(outlier_values.min()),
                'outlier_max': float(outlier_values.max()),
                'outlier_mean': float(outlier_values.mean())
            })

            # Get an overall range of data
            result.update({
                'data_min': float(non_null.min()),
                'data_max': float(non_null.max()),
                'data_mean': float(non_null.mean()),
                'data_median': float(non_null.median()),
                'data_std': float(non_null.std())
            })

        return result

    def _detect_outliers(self, series: pd.Series, config: OutlierConfig) -> pd.Series:
        """
        Detect outliers in a series using the specified method.

        Args:
            series (pd.Series): Data series
            config (OutlierConfig): Outlier detection configuration

        Returns:
            pd.Series: Boolean series where True indicates an outlier
        """
        method = config.method
        threshold = config.threshold

        if method == OutlierMethod.ZSCORE:
            # Z-score method
            z_scores = np.abs(stats.zscore(series, nan_policy='omit'))
            return pd.Series(z_scores > threshold, index=series.index)

        elif method == OutlierMethod.IQR:
            # Interquartile range method
            q1 = series.quantile(0.25)
            q3 = series.quantile(0.75)
            iqr = q3 - q1
            lower_bound = q1 - (threshold * iqr)
            upper_bound = q3 + (threshold * iqr)
            return (series < lower_bound) | (series > upper_bound)

        elif method == OutlierMethod.ISOLATION_FOREST:
            try:
                from sklearn.ensemble import IsolationForest
                contamination = config.params.get('contamination', 'auto')
                model = IsolationForest(
                    contamination=contamination,
                    random_state=42,
                    **{k: v for k, v in config.params.items() if k != 'contamination'}
                )

                # Reshape for sklearn
                X = series.values.reshape(-1, 1)
                # Fit and predict
                predictions = model.fit_predict(X)
                # Convert to boolean series (outliers are -1, inliers are 1)
                return pd.Series(predictions == -1, index=series.index)
            except ImportError:
                # Fallback to IQR if scikit-learn is not available
                return self._detect_outliers(series, OutlierConfig(
                    column=config.column,
                    method=OutlierMethod.IQR,
                    threshold=config.threshold
                ))

        elif method == OutlierMethod.LOF:
            try:
                from sklearn.neighbors import LocalOutlierFactor
                contamination = config.params.get('contamination', 0.1)
                n_neighbors = config.params.get('n_neighbors', 20)

                model = LocalOutlierFactor(
                    n_neighbors=n_neighbors,
                    contamination=contamination,
                    **{k: v for k, v in config.params.items()
                       if k not in ['contamination', 'n_neighbors']}
                )

                # Reshape for sklearn
                X = series.values.reshape(-1, 1)
                # Fit and predict
                predictions = model.fit_predict(X)
                # Convert to boolean series (outliers are -1, inliers are 1)
                return pd.Series(predictions == -1, index=series.index)
            except ImportError:
                # Fallback to IQR if scikit-learn is not available
                return self._detect_outliers(series, OutlierConfig(
                    column=config.column,
                    method=OutlierMethod.IQR,
                    threshold=config.threshold
                ))

        elif method == OutlierMethod.DBSCAN:
            try:
                from sklearn.cluster import DBSCAN
                eps = config.params.get('eps', 0.5)
                min_samples = config.params.get('min_samples', 5)

                model = DBSCAN(
                    eps=eps,
                    min_samples=min_samples,
                    **{k: v for k, v in config.params.items()
                       if k not in ['eps', 'min_samples']}
                )

                # Reshape for sklearn
                X = series.values.reshape(-1, 1)
                # Fit and predict
                predictions = model.fit_predict(X)
                # Convert to boolean series (outliers are -1, inliers are non-negative)
                return pd.Series(predictions == -1, index=series.index)
            except ImportError:
                # Fallback to IQR if scikit-learn is not available
                return self._detect_outliers(series, OutlierConfig(
                    column=config.column,
                    method=OutlierMethod.IQR,
                    threshold=config.threshold
                ))

        else:
            # Default to Z-score method
            z_scores = np.abs(stats.zscore(series, nan_policy='omit'))
            return pd.Series(z_scores > threshold, index=series.index)

    def validate(self, data: pd.DataFrame) -> ValidationResult:
        """
        Validate DataFrame for outliers.

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

        # Get columns to check
        columns_to_check = [config.column for config in self.configs]

        # If no columns specified, check all numeric columns
        if not columns_to_check:
            columns_to_check = data.select_dtypes(include=['number']).columns.tolist()

        # Validate each column
        column_results = {}
        overall_valid = True
        total_outliers = 0

        for column in columns_to_check:
            if column in data.columns:
                result = self.validate_column(data, column)
                column_results[column] = result

                # Update overall validity
                if 'valid' in result and not result['valid']:
                    overall_valid = False

                # Count total outliers
                if 'outlier_count' in result:
                    total_outliers += result['outlier_count']

        # Prepare overall results
        result = {
            'valid': overall_valid,
            'total_outliers': total_outliers,
            'outlier_ratio': total_outliers / len(data) if len(data) > 0 else 0,
            'column_results': column_results
        }

        return result

    def is_valid(self, data: pd.DataFrame) -> bool:
        """
        Check if data is valid (contains no significant outliers).

        Args:
            data (pd.DataFrame): Data to check

        Returns:
            bool: True if data is valid, False otherwise
        """
        return self.validate(data)['valid']

    @classmethod
    def auto_config(cls, data: pd.DataFrame, method: str = OutlierMethod.IQR, threshold: float = 1.5,
                    sensitivity: str = 'medium') -> 'OutlierValidator':
        """
        Automatically configure an outlier validator based on data characteristics.

        Args:
            data (pd.DataFrame): Sample data to base configuration on
            method (str): Outlier detection method
            threshold (float): Base threshold for outlier detection
            sensitivity (str): Sensitivity level (low, medium, high)

        Returns:
            OutlierValidator: Configured validator
        """
        # Adjust threshold based on sensitivity
        if sensitivity == 'low':
            threshold *= 1.5  # Less sensitive
        elif sensitivity == 'high':
            threshold *= 0.7  # More sensitive

        # Get numeric columns
        numeric_columns = data.select_dtypes(include=['number']).columns.tolist()

        # Create configs for each numeric column
        configs = []

        for column in numeric_columns:
            series = data[column]
            non_null = series.dropna()

            if len(non_null) == 0:
                continue

            # Skip columns with low variance
            if non_null.std() == 0:
                continue

            # Choose appropriate method based on distribution
            # Try to determine if normal distribution
            if method == 'auto':
                try:
                    # Shapiro-Wilk test for normality (works best for small samples)
                    if len(non_null) <= 5000:
                        _, p_value = stats.shapiro(non_null.sample(min(len(non_null), 5000)))
                        is_normal = p_value > 0.05
                    else:
                        # For larger datasets, use skewness and kurtosis
                        skewness = abs(stats.skew(non_null))
                        kurtosis = abs(stats.kurtosis(non_null))
                        is_normal = skewness < 0.5 and kurtosis < 1.0

                    column_method = OutlierMethod.ZSCORE if is_normal else OutlierMethod.IQR
                except:
                    # Default to IQR which is more robust
                    column_method = OutlierMethod.IQR
            else:
                column_method = method

            # Create configuration
            config = OutlierConfig(
                column=column,
                method=column_method,
                threshold=threshold,
                params={'max_outlier_ratio': 0.05}  # Allow up to 5% outliers by default
            )

            configs.append(config)

        return cls(configs=configs)