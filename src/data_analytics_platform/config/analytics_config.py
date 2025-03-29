# src/config/analytics_config.py
from typing import Dict, Any

# Fix the import path
from data_analytics_platform.config.base_config import BaseConfig
# or use relative import:
# from .base_config import BaseConfig


class AnalyticsConfig(BaseConfig):
    """
    Configuration for analytics module.
    Contains settings for statistical tests, data preprocessing, and analysis parameters.
    """

    def __init__(self, env_prefix: str = "ANALYTICS"):
        """
        Initialize analytics configuration.

        Args:
            env_prefix (str): Prefix for environment variables
        """
        super().__init__("analytics", env_prefix)

        # Default configuration
        self._default_config = {
            'default_confidence_level': 0.95,
            'use_bonferroni_correction': True,
            'min_sample_size': 30,
            'normality_test_threshold': 0.05,
            'cache_results': True,
            'cache_dir': '.cache/analytics',
            'preprocessing': {
                'handle_missing_values': 'mean',  # Options: mean, median, mode, drop
                'handle_outliers': 'winsorize',  # Options: winsorize, trim, none
                'outlier_threshold': 3,  # Number of standard deviations
                'auto_normalize': True,
                'auto_scale': False
            },
            'visualizations': {
                'default_chart_type': 'bar',
                'include_summary_statistics': True,
                'show_p_values': True,
                'show_confidence_intervals': True
            },
            'tests': {
                't_test': {
                    'default_alternative': 'two-sided',  # Options: two-sided, less, greater
                    'var_equal': False,
                    'report_effect_size': True
                },
                'chi_square': {
                    'yates_correction': True,
                    'report_cramers_v': True
                },
                'anova': {
                    'type': 1,  # Options: 1, 2, 3
                    'post_hoc': 'tukey'  # Options: tukey, bonferroni, scheffe
                }
            }
        }

    def get_test_config(self, test_name: str) -> Dict[str, Any]:
        """
        Get configuration for a specific statistical test.

        Args:
            test_name (str): Name of the test (t_test, chi_square, etc.)

        Returns:
            Dict[str, Any]: Test configuration

        Raises:
            ValueError: If test is not found
        """
        tests = self.get('tests', {})
        if test_name not in tests:
            raise ValueError(f"Test configuration not found: {test_name}")

        return tests[test_name]

    def get_preprocessing_config(self) -> Dict[str, Any]:
        """
        Get data preprocessing configuration.

        Returns:
            Dict[str, Any]: Preprocessing configuration
        """
        return self.get('preprocessing', {})

    def get_confidence_level(self) -> float:
        """
        Get the default confidence level for statistical tests.

        Returns:
            float: Confidence level (0.0 to 1.0)
        """
        return self.get('default_confidence_level', 0.95)

    def set_confidence_level(self, level: float) -> None:
        """
        Set the default confidence level for statistical tests.

        Args:
            level (float): Confidence level (0.0 to 1.0)

        Raises:
            ValueError: If level is outside valid range
        """
        if not 0 < level < 1:
            raise ValueError("Confidence level must be between 0 and 1")

        self.set('default_confidence_level', level)