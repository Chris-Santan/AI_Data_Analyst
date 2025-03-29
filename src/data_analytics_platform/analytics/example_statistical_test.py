import pandas as pd
import numpy as np
from data_analytics_platform.analytics.statistical_tests.test_runner import StatisticalTestRunner, TestType
from data_analytics_platform.analytics.statistical_tests.test_factory import StatisticalTestFactory


def statistical_test_example():
    """Example of running statistical tests."""
    # Create sample data
    np.random.seed(42)
    group_a = pd.Series(np.random.normal(loc=10, scale=2, size=30))
    group_b = pd.Series(np.random.normal(loc=12, scale=2, size=30))

    # Create test factory and runner
    test_factory = StatisticalTestFactory()
    test_runner = StatisticalTestRunner()

    # Run a t-test
    test = test_factory.create_test(TestType.T_TEST)
    test_result = test.run_test((group_a, group_b))

    # Print results
    print(f"Test type: {test_result['test_type']}")
    print(f"T-statistic: {test_result['statistic']:.4f}")
    print(f"P-value: {test_result['p_value']:.4f}")
    print(f"Interpretation: {test_result['interpretation']}")
    print(
        f"Effect size (Cohen's d): {test_result['effect_size']['cohen_d']:.4f} - {test_result['effect_size']['interpretation']} effect")

    # Let the runner suggest appropriate tests
    data = pd.DataFrame({
        'group': ['A'] * 30 + ['B'] * 30,
        'value': pd.concat([group_a, group_b]).reset_index(drop=True)
    })

    suggestions = test_runner.suggest_tests(data)
    print("\nSuggested tests:")
    for test_type, confidence, explanation in suggestions:
        print(f"- {test_type.name}: {confidence:.2f} confidence - {explanation}")


if __name__ == "__main__":
    statistical_test_example()