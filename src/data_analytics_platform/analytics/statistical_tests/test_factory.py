from typing import Dict, Type
from .base_test import BaseStatisticalTest
from .t_test import TTest
from .test_runner import TestType


class StatisticalTestFactory:
    """Factory for creating statistical test instances."""

    _test_classes: Dict[TestType, Type[BaseStatisticalTest]] = {
        TestType.T_TEST: TTest,
        # Add other test types as they are implemented
    }

    @classmethod
    def create_test(cls, test_type: TestType) -> BaseStatisticalTest:
        """
        Create an instance of the specified test type.

        Args:
            test_type: The type of test to create

        Returns:
            An instance of the requested test

        Raises:
            ValueError: If the test type is not supported
        """
        if test_type not in cls._test_classes:
            raise ValueError(f"Unsupported test type: {test_type}")

        return cls._test_classes[test_type]()

    @classmethod
    def register_test(cls, test_type: TestType, test_class: Type[BaseStatisticalTest]) -> None:
        """
        Register a new test class for a test type.

        Args:
            test_type: The type of test
            test_class: The class implementing the test
        """
        cls._test_classes[test_type] = test_class