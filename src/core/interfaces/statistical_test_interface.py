from typing import Dict, Any

from typing import Protocol, TypeVar, Generic

T = TypeVar('T')


class StatisticalTestInterface(Protocol, Generic[T]):
    """Protocol for statistical tests."""

    def run_test(self, data: T) -> Dict[str, Any]:
        """Run the statistical test.

        Args:
            data (T): Input data for the test.

        Returns:
            Dict[str, Any]: Test results and statistical metrics.
        """
        ...

    def validate_input(self, data: T) -> bool:
        """Validate input data for the statistical test.

        Args:
            data (T): Input data to validate.

        Returns:
            bool: True if input is valid, False otherwise.
        """
        ...