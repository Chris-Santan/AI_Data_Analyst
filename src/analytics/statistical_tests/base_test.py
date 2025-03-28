from abc import ABC, abstractmethod
from typing import Dict, Any, Generic, TypeVar

T = TypeVar('T')


class BaseStatisticalTest(ABC, Generic[T]):
    """Base class for all statistical tests."""

    @abstractmethod
    def validate_input(self, data: T) -> bool:
        """Validate that the input data is appropriate for this test."""
        pass

    @abstractmethod
    def run_test(self, data: T) -> Dict[str, Any]:
        """Run the statistical test and return results."""
        pass

    @property
    @abstractmethod
    def name(self) -> str:
        """Get the name of the test."""
        pass

    @property
    @abstractmethod
    def description(self) -> str:
        """Get a description of what the test does."""
        pass