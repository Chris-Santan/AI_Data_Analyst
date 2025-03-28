# src/core/interfaces/__init__.py
from .database_interface import DatabaseConnectionInterface
from .query_interface import QueryExecutionInterface
from .statistical_test_interface import StatisticalTestInterface
from .ai_interface import AIQueryGeneratorInterface

__all__ = [
    'DatabaseConnectionInterface',
    'QueryExecutionInterface',
    'StatisticalTestInterface',
    'AIQueryGeneratorInterface'
]