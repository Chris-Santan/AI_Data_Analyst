# src/data_analytics_platform/core/exceptions/__init__.py
from .custom_exceptions import (
    DataAnalyticsPlatformError,
    DatabaseConnectionError,
    QueryExecutionError,
    StatisticalTestError,
    AIGenerationError
)
from .validation_exceptions import (
    ValidationError,
    SchemaValidationError,
    OutlierValidationError
)

__all__ = [
    'DataAnalyticsPlatformError',
    'DatabaseConnectionError',
    'QueryExecutionError',
    'StatisticalTestError',
    'AIGenerationError',
    'ValidationError',
    'SchemaValidationError',
    'OutlierValidationError'
]