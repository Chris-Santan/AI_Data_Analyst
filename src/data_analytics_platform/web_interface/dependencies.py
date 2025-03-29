# src/web_interface/dependencies.py
from fastapi import Depends, HTTPException, status
import logging

from data_analytics_platform.database.connection import DatabaseConnection
from data_analytics_platform.database.query_service import QueryService
from data_analytics_platform.database.error_handler import DatabaseErrorHandler
from data_analytics_platform.config.base_config import BaseConfig
from data_analytics_platform.core.exceptions.custom_exceptions import DatabaseConnectionError

# Get logger
logger = logging.getLogger(__name__)

# Create application config
app_config = BaseConfig("app")
app_config.load_config()


def get_db_connection():
    """
    Get a database connection from environment variables.
    """
    connection = None
    try:
        # Create connection
        connection = DatabaseConnection()

        # Connect to database
        connection.connect()

        yield connection

    except DatabaseConnectionError as e:
        logger.error(f"Database connection error: {str(e)}")
        raise HTTPException(
            status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
            detail=f"Database connection error: {e.get_user_message()}"
        )
    finally:
        # Disconnect when done
        if connection:
            connection.disconnect()


def get_query_service(
        connection: DatabaseConnection = Depends(get_db_connection)
):
    """
    Get a query service with the database connection.
    """
    # Create error handler
    error_handler = DatabaseErrorHandler()

    # Create query service
    query_service = QueryService(
        connection=connection,
        error_handler=error_handler,
        max_results=app_config.get("max_query_results", 10000),
        default_timeout=app_config.get("query_timeout", 30)
    )

    return query_service