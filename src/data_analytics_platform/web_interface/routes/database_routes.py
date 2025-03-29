# src/web_interface/routes/database_routes.py
from fastapi import APIRouter, Depends, HTTPException, status
import logging

from data_analytics_platform.web_interface.models import (
    TableSummary,
    DatabaseSummary,
    ColumnInfo
)
from data_analytics_platform.web_interface.dependencies import get_db_connection, get_query_service
from data_analytics_platform.database.connection import DatabaseConnection
from data_analytics_platform.database.schema_retriever import SchemaRetriever
from data_analytics_platform.database.query_service import QueryService
from data_analytics_platform.core.exceptions.custom_exceptions import DatabaseConnectionError

# Get logger
logger = logging.getLogger(__name__)

# Create router
router = APIRouter()


@router.get("/tables")
async def get_tables(
        connection: DatabaseConnection = Depends(get_db_connection)
):
    """
    Get a list of all tables in the database.
    """
    try:
        # Create schema retriever
        schema_retriever = SchemaRetriever(connection)

        # Get all tables
        tables = schema_retriever.get_all_tables()

        return {"tables": tables}

    except DatabaseConnectionError as e:
        logger.error(f"Database connection error: {str(e)}")
        raise HTTPException(
            status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
            detail=f"Database connection error: {e.get_user_message()}"
        )
    except Exception as e:
        logger.error(f"Unexpected error getting tables: {str(e)}", exc_info=True)
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"An unexpected error occurred: {str(e)}"
        )


@router.get("/schema")
async def get_database_schema(
        connection: DatabaseConnection = Depends(get_db_connection)
):
    """
    Get the database schema.
    """
    try:
        # Create schema retriever
        schema_retriever = SchemaRetriever(connection)

        # Get complete schema
        schema = schema_retriever.get_database_schema()

        return schema

    except DatabaseConnectionError as e:
        logger.error(f"Database connection error: {str(e)}")
        raise HTTPException(
            status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
            detail=f"Database connection error: {e.get_user_message()}"
        )
    except Exception as e:
        logger.error(f"Unexpected error getting schema: {str(e)}", exc_info=True)
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"An unexpected error occurred: {str(e)}"
        )


@router.get("/schema/{table_name}")
async def get_table_schema(
        table_name: str,
        connection: DatabaseConnection = Depends(get_db_connection)
):
    """
    Get schema for a specific table.
    """
    try:
        # Create schema retriever
        schema_retriever = SchemaRetriever(connection)

        # Get table schema
        table_schema = schema_retriever.get_table_schema(table_name)

        return table_schema

    except DatabaseConnectionError as e:
        logger.error(f"Database connection error: {str(e)}")
        raise HTTPException(
            status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
            detail=f"Database connection error: {e.get_user_message()}"
        )
    except Exception as e:
        logger.error(f"Unexpected error getting table schema: {str(e)}", exc_info=True)
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"An unexpected error occurred: {str(e)}"
        )


@router.get("/summary")
async def get_database_summary(
        connection: DatabaseConnection = Depends(get_db_connection),
        query_service: QueryService = Depends(get_query_service)
):
    """
    Get a summary of the database including tables and record counts.
    """
    try:
        # Create schema retriever
        schema_retriever = SchemaRetriever(connection)

        # Get schema summary
        summary = schema_retriever.get_schema_summary()

        # Create database summary
        db_summary = DatabaseSummary(
            database_name=connection.get_connection_info().get("database", "unknown"),
            table_count=summary["table_count"],
            tables=[]
        )

        # Add table information
        for table_name, table_info in summary["tables"].items():
            # Get column metadata
            columns = schema_retriever.get_column_metadata(table_name)

            # Get row count
            try:
                row_count = query_service.execute_scalar(f"SELECT COUNT(*) FROM {table_name}")
            except:
                row_count = None

            # Create table summary
            table_summary = TableSummary(
                name=table_name,
                column_count=table_info["column_count"],
                row_count=row_count,
                columns=[
                    ColumnInfo(name=col["name"], type=col["type"])
                    for col in columns
                ]
            )

            db_summary.tables.append(table_summary)

        return db_summary

    except DatabaseConnectionError as e:
        logger.error(f"Database connection error: {str(e)}")
        raise HTTPException(
            status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
            detail=f"Database connection error: {e.get_user_message()}"
        )
    except Exception as e:
        logger.error(f"Unexpected error getting database summary: {str(e)}", exc_info=True)
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"An unexpected error occurred: {str(e)}"
        )


@router.get("/relationships/{table_name}")
async def get_table_relationships(
        table_name: str,
        connection: DatabaseConnection = Depends(get_db_connection)
):
    """
    Get relationships for a specific table.
    """
    try:
        # Create schema retriever
        schema_retriever = SchemaRetriever(connection)

        # Get table relationships
        relationships = schema_retriever.get_table_relationships(table_name)

        return relationships

    except DatabaseConnectionError as e:
        logger.error(f"Database connection error: {str(e)}")
        raise HTTPException(
            status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
            detail=f"Database connection error: {e.get_user_message()}"
        )
    except Exception as e:
        logger.error(f"Unexpected error getting table relationships: {str(e)}", exc_info=True)
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"An unexpected error occurred: {str(e)}"
        )