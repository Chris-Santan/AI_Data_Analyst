# src/web_interface/routes/query_routes.py
from fastapi import APIRouter, Depends, HTTPException, Query, status
import time
import logging

from data_analytics_platform.web_interface.models import (
    QueryRequest,
    PaginatedQueryRequest,
    QueryResultResponse,
    PaginatedQueryResultResponse
)
from data_analytics_platform.web_interface.dependencies import get_query_service
from data_analytics_platform.database.query_service import QueryService
from data_analytics_platform.core.exceptions.custom_exceptions import QueryExecutionError, DatabaseConnectionError

# Get logger
logger = logging.getLogger(__name__)

# Create router
router = APIRouter()


@router.post("/execute", response_model=QueryResultResponse)
async def execute_query(
        request: QueryRequest,
        query_service: QueryService = Depends(get_query_service)
):
    """
    Execute a SQL query and return the results.
    """
    try:
        # Execute the query
        result = query_service.execute_query(
            query=request.query,
            parameters=request.parameters,
            timeout=request.timeout,
            limit=request.limit
        )

        # Create response
        response = QueryResultResponse(
            rows=result.rows,
            column_names=result.column_names,
            column_types=result.get_column_types(),
            row_count=result.row_count,
            execution_time=result.execution_time,
            query=result.query,
            timestamp=time.time()
        )

        return response

    except QueryExecutionError as e:
        logger.error(f"Query execution error: {str(e)}")
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail=f"Query execution error: {e.get_user_message()}"
        )
    except DatabaseConnectionError as e:
        logger.error(f"Database connection error: {str(e)}")
        raise HTTPException(
            status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
            detail=f"Database connection error: {e.get_user_message()}"
        )
    except Exception as e:
        logger.error(f"Unexpected error executing query: {str(e)}", exc_info=True)
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"An unexpected error occurred: {str(e)}"
        )


@router.post("/paginate", response_model=PaginatedQueryResultResponse)
async def paginate_query(
        request: PaginatedQueryRequest,
        query_service: QueryService = Depends(get_query_service)
):
    """
    Execute a SQL query with pagination and return the results.
    """
    try:
        # Execute the paginated query
        paginated_result = query_service.paginate_query(
            query=request.query,
            page=request.page,
            page_size=request.page_size,
            parameters=request.parameters
        )

        return paginated_result

    except QueryExecutionError as e:
        logger.error(f"Query execution error: {str(e)}")
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail=f"Query execution error: {e.get_user_message()}"
        )
    except ValueError as e:
        logger.error(f"Invalid pagination parameters: {str(e)}")
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail=f"Invalid pagination parameters: {str(e)}"
        )
    except Exception as e:
        logger.error(f"Unexpected error paginating query: {str(e)}", exc_info=True)
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"An unexpected error occurred: {str(e)}"
        )


@router.post("/execute/csv")
async def execute_query_csv(
        request: QueryRequest,
        query_service: QueryService = Depends(get_query_service)
):
    """
    Execute a SQL query and return the results as CSV.
    """
    try:
        # Execute the query
        result = query_service.execute_query(
            query=request.query,
            parameters=request.parameters,
            timeout=request.timeout,
            limit=request.limit
        )

        # Convert to DataFrame and then to CSV
        df = result.to_dataframe()
        csv_content = df.to_csv(index=False)

        # Return CSV response
        from fastapi.responses import PlainTextResponse
        return PlainTextResponse(
            content=csv_content,
            media_type="text/csv",
            headers={
                "Content-Disposition": "attachment; filename=query_result.csv"
            }
        )

    except QueryExecutionError as e:
        logger.error(f"Query execution error: {str(e)}")
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail=f"Query execution error: {e.get_user_message()}"
        )
    except Exception as e:
        logger.error(f"Unexpected error executing query for CSV: {str(e)}", exc_info=True)
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"An unexpected error occurred: {str(e)}"
        )


@router.get("/history")
async def get_query_history(
        limit: int = Query(10, ge=1, le=100),
        query_service: QueryService = Depends(get_query_service)
):
    """
    Get the query execution history.
    """
    history = query_service.get_query_history()

    # Limit the history to the requested number of entries
    if limit and limit < len(history):
        history = history[-limit:]

    return {"history": history}


@router.post("/analyze")
async def analyze_query_results(
        request: QueryRequest,
        query_service: QueryService = Depends(get_query_service)
):
    """
    Execute a query and return analysis of the results.
    """
    try:
        # Execute the query
        result = query_service.execute_query(
            query=request.query,
            parameters=request.parameters,
            timeout=request.timeout,
            limit=request.limit
        )

        # Generate analysis
        analysis = query_service.describe_query_results(result)

        return {
            "query": request.query,
            "row_count": result.row_count,
            "execution_time": result.execution_time,
            "analysis": analysis
        }

    except QueryExecutionError as e:
        logger.error(f"Query execution error: {str(e)}")
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail=f"Query execution error: {e.get_user_message()}"
        )
    except Exception as e:
        logger.error(f"Unexpected error analyzing query results: {str(e)}", exc_info=True)
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"An unexpected error occurred: {str(e)}"
        )