# src/web_interface/models.py
from typing import Dict, Any, List, Optional, Union
from pydantic import BaseModel, Field
from datetime import datetime

class QueryRequest(BaseModel):
    """Query request model for API."""
    query: str = Field(..., description="SQL query to execute")
    parameters: Optional[Dict[str, Any]] = Field(None, description="Query parameters")
    timeout: Optional[int] = Field(None, description="Query timeout in seconds")
    limit: Optional[int] = Field(None, description="Maximum number of rows to return")

class PaginatedQueryRequest(QueryRequest):
    """Paginated query request model."""
    page: int = Field(1, ge=1, description="Page number (starting from 1)")
    page_size: int = Field(100, ge=1, le=1000, description="Number of rows per page")

class ColumnInfo(BaseModel):
    """Column information model."""
    name: str
    type: str

class PaginationInfo(BaseModel):
    """Pagination information model."""
    page: int
    page_size: int
    total: int
    total_pages: int
    has_next: bool
    has_prev: bool

class QueryResultResponse(BaseModel):
    """Query result response model."""
    rows: List[Dict[str, Any]]
    column_names: List[str]
    column_types: Dict[str, str]
    row_count: int
    execution_time: float
    query: str
    timestamp: datetime = Field(default_factory=datetime.now)

class PaginatedQueryResultResponse(BaseModel):
    """Paginated query result response model."""
    data: List[Dict[str, Any]]
    pagination: PaginationInfo
    metadata: Dict[str, Any]

class TableSummary(BaseModel):
    """Table summary model."""
    name: str
    column_count: int
    row_count: Optional[int] = None
    columns: List[ColumnInfo]

class DatabaseSummary(BaseModel):
    """Database summary model."""
    database_name: str
    table_count: int
    tables: List[TableSummary]

class ErrorResponse(BaseModel):
    """Error response model."""
    error: str
    status_code: int
    message: Optional[str] = None