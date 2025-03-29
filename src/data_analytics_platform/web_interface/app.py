# src/data_analytics_platform/web_interface/app.py
from fastapi import FastAPI, HTTPException
from fastapi.middleware.cors import CORSMiddleware

from data_analytics_platform.config.logging_config import LoggingConfig

# Configure logging
logging_config = LoggingConfig()
logging_config.configure()
logger = logging_config.get_logger(__name__)

# Create FastAPI app
app = FastAPI(
    title="Data Analytics Platform API",
    description="API for data analytics and query execution",
    version="0.1.0"
)

# Add CORS middleware
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],  # In production, replace with specific origins
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# Root endpoint
@app.get("/")
async def root():
    return {"message": "Welcome to the Data Analytics Platform API"}

# Health check endpoint
@app.get("/health")
async def health_check():
    return {
        "status": "ok",
        "api_version": app.version
    }

# Import and include routers
from data_analytics_platform.web_interface.routes.query_routes import router as query_router
from data_analytics_platform.web_interface.routes.database_routes import router as database_router

app.include_router(query_router, prefix="/api/queries", tags=["queries"])
app.include_router(database_router, prefix="/api/database", tags=["database"])

# Error handlers
@app.exception_handler(HTTPException)
async def http_exception_handler(request, exc):
    logger.error(f"HTTP error: {exc.detail}")
    return {
        "error": exc.detail,
        "status_code": exc.status_code
    }

@app.exception_handler(Exception)
async def general_exception_handler(request, exc):
    logger.error(f"Unhandled exception: {str(exc)}", exc_info=True)
    return {
        "error": "Internal server error",
        "status_code": 500,
        "message": str(exc)
    }