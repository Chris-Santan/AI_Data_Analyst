# src/data_analytics_platform/web_interface/run_api.py
import uvicorn
import os
import sys
import logging
from pathlib import Path

# Add parent directory to sys.path
sys.path.insert(0, str(Path(__file__).parent.parent.parent.parent))

from data_analytics_platform.config.logging_config import LoggingConfig

# Configure logging
logging_config = LoggingConfig()
logging_config.configure()
logger = logging.getLogger(__name__)


def run_api():
    """Run the FastAPI application."""
    logger.info("Starting Data Analytics Platform API")

    # Get port from environment or use default
    port = int(os.getenv("API_PORT", "8000"))

    # Run API
    uvicorn.run(
        "data_analytics_platform.web_interface.app:app",
        host="0.0.0.0",
        port=port,
        reload=True
    )


if __name__ == "__main__":
    run_api()