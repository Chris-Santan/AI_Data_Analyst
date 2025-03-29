# tests/conftest.py
import os
import sys
import logging
from pathlib import Path

# Add project root to Python path to help with imports in tests
project_root = Path(__file__).parent.parent
sys.path.insert(0, str(project_root))

# Configure minimal logging for tests
logging.basicConfig(
    level=logging.WARNING,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)

# Disable SQLAlchemy INFO messages during tests
logging.getLogger('sqlalchemy.engine').setLevel(logging.WARNING)