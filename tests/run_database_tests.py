#!/usr/bin/env python
"""
Test runner for database connectivity module tests.
This script runs all unit tests for the database connection components.
"""

# First add these imports at the very top of the file
import os
import sys

# Go up two directory levels (tests -> project root)
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '../')))
# Import the setup script
import setup_path

import unittest
import logging

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='[%(asctime)s] %(levelname)s: %(message)s',
    datefmt='%H:%M:%S'
)
logger = logging.getLogger(__name__)


def run_database_tests():
    """Run all database connectivity tests."""
    # Discover and run the tests
    logger.info("Starting database connectivity tests")

    test_loader = unittest.TestLoader()
    test_suite = unittest.TestSuite()

    # Add test modules with correct import paths for the database test modules
    from tests.unit.database.test_connection import TestDatabaseConnection
    from tests.unit.database.test_config import TestDatabaseConfig
    from tests.unit.database.test_auth_manager import TestAuthenticationManager
    from tests.unit.database.test_query_executor import TestQueryExecutor
    from tests.unit.database.test_schema_retriever import TestSchemaRetriever

    # Add all test classes
    test_suite.addTest(test_loader.loadTestsFromTestCase(TestDatabaseConnection))
    test_suite.addTest(test_loader.loadTestsFromTestCase(TestDatabaseConfig))
    test_suite.addTest(test_loader.loadTestsFromTestCase(TestAuthenticationManager))
    test_suite.addTest(test_loader.loadTestsFromTestCase(TestQueryExecutor))
    test_suite.addTest(test_loader.loadTestsFromTestCase(TestSchemaRetriever))

    # Run the tests
    test_runner = unittest.TextTestRunner(verbosity=2)
    result = test_runner.run(test_suite)

    # Display summary
    logger.info("Test results:")
    logger.info(f"Tests run: {result.testsRun}")
    logger.info(f"Errors: {len(result.errors)}")
    logger.info(f"Failures: {len(result.failures)}")
    logger.info(f"Skipped: {len(result.skipped)}")

    # Return non-zero exit code if there were failures or errors
    return len(result.errors) + len(result.failures)


def discover_and_run_tests():
    """Discover and run all database tests in the tests/unit/database directory."""
    logger.info("Discovering and running database tests")

    test_loader = unittest.TestLoader()
    start_dir = os.path.join(os.path.dirname(__file__), 'unit', 'database')

    # Pattern to match all test files
    pattern = 'test_*.py'

    # Discover tests
    test_suite = test_loader.discover(start_dir, pattern=pattern)

    # Run the tests
    test_runner = unittest.TextTestRunner(verbosity=2)
    result = test_runner.run(test_suite)

    # Display summary
    logger.info("Test results:")
    logger.info(f"Tests run: {result.testsRun}")
    logger.info(f"Errors: {len(result.errors)}")
    logger.info(f"Failures: {len(result.failures)}")
    logger.info(f"Skipped: {len(result.skipped)}")

    # Return non-zero exit code if there were failures or errors
    return len(result.errors) + len(result.failures)


if __name__ == "__main__":
    try:
        # Check for discovery mode
        if "--discover" in sys.argv:
            exit_code = discover_and_run_tests()
        else:
            exit_code = run_database_tests()

        # Exit with appropriate code
        sys.exit(exit_code)
    except KeyboardInterrupt:
        logger.info("Tests interrupted by user")
        sys.exit(1)
    except Exception as e:
        logger.error(f"Error running tests: {str(e)}")
        sys.exit(1)