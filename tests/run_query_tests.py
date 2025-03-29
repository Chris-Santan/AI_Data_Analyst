#!/usr/bin/env python
# tests/run_query_tests.py
import unittest
import sys
import os
from pathlib import Path

# Add parent directory to sys.path
sys.path.insert(0, str(Path(__file__).parent.parent))


def run_query_tests():
    """Run specifically the query execution tests."""
    # Create test suite
    test_suite = unittest.TestSuite()

    # Define query test files - be explicit about which files to run
    query_test_files = [
        "unit/database/test_query_executor.py",
        "unit/database/test_query_service.py",
        "integration/database/test_query_integration.py"
    ]

    # Add each test file to the suite
    for test_file in query_test_files:
        test_path = Path(__file__).parent / test_file
        if not test_path.exists():
            print(f"Warning: Test file {test_path} not found")
            continue

        print(f"Adding tests from {test_path}...")

        # Use the file's directory as the start_dir and the filename as the pattern
        directory = test_path.parent
        pattern = test_path.name

        tests = unittest.defaultTestLoader.discover(
            start_dir=str(directory),
            pattern=pattern
        )
        test_suite.addTests(tests)

    # Run tests
    print("\nRunning query execution tests...\n")
    test_runner = unittest.TextTestRunner(verbosity=2)
    result = test_runner.run(test_suite)

    # Print summary
    print("\nQuery Test Summary:")
    print(f"  Ran {result.testsRun} tests")
    print(f"  Failures: {len(result.failures)}")
    print(f"  Errors: {len(result.errors)}")
    print(f"  Skipped: {len(result.skipped)}")

    # Return appropriate exit code
    return 0 if result.wasSuccessful() else 1


if __name__ == "__main__":
    sys.exit(run_query_tests())