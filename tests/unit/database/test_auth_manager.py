import os
import sys
# Go up three directory levels (database -> unit -> tests -> project root)
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '../../../')))
# Import the setup script

import unittest
import base64
from unittest.mock import patch
from pathlib import Path

from data_analytics_platform.database.auth_manager import AuthenticationManager
from data_analytics_platform.core.exceptions.custom_exceptions import DatabaseConnectionError

class TestAuthenticationManager(unittest.TestCase):
    """Test cases for the AuthenticationManager class."""

    def setUp(self):
        """Set up test environment before each test."""
        self.auth_manager = AuthenticationManager(app_name="test_app")

    def tearDown(self):
        """Clean up after each test."""
        # Clear environment variables
        if "DB_USERNAME" in os.environ:
            del os.environ["DB_USERNAME"]
        if "DB_PASSWORD" in os.environ:
            del os.environ["DB_PASSWORD"]

    def test_get_basic_auth_credentials(self):
        """Test getting basic authentication credentials."""
        credentials = self.auth_manager.get_basic_auth_credentials(
            username="testuser",
            password="testpass"
        )

        self.assertEqual(credentials["auth_type"], self.auth_manager.BASIC_AUTH)
        self.assertEqual(credentials["username"], "testuser")
        self.assertEqual(credentials["password"], "testpass")

    def test_get_env_credentials(self):
        """Test getting credentials from environment variables."""
        # Set environment variables
        os.environ["DB_USERNAME"] = "envuser"
        os.environ["DB_PASSWORD"] = "envpass"

        credentials = self.auth_manager.get_env_credentials()

        self.assertEqual(credentials["auth_type"], self.auth_manager.ENV_AUTH)
        self.assertEqual(credentials["username"], "envuser")
        self.assertEqual(credentials["password"], "envpass")

    def test_get_env_credentials_custom_vars(self):
        """Test getting credentials from custom environment variables."""
        # Set environment variables
        os.environ["CUSTOM_USER"] = "customuser"
        os.environ["CUSTOM_PASS"] = "custompass"

        credentials = self.auth_manager.get_env_credentials(
            username_var="CUSTOM_USER",
            password_var="CUSTOM_PASS"
        )

        self.assertEqual(credentials["auth_type"], self.auth_manager.ENV_AUTH)
        self.assertEqual(credentials["username"], "customuser")
        self.assertEqual(credentials["password"], "custompass")

        # Clean up
        del os.environ["CUSTOM_USER"]
        del os.environ["CUSTOM_PASS"]

    def test_get_env_credentials_missing(self):
        """Test handling missing environment variables."""
        with self.assertRaises(DatabaseConnectionError):
            self.auth_manager.get_env_credentials()

    @patch('keyring.get_password')
    def test_get_keyring_credentials(self, mock_get_password):
        """Test getting credentials from keyring."""
        # Mock keyring.get_password
        mock_get_password.return_value = "keyringpass"

        credentials = self.auth_manager.get_keyring_credentials(
            service_name="test_service",
            username="keyringuser"
        )

        self.assertEqual(credentials["auth_type"], self.auth_manager.KEYRING_AUTH)
        self.assertEqual(credentials["username"], "keyringuser")
        self.assertEqual(credentials["password"], "keyringpass")

        # Verify keyring was called correctly
        mock_get_password.assert_called_once_with("test_service", "keyringuser")

    @patch('keyring.get_password')
    def test_get_keyring_credentials_missing(self, mock_get_password):
        """Test handling missing keyring credentials."""
        # Mock keyring.get_password to return None (no password found)
        mock_get_password.return_value = None

        with self.assertRaises(DatabaseConnectionError):
            self.auth_manager.get_keyring_credentials(
                service_name="test_service",
                username="keyringuser"
            )

    @patch('keyring.set_password')
    def test_set_keyring_credentials(self, mock_set_password):
        """Test setting credentials in keyring."""
        self.auth_manager.set_keyring_credentials(
            service_name="test_service",
            username="keyringuser",
            password="keyringpass"
        )

        # Verify keyring.set_password was called correctly
        mock_set_password.assert_called_once_with(
            "test_service", "keyringuser", "keyringpass"
        )

    def test_get_ssl_credentials(self):
        """Test getting SSL credentials."""
        # Mock file existence
        with patch.object(Path, 'exists', return_value=True):
            with patch.object(Path, 'absolute', return_value=Path("/path/to/cert.pem")):
                credentials = self.auth_manager.get_ssl_credentials(
                    cert_path="/path/to/cert.pem",
                    key_path="/path/to/key.pem",
                    ca_path="/path/to/ca.pem"
                )

                self.assertEqual(credentials["auth_type"], self.auth_manager.SSL_AUTH)
                self.assertEqual(credentials["ssl_cert"], str(Path("/path/to/cert.pem").absolute()))
                self.assertEqual(credentials["ssl_key"], str(Path("/path/to/key.pem").absolute()))
                self.assertEqual(credentials["ssl_ca"], str(Path("/path/to/ca.pem").absolute()))

    def test_get_ssl_credentials_file_not_found(self):
        """Test handling non-existent SSL certificate files."""
        # Mock file existence to return False
        with patch.object(Path, 'exists', return_value=False):
            with self.assertRaises(DatabaseConnectionError):
                self.auth_manager.get_ssl_credentials(
                    cert_path="/path/to/nonexistent.pem"
                )

    def test_get_token_auth_credentials(self):
        """Test getting token-based auth credentials."""
        token = "eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.example"
        credentials = self.auth_manager.get_token_auth_credentials(token)

        self.assertEqual(credentials["auth_type"], self.auth_manager.TOKEN_AUTH)
        self.assertEqual(credentials["token"], token)

    def test_get_iam_credentials(self):
        """Test getting IAM role-based auth credentials."""
        credentials = self.auth_manager.get_iam_credentials(
            role_arn="arn:aws:iam::123456789012:role/example",
            region="us-west-2"
        )

        self.assertEqual(credentials["auth_type"], self.auth_manager.IAM_AUTH)
        self.assertEqual(credentials["role_arn"], "arn:aws:iam::123456789012:role/example")
        self.assertEqual(credentials["region"], "us-west-2")

    def test_encrypt_decrypt_credentials(self):
        """Test encrypting and decrypting credentials."""
        # Create test credentials
        credentials = {
            "auth_type": "basic",
            "username": "testuser",
            "password": "testpass"
        }

        # Encrypt credentials
        encrypted = self.auth_manager.encrypt_credentials(credentials)

        # Generate a key if not already set
        key = self.auth_manager._encryption_key
        self.assertIsNotNone(key)

        # Decrypt credentials
        decrypted = self.auth_manager.decrypt_credentials(encrypted, key)

        # Verify decrypted matches original
        self.assertEqual(decrypted, credentials)

    def test_decrypt_invalid_credentials(self):
        """Test handling invalid encrypted credentials."""
        # Create invalid encrypted data
        invalid_encrypted = base64.urlsafe_b64encode(b"invalid_data").decode()

        # Generate a key
        key = base64.urlsafe_b64encode(os.urandom(32))

        # Attempt to decrypt
        with self.assertRaises(DatabaseConnectionError):
            self.auth_manager.decrypt_credentials(invalid_encrypted, key)

    def test_get_auth_params_basic(self):
        """Test getting auth parameters for basic authentication."""
        credentials = {
            "auth_type": self.auth_manager.BASIC_AUTH,
            "username": "testuser",
            "password": "testpass"
        }

        params = self.auth_manager.get_auth_params(credentials)

        self.assertEqual(params["username"], "testuser")
        self.assertEqual(params["password"], "testpass")

    def test_get_auth_params_ssl(self):
        """Test getting auth parameters for SSL authentication."""
        credentials = {
            "auth_type": self.auth_manager.SSL_AUTH,
            "ssl_cert": "/path/to/cert.pem",
            "ssl_key": "/path/to/key.pem",
            "ssl_ca": "/path/to/ca.pem"
        }

        params = self.auth_manager.get_auth_params(credentials)

        self.assertIn("connect_args", params)
        self.assertIn("ssl", params["connect_args"])
        self.assertEqual(params["connect_args"]["ssl"]["cert"], "/path/to/cert.pem")
        self.assertEqual(params["connect_args"]["ssl"]["key"], "/path/to/key.pem")
        self.assertEqual(params["connect_args"]["ssl"]["ca"], "/path/to/ca.pem")

    def test_get_auth_params_ssl_with_user(self):
        """Test getting auth parameters for SSL with username/password."""
        credentials = {
            "auth_type": self.auth_manager.SSL_AUTH,
            "username": "testuser",
            "password": "testpass",
            "ssl_cert": "/path/to/cert.pem"
        }

        params = self.auth_manager.get_auth_params(credentials)

        self.assertEqual(params["username"], "testuser")
        self.assertEqual(params["password"], "testpass")
        self.assertIn("connect_args", params)
        self.assertIn("ssl", params["connect_args"])
        self.assertEqual(params["connect_args"]["ssl"]["cert"], "/path/to/cert.pem")

    def test_get_auth_params_token(self):
        """Test getting auth parameters for token authentication."""
        credentials = {
            "auth_type": self.auth_manager.TOKEN_AUTH,
            "token": "eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.example"
        }

        params = self.auth_manager.get_auth_params(credentials)

        self.assertEqual(params["password"], "eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.example")

    def test_get_auth_params_iam(self):
        """Test getting auth parameters for IAM authentication."""
        credentials = {
            "auth_type": self.auth_manager.IAM_AUTH,
            "role_arn": "arn:aws:iam::123456789012:role/example",
            "region": "us-west-2"
        }

        params = self.auth_manager.get_auth_params(credentials)

        self.assertEqual(params["aws_role_arn"], "arn:aws:iam::123456789012:role/example")
        self.assertEqual(params["aws_region"], "us-west-2")

    def test_get_auth_params_invalid_type(self):
        """Test handling invalid authentication type."""
        credentials = {
            "auth_type": "invalid_type"
        }

        with self.assertRaises(DatabaseConnectionError):
            self.auth_manager.get_auth_params(credentials)

    def test_get_auth_params_missing_auth_type(self):
        """Test handling credentials without auth_type."""
        credentials = {
            "username": "testuser",
            "password": "testpass"
        }

        with self.assertRaises(DatabaseConnectionError):
            self.auth_manager.get_auth_params(credentials)

    def test_get_auth_params_missing_required_fields(self):
        """Test handling credentials with missing required fields."""
        # Missing username for basic auth
        credentials = {
            "auth_type": self.auth_manager.BASIC_AUTH,
            "password": "testpass"
        }

        with self.assertRaises(DatabaseConnectionError):
            self.auth_manager.get_auth_params(credentials)

        # Missing token for token auth
        credentials = {
            "auth_type": self.auth_manager.TOKEN_AUTH
        }

        with self.assertRaises(DatabaseConnectionError):
            self.auth_manager.get_auth_params(credentials)

        # Missing role_arn for IAM auth
        credentials = {
            "auth_type": self.auth_manager.IAM_AUTH,
            "region": "us-west-2"
        }

        with self.assertRaises(DatabaseConnectionError):
            self.auth_manager.get_auth_params(credentials)


if __name__ == "__main__":
    unittest.main()