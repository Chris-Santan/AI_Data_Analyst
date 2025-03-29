import os
import base64
from pathlib import Path
from typing import Dict, Any, Optional
from dotenv import load_dotenv
import keyring
from cryptography.fernet import Fernet
from cryptography.hazmat.primitives import hashes
from cryptography.hazmat.primitives.kdf.pbkdf2 import PBKDF2HMAC

from data_analytics_platform.core.exceptions.custom_exceptions import DatabaseConnectionError


class AuthenticationManager:
    """
    Manages database authentication credentials with support for multiple
    authentication methods and secure credential storage.
    """

    # Constants for authentication methods
    BASIC_AUTH = "basic"
    ENV_AUTH = "environment"
    KEYRING_AUTH = "keyring"
    SSL_AUTH = "ssl"
    IAM_AUTH = "iam"
    TOKEN_AUTH = "token"

    def __init__(self, app_name: str = "data_analytics_platform"):
        """
        Initialize the authentication manager.

        Args:
            app_name (str): Name of the application for keyring storage
        """
        self.app_name = app_name
        self._encryption_key = None

        # Try to load environment variables from .env file if it exists
        env_path = Path('.env')
        if env_path.exists():
            load_dotenv(dotenv_path=env_path)

    def get_basic_auth_credentials(self, username: str, password: str) -> Dict[str, str]:
        """
        Get basic username/password credentials.

        Args:
            username (str): Database username
            password (str): Database password

        Returns:
            Dict[str, str]: Credential dictionary
        """
        return {
            "auth_type": self.BASIC_AUTH,
            "username": username,
            "password": password
        }

    def get_env_credentials(self,
                            username_var: str = "DB_USERNAME",
                            password_var: str = "DB_PASSWORD") -> Dict[str, str]:
        """
        Get credentials from environment variables.

        Args:
            username_var (str): Environment variable name for username
            password_var (str): Environment variable name for password

        Returns:
            Dict[str, str]: Credential dictionary

        Raises:
            DatabaseConnectionError: If environment variables are not set
        """
        username = os.getenv(username_var)
        password = os.getenv(password_var)

        if not username or not password:
            raise DatabaseConnectionError(
                f"Environment variables {username_var} and/or {password_var} not set"
            )

        return {
            "auth_type": self.ENV_AUTH,
            "username": username,
            "password": password
        }

    def get_keyring_credentials(self, service_name: str, username: str) -> Dict[str, str]:
        """
        Get credentials from system keyring.

        Args:
            service_name (str): Name of the service in keyring
            username (str): Username to retrieve password for

        Returns:
            Dict[str, str]: Credential dictionary

        Raises:
            DatabaseConnectionError: If credentials are not found in keyring
        """
        password = keyring.get_password(service_name, username)

        if not password:
            raise DatabaseConnectionError(
                f"No password found in keyring for {service_name}/{username}"
            )

        return {
            "auth_type": self.KEYRING_AUTH,
            "username": username,
            "password": password
        }

    def set_keyring_credentials(self, service_name: str, username: str, password: str) -> None:
        """
        Store credentials in system keyring.

        Args:
            service_name (str): Name of the service in keyring
            username (str): Username to store
            password (str): Password to store
        """
        keyring.set_password(service_name, username, password)

    def get_ssl_credentials(self,
                            cert_path: str,
                            key_path: Optional[str] = None,
                            ca_path: Optional[str] = None) -> Dict[str, str]:
        """
        Get SSL certificate credentials.

        Args:
            cert_path (str): Path to SSL certificate file
            key_path (Optional[str]): Path to SSL key file
            ca_path (Optional[str]): Path to CA certificate file

        Returns:
            Dict[str, str]: Credential dictionary

        Raises:
            DatabaseConnectionError: If certificate files don't exist
        """
        cert_path_obj = Path(cert_path)
        if not cert_path_obj.exists():
            raise DatabaseConnectionError(f"SSL certificate not found at {cert_path}")

        credentials = {
            "auth_type": self.SSL_AUTH,
            "ssl_cert": str(cert_path_obj.absolute())
        }

        if key_path:
            key_path_obj = Path(key_path)
            if not key_path_obj.exists():
                raise DatabaseConnectionError(f"SSL key not found at {key_path}")
            credentials["ssl_key"] = str(key_path_obj.absolute())

        if ca_path:
            ca_path_obj = Path(ca_path)
            if not ca_path_obj.exists():
                raise DatabaseConnectionError(f"CA certificate not found at {ca_path}")
            credentials["ssl_ca"] = str(ca_path_obj.absolute())

        return credentials

    def get_token_auth_credentials(self, token: str) -> Dict[str, str]:
        """
        Get token-based authentication credentials.

        Args:
            token (str): Authentication token

        Returns:
            Dict[str, str]: Credential dictionary
        """
        return {
            "auth_type": self.TOKEN_AUTH,
            "token": token
        }

    def get_iam_credentials(self, role_arn: str, region: str) -> Dict[str, str]:
        """
        Get IAM role-based authentication credentials.

        Args:
            role_arn (str): ARN of the IAM role
            region (str): AWS region

        Returns:
            Dict[str, str]: Credential dictionary
        """
        return {
            "auth_type": self.IAM_AUTH,
            "role_arn": role_arn,
            "region": region
        }

    def encrypt_credentials(self, credentials: Dict[str, str],
                            key: Optional[str] = None) -> str:
        """
        Encrypt credentials for secure storage.

        Args:
            credentials (Dict[str, str]): Credentials to encrypt
            key (Optional[str]): Encryption key, generates one if not provided

        Returns:
            str: Encrypted credentials as a string
        """
        if not key:
            # Generate key if not provided
            if not self._encryption_key:
                salt = os.urandom(16)
                kdf = PBKDF2HMAC(
                    algorithm=hashes.SHA256(),
                    length=32,
                    salt=salt,
                    iterations=100000,
                )
                key_material = base64.urlsafe_b64encode(os.urandom(32))
                self._encryption_key = base64.urlsafe_b64encode(
                    kdf.derive(key_material)
                )
            key = self._encryption_key

        # Convert credentials to string
        credential_str = str(credentials)

        # Create Fernet cipher
        cipher = Fernet(key)

        # Encrypt credentials
        encrypted_credentials = cipher.encrypt(credential_str.encode())

        return base64.urlsafe_b64encode(encrypted_credentials).decode()

    def decrypt_credentials(self, encrypted_credentials: str, key: str) -> Dict[str, str]:
        """
        Decrypt stored credentials.

        Args:
            encrypted_credentials (str): Encrypted credentials
            key (str): Decryption key

        Returns:
            Dict[str, str]: Decrypted credentials

        Raises:
            DatabaseConnectionError: If decryption fails
        """
        try:
            # Create Fernet cipher
            cipher = Fernet(key)

            # Decrypt credentials
            decrypted = cipher.decrypt(
                base64.urlsafe_b64decode(encrypted_credentials)
            )

            # Convert back to dictionary
            return eval(decrypted.decode())
        except Exception as e:
            raise DatabaseConnectionError(
                f"Failed to decrypt credentials: {str(e)}"
            ) from e

    def get_auth_params(self, credentials: Dict[str, str]) -> Dict[str, Any]:
        """
        Get SQLAlchemy connection parameters from credentials.

        Args:
            credentials (Dict[str, str]): Authentication credentials

        Returns:
            Dict[str, Any]: Connection parameters for SQLAlchemy

        Raises:
            DatabaseConnectionError: If credential format is invalid
        """
        if "auth_type" not in credentials:
            raise DatabaseConnectionError("Invalid credential format: missing auth_type")

        auth_type = credentials["auth_type"]

        if auth_type in [self.BASIC_AUTH, self.ENV_AUTH, self.KEYRING_AUTH]:
            # Username/password authentication
            if "username" not in credentials or "password" not in credentials:
                raise DatabaseConnectionError(
                    f"Invalid {auth_type} credentials: missing username or password"
                )
            return {
                "username": credentials["username"],
                "password": credentials["password"]
            }

        elif auth_type == self.SSL_AUTH:
            # SSL certificate authentication
            connect_args = {"ssl": {}}

            if "ssl_cert" in credentials:
                connect_args["ssl"]["cert"] = credentials["ssl_cert"]

            if "ssl_key" in credentials:
                connect_args["ssl"]["key"] = credentials["ssl_key"]

            if "ssl_ca" in credentials:
                connect_args["ssl"]["ca"] = credentials["ssl_ca"]

            if "username" in credentials and "password" in credentials:
                return {
                    "username": credentials["username"],
                    "password": credentials["password"],
                    "connect_args": connect_args
                }
            else:
                return {"connect_args": connect_args}

        elif auth_type == self.TOKEN_AUTH:
            # Token-based authentication
            if "token" not in credentials:
                raise DatabaseConnectionError(
                    "Invalid token credentials: missing token"
                )
            return {
                "password": credentials["token"]
            }

        elif auth_type == self.IAM_AUTH:
            # IAM role-based authentication
            if "role_arn" not in credentials or "region" not in credentials:
                raise DatabaseConnectionError(
                    "Invalid IAM credentials: missing role_arn or region"
                )
            return {
                "aws_role_arn": credentials["role_arn"],
                "aws_region": credentials["region"]
            }

        else:
            raise DatabaseConnectionError(f"Unsupported authentication type: {auth_type}")