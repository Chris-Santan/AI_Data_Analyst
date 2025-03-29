# src/config/base_config.py
import os
import json
import yaml
from pathlib import Path
from typing import Dict, Any, Optional, Union
from dotenv import load_dotenv


class BaseConfig:
    """
    Base configuration class with common functionality for all configuration components.
    Supports loading from environment variables, config files (JSON, YAML), and defaults.
    """

    def __init__(self, config_name: str, env_prefix: str = ""):
        """
        Initialize base configuration.

        Args:
            config_name (str): Name of this configuration component
            env_prefix (str): Prefix for environment variables
        """
        self.config_name = config_name
        self.env_prefix = env_prefix
        self._config_data = {}
        self._config_file_path = None

        # Try to load environment variables from .env file if it exists
        env_path = Path('.env')
        if env_path.exists():
            load_dotenv(dotenv_path=env_path)

    def load_from_env(self) -> Dict[str, Any]:
        """
        Load configuration from environment variables.

        Returns:
            Dict[str, Any]: Configuration values from environment variables
        """
        env_config = {}
        prefix = f"{self.env_prefix}_" if self.env_prefix else ""

        for key, value in os.environ.items():
            # Check if the environment variable starts with our prefix
            if self.env_prefix and key.startswith(prefix):
                # Remove prefix from key
                config_key = key[len(prefix):]
                env_config[config_key.lower()] = self._parse_env_value(value)
            elif not self.env_prefix:
                # If no prefix is set, include all environment variables
                env_config[key.lower()] = self._parse_env_value(value)

        return env_config

    def _parse_env_value(self, value: str) -> Any:
        """
        Parse environment variable value to appropriate type.

        Args:
            value (str): Environment variable value

        Returns:
            Any: Parsed value
        """
        # Try to convert to appropriate data type
        if value.lower() in ('true', 'yes', '1'):
            return True
        elif value.lower() in ('false', 'no', '0'):
            return False
        elif value.isdigit():
            return int(value)
        elif value.replace('.', '', 1).isdigit() and value.count('.') <= 1:
            return float(value)
        else:
            return value

    def load_from_file(self, file_path: Union[str, Path]) -> Dict[str, Any]:
        """
        Load configuration from a file.

        Args:
            file_path (Union[str, Path]): Path to configuration file

        Returns:
            Dict[str, Any]: Configuration values from file

        Raises:
            ValueError: If file doesn't exist or format is not supported
        """
        path = Path(file_path) if isinstance(file_path, str) else file_path
        if not path.exists():
            raise ValueError(f"Configuration file not found: {path}")

        suffix = path.suffix.lower()

        with open(path, 'r') as f:
            if suffix == '.json':
                return json.load(f)
            elif suffix in ('.yml', '.yaml'):
                return yaml.safe_load(f)
            else:
                raise ValueError(f"Unsupported configuration file format: {suffix}")

    def set_config_file(self, file_path: Union[str, Path]) -> None:
        """
        Set the configuration file path.

        Args:
            file_path (Union[str, Path]): Path to configuration file

        Raises:
            ValueError: If file doesn't exist
        """
        path = Path(file_path) if isinstance(file_path, str) else file_path
        if not path.exists():
            raise ValueError(f"Configuration file not found: {path}")
        self._config_file_path = path

    def load_config(self, defaults: Optional[Dict[str, Any]] = None,
                    config_file: Optional[Union[str, Path]] = None,
                    env_override: bool = True) -> Dict[str, Any]:
        """
        Load configuration from defaults, file, and environment variables.

        Args:
            defaults (Optional[Dict[str, Any]]): Default configuration values
            config_file (Optional[Union[str, Path]]): Path to configuration file
            env_override (bool): Whether environment variables should override file values

        Returns:
            Dict[str, Any]: Combined configuration
        """
        # Start with defaults
        config = defaults.copy() if defaults else {}

        # Load from file if provided
        if config_file:
            self.set_config_file(config_file)

        if self._config_file_path:
            file_config = self.load_from_file(self._config_file_path)
            # Update config with file values
            for key, value in file_config.items():
                config[key] = value

        # Load from environment if set to override
        if env_override:
            env_config = self.load_from_env()
            # Update config with environment values
            for key, value in env_config.items():
                config[key] = value

        # Store the configuration
        self._config_data = config
        return config

    def get(self, key: str, default: Any = None) -> Any:
        """
        Get a configuration value.

        Args:
            key (str): Configuration key
            default (Any): Default value if key is not found

        Returns:
            Any: Configuration value
        """
        return self._config_data.get(key, default)

    def set(self, key: str, value: Any) -> None:
        """
        Set a configuration value.

        Args:
            key (str): Configuration key
            value (Any): Configuration value
        """
        self._config_data[key] = value

    def as_dict(self) -> Dict[str, Any]:
        """
        Get the entire configuration as a dictionary.

        Returns:
            Dict[str, Any]: Configuration dictionary
        """
        return self._config_data.copy()