# src/config/logging_config.py
import logging
import logging.config
from pathlib import Path
from typing import Optional, Union

# Fix the import path
from data_analytics_platform.config.base_config import BaseConfig
# or use relative import:
# from .base_config import BaseConfig


class LoggingConfig(BaseConfig):
    """
    Configuration for application logging.
    Supports different log levels, formats, and outputs.
    """

    # Default log levels
    DEBUG = logging.DEBUG
    INFO = logging.INFO
    WARNING = logging.WARNING
    ERROR = logging.ERROR
    CRITICAL = logging.CRITICAL

    # Default log format
    DEFAULT_FORMAT = '%(asctime)s - %(name)s - %(levelname)s - %(message)s'

    def __init__(self, env_prefix: str = "LOG"):
        """
        Initialize logging configuration.

        Args:
            env_prefix (str): Prefix for environment variables
        """
        super().__init__("logging", env_prefix)

        # Default configuration
        self._default_config = {
            'version': 1,
            'disable_existing_loggers': False,
            'formatters': {
                'standard': {
                    'format': self.DEFAULT_FORMAT
                },
                'detailed': {
                    'format': '%(asctime)s - %(name)s - %(levelname)s - %(pathname)s:%(lineno)d - %(message)s'
                }
            },
            'handlers': {
                'console': {
                    'class': 'logging.StreamHandler',
                    'level': 'INFO',
                    'formatter': 'standard',
                    'stream': 'ext://sys.stdout'
                },
                'file': {
                    'class': 'logging.handlers.RotatingFileHandler',
                    'level': 'DEBUG',
                    'formatter': 'detailed',
                    'filename': 'logs/app.log',
                    'maxBytes': 10485760,  # 10MB
                    'backupCount': 5
                },
                'error_file': {
                    'class': 'logging.handlers.RotatingFileHandler',
                    'level': 'ERROR',
                    'formatter': 'detailed',
                    'filename': 'logs/error.log',
                    'maxBytes': 10485760,  # 10MB
                    'backupCount': 5
                }
            },
            'loggers': {
                '': {  # root logger
                    'handlers': ['console', 'file', 'error_file'],
                    'level': 'DEBUG',
                    'propagate': True
                }
            }
        }

    def configure(self, config_file: Optional[Union[str, Path]] = None) -> None:
        """
        Configure logging system.

        Args:
            config_file (Optional[Union[str, Path]]): Path to logging configuration file
        """
        # Load configuration
        config = self.load_config(
            defaults=self._default_config,
            config_file=config_file,
            env_override=True
        )

        # Create logs directory if it doesn't exist
        logs_dir = Path('logs')
        logs_dir.mkdir(exist_ok=True)

        # Apply configuration
        logging.config.dictConfig(config)

        logging.info(f"Logging configured with level: {self.get_root_level()}")

    def get_root_level(self) -> str:
        """
        Get the root logger level name.

        Returns:
            str: Level name (DEBUG, INFO, etc.)
        """
        return logging.getLevelName(logging.getLogger().level)

    def set_level(self, logger_name: str = '', level: Union[int, str] = logging.INFO) -> None:
        """
        Set log level for a specific logger.

        Args:
            logger_name (str): Logger name (empty for root logger)
            level (Union[int, str]): Log level (can be name or level number)
        """
        if isinstance(level, str):
            level = getattr(logging, level.upper(), logging.INFO)

        logging.getLogger(logger_name).setLevel(level)

    def get_logger(self, name: str) -> logging.Logger:
        """
        Get a logger with the specified name.

        Args:
            name (str): Logger name

        Returns:
            logging.Logger: Configured logger
        """
        return logging.getLogger(name)