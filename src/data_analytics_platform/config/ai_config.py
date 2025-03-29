# src/config/ai_config.py
from typing import Dict, Any, Optional

# Fix the import path
from data_analytics_platform.config.base_config import BaseConfig
# or use relative import:
# from .base_config import BaseConfig


class AIConfig(BaseConfig):
    """
    Configuration for AI module.
    Contains settings for LLM providers, API keys, model parameters, etc.
    """

    # Default model providers
    OPENAI = "openai"
    ANTHROPIC = "anthropic"
    HUGGINGFACE = "huggingface"
    AZURE = "azure"

    def __init__(self, env_prefix: str = "AI"):
        """
        Initialize AI configuration.

        Args:
            env_prefix (str): Prefix for environment variables
        """
        super().__init__("ai", env_prefix)

        # Default configuration
        self._default_config = {
            'provider': 'openai',
            'api_key_env_var': 'OPENAI_API_KEY',
            'model': 'gpt-3.5-turbo',
            'temperature': 0.7,
            'max_tokens': 1000,
            'timeout': 30,
            'retry_count': 3,
            'cache_enabled': True,
            'cache_dir': '.cache/ai',
            'providers': {
                'openai': {
                    'api_base': 'https://api.openai.com/v1',
                    'models': {
                        'embedding': 'text-embedding-ada-002',
                        'chat': 'gpt-3.5-turbo',
                        'completion': 'text-davinci-003'
                    }
                },
                'anthropic': {
                    'api_base': 'https://api.anthropic.com',
                    'models': {
                        'chat': 'claude-2'
                    }
                },
                'azure': {
                    'api_type': 'azure',
                    'api_version': '2023-05-15',
                    'models': {
                        'embedding': 'text-embedding-ada-002',
                        'chat': 'gpt-35-turbo',
                        'completion': 'text-davinci-003'
                    }
                }
            }
        }

    def get_provider_config(self, provider: Optional[str] = None) -> Dict[str, Any]:
        """
        Get configuration for a specific provider.

        Args:
            provider (Optional[str]): Provider name (uses default if None)

        Returns:
            Dict[str, Any]: Provider configuration

        Raises:
            ValueError: If provider is not found
        """
        if not provider:
            provider = self.get('provider')

        providers = self.get('providers', {})
        if provider not in providers:
            raise ValueError(f"Provider not found: {provider}")

        return providers[provider]

    def get_api_key(self, provider: Optional[str] = None) -> str:
        """
        Get API key for a provider.

        Args:
            provider (Optional[str]): Provider name (uses default if None)

        Returns:
            str: API key from environment variable

        Raises:
            ValueError: If API key is not found
        """
        if not provider:
            provider = self.get('provider')

        # Get the environment variable name for the API key
        api_key_var = self.get('api_key_env_var')
        if provider != self.get('provider'):
            # If a different provider is specified, try to get its API key env var
            provider_config = self.get_provider_config(provider)
            api_key_var = provider_config.get('api_key_env_var', f"{provider.upper()}_API_KEY")

        # Get the API key from environment
        import os
        api_key = os.getenv(api_key_var)

        if not api_key:
            raise ValueError(f"API key not found for provider {provider}. "
                             f"Set the {api_key_var} environment variable.")

        return api_key

    def get_model_config(self, model_type: str, provider: Optional[str] = None) -> Dict[str, Any]:
        """
        Get configuration for a specific model type.

        Args:
            model_type (str): Model type (embedding, chat, completion)
            provider (Optional[str]): Provider name (uses default if None)

        Returns:
            Dict[str, Any]: Model configuration

        Raises:
            ValueError: If model type is not found
        """
        if not provider:
            provider = self.get('provider')

        provider_config = self.get_provider_config(provider)
        models = provider_config.get('models', {})

        if model_type not in models:
            raise ValueError(f"Model type not found: {model_type}")

        return {
            'name': models[model_type],
            'provider': provider,
            'temperature': self.get('temperature'),
            'max_tokens': self.get('max_tokens'),
            'timeout': self.get('timeout')
        }