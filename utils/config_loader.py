"""
utils/config_loader.py
Configuration loader with validation and environment variable substitution
"""

import os
import yaml
import re
from typing import Dict, Any, Optional
from pathlib import Path

class ConfigurationError(Exception):
    """Raised when configuration is invalid"""
    pass

class ConfigLoader:
    """Loads and validates YAML configuration"""
    
    def __init__(self, config_path: str = "config/settings.yaml"):
        """
        Initialize configuration loader
        
        Args:
            config_path: Path to YAML configuration file
        """
        self.config_path = Path(config_path)
        if not self.config_path.exists():
            raise FileNotFoundError(f"Configuration file not found: {config_path}")
        
        self._config = None
        self._environment = os.getenv('ENVIRONMENT', 'dev')
    
    def load(self, environment: Optional[str] = None) -> Dict[str, Any]:
        """
        Load configuration for specified environment
        
        Args:
            environment: Environment name (dev/staging/prod)
            
        Returns:
            Configuration dictionary
        """
        if environment:
            self._environment = environment
        
        # Load YAML file
        with open(self.config_path, 'r') as f:
            raw_config = yaml.safe_load(f)
        
        # Substitute environment variables
        config = self._substitute_env_vars(raw_config)
        
        # Merge global and environment-specific settings
        merged_config = self._merge_configs(config)
        
        # Validate configuration
        self._validate(merged_config)
        
        self._config = merged_config
        return merged_config
    
    def _substitute_env_vars(self, config: Dict) -> Dict:
        """
        Recursively substitute ${VAR_NAME} with environment variable values
        
        Example: "${DEV_SUBSCRIPTION_ID}" -> actual subscription ID from env
        """
        if isinstance(config, dict):
            return {k: self._substitute_env_vars(v) for k, v in config.items()}
        elif isinstance(config, list):
            return [self._substitute_env_vars(item) for item in config]
        elif isinstance(config, str):
            # Find all ${VAR_NAME} patterns
            pattern = r'\$\{([^}]+)\}'
            matches = re.findall(pattern, config)
            
            result = config
            for var_name in matches:
                env_value = os.getenv(var_name)
                if env_value is None:
                    print(f"Warning: Environment variable {var_name} not set, using placeholder")
                    env_value = f"${{{var_name}}}"
                result = result.replace(f"${{{var_name}}}", env_value)
            
            return result
        else:
            return config
    
    def _merge_configs(self, config: Dict) -> Dict:
        """Merge global settings with environment-specific settings"""
        
        # Start with global settings (excluding 'environments' key)
        merged = {k: v for k, v in config.items() if k != 'environments'}
        
        # Get environment-specific config
        if 'environments' not in config:
            raise ConfigurationError("No 'environments' section in config")
        
        if self._environment not in config['environments']:
            raise ConfigurationError(
                f"Environment '{self._environment}' not found. "
                f"Available: {list(config['environments'].keys())}"
            )
        
        env_config = config['environments'][self._environment]
        
        # Deep merge environment config (environment settings override global)
        merged = self._deep_merge(merged, env_config)
        
        # Add metadata
        merged['_metadata'] = {
            'environment': self._environment,
            'loaded_at': self._get_timestamp(),
            'config_path': str(self.config_path)
        }
        
        return merged
    
    def _deep_merge(self, base: Dict, override: Dict) -> Dict:
        """Deep merge two dictionaries"""
        result = base.copy()
        
        for key, value in override.items():
            if key in result and isinstance(result[key], dict) and isinstance(value, dict):
                result[key] = self._deep_merge(result[key], value)
            else:
                result[key] = value
        
        return result
    
    def _validate(self, config: Dict):
        """Validate required configuration fields"""
        
        required_fields = [
            'azure.resource_group',
            'storage.account_name',
            'keyvault.name',
            'eventhub.namespace',
            'databricks.workspace_url'
        ]
        
        for field_path in required_fields:
            if not self._get_nested_value(config, field_path):
                raise ConfigurationError(f"Required field missing: {field_path}")
    
    def _get_nested_value(self, config: Dict, path: str) -> Any:
        """Get nested dictionary value using dot notation"""
        keys = path.split('.')
        value = config
        
        for key in keys:
            if isinstance(value, dict) and key in value:
                value = value[key]
            else:
                return None
        
        return value
    
    def _get_timestamp(self) -> str:
        """Get current timestamp"""
        from datetime import datetime
        return datetime.utcnow().isoformat()
    
    def get(self, path: str, default: Any = None) -> Any:
        """
        Get configuration value using dot notation
        
        Example:
            config.get('azure.resource_group')
            config.get('databricks.cluster.min_workers', default=2)
        """
        if self._config is None:
            self.load()
        
        value = self._get_nested_value(self._config, path)
        return value if value is not None else default
    
    def __getitem__(self, key: str) -> Any:
        """Allow dictionary-style access"""
        if self._config is None:
            self.load()
        return self._config[key]
    
    def to_dict(self) -> Dict:
        """Return full configuration as dictionary"""
        if self._config is None:
            self.load()
        return self._config

# Singleton instance
_config_loader = None

def get_config(environment: Optional[str] = None, config_path: str = "config/settings.yaml") -> Dict[str, Any]:
    """
    Get configuration (singleton pattern)
    
    Args:
        environment: Environment name (dev/staging/prod)
        config_path: Path to config file
        
    Returns:
        Configuration dictionary
    """
    global _config_loader
    
    if _config_loader is None:
        _config_loader = ConfigLoader(config_path)
    
    return _config_loader.load(environment)

def reload_config(environment: Optional[str] = None):
    """Force reload configuration"""
    global _config_loader
    _config_loader = None
    return get_config(environment)