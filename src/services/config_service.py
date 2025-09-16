"""
Configuration service for MySQL Replication ETL
"""

import json
import yaml
from typing import Dict, Any
from pathlib import Path

from ..exceptions import ConfigurationError
from ..models.config import ETLConfig


class ConfigService:
    """Service for loading and managing configuration"""
    
    def __init__(self):
        self._config: ETLConfig = None
    
    def load_config(self, config_path: str) -> ETLConfig:
        """Load configuration from file"""
        try:
            config_path = Path(config_path)
            if not config_path.exists():
                raise ConfigurationError(f"Configuration file not found: {config_path}")
            
            with open(config_path, 'r', encoding='utf-8') as f:
                if config_path.suffix.lower() == '.json':
                    config_dict = json.load(f)
                elif config_path.suffix.lower() in ['.yml', '.yaml']:
                    config_dict = yaml.safe_load(f)
                else:
                    raise ConfigurationError(f"Unsupported configuration format: {config_path.suffix}")
            
            self._config = ETLConfig.from_dict(config_dict)
            return self._config
            
        except json.JSONDecodeError as e:
            raise ConfigurationError(f"Invalid JSON configuration: {e}")
        except yaml.YAMLError as e:
            raise ConfigurationError(f"Invalid YAML configuration: {e}")
        except Exception as e:
            raise ConfigurationError(f"Error loading configuration: {e}")
    
    def get_config(self) -> ETLConfig:
        """Get current configuration"""
        if self._config is None:
            raise ConfigurationError("Configuration not loaded")
        return self._config
    
    def validate_config(self, config: ETLConfig) -> bool:
        """Validate configuration"""
        try:
            # Test all source connection parameters
            for source_name, source_config in config.sources.items():
                source_params = source_config.to_connection_params()
                if not all(key in source_params for key in ['host', 'port', 'user', 'password']):
                    return False
            
            # Test all target connection parameters
            for target_name, target_config in config.targets.items():
                target_params = target_config.to_connection_params()
                if not all(key in target_params for key in ['host', 'port', 'user', 'password', 'database']):
                    return False
            
            # Validate table mappings
            for table_key, table_mapping in config.mapping.items():
                if not table_mapping.target_table or not table_mapping.primary_key:
                    return False
                
                # Check if primary key exists in column mapping
                if table_mapping.primary_key not in table_mapping.column_mapping:
                    return False
                
                # Validate target table format (target_name.table_name)
                try:
                    config.parse_target_table(table_mapping.target_table)
                except ConfigurationError:
                    return False
            
            return True
            
        except Exception:
            return False
