"""
Configuration models for MySQL Replication ETL
"""

from dataclasses import dataclass
from typing import Dict, Any, Optional, List, Tuple
from ..exceptions import ConfigurationError


@dataclass
class DatabaseConfig:
    """Database connection configuration"""
    host: str
    port: int = 3306
    user: str = ""
    password: str = ""
    database: str = ""
    charset: str = "utf8mb4"
    autocommit: bool = False
    
    def __post_init__(self):
        """Validate configuration after initialization"""
        if not self.host:
            raise ConfigurationError("Host is required")
        if not self.user:
            raise ConfigurationError("User is required")
        if not self.password:
            raise ConfigurationError("Password is required")
        if not self.database:
            raise ConfigurationError("Database is required")
        if not (1 <= self.port <= 65535):
            raise ConfigurationError("Port must be between 1 and 65535")
    
    def to_connection_params(self) -> Dict[str, Any]:
        """Convert to pymysql connection parameters"""
        return {
            'host': self.host,
            'port': self.port,
            'user': self.user,
            'password': self.password,
            'database': self.database,
            'charset': self.charset,
            'autocommit': self.autocommit
        }


@dataclass
class ReplicationConfig:
    """Replication stream configuration"""
    server_id: int = 100
    log_file: Optional[str] = None
    log_pos: int = 4
    resume_stream: bool = True
    blocking: bool = True
    only_events: Optional[List[str]] = None
    
    def __post_init__(self):
        """Validate configuration after initialization"""
        if self.server_id <= 0:
            raise ConfigurationError("Server ID must be positive")


@dataclass
class ColumnMapping:
    """Column mapping configuration"""
    column: str
    primary_key: bool = False
    transform: Optional[str] = None
    value: Optional[Any] = None
    
    def __post_init__(self):
        """Validate configuration after initialization"""
        if not self.column:
            raise ConfigurationError("Column name is required")
        if self.transform and self.value is not None:
            raise ConfigurationError("Cannot specify both transform and static value")


@dataclass
class TableMapping:
    """Table mapping configuration"""
    target_table: str
    primary_key: str
    column_mapping: Dict[str, ColumnMapping]
    
    def __post_init__(self):
        """Validate configuration after initialization"""
        if not self.target_table:
            raise ConfigurationError("Target table is required")
        if not self.primary_key:
            raise ConfigurationError("Primary key is required")
        if not self.column_mapping:
            raise ConfigurationError("Column mapping is required")


@dataclass
class ETLConfig:
    """Main ETL configuration"""
    sources: Dict[str, DatabaseConfig]
    targets: Dict[str, DatabaseConfig]
    replication: ReplicationConfig
    mapping: Dict[str, TableMapping]
    monitoring: Optional[Dict[str, Any]] = None
    
    def __post_init__(self):
        """Validate configuration after initialization"""
        if not self.sources:
            raise ConfigurationError("At least one source is required")
        if not self.targets:
            raise ConfigurationError("At least one target is required")
        if not self.mapping:
            raise ConfigurationError("Table mapping is required")
    
    @classmethod
    def from_dict(cls, config_dict: Dict[str, Any]) -> 'ETLConfig':
        """Create ETLConfig from dictionary"""
        try:
            # Convert sources config
            sources = {}
            sources_data = config_dict.get('sources', {})
            if not isinstance(sources_data, dict):
                raise ConfigurationError("Sources must be a dictionary")
            for source_name, source_config in sources_data.items():
                sources[source_name] = DatabaseConfig(**source_config)
            
            # Convert targets config
            targets = {}
            targets_data = config_dict.get('targets', {})
            if not isinstance(targets_data, dict):
                raise ConfigurationError("Targets must be a dictionary")
            for target_name, target_config in targets_data.items():
                targets[target_name] = DatabaseConfig(**target_config)
            
            # Convert replication config
            replication_config = ReplicationConfig(**config_dict.get('replication', {}))
            
            # Convert mapping
            mapping = {}
            for table_key, table_config in config_dict.get('mapping', {}).items():
                column_mapping = {}
                for col_key, col_config in table_config.get('column_mapping', {}).items():
                    if isinstance(col_config, dict):
                        column_mapping[col_key] = ColumnMapping(**col_config)
                    else:
                        # Simple string mapping
                        column_mapping[col_key] = ColumnMapping(column=col_config)
                
                mapping[table_key] = TableMapping(
                    target_table=table_config['target_table'],
                    primary_key=table_config['primary_key'],
                    column_mapping=column_mapping
                )
            
            return cls(
                sources=sources,
                targets=targets,
                replication=replication_config,
                mapping=mapping,
                monitoring=config_dict.get('monitoring')
            )
        except KeyError as e:
            raise ConfigurationError(f"Missing required configuration key: {e}")
        except TypeError as e:
            raise ConfigurationError(f"Invalid configuration format: {e}")
    
    def get_source_config(self, source_name: str) -> DatabaseConfig:
        """Get source configuration by name"""
        if source_name not in self.sources:
            raise ConfigurationError(f"Source '{source_name}' not found")
        return self.sources[source_name]
    
    def get_target_config(self, target_name: str) -> DatabaseConfig:
        """Get target configuration by name"""
        if target_name not in self.targets:
            raise ConfigurationError(f"Target '{target_name}' not found")
        return self.targets[target_name]
    
    def parse_target_table(self, target_table: str) -> Tuple[str, str]:
        """Parse target table string like 'target1.users' into (target_name, table_name)"""
        if '.' not in target_table:
            raise ConfigurationError(f"Target table must be in format 'target_name.table_name', got: {target_table}")
        
        target_name, table_name = target_table.split('.', 1)
        if target_name not in self.targets:
            raise ConfigurationError(f"Target '{target_name}' not found in configuration")
        
        return target_name, table_name
