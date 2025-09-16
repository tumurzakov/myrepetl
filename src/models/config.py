"""
Configuration models for MySQL Replication ETL
"""

from dataclasses import dataclass
from typing import Dict, Any, Optional, List
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
    source: DatabaseConfig
    target: DatabaseConfig
    replication: ReplicationConfig
    mapping: Dict[str, TableMapping]
    monitoring: Optional[Dict[str, Any]] = None
    
    def __post_init__(self):
        """Validate configuration after initialization"""
        if not self.mapping:
            raise ConfigurationError("Table mapping is required")
    
    @classmethod
    def from_dict(cls, config_dict: Dict[str, Any]) -> 'ETLConfig':
        """Create ETLConfig from dictionary"""
        try:
            # Convert source config
            source_config = DatabaseConfig(**config_dict['source'])
            
            # Convert target config
            target_config = DatabaseConfig(**config_dict['target'])
            
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
                source=source_config,
                target=target_config,
                replication=replication_config,
                mapping=mapping,
                monitoring=config_dict.get('monitoring')
            )
        except KeyError as e:
            raise ConfigurationError(f"Missing required configuration key: {e}")
        except TypeError as e:
            raise ConfigurationError(f"Invalid configuration format: {e}")
