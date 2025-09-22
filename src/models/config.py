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
    filter: Optional[Dict[str, Any]] = None
    init_query: Optional[str] = None
    init_if_table_empty: bool = True  # Default to True for backward compatibility
    source_table: Optional[str] = None
    source: Optional[str] = None
    target: Optional[str] = None
    
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
                    column_mapping=column_mapping,
                    filter=table_config.get('filter'),
                    init_query=table_config.get('init_query'),
                    init_if_table_empty=table_config.get('init_if_table_empty', True),
                    source_table=table_config.get('source_table'),
                    source=table_config.get('source'),
                    target=table_config.get('target')
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
    
    
    def get_mapping_by_source_table(self, source_table: str) -> Optional[TableMapping]:
        """Get table mapping by source_table field"""
        for mapping in self.mapping.values():
            if mapping.source_table == source_table:
                return mapping
        return None
    
    def get_mapping_by_source_and_table(self, source_name: str, schema: str, table: str) -> Optional[TableMapping]:
        """Get table mapping by source name, schema and table"""
        for mapping in self.mapping.values():
            # Check if mapping has explicit source field and matches
            if mapping.source == source_name and mapping.source_table:
                if '.' in mapping.source_table:
                    parts = mapping.source_table.split('.')
                    if len(parts) == 2:
                        mapping_schema, mapping_table = parts
                        if mapping_schema == schema and mapping_table == table:
                            return mapping
                    elif len(parts) == 3:
                        mapping_source, mapping_schema, mapping_table = parts
                        if mapping_source == source_name and mapping_schema == schema and mapping_table == table:
                            return mapping
                elif mapping.source_table == table:
                    # Use source config database as schema
                    source_config = self.get_source_config(source_name)
                    if source_config.database == schema:
                        return mapping
        return None
    
    def get_target_table_name(self, source_name: str, schema: str, table: str) -> Optional[str]:
        """Get target table name for a given source table"""
        mapping = self.get_mapping_by_source_and_table(source_name, schema, table)
        if mapping and mapping.target and mapping.target_table:
            # Build full target table name: target.database.table
            target_config = self.get_target_config(mapping.target)
            return f"{target_config.database}.{mapping.target_table}"
        return None
    
    def parse_source_table(self, source_table: str) -> Tuple[str, str]:
        """Parse source table string like 'source1.users' into (source_name, table_name)"""
        if '.' not in source_table:
            raise ConfigurationError(f"Source table must be in format 'source_name.table_name', got: {source_table}")
        
        source_name, table_name = source_table.split('.', 1)
        if source_name not in self.sources:
            raise ConfigurationError(f"Source '{source_name}' not found in configuration")
        
        return source_name, table_name
    
    def get_tables_for_source(self, source_name: str) -> List[Tuple[str, str]]:
        """Get list of (schema, table) tuples for a specific source"""
        tables = []
        
        for mapping_key, table_mapping in self.mapping.items():
            # Only process mappings that explicitly specify this source
            if table_mapping.source != source_name:
                continue
                
            if not table_mapping.source_table:
                continue
                
            # Handle schema.table format
            if '.' in table_mapping.source_table:
                parts = table_mapping.source_table.split('.')
                if len(parts) == 2:
                    # Format: schema.table
                    schema, table_name = parts
                    tables.append((schema, table_name))
                elif len(parts) == 3:
                    # Format: source_name.schema.table
                    mapping_source_name, schema, table_name = parts
                    if mapping_source_name == source_name:
                        tables.append((schema, table_name))
            else:
                # Simple table name - use source config database as schema
                source_config = self.get_source_config(source_name)
                schema = source_config.database
                tables.append((schema, table_mapping.source_table))
        
        return tables