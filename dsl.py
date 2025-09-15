"""
DSL (Domain Specific Language) for ETL Configuration

This module provides Python-based DSL for describing:
- Replication sources
- Target tables
- Transformation rules
"""

from typing import Dict, List, Any, Optional, Callable
from dataclasses import dataclass, field
from enum import Enum
import json


class OperationType(Enum):
    """Types of database operations"""
    INSERT = "insert"
    UPDATE = "update"
    DELETE = "delete"


class FieldType(Enum):
    """Field types for target tables"""
    INTEGER = "integer"
    VARCHAR = "varchar"
    TEXT = "text"
    DATETIME = "datetime"
    DECIMAL = "decimal"
    BOOLEAN = "boolean"
    JSON = "json"


@dataclass
class ReplicationSource:
    """
    DSL for describing MySQL replication sources
    """
    name: str
    host: str
    port: int = 3306
    user: str = "replication"
    password: str = ""
    database: str = ""
    tables: List[str] = field(default_factory=list)
    server_id: int = 1
    auto_position: bool = True
    charset: str = "utf8mb4"
    
    def __post_init__(self):
        """Validate source configuration"""
        if not self.name:
            raise ValueError("Source name is required")
        if not self.host:
            raise ValueError("Host is required")
        if self.port <= 0 or self.port > 65535:
            raise ValueError("Port must be between 1 and 65535")
    
    def to_dict(self) -> Dict[str, Any]:
        """Convert to dictionary for serialization"""
        return {
            "name": self.name,
            "host": self.host,
            "port": self.port,
            "user": self.user,
            "password": self.password,
            "database": self.database,
            "tables": self.tables,
            "server_id": self.server_id,
            "auto_position": self.auto_position,
            "charset": self.charset
        }
    
    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> 'ReplicationSource':
        """Create from dictionary"""
        return cls(**data)


@dataclass
class FieldDefinition:
    """Definition of a field in target table"""
    name: str
    field_type: FieldType
    length: Optional[int] = None
    nullable: bool = True
    default_value: Any = None
    primary_key: bool = False
    auto_increment: bool = False
    
    def to_dict(self) -> Dict[str, Any]:
        return {
            "name": self.name,
            "field_type": self.field_type.value,
            "length": self.length,
            "nullable": self.nullable,
            "default_value": self.default_value,
            "primary_key": self.primary_key,
            "auto_increment": self.auto_increment
        }
    
    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> 'FieldDefinition':
        """Create from dictionary"""
        field_data = data.copy()
        # Convert string field_type to enum
        if 'field_type' in field_data and isinstance(field_data['field_type'], str):
            field_data['field_type'] = FieldType(field_data['field_type'])
        return cls(**field_data)


@dataclass
class TargetTable:
    """
    DSL for describing target tables
    """
    name: str
    database: str
    fields: List[FieldDefinition] = field(default_factory=list)
    indexes: List[str] = field(default_factory=list)
    engine: str = "InnoDB"
    charset: str = "utf8mb4"
    
    def __post_init__(self):
        """Validate table configuration"""
        if not self.name:
            raise ValueError("Table name is required")
        if not self.database:
            raise ValueError("Database name is required")
        
        # Ensure id field exists as primary key
        has_id = any(field.name == "id" and field.primary_key for field in self.fields)
        if not has_id:
            # Add default id field
            id_field = FieldDefinition(
                name="id",
                field_type=FieldType.INTEGER,
                primary_key=True,
                auto_increment=True,
                nullable=False
            )
            self.fields.insert(0, id_field)
    
    def add_field(self, name: str, field_type: FieldType, **kwargs) -> 'TargetTable':
        """Add a field to the table"""
        field_def = FieldDefinition(name=name, field_type=field_type, **kwargs)
        self.fields.append(field_def)
        return self
    
    def to_dict(self) -> Dict[str, Any]:
        """Convert to dictionary for serialization"""
        return {
            "name": self.name,
            "database": self.database,
            "fields": [field.to_dict() for field in self.fields],
            "indexes": self.indexes,
            "engine": self.engine,
            "charset": self.charset
        }
    
    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> 'TargetTable':
        """Create from dictionary"""
        fields_data = data.pop("fields", [])
        fields = [FieldDefinition.from_dict(field_data) for field_data in fields_data]
        return cls(fields=fields, **data)


@dataclass
class FieldMapping:
    """Mapping between source and target fields"""
    source_field: str
    target_field: str
    transformation: Optional[Callable] = None
    
    def to_dict(self) -> Dict[str, Any]:
        return {
            "source_field": self.source_field,
            "target_field": self.target_field,
            "transformation": self.transformation.__name__ if self.transformation else None
        }


@dataclass
class TransformationRule:
    """
    DSL for describing transformation rules
    """
    name: str
    source_table: str
    target_table: str
    field_mappings: List[FieldMapping] = field(default_factory=list)
    filters: List[str] = field(default_factory=list)
    operations: List[OperationType] = field(default_factory=lambda: [OperationType.INSERT, OperationType.UPDATE, OperationType.DELETE])
    
    def __post_init__(self):
        """Validate transformation rule"""
        if not self.name:
            raise ValueError("Rule name is required")
        if not self.source_table:
            raise ValueError("Source table is required")
        if not self.target_table:
            raise ValueError("Target table is required")
    
    def add_mapping(self, source_field: str, target_field: str, transformation: Optional[Callable] = None) -> 'TransformationRule':
        """Add field mapping"""
        mapping = FieldMapping(source_field=source_field, target_field=target_field, transformation=transformation)
        self.field_mappings.append(mapping)
        return self
    
    def add_filter(self, filter_condition: str) -> 'TransformationRule':
        """Add filter condition"""
        self.filters.append(filter_condition)
        return self
    
    def to_dict(self) -> Dict[str, Any]:
        """Convert to dictionary for serialization"""
        return {
            "name": self.name,
            "source_table": self.source_table,
            "target_table": self.target_table,
            "field_mappings": [mapping.to_dict() for mapping in self.field_mappings],
            "filters": self.filters,
            "operations": [op.value for op in self.operations]
        }
    
    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> 'TransformationRule':
        """Create from dictionary"""
        mappings_data = data.pop("field_mappings", [])
        mappings = [FieldMapping(**mapping_data) for mapping_data in mappings_data]
        operations_data = data.pop("operations", [])
        operations = [OperationType(op) for op in operations_data]
        return cls(field_mappings=mappings, operations=operations, **data)


@dataclass
class ETLPipeline:
    """
    Complete ETL pipeline configuration
    """
    name: str
    sources: List[ReplicationSource] = field(default_factory=list)
    target_tables: List[TargetTable] = field(default_factory=list)
    transformation_rules: List[TransformationRule] = field(default_factory=list)
    
    def __post_init__(self):
        """Validate pipeline configuration"""
        if not self.name:
            raise ValueError("Pipeline name is required")
    
    def add_source(self, source: ReplicationSource) -> 'ETLPipeline':
        """Add replication source"""
        self.sources.append(source)
        return self
    
    def add_target_table(self, table: TargetTable) -> 'ETLPipeline':
        """Add target table"""
        self.target_tables.append(table)
        return self
    
    def add_transformation_rule(self, rule: TransformationRule) -> 'ETLPipeline':
        """Add transformation rule"""
        self.transformation_rules.append(rule)
        return self
    
    def to_dict(self) -> Dict[str, Any]:
        """Convert to dictionary for serialization"""
        return {
            "name": self.name,
            "sources": [source.to_dict() for source in self.sources],
            "target_tables": [table.to_dict() for table in self.target_tables],
            "transformation_rules": [rule.to_dict() for rule in self.transformation_rules]
        }
    
    def to_json(self, indent: int = 2) -> str:
        """Convert to JSON string"""
        return json.dumps(self.to_dict(), indent=indent, default=str)
    
    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> 'ETLPipeline':
        """Create from dictionary"""
        sources_data = data.pop("sources", [])
        sources = [ReplicationSource.from_dict(source_data) for source_data in sources_data]
        
        tables_data = data.pop("target_tables", [])
        tables = [TargetTable.from_dict(table_data) for table_data in tables_data]
        
        rules_data = data.pop("transformation_rules", [])
        rules = [TransformationRule.from_dict(rule_data) for rule_data in rules_data]
        
        return cls(sources=sources, target_tables=tables, transformation_rules=rules, **data)
    
    @classmethod
    def from_json(cls, json_str: str) -> 'ETLPipeline':
        """Create from JSON string"""
        data = json.loads(json_str)
        return cls.from_dict(data)


# Helper functions for common transformations
def uppercase_transform(value: Any) -> str:
    """Transform value to uppercase"""
    return str(value).upper() if value is not None else None


def lowercase_transform(value: Any) -> str:
    """Transform value to lowercase"""
    return str(value).lower() if value is not None else None


def trim_transform(value: Any) -> str:
    """Trim whitespace from value"""
    return str(value).strip() if value is not None else None


def datetime_transform(value: Any) -> str:
    """Transform to datetime format"""
    if value is None:
        return None
    # Add your datetime transformation logic here
    return str(value)


def json_transform(value: Any) -> str:
    """Transform to JSON string"""
    if value is None:
        return None
    return json.dumps(value) if not isinstance(value, str) else value
