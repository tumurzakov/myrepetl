"""
Event models for MySQL Replication ETL
"""

from dataclasses import dataclass
from typing import Dict, Any, Optional
from enum import Enum


class EventType(Enum):
    """Types of binlog events"""
    INSERT = "insert"
    UPDATE = "update"
    DELETE = "delete"
    OTHER = "other"


@dataclass
class BinlogEvent:
    """Base binlog event"""
    schema: str
    table: str
    event_type: EventType = None
    timestamp: Optional[int] = None
    source_name: Optional[str] = None
    log_pos: Optional[int] = None
    server_id: Optional[int] = None
    event_id: Optional[str] = None
    
    def __post_init__(self):
        """Validate event after initialization"""
        if not self.schema:
            raise ValueError("Schema is required")
        if not self.table:
            raise ValueError("Table is required")
        
        # Generate event ID if not provided
        if not self.event_id:
            import uuid
            self.event_id = str(uuid.uuid4())[:8]


@dataclass
class InsertEvent(BinlogEvent):
    """INSERT event"""
    values: Dict[str, Any] = None
    
    def __post_init__(self):
        super().__post_init__()
        self.event_type = EventType.INSERT
        if not self.values:
            raise ValueError("Values are required for INSERT event")


@dataclass
class UpdateEvent(BinlogEvent):
    """UPDATE event"""
    before_values: Dict[str, Any] = None
    after_values: Dict[str, Any] = None
    
    def __post_init__(self):
        super().__post_init__()
        self.event_type = EventType.UPDATE
        if not self.before_values:
            raise ValueError("Before values are required for UPDATE event")
        if not self.after_values:
            raise ValueError("After values are required for UPDATE event")


@dataclass
class DeleteEvent(BinlogEvent):
    """DELETE event"""
    values: Dict[str, Any] = None
    
    def __post_init__(self):
        super().__post_init__()
        self.event_type = EventType.DELETE
        if not self.values:
            raise ValueError("Values are required for DELETE event")


@dataclass
class InitQueryEvent:
    """Init query event for initial data loading"""
    mapping_key: str
    source_name: str
    target_name: str
    target_table: str
    row_data: Dict[str, Any]
    columns: list
    init_query: str
    primary_key: str
    column_mapping: Dict[str, Any]
    filter_config: Optional[Dict[str, Any]] = None
    event_id: Optional[str] = None
    
    def __post_init__(self):
        """Validate event after initialization"""
        if not self.mapping_key:
            raise ValueError("Mapping key is required")
        if not self.source_name:
            raise ValueError("Source name is required")
        if not self.target_name:
            raise ValueError("Target name is required")
        if not self.target_table:
            raise ValueError("Target table is required")
        if not self.row_data:
            raise ValueError("Row data is required")
        if not self.init_query:
            raise ValueError("Init query is required")
        if not self.primary_key:
            raise ValueError("Primary key is required")
        
        # Generate event ID if not provided
        if not self.event_id:
            import uuid
            self.event_id = str(uuid.uuid4())[:8]
