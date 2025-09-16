"""
Replication service for MySQL Replication ETL
"""

from typing import Dict, Any, Optional, Generator
from pymysqlreplication import BinLogStreamReader
from pymysqlreplication import row_event
from pymysqlreplication.constants import BINLOG

from ..exceptions import ReplicationError
from ..models.config import DatabaseConfig, ReplicationConfig, ETLConfig
from ..models.events import BinlogEvent, InsertEvent, UpdateEvent, DeleteEvent, EventType
from .database_service import DatabaseService


class ReplicationService:
    """Service for MySQL replication operations"""
    
    def __init__(self, database_service: DatabaseService):
        self.database_service = database_service
        self._stream: Optional[BinLogStreamReader] = None
    
    def connect_to_replication(self, source_config: DatabaseConfig, replication_config: ReplicationConfig) -> BinLogStreamReader:
        """Connect to MySQL replication stream"""
        try:
            # Get master status for starting position
            master_status = self.database_service.get_master_status(source_config)
            
            # Prepare connection parameters
            connection_params = {
                'host': source_config.host,
                'port': source_config.port,
                'user': source_config.user,
                'password': source_config.password,
                'charset': source_config.charset
            }
            
            # Configure event filters
            only_events = [
                row_event.WriteRowsEvent,
                row_event.UpdateRowsEvent,
                row_event.DeleteRowsEvent
            ]
            
            # Create binlog stream
            self._stream = BinLogStreamReader(
                connection_settings=connection_params,
                server_id=replication_config.server_id,
                log_file=master_status.get('file'),
                log_pos=master_status.get('position', replication_config.log_pos),
                resume_stream=replication_config.resume_stream,
                blocking=replication_config.blocking,
                only_events=only_events
            )
            
            return self._stream
            
        except Exception as e:
            raise ReplicationError(f"Failed to connect to replication: {e}")
    
    def get_events(self) -> Generator[BinlogEvent, None, None]:
        """Get binlog events from stream"""
        if not self._stream:
            raise ReplicationError("Replication stream not connected")
        
        try:
            for binlog_event in self._stream:
                yield self._convert_binlog_event(binlog_event)
        except Exception as e:
            raise ReplicationError(f"Error reading binlog events: {e}")
    
    def _convert_binlog_event(self, binlog_event) -> BinlogEvent:
        """Convert pymysqlreplication event to our event model"""
        schema = binlog_event.schema
        table = binlog_event.table
        
        if isinstance(binlog_event, row_event.WriteRowsEvent):
            return InsertEvent(
                schema=schema,
                table=table,
                event_type=EventType.INSERT,
                values=binlog_event.rows[0]["values"] if binlog_event.rows else {}
            )
        elif isinstance(binlog_event, row_event.UpdateRowsEvent):
            row = binlog_event.rows[0] if binlog_event.rows else {}
            return UpdateEvent(
                schema=schema,
                table=table,
                event_type=EventType.UPDATE,
                before_values=row.get("before_values", {}),
                after_values=row.get("after_values", {})
            )
        elif isinstance(binlog_event, row_event.DeleteRowsEvent):
            return DeleteEvent(
                schema=schema,
                table=table,
                event_type=EventType.DELETE,
                values=binlog_event.rows[0]["values"] if binlog_event.rows else {}
            )
        else:
            # Generic event for other types
            return BinlogEvent(
                schema=schema,
                table=table,
                event_type=EventType.OTHER
            )
    
    def get_table_mapping(self, config: ETLConfig, schema: str, table: str) -> Optional[Dict[str, Any]]:
        """Get table mapping configuration"""
        mapping_key = f"{schema}.{table}"
        return config.mapping.get(mapping_key)
    
    def close(self) -> None:
        """Close replication stream"""
        if self._stream:
            try:
                self._stream.close()
            except Exception:
                pass
            finally:
                self._stream = None
