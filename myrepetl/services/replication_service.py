"""
Replication service for MySQL Replication ETL
"""

from typing import Dict, Any, Optional, Generator, Tuple
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
        self._streams: Dict[str, BinLogStreamReader] = {}
        self._shutdown_requested = False
    
    def request_shutdown(self) -> None:
        """Запрос на остановку сервиса"""
        self._shutdown_requested = True
        # Закрываем все потоки
        self.close()
    
    def connect_to_replication(self, source_name: str, source_config: DatabaseConfig, replication_config: ReplicationConfig) -> BinLogStreamReader:
        """Connect to MySQL replication stream for a specific source"""
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
            stream = BinLogStreamReader(
                connection_settings=connection_params,
                server_id=replication_config.server_id,
                log_file=master_status.get('file'),
                log_pos=master_status.get('position', replication_config.log_pos),
                resume_stream=replication_config.resume_stream,
                blocking=replication_config.blocking,
                only_events=only_events
            )
            
            self._streams[source_name] = stream
            return stream
            
        except Exception as e:
            raise ReplicationError(f"Failed to connect to replication for source '{source_name}': {e}")
    
    def get_events(self, source_name: str) -> Generator[BinlogEvent, None, None]:
        """Get binlog events from specific source stream"""
        if source_name not in self._streams:
            raise ReplicationError(f"Replication stream for source '{source_name}' not connected")
        
        stream = self._streams[source_name]
        try:
            for binlog_event in stream:
                # Проверяем флаг остановки перед каждым событием
                if self._shutdown_requested:
                    break
                yield self._convert_binlog_event(binlog_event, source_name)
        except Exception as e:
            if self._shutdown_requested:
                # Если остановка запрошена, не поднимаем исключение
                return
            raise ReplicationError(f"Error reading binlog events from source '{source_name}': {e}")
    
    def get_all_events(self) -> Generator[Tuple[str, BinlogEvent], None, None]:
        """Get binlog events from all connected streams"""
        if not self._streams:
            raise ReplicationError("No replication streams connected")
        
        # Round-robin approach: process one event from each source in turn
        source_names = list(self._streams.keys())
        current_source_index = 0
        
        while not self._shutdown_requested:
            try:
                source_name = source_names[current_source_index]
                stream = self._streams[source_name]
                
                # Try to get one event from current source
                try:
                    binlog_event = stream.fetchone()
                    if binlog_event is not None:
                        yield source_name, self._convert_binlog_event(binlog_event, source_name)
                except Exception as e:
                    if "no more data" in str(e).lower() or "no data" in str(e).lower():
                        # No more events from this source, continue to next
                        pass
                    elif self._shutdown_requested:
                        break
                    else:
                        raise ReplicationError(f"Error reading binlog events from source '{source_name}': {e}")
                
                # Move to next source
                current_source_index = (current_source_index + 1) % len(source_names)
                
            except Exception as e:
                if self._shutdown_requested:
                    break
                raise ReplicationError(f"Error in event processing loop: {e}")
    
    def _convert_binlog_event(self, binlog_event, source_name: str) -> BinlogEvent:
        """Convert pymysqlreplication event to our event model"""
        schema = binlog_event.schema
        table = binlog_event.table
        
        if isinstance(binlog_event, row_event.WriteRowsEvent):
            return InsertEvent(
                schema=schema,
                table=table,
                event_type=EventType.INSERT,
                values=binlog_event.rows[0]["values"] if binlog_event.rows else {},
                source_name=source_name
            )
        elif isinstance(binlog_event, row_event.UpdateRowsEvent):
            row = binlog_event.rows[0] if binlog_event.rows else {}
            return UpdateEvent(
                schema=schema,
                table=table,
                event_type=EventType.UPDATE,
                before_values=row.get("before_values", {}),
                after_values=row.get("after_values", {}),
                source_name=source_name
            )
        elif isinstance(binlog_event, row_event.DeleteRowsEvent):
            return DeleteEvent(
                schema=schema,
                table=table,
                event_type=EventType.DELETE,
                values=binlog_event.rows[0]["values"] if binlog_event.rows else {},
                source_name=source_name
            )
        else:
            # Generic event for other types
            return BinlogEvent(
                schema=schema,
                table=table,
                event_type=EventType.OTHER,
                source_name=source_name
            )
    
    def get_table_mapping(self, config: ETLConfig, schema: str, table: str, source_name: str = None) -> Optional[Dict[str, Any]]:
        """Get table mapping configuration"""
        # Try new format first: source_name.table
        if source_name:
            mapping_key = f"{source_name}.{table}"
            if mapping_key in config.mapping:
                return config.mapping.get(mapping_key)
        
        # Fallback to old format: schema.table (for backward compatibility)
        mapping_key = f"{schema}.{table}"
        return config.mapping.get(mapping_key)
    
    def close(self, source_name: str = None) -> None:
        """Close replication stream(s)"""
        if source_name:
            # Close specific stream
            if source_name in self._streams:
                try:
                    stream = self._streams[source_name]
                    # Принудительно закрываем поток
                    if hasattr(stream, 'close'):
                        stream.close()
                    # Дополнительно пытаемся закрыть соединение
                    if hasattr(stream, '_stream_connection') and stream._stream_connection:
                        try:
                            stream._stream_connection.close()
                        except Exception:
                            pass
                except Exception:
                    pass
                finally:
                    del self._streams[source_name]
        else:
            # Close all streams
            for stream_name, stream in list(self._streams.items()):
                try:
                    # Принудительно закрываем поток
                    if hasattr(stream, 'close'):
                        stream.close()
                    # Дополнительно пытаемся закрыть соединение
                    if hasattr(stream, '_stream_connection') and stream._stream_connection:
                        try:
                            stream._stream_connection.close()
                        except Exception:
                            pass
                except Exception:
                    pass
            self._streams.clear()
