"""
Source thread service for MySQL Replication ETL
Handles individual source replication streams in separate threads
"""

import threading
import time
from typing import Dict, Any, Optional, List, Tuple
from pymysqlreplication import BinLogStreamReader
from pymysqlreplication import row_event
from pymysqlreplication.constants import BINLOG

from ..exceptions import ReplicationError
from ..models.config import DatabaseConfig, ReplicationConfig, ETLConfig
from ..models.events import BinlogEvent, InsertEvent, UpdateEvent, DeleteEvent, EventType
from .database_service import DatabaseService
from .message_bus import MessageBus, MessageType, Message
import structlog


class SourceThread:
    """Individual source thread for replication"""
    
    def __init__(self, source_name: str, source_config: DatabaseConfig, 
                 replication_config: ReplicationConfig, tables: List[Tuple[str, str]],
                 message_bus: MessageBus, database_service: DatabaseService):
        self.source_name = source_name
        self.source_config = source_config
        self.replication_config = replication_config
        self.tables = tables
        self.message_bus = message_bus
        self.database_service = database_service
        
        self.logger = structlog.get_logger()
        
        # Thread management
        self._thread: Optional[threading.Thread] = None
        self._shutdown_requested = False
        self._shutdown_lock = threading.Lock()
        
        # Replication stream
        self._stream: Optional[BinLogStreamReader] = None
        self._stream_lock = threading.Lock()
        
        # Statistics
        self._stats = {
            'events_processed': 0,
            'errors_count': 0,
            'last_event_time': None,
            'is_running': False
        }
        self._stats_lock = threading.Lock()
        
        self.logger.info("Source thread created", source_name=source_name)
    
    def start(self) -> None:
        """Start the source thread"""
        with self._shutdown_lock:
            if self._shutdown_requested:
                self.logger.warning("Cannot start source thread, shutdown already requested", 
                                  source_name=self.source_name)
                return
        
        if self._thread and self._thread.is_alive():
            self.logger.warning("Source thread already running", source_name=self.source_name)
            return
        
        self._thread = threading.Thread(target=self._run, name=f"source_{self.source_name}")
        self._thread.daemon = True
        self._thread.start()
        
        self.logger.info("Source thread started", source_name=self.source_name)
    
    def stop(self) -> None:
        """Stop the source thread"""
        with self._shutdown_lock:
            self._shutdown_requested = True
        
        # Close replication stream
        with self._stream_lock:
            if self._stream:
                try:
                    self._stream.close()
                except Exception as e:
                    self.logger.error("Error closing replication stream", 
                                    source_name=self.source_name, error=str(e))
                finally:
                    self._stream = None
        
        # Wait for thread to finish
        if self._thread and self._thread.is_alive():
            self._thread.join(timeout=5.0)
            if self._thread.is_alive():
                self.logger.warning("Source thread did not stop gracefully", 
                                  source_name=self.source_name)
        
        with self._stats_lock:
            self._stats['is_running'] = False
        
        self.logger.info("Source thread stopped", source_name=self.source_name)
    
    def is_running(self) -> bool:
        """Check if thread is running"""
        with self._stats_lock:
            return self._stats['is_running']
    
    def get_stats(self) -> Dict[str, Any]:
        """Get thread statistics"""
        with self._stats_lock:
            return self._stats.copy()
    
    def _run(self) -> None:
        """Main thread loop"""
        try:
            with self._stats_lock:
                self._stats['is_running'] = True
            
            self.logger.info("Source thread started processing", source_name=self.source_name)
            
            # Connect to replication stream
            self._connect_to_replication()
            
            # Process events
            self._process_events()
            
        except Exception as e:
            self.logger.error("Error in source thread", 
                            source_name=self.source_name, error=str(e))
            self.message_bus.publish_error(self.source_name, e)
        finally:
            with self._stats_lock:
                self._stats['is_running'] = False
            
            self.logger.info("Source thread finished", source_name=self.source_name)
    
    def _connect_to_replication(self) -> None:
        """Connect to MySQL replication stream"""
        try:
            # Get master status for starting position
            master_status = self.database_service.get_master_status(self.source_config)
            
            # Prepare connection parameters
            connection_params = {
                'host': self.source_config.host,
                'port': self.source_config.port,
                'user': self.source_config.user,
                'password': self.source_config.password,
                'charset': self.source_config.charset
            }
            
            # Configure event filters
            only_events = [
                row_event.WriteRowsEvent,
                row_event.UpdateRowsEvent,
                row_event.DeleteRowsEvent
            ]
            
            # Configure table filters if tables are specified
            only_tables = None
            only_schemas = None
            if self.tables:
                only_tables = set()
                only_schemas = set()
                for schema, table in self.tables:
                    only_tables.add(table)
                    only_schemas.add(schema)
                
                only_tables = list(only_tables)
                only_schemas = list(only_schemas)
                
                self.logger.info("Configuring table filters for replication", 
                               source_name=self.source_name,
                               only_tables=only_tables,
                               only_schemas=only_schemas)
            
            # Create binlog stream
            stream = BinLogStreamReader(
                connection_settings=connection_params,
                server_id=self.replication_config.server_id,
                log_file=master_status.get('file'),
                log_pos=master_status.get('position', self.replication_config.log_pos),
                resume_stream=self.replication_config.resume_stream,
                blocking=self.replication_config.blocking,
                only_events=only_events,
                only_tables=only_tables,
                only_schemas=only_schemas
            )
            
            with self._stream_lock:
                self._stream = stream
            
            self.logger.info("Connected to replication stream", 
                           source_name=self.source_name,
                           log_file=master_status.get('file'),
                           log_pos=master_status.get('position', self.replication_config.log_pos))
            
        except Exception as e:
            raise ReplicationError(f"Failed to connect to replication for source '{self.source_name}': {e}")
    
    def _process_events(self) -> None:
        """Process binlog events from the stream"""
        with self._stream_lock:
            if not self._stream:
                raise ReplicationError(f"Replication stream not connected for source '{self.source_name}'")
            
            stream = self._stream
        
        try:
            for binlog_event in stream:
                # Check shutdown flag
                if self._is_shutdown_requested():
                    self.logger.info("Shutdown requested, stopping event processing", 
                                   source_name=self.source_name)
                    break
                
                # Convert and publish event
                event = self._convert_binlog_event(binlog_event)
                if event:
                    self.message_bus.publish_binlog_event(self.source_name, event)
                    
                    with self._stats_lock:
                        self._stats['events_processed'] += 1
                        self._stats['last_event_time'] = time.time()
                    
                    self.logger.debug("Event published", 
                                    source_name=self.source_name,
                                    event_type=type(event).__name__,
                                    table=f"{event.schema}.{event.table}")
                
        except Exception as e:
            if self._is_shutdown_requested():
                # If shutdown requested, don't raise exception
                return
            
            with self._stats_lock:
                self._stats['errors_count'] += 1
            
            self.logger.error("Error reading binlog events", 
                            source_name=self.source_name, error=str(e))
            self.message_bus.publish_error(self.source_name, e)
            raise ReplicationError(f"Error reading binlog events from source '{self.source_name}': {e}")
    
    def _convert_binlog_event(self, binlog_event) -> Optional[BinlogEvent]:
        """Convert pymysqlreplication event to our event model"""
        try:
            schema = binlog_event.schema
            table = binlog_event.table
            
            if isinstance(binlog_event, row_event.WriteRowsEvent):
                return InsertEvent(
                    schema=schema,
                    table=table,
                    event_type=EventType.INSERT,
                    values=binlog_event.rows[0]["values"] if binlog_event.rows else {},
                    source_name=self.source_name
                )
            elif isinstance(binlog_event, row_event.UpdateRowsEvent):
                row = binlog_event.rows[0] if binlog_event.rows else {}
                return UpdateEvent(
                    schema=schema,
                    table=table,
                    event_type=EventType.UPDATE,
                    before_values=row.get("before_values", {}),
                    after_values=row.get("after_values", {}),
                    source_name=self.source_name
                )
            elif isinstance(binlog_event, row_event.DeleteRowsEvent):
                return DeleteEvent(
                    schema=schema,
                    table=table,
                    event_type=EventType.DELETE,
                    values=binlog_event.rows[0]["values"] if binlog_event.rows else {},
                    source_name=self.source_name
                )
            else:
                # Generic event for other types
                return BinlogEvent(
                    schema=schema,
                    table=table,
                    event_type=EventType.OTHER,
                    source_name=self.source_name
                )
        except Exception as e:
            self.logger.error("Error converting binlog event", 
                            source_name=self.source_name, error=str(e))
            return None
    
    def _is_shutdown_requested(self) -> bool:
        """Check if shutdown is requested"""
        with self._shutdown_lock:
            return self._shutdown_requested


class SourceThreadService:
    """Service for managing source threads"""
    
    def __init__(self, message_bus: MessageBus, database_service: DatabaseService):
        self.message_bus = message_bus
        self.database_service = database_service
        self.logger = structlog.get_logger()
        
        # Thread management
        self._source_threads: Dict[str, SourceThread] = {}
        self._threads_lock = threading.RLock()
        
        # Shutdown flag
        self._shutdown_requested = False
        self._shutdown_lock = threading.Lock()
        
        self.logger.info("Source thread service initialized")
    
    def start_source(self, source_name: str, source_config: DatabaseConfig, 
                    replication_config: ReplicationConfig, tables: List[Tuple[str, str]]) -> None:
        """Start a source thread"""
        with self._threads_lock:
            if source_name in self._source_threads:
                self.logger.warning("Source thread already exists", source_name=source_name)
                return
            
            source_thread = SourceThread(
                source_name=source_name,
                source_config=source_config,
                replication_config=replication_config,
                tables=tables,
                message_bus=self.message_bus,
                database_service=self.database_service
            )
            
            self._source_threads[source_name] = source_thread
            source_thread.start()
            
            self.logger.info("Source thread started", source_name=source_name)
    
    def stop_source(self, source_name: str) -> None:
        """Stop a specific source thread"""
        with self._threads_lock:
            if source_name not in self._source_threads:
                self.logger.warning("Source thread not found", source_name=source_name)
                return
            
            source_thread = self._source_threads[source_name]
            source_thread.stop()
            del self._source_threads[source_name]
            
            self.logger.info("Source thread stopped", source_name=source_name)
    
    def stop_all_sources(self) -> None:
        """Stop all source threads"""
        with self._shutdown_lock:
            self._shutdown_requested = True
        
        with self._threads_lock:
            source_names = list(self._source_threads.keys())
        
        for source_name in source_names:
            self.stop_source(source_name)
        
        self.logger.info("All source threads stopped")
    
    def get_source_stats(self, source_name: str) -> Optional[Dict[str, Any]]:
        """Get statistics for a specific source"""
        with self._threads_lock:
            if source_name not in self._source_threads:
                return None
            return self._source_threads[source_name].get_stats()
    
    def get_all_stats(self) -> Dict[str, Dict[str, Any]]:
        """Get statistics for all sources"""
        with self._threads_lock:
            return {name: thread.get_stats() for name, thread in self._source_threads.items()}
    
    def is_source_running(self, source_name: str) -> bool:
        """Check if a source is running"""
        with self._threads_lock:
            if source_name not in self._source_threads:
                return False
            return self._source_threads[source_name].is_running()
    
    def get_running_sources(self) -> List[str]:
        """Get list of running sources"""
        with self._threads_lock:
            return [name for name, thread in self._source_threads.items() if thread.is_running()]

