"""
MySQL Replication Client

This module provides functionality to connect to MySQL replication stream
and process binlog events in real-time.
"""

import logging
import threading
import time
from typing import Dict, List, Any, Optional, Callable
from dataclasses import dataclass
from queue import Queue, Empty
import pymysql
from pymysqlreplication import BinLogStreamReader
from pymysqlreplication.row_event import (
    DeleteRowsEvent,
    UpdateRowsEvent,
    WriteRowsEvent
)
from pymysqlreplication.event import QueryEvent
from pymysqlreplication.constants.BINLOG import DELETE_ROWS_EVENT_V2, UPDATE_ROWS_EVENT_V2, WRITE_ROWS_EVENT_V2

from dsl import ReplicationSource, OperationType


@dataclass
class ReplicationEvent:
    """Represents a replication event"""
    event_type: OperationType
    table: str
    database: str
    rows: List[Dict[str, Any]]
    timestamp: float
    binlog_position: int
    
    def to_dict(self) -> Dict[str, Any]:
        return {
            "event_type": self.event_type.value,
            "table": self.table,
            "database": self.database,
            "rows": self.rows,
            "timestamp": self.timestamp,
            "binlog_position": self.binlog_position
        }


class MySQLReplicationClient:
    """
    MySQL Replication Client for reading binlog events
    """
    
    def __init__(self, source: ReplicationSource):
        self.source = source
        self.logger = logging.getLogger(f"MySQLReplicationClient.{source.name}")
        self.stream_reader: Optional[BinLogStreamReader] = None
        self.is_running = False
        self.event_queue = Queue()
        self.event_handlers: List[Callable[[ReplicationEvent], None]] = []
        self._thread: Optional[threading.Thread] = None
        
    def add_event_handler(self, handler: Callable[[ReplicationEvent], None]):
        """Add event handler for processing replication events"""
        self.event_handlers.append(handler)
    
    def _connect(self) -> bool:
        """Connect to MySQL replication stream"""
        try:
            mysql_settings = {
                'host': self.source.host,
                'port': self.source.port,
                'user': self.source.user,
                'passwd': self.source.password,
                'charset': self.source.charset
            }
            
            # Create binlog stream reader
            self.stream_reader = BinLogStreamReader(
                connection_settings=mysql_settings,
                server_id=self.source.server_id,
                only_events=[DeleteRowsEvent, UpdateRowsEvent, WriteRowsEvent, QueryEvent],
                only_tables=self.source.tables if self.source.tables else None,
                only_schemas=[self.source.database] if self.source.database else None,
                auto_position=self.source.auto_position,
                resume_stream=True
            )
            
            self.logger.info(f"Connected to MySQL replication stream on {self.source.host}:{self.source.port}")
            return True
            
        except Exception as e:
            self.logger.error(f"Failed to connect to MySQL replication: {e}")
            return False
    
    def _disconnect(self):
        """Disconnect from MySQL replication stream"""
        if self.stream_reader:
            self.stream_reader.close()
            self.stream_reader = None
            self.logger.info("Disconnected from MySQL replication stream")
    
    def _process_binlog_event(self, binlog_event):
        """Process individual binlog event"""
        try:
            event_type = None
            rows = []
            
            if isinstance(binlog_event, WriteRowsEvent):
                event_type = OperationType.INSERT
                rows = [dict(row["values"]) for row in binlog_event.rows]
            elif isinstance(binlog_event, UpdateRowsEvent):
                event_type = OperationType.UPDATE
                rows = [dict(row["after_values"]) for row in binlog_event.rows]
            elif isinstance(binlog_event, DeleteRowsEvent):
                event_type = OperationType.DELETE
                rows = [dict(row["values"]) for row in binlog_event.rows]
            
            if event_type and rows:
                # Create replication event
                replication_event = ReplicationEvent(
                    event_type=event_type,
                    table=binlog_event.table,
                    database=binlog_event.schema,
                    rows=rows,
                    timestamp=time.time(),
                    binlog_position=binlog_event.packet.log_pos
                )
                
                # Add to queue for processing
                self.event_queue.put(replication_event)
                
        except Exception as e:
            self.logger.error(f"Error processing binlog event: {e}")
    
    def _event_processor(self):
        """Process events from queue"""
        while self.is_running:
            try:
                # Get event from queue with timeout
                event = self.event_queue.get(timeout=1.0)
                
                # Process event with all handlers
                for handler in self.event_handlers:
                    try:
                        handler(event)
                    except Exception as e:
                        self.logger.error(f"Error in event handler: {e}")
                
                self.event_queue.task_done()
                
            except Empty:
                continue
            except Exception as e:
                self.logger.error(f"Error in event processor: {e}")
    
    def start(self):
        """Start replication client"""
        if self.is_running:
            self.logger.warning("Replication client is already running")
            return
        
        if not self._connect():
            raise RuntimeError("Failed to connect to MySQL replication")
        
        self.is_running = True
        
        # Start event processor thread
        self._thread = threading.Thread(target=self._event_processor, daemon=True)
        self._thread.start()
        
        # Start reading binlog events
        try:
            for binlog_event in self.stream_reader:
                if not self.is_running:
                    break
                self._process_binlog_event(binlog_event)
        except Exception as e:
            self.logger.error(f"Error reading binlog events: {e}")
        finally:
            self.stop()
    
    def stop(self):
        """Stop replication client"""
        if not self.is_running:
            return
        
        self.logger.info("Stopping replication client...")
        self.is_running = False
        
        # Wait for event processor to finish
        if self._thread and self._thread.is_alive():
            self._thread.join(timeout=5.0)
        
        # Disconnect from MySQL
        self._disconnect()
        
        self.logger.info("Replication client stopped")
    
    def get_status(self) -> Dict[str, Any]:
        """Get current status of replication client"""
        return {
            "is_running": self.is_running,
            "source": self.source.to_dict(),
            "queue_size": self.event_queue.qsize(),
            "handlers_count": len(self.event_handlers)
        }


class ReplicationManager:
    """
    Manages multiple replication clients
    """
    
    def __init__(self):
        self.logger = logging.getLogger("ReplicationManager")
        self.clients: Dict[str, MySQLReplicationClient] = {}
        self.is_running = False
    
    def add_source(self, source: ReplicationSource) -> MySQLReplicationClient:
        """Add replication source"""
        if source.name in self.clients:
            raise ValueError(f"Source '{source.name}' already exists")
        
        client = MySQLReplicationClient(source)
        self.clients[source.name] = client
        self.logger.info(f"Added replication source: {source.name}")
        return client
    
    def remove_source(self, source_name: str):
        """Remove replication source"""
        if source_name in self.clients:
            client = self.clients[source_name]
            if client.is_running:
                client.stop()
            del self.clients[source_name]
            self.logger.info(f"Removed replication source: {source_name}")
    
    def start_all(self):
        """Start all replication clients"""
        if self.is_running:
            self.logger.warning("Replication manager is already running")
            return
        
        self.is_running = True
        
        # Start all clients in separate threads
        for client in self.clients.values():
            threading.Thread(target=client.start, daemon=True).start()
        
        self.logger.info(f"Started {len(self.clients)} replication clients")
    
    def stop_all(self):
        """Stop all replication clients"""
        if not self.is_running:
            return
        
        self.logger.info("Stopping all replication clients...")
        self.is_running = False
        
        for client in self.clients.values():
            client.stop()
        
        self.logger.info("All replication clients stopped")
    
    def get_status(self) -> Dict[str, Any]:
        """Get status of all replication clients"""
        return {
            "is_running": self.is_running,
            "clients": {
                name: client.get_status() 
                for name, client in self.clients.items()
            }
        }
