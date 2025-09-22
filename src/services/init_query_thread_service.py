"""
Init query thread service for MySQL Replication ETL
Handles initial data loading from source databases in separate threads
"""

import threading
import time
from typing import Dict, Any, Optional, List
from queue import Queue, Empty

from ..exceptions import ETLException
from ..models.config import DatabaseConfig, ETLConfig
from ..models.events import InitQueryEvent
from .database_service import DatabaseService
from .message_bus import MessageBus, MessageType, Message
import structlog


class InitQueryThread:
    """Individual init query thread for processing initial data"""
    
    def __init__(self, mapping_key: str, source_name: str, target_name: str,
                 message_bus: MessageBus, database_service: DatabaseService,
                 config: ETLConfig):
        self.mapping_key = mapping_key
        self.source_name = source_name
        self.target_name = target_name
        self.message_bus = message_bus
        self.database_service = database_service
        self.config = config
        
        self.logger = structlog.get_logger()
        
        # Thread management
        self._thread: Optional[threading.Thread] = None
        self._shutdown_requested = False
        self._shutdown_lock = threading.Lock()
        
        # Statistics
        self._stats = {
            'rows_processed': 0,
            'errors_count': 0,
            'last_activity_time': None,
            'is_running': False,
            'is_completed': False
        }
        self._stats_lock = threading.Lock()
        
        self.logger.info("Init query thread created", 
                        mapping_key=mapping_key, 
                        source_name=source_name, 
                        target_name=target_name)
    
    def start(self) -> None:
        """Start the init query thread"""
        with self._shutdown_lock:
            if self._shutdown_requested:
                self.logger.warning("Cannot start init query thread, shutdown already requested", 
                                  mapping_key=self.mapping_key)
                return
        
        if self._thread and self._thread.is_alive():
            self.logger.warning("Init query thread already running", mapping_key=self.mapping_key)
            return
        
        self._thread = threading.Thread(target=self._run, name=f"init_query_{self.mapping_key}")
        self._thread.daemon = True
        self._thread.start()
        
        self.logger.info("Init query thread started", mapping_key=self.mapping_key)
    
    def stop(self) -> None:
        """Stop the init query thread"""
        with self._shutdown_lock:
            self._shutdown_requested = True
        
        # Wait for thread to finish
        if self._thread and self._thread.is_alive():
            self._thread.join(timeout=10.0)
            if self._thread.is_alive():
                self.logger.warning("Init query thread did not stop gracefully", 
                                  mapping_key=self.mapping_key)
        
        with self._stats_lock:
            self._stats['is_running'] = False
        
        self.logger.info("Init query thread stopped", mapping_key=self.mapping_key)
    
    def is_running(self) -> bool:
        """Check if thread is running"""
        with self._stats_lock:
            return self._stats['is_running']
    
    def is_completed(self) -> bool:
        """Check if init query processing is completed"""
        with self._stats_lock:
            return self._stats['is_completed']
    
    def get_stats(self) -> Dict[str, Any]:
        """Get thread statistics"""
        with self._stats_lock:
            return self._stats.copy()
    
    def _run(self) -> None:
        """Main thread execution loop"""
        try:
            with self._stats_lock:
                self._stats['is_running'] = True
                self._stats['last_activity_time'] = time.time()
            
            self.logger.info("Starting init query processing", mapping_key=self.mapping_key)
            
            # Get table mapping configuration
            table_mapping = self.config.mapping.get(self.mapping_key)
            if not table_mapping:
                raise ETLException(f"Table mapping not found for {self.mapping_key}")
            
            if not table_mapping.init_query:
                self.logger.info("No init query configured, skipping", mapping_key=self.mapping_key)
                with self._stats_lock:
                    self._stats['is_completed'] = True
                return
            
            # Get target information
            if table_mapping.target:
                target_name = table_mapping.target
                target_table_name = table_mapping.target_table
            else:
                # Legacy format
                target_name, target_table_name = self.config.parse_target_table(table_mapping.target_table)
            
            # Check if target table is empty
            try:
                if not self.database_service.is_table_empty(target_table_name, target_name):
                    self.logger.info("Target table not empty, skipping init query", 
                                   mapping_key=self.mapping_key, 
                                   target_table=target_table_name)
                    with self._stats_lock:
                        self._stats['is_completed'] = True
                    return
            except Exception as e:
                self.logger.warning("Could not check if target table is empty, proceeding with init query", 
                                  mapping_key=self.mapping_key, 
                                  target_table=target_table_name, 
                                  error=str(e))
            
            # Get source configuration
            source_config = self.config.get_source_config(self.source_name)
            source_connection_name = f"init_source_{self.source_name}"
            
            try:
                # Connect to source database
                self.database_service.connect(source_config, source_connection_name)
                
                # Execute init query
                results, columns = self.database_service.execute_init_query(
                    table_mapping.init_query, source_connection_name
                )
                
                self.logger.info("Init query executed successfully", 
                               mapping_key=self.mapping_key, 
                               rows_count=len(results))
                
                # Process each row from init query
                for row_data in results:
                    if self._is_shutdown_requested():
                        self.logger.info("Shutdown requested, stopping init query processing", 
                                       mapping_key=self.mapping_key)
                        break
                    
                    # Convert row to dictionary using column names
                    row_dict = dict(zip(columns, row_data))
                    
                    # Create init query event
                    init_query_event = InitQueryEvent(
                        mapping_key=self.mapping_key,
                        source_name=self.source_name,
                        target_name=target_name,
                        target_table=target_table_name,
                        row_data=row_dict,
                        columns=columns,
                        init_query=table_mapping.init_query,
                        primary_key=table_mapping.primary_key,
                        column_mapping=table_mapping.column_mapping,
                        filter_config=table_mapping.filter
                    )
                    
                    # Publish event to message bus
                    success = self.message_bus.publish_init_query_event(
                        source=self.source_name,
                        event_data=init_query_event,
                        target=target_name
                    )
                    
                    if success:
                        with self._stats_lock:
                            self._stats['rows_processed'] += 1
                            self._stats['last_activity_time'] = time.time()
                        
                        self.logger.debug("Init query row published to message bus", 
                                        mapping_key=self.mapping_key, 
                                        row=row_dict)
                    else:
                        with self._stats_lock:
                            self._stats['errors_count'] += 1
                        
                        self.logger.warning("Failed to publish init query row to message bus", 
                                          mapping_key=self.mapping_key, 
                                          row=row_dict)
                
                self.logger.info("Init query processing completed", 
                               mapping_key=self.mapping_key, 
                               processed_rows=len(results))
                
                with self._stats_lock:
                    self._stats['is_completed'] = True
                
            except Exception as e:
                with self._stats_lock:
                    self._stats['errors_count'] += 1
                
                self.logger.error("Error executing init query", 
                                mapping_key=self.mapping_key, 
                                error=str(e))
                raise
            finally:
                # Close source connection
                try:
                    self.database_service.close_connection(source_connection_name)
                except Exception as e:
                    self.logger.warning("Error closing source connection", 
                                      connection_name=source_connection_name, 
                                      error=str(e))
        
        except Exception as e:
            with self._stats_lock:
                self._stats['errors_count'] += 1
            
            self.logger.error("Fatal error in init query thread", 
                            mapping_key=self.mapping_key, 
                            error=str(e))
        finally:
            with self._stats_lock:
                self._stats['is_running'] = False
                self._stats['last_activity_time'] = time.time()
    
    def _is_shutdown_requested(self) -> bool:
        """Check if shutdown is requested"""
        with self._shutdown_lock:
            return self._shutdown_requested


class InitQueryThreadService:
    """Service for managing init query threads"""
    
    def __init__(self, message_bus: MessageBus, database_service: DatabaseService):
        self.logger = structlog.get_logger()
        self.message_bus = message_bus
        self.database_service = database_service
        
        # Thread management
        self._threads: Dict[str, InitQueryThread] = {}
        self._threads_lock = threading.RLock()
        
        self.logger.info("Init query thread service initialized")
    
    def start_init_query_threads(self, config: ETLConfig) -> None:
        """Start init query threads for all configured mappings"""
        with self._threads_lock:
            self.logger.info("Starting init query threads")
            
            for mapping_key, table_mapping in config.mapping.items():
                if not table_mapping.init_query:
                    continue
                
                # Get source name from mapping key or source_table field
                if table_mapping.source_table:
                    try:
                        source_name, _ = config.parse_source_table(table_mapping.source_table)
                    except Exception as e:
                        self.logger.warning("Invalid source_table format, using mapping key", 
                                          source_table=table_mapping.source_table, error=str(e))
                        if '.' not in mapping_key:
                            self.logger.warning("Invalid mapping key format, skipping init query", 
                                              mapping_key=mapping_key)
                            continue
                        source_name = mapping_key.split('.')[0]
                else:
                    if '.' not in mapping_key:
                        self.logger.warning("Invalid mapping key format, skipping init query", 
                                          mapping_key=mapping_key)
                        continue
                    source_name = mapping_key.split('.')[0]
                
                if source_name not in config.sources:
                    self.logger.warning("Source not found in configuration, skipping init query", 
                                      source_name=source_name, mapping_key=mapping_key)
                    continue
                
                # Get target name
                if table_mapping.target:
                    target_name = table_mapping.target
                else:
                    # Legacy format
                    try:
                        target_name, _ = config.parse_target_table(table_mapping.target_table)
                    except Exception as e:
                        self.logger.warning("Invalid target_table format, skipping init query", 
                                          target_table=table_mapping.target_table, error=str(e))
                        continue
                
                if target_name not in config.targets:
                    self.logger.warning("Target not found in configuration, skipping init query", 
                                      target_name=target_name, mapping_key=mapping_key)
                    continue
                
                try:
                    # Create and start init query thread
                    thread = InitQueryThread(
                        mapping_key=mapping_key,
                        source_name=source_name,
                        target_name=target_name,
                        message_bus=self.message_bus,
                        database_service=self.database_service,
                        config=config
                    )
                    
                    self._threads[mapping_key] = thread
                    thread.start()
                    
                    self.logger.info("Init query thread started", 
                                   mapping_key=mapping_key, 
                                   source_name=source_name, 
                                   target_name=target_name)
                
                except Exception as e:
                    self.logger.error("Failed to start init query thread", 
                                    mapping_key=mapping_key, error=str(e))
                    raise
    
    def stop_all_init_query_threads(self) -> None:
        """Stop all init query threads"""
        with self._threads_lock:
            self.logger.info("Stopping all init query threads")
            
            for mapping_key, thread in self._threads.items():
                try:
                    thread.stop()
                except Exception as e:
                    self.logger.error("Error stopping init query thread", 
                                    mapping_key=mapping_key, error=str(e))
            
            self._threads.clear()
            self.logger.info("All init query threads stopped")
    
    def stop_init_query_thread(self, mapping_key: str) -> None:
        """Stop specific init query thread"""
        with self._threads_lock:
            if mapping_key in self._threads:
                try:
                    self._threads[mapping_key].stop()
                    del self._threads[mapping_key]
                    self.logger.info("Init query thread stopped", mapping_key=mapping_key)
                except Exception as e:
                    self.logger.error("Error stopping init query thread", 
                                    mapping_key=mapping_key, error=str(e))
            else:
                self.logger.warning("Init query thread not found", mapping_key=mapping_key)
    
    def get_all_stats(self) -> Dict[str, Dict[str, Any]]:
        """Get statistics for all init query threads"""
        with self._threads_lock:
            stats = {}
            for mapping_key, thread in self._threads.items():
                stats[mapping_key] = thread.get_stats()
            return stats
    
    def get_thread_stats(self, mapping_key: str) -> Optional[Dict[str, Any]]:
        """Get statistics for specific init query thread"""
        with self._threads_lock:
            if mapping_key in self._threads:
                return self._threads[mapping_key].get_stats()
            return None
    
    def is_thread_running(self, mapping_key: str) -> bool:
        """Check if specific init query thread is running"""
        with self._threads_lock:
            if mapping_key in self._threads:
                return self._threads[mapping_key].is_running()
            return False
    
    def is_thread_completed(self, mapping_key: str) -> bool:
        """Check if specific init query thread is completed"""
        with self._threads_lock:
            if mapping_key in self._threads:
                return self._threads[mapping_key].is_completed()
            return False
    
    def are_all_completed(self) -> bool:
        """Check if all init query threads are completed"""
        with self._threads_lock:
            if not self._threads:
                return True
            
            for thread in self._threads.values():
                if not thread.is_completed():
                    return False
            return True
    
    def get_active_threads_count(self) -> int:
        """Get count of active init query threads"""
        with self._threads_lock:
            return len([t for t in self._threads.values() if t.is_running()])
    
    def get_completed_threads_count(self) -> int:
        """Get count of completed init query threads"""
        with self._threads_lock:
            return len([t for t in self._threads.values() if t.is_completed()])
