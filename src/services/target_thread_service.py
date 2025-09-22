"""
Target thread service for MySQL Replication ETL
Handles individual target database operations in separate threads
"""

import threading
import time
import queue
from typing import Dict, Any, Optional, List, Callable
from queue import Queue, Empty

from ..exceptions import ETLException
from ..models.config import DatabaseConfig, ETLConfig
from ..models.events import BinlogEvent, InsertEvent, UpdateEvent, DeleteEvent, InitQueryEvent
from .database_service import DatabaseService
from .transform_service import TransformService
from .filter_service import FilterService
from .message_bus import MessageBus, MessageType, Message
from ..utils import SQLBuilder
import structlog


class TargetThread:
    """Individual target thread for processing events"""
    
    def __init__(self, target_name: str, target_config: DatabaseConfig,
                 message_bus: MessageBus, database_service: DatabaseService,
                 transform_service: TransformService, filter_service: FilterService,
                 config: ETLConfig):
        self.target_name = target_name
        self.target_config = target_config
        self.message_bus = message_bus
        self.database_service = database_service
        self.transform_service = transform_service
        self.filter_service = filter_service
        self.config = config
        
        self.logger = structlog.get_logger()
        
        # Thread management
        self._thread: Optional[threading.Thread] = None
        self._shutdown_requested = False
        self._shutdown_lock = threading.Lock()
        
        # Event processing queue - increased size to handle high throughput
        self._event_queue = Queue(maxsize=10000)
        
        # Statistics
        self._stats = {
            'events_processed': 0,
            'inserts_processed': 0,
            'updates_processed': 0,
            'deletes_processed': 0,
            'init_query_events_processed': 0,
            'errors_count': 0,
            'queue_overflow_count': 0,
            'last_event_time': None,
            'is_running': False,
            'queue_size': 0,
            'max_queue_size': 0,
            'queue_usage_percent': 0
        }
        self._stats_lock = threading.Lock()
        
        # Message bus subscription
        self._message_bus_subscription = None
        
        self.logger.info("Target thread created", target_name=target_name)
    
    def start(self) -> None:
        """Start the target thread"""
        with self._shutdown_lock:
            if self._shutdown_requested:
                self.logger.warning("Cannot start target thread, shutdown already requested", 
                                  target_name=self.target_name)
                return
        
        if self._thread and self._thread.is_alive():
            self.logger.warning("Target thread already running", target_name=self.target_name)
            return
        
        # Subscribe to binlog events
        self._message_bus_subscription = self._handle_binlog_event
        self.message_bus.subscribe(MessageType.BINLOG_EVENT, self._message_bus_subscription)
        
        # Subscribe to init query events
        self._init_query_subscription = self._handle_init_query_event
        self.message_bus.subscribe(MessageType.INIT_QUERY_EVENT, self._init_query_subscription)
        
        # Subscribe to shutdown messages
        self.message_bus.subscribe(MessageType.SHUTDOWN, self._handle_shutdown)
        
        self._thread = threading.Thread(target=self._run, name=f"target_{self.target_name}")
        self._thread.daemon = True
        self._thread.start()
        
        self.logger.info("Target thread started", target_name=self.target_name)
    
    def stop(self) -> None:
        """Stop the target thread"""
        with self._shutdown_lock:
            self._shutdown_requested = True
        
        # Unsubscribe from message bus
        if self._message_bus_subscription:
            self.message_bus.unsubscribe(MessageType.BINLOG_EVENT, self._message_bus_subscription)
        if self._init_query_subscription:
            self.message_bus.unsubscribe(MessageType.INIT_QUERY_EVENT, self._init_query_subscription)
        self.message_bus.unsubscribe(MessageType.SHUTDOWN, self._handle_shutdown)
        
        # Wait for thread to finish
        if self._thread and self._thread.is_alive():
            self._thread.join(timeout=5.0)
            if self._thread.is_alive():
                self.logger.warning("Target thread did not stop gracefully", 
                                  target_name=self.target_name)
        
        with self._stats_lock:
            self._stats['is_running'] = False
        
        self.logger.info("Target thread stopped", target_name=self.target_name)
    
    def is_running(self) -> bool:
        """Check if thread is running"""
        with self._stats_lock:
            return self._stats['is_running']
    
    def get_stats(self) -> Dict[str, Any]:
        """Get thread statistics"""
        with self._stats_lock:
            stats = self._stats.copy()
            current_queue_size = self._event_queue.qsize()
            max_queue_size = self._event_queue.maxsize
            stats['queue_size'] = current_queue_size
            stats['max_queue_size'] = max_queue_size
            stats['queue_usage_percent'] = (current_queue_size / max_queue_size * 100) if max_queue_size > 0 else 0
            return stats
    
    def _run(self) -> None:
        """Main thread loop"""
        try:
            with self._stats_lock:
                self._stats['is_running'] = True
            
            self.logger.info("Target thread started processing", target_name=self.target_name)
            
            # Process events from queue
            last_stats_time = time.time()
            while not self._is_shutdown_requested():
                try:
                    # Get event from queue with shorter timeout for better responsiveness
                    event = self._event_queue.get(timeout=0.1)
                    
                    # Process event
                    self._process_event(event)
                    
                    # Mark task as done
                    self._event_queue.task_done()
                    
                    # Log queue statistics every 30 seconds
                    current_time = time.time()
                    if current_time - last_stats_time > 30:
                        self._log_queue_stats()
                        last_stats_time = current_time
                    
                except Empty:
                    # Timeout reached, continue to check shutdown
                    continue
                except Exception as e:
                    import traceback
                    error_traceback = traceback.format_exc()
                    self.logger.error("Error processing event", 
                                    target_name=self.target_name, 
                                    error=str(e),
                                    error_type=type(e).__name__,
                                    event_type=type(event).__name__ if 'event' in locals() else None,
                                    event_id=getattr(event, 'event_id', None) if 'event' in locals() and event else None,
                                    schema=getattr(event, 'schema', None) if 'event' in locals() and event else None,
                                    table=getattr(event, 'table', None) if 'event' in locals() and event else None,
                                    traceback=error_traceback)
                    with self._stats_lock:
                        self._stats['errors_count'] += 1
            
        except Exception as e:
            self.logger.error("Error in target thread", 
                            target_name=self.target_name, error=str(e))
            self.message_bus.publish_error(self.target_name, e)
        finally:
            with self._stats_lock:
                self._stats['is_running'] = False
            
            self.logger.info("Target thread finished", target_name=self.target_name)
    
    def _handle_binlog_event(self, message: Message) -> None:
        """Handle binlog event message from message bus"""
        try:
            self.logger.debug("Received binlog event message", 
                            target_name=self.target_name,
                            message_target=message.target,
                            message_source=message.source,
                            message_type=message.message_type,
                            data_type=type(message.data).__name__ if message.data else None)
            
            if message.target and message.target != self.target_name:
                # Event is targeted to a different target
                self.logger.debug("Event targeted to different target, skipping", 
                                target_name=self.target_name,
                                message_target=message.target)
                return
            
            event = message.data
            if not event:
                self.logger.warning("Empty event data in message", 
                                  target_name=self.target_name,
                                  message_target=message.target,
                                  message_source=message.source)
                return
                
            if not isinstance(event, BinlogEvent):
                self.logger.warning("Invalid event type in message", 
                                  target_name=self.target_name,
                                  event_type=type(event).__name__,
                                  message_target=message.target,
                                  message_source=message.source)
                return
            
            current_queue_size = self._event_queue.qsize()
            max_queue_size = self._event_queue.maxsize
            
            self.logger.debug("Adding binlog event to processing queue", 
                            target_name=self.target_name,
                            event_id=getattr(event, 'event_id', None),
                            schema=getattr(event, 'schema', None),
                            table=getattr(event, 'table', None),
                            event_type=type(event).__name__,
                            current_queue_size=current_queue_size,
                            max_queue_size=max_queue_size,
                            queue_usage_percent=(current_queue_size / max_queue_size * 100) if max_queue_size > 0 else 0)
            
            # Check if queue is nearly full and warn
            if current_queue_size > max_queue_size * 0.8:  # 80% full
                self.logger.warning("Event queue is nearly full", 
                                  target_name=self.target_name,
                                  current_queue_size=current_queue_size,
                                  max_queue_size=max_queue_size,
                                  queue_usage_percent=(current_queue_size / max_queue_size * 100))
            
            # Add event to processing queue with overflow handling
            try:
                self._event_queue.put_nowait(event)
                self.logger.debug("Binlog event added to queue successfully", 
                                target_name=self.target_name,
                                queue_size=self._event_queue.qsize())
            except queue.Full:
                self.logger.error("Event queue is full, dropping event", 
                                target_name=self.target_name,
                                event_id=getattr(event, 'event_id', None),
                                schema=getattr(event, 'schema', None),
                                table=getattr(event, 'table', None),
                                event_type=type(event).__name__,
                                queue_size=self._event_queue.qsize(),
                                max_queue_size=max_queue_size)
                with self._stats_lock:
                    self._stats['errors_count'] += 1
                    self._stats['queue_overflow_count'] += 1
                return
            
        except Exception as e:
            import traceback
            error_traceback = traceback.format_exc()
            self.logger.error("Error handling binlog event message", 
                            target_name=self.target_name, 
                            error=str(e),
                            error_type=type(e).__name__,
                            message_target=getattr(message, 'target', None) if message else None,
                            message_source=getattr(message, 'source', None) if message else None,
                            message_data_type=type(message.data).__name__ if message and message.data else None,
                            traceback=error_traceback)
            with self._stats_lock:
                self._stats['errors_count'] += 1
    
    def _handle_init_query_event(self, message: Message) -> None:
        """Handle init query event message from message bus"""
        try:
            self.logger.info("Received init query event message", 
                            target_name=self.target_name,
                            message_target=message.target,
                            message_source=message.source,
                            message_type=message.message_type,
                            data_type=type(message.data).__name__ if message.data else None)
            
            if message.target and message.target != self.target_name:
                # Event is targeted to a different target
                self.logger.debug("Init query event targeted to different target, skipping", 
                                target_name=self.target_name,
                                message_target=message.target)
                return
            
            event_data = message.data
            if not event_data:
                self.logger.warning("Empty init query event data in message", 
                                  target_name=self.target_name,
                                  message_target=message.target,
                                  message_source=message.source)
                return
                
            if not isinstance(event_data, InitQueryEvent):
                self.logger.warning("Invalid init query event type in message", 
                                  target_name=self.target_name,
                                  event_type=type(event_data).__name__,
                                  message_target=message.target,
                                  message_source=message.source)
                return
            
            self.logger.info("Processing init query event directly", 
                            target_name=self.target_name,
                            event_id=getattr(event_data, 'event_id', None),
                            mapping_key=getattr(event_data, 'mapping_key', None),
                            target_table=getattr(event_data, 'target_table', None))
            
            # Process init query event directly (no queue needed for init queries)
            self._process_init_query_event(event_data)
            
            self.logger.info("Init query event processed successfully", 
                            target_name=self.target_name,
                            event_id=getattr(event_data, 'event_id', None))
            
        except Exception as e:
            import traceback
            error_traceback = traceback.format_exc()
            self.logger.error("Error handling init query event message", 
                            target_name=self.target_name, 
                            error=str(e),
                            error_type=type(e).__name__,
                            message_target=getattr(message, 'target', None) if message else None,
                            message_source=getattr(message, 'source', None) if message else None,
                            message_data_type=type(message.data).__name__ if message and message.data else None,
                            traceback=error_traceback)
            with self._stats_lock:
                self._stats['errors_count'] += 1
    
    def _handle_shutdown(self, message: Message) -> None:
        """Handle shutdown message"""
        self.logger.info("Shutdown message received", 
                        target_name=self.target_name,
                        source=message.source)
        with self._shutdown_lock:
            self._shutdown_requested = True
    
    def _process_event(self, event: BinlogEvent) -> None:
        """Process a single binlog event"""
        try:
            self.logger.debug("Starting to process binlog event", 
                            target_name=self.target_name,
                            event_type=type(event).__name__,
                            event_id=getattr(event, 'event_id', None),
                            schema=getattr(event, 'schema', None),
                            table=getattr(event, 'table', None),
                            source_name=getattr(event, 'source_name', None))
            
            # Check target connection before processing
            if not self._ensure_target_connection():
                self.logger.warning("Target connection not available, skipping event", 
                                  target_name=self.target_name,
                                  schema=event.schema,
                                  table=event.table)
                return
            
            # Get table mapping
            table_mapping = self._get_table_mapping(event.schema, event.table, event.source_name)
            
            if not table_mapping:
                self.logger.debug("No mapping found for table", 
                                schema=event.schema, 
                                table=event.table, 
                                source_name=event.source_name,
                                target_name=self.target_name)
                return
            
            # Get target information from mapping
            if table_mapping.target:
                # New format: use target field
                target_name = table_mapping.target
                target_table_name = table_mapping.target_table
            else:
                # Legacy format: parse target_table (should not happen in new configs)
                self.logger.warning("Using legacy target table format, consider updating configuration", 
                                  schema=event.schema, 
                                  table=event.table, 
                                  source_name=event.source_name,
                                  target_table=table_mapping.target_table)
                target_name, target_table_name = self.config.parse_target_table(table_mapping.target_table)
            
            if target_name != self.target_name:
                # Event is for a different target
                return
            
            # Process based on event type
            if isinstance(event, InsertEvent):
                self._process_insert_event(event, table_mapping, target_table_name)
                with self._stats_lock:
                    self._stats['inserts_processed'] += 1
            elif isinstance(event, UpdateEvent):
                self._process_update_event(event, table_mapping, target_table_name)
                with self._stats_lock:
                    self._stats['updates_processed'] += 1
            elif isinstance(event, DeleteEvent):
                self._process_delete_event(event, table_mapping, target_table_name)
                with self._stats_lock:
                    self._stats['deletes_processed'] += 1
            else:
                self.logger.debug("Unhandled event type", 
                                event_type=type(event).__name__,
                                target_name=self.target_name)
            
            with self._stats_lock:
                self._stats['events_processed'] += 1
                self._stats['last_event_time'] = time.time()
                
        except Exception as e:
            self.logger.warning("Error processing event (ignoring, will retry later)", 
                            error=str(e), 
                            schema=event.schema, 
                            table=event.table,
                            source_name=event.source_name,
                            target_name=self.target_name)
            with self._stats_lock:
                self._stats['errors_count'] += 1
    
    def _get_table_mapping(self, schema: str, table: str, source_name: str = None) -> Optional[Dict[str, Any]]:
        """Get table mapping configuration"""
        self.logger.debug("Looking for table mapping", 
                        target_name=self.target_name,
                        schema=schema,
                        table=table,
                        source_name=source_name)
        
        # First try to find mapping using new method
        if source_name:
            mapping = self.config.get_mapping_by_source_and_table(source_name, schema, table)
            if mapping:
                self.logger.debug("Found mapping using new method", 
                                target_name=self.target_name,
                                schema=schema,
                                table=table,
                                source_name=source_name,
                                mapping_target=getattr(mapping, 'target', None),
                                mapping_target_table=getattr(mapping, 'target_table', None))
                return mapping
        
        # Fallback to old format: try by source_table field
        if source_name:
            source_table = f"{source_name}.{table}"
            mapping = self.config.get_mapping_by_source_table(source_table)
            if mapping:
                self.logger.debug("Found mapping using source_table method", 
                                target_name=self.target_name,
                                source_table=source_table,
                                mapping_target=getattr(mapping, 'target', None),
                                mapping_target_table=getattr(mapping, 'target_table', None))
                return mapping
        
        # Fallback to mapping key (for backward compatibility)
        if source_name:
            mapping_key = f"{source_name}.{table}"
            if mapping_key in self.config.mapping:
                mapping = self.config.mapping.get(mapping_key)
                self.logger.debug("Found mapping using mapping key", 
                                target_name=self.target_name,
                                mapping_key=mapping_key,
                                mapping_target=getattr(mapping, 'target', None),
                                mapping_target_table=getattr(mapping, 'target_table', None))
                return mapping
        
        # Fallback to schema.table format
        mapping_key = f"{schema}.{table}"
        mapping = self.config.mapping.get(mapping_key)
        if mapping:
            self.logger.debug("Found mapping using schema.table format", 
                            target_name=self.target_name,
                            mapping_key=mapping_key,
                            mapping_target=getattr(mapping, 'target', None),
                            mapping_target_table=getattr(mapping, 'target_table', None))
            return mapping
        
        self.logger.debug("No mapping found for table", 
                        target_name=self.target_name,
                        schema=schema,
                        table=table,
                        source_name=source_name,
                        available_mappings=list(self.config.mapping.keys())[:10])  # Show first 10 mappings
        return None
    
    def _process_init_query_event(self, event: InitQueryEvent) -> None:
        """Process init query event"""
        import uuid
        operation_id = str(uuid.uuid4())[:8]
        
        try:
            self.logger.info("Processing init query event", 
                            operation_id=operation_id,
                            event_id=event.event_id,
                            mapping_key=event.mapping_key,
                            target_table=event.target_table)
            
            # Check target connection before processing
            if not self._ensure_target_connection():
                self.logger.warning("Target connection not available, skipping init query event", 
                                  target_name=self.target_name,
                                  mapping_key=event.mapping_key)
                return
            
            # Apply filters if configured
            if event.filter_config:
                if not self.filter_service.apply_filter(event.row_data, event.filter_config):
                    self.logger.debug("Init query row filtered out", 
                                    mapping_key=event.mapping_key, 
                                    row=event.row_data)
                    return
            
            # Apply transformations
            source_table = event.mapping_key  # mapping_key is already in format "source_name.table_name"
            transformed_data = self.transform_service.apply_column_transforms(
                event.row_data, event.column_mapping, source_table
            )
            
            # Build and execute UPSERT
            sql, values = SQLBuilder.build_upsert_sql(
                event.target_table,
                transformed_data,
                event.primary_key
            )
            
            # Execute the UPSERT
            self.database_service.execute_update(sql, values, self.target_name)
            
            # Update statistics
            with self._stats_lock:
                self._stats['init_query_events_processed'] += 1
                self._stats['last_event_time'] = time.time()
            
            self.logger.debug("Init query event processed successfully", 
                            operation_id=operation_id,
                            event_id=event.event_id,
                            mapping_key=event.mapping_key,
                            original=event.row_data, 
                            transformed=transformed_data)
            
        except Exception as e:
            with self._stats_lock:
                self._stats['errors_count'] += 1
            
            self.logger.error("Error processing init query event", 
                            operation_id=operation_id,
                            event_id=event.event_id,
                            mapping_key=event.mapping_key,
                            error=str(e))
            raise
    
    def _process_insert_event(self, event: InsertEvent, table_mapping, target_table_name: str) -> None:
        """Process INSERT event"""
        import uuid
        operation_id = str(uuid.uuid4())[:8]
        
        try:
            self.logger.info("Processing INSERT event", 
                            operation_id=operation_id,
                            event_id=event.event_id,
                            table=event.table, 
                            schema=event.schema,
                            source_name=event.source_name,
                            target_name=self.target_name,
                            timestamp=event.timestamp,
                            log_pos=event.log_pos,
                            server_id=event.server_id,
                            original_data=event.values)
            
            # Apply filters if configured
            if table_mapping.filter:
                self.logger.debug("Applying filter to INSERT event", 
                                operation_id=operation_id,
                                filter=table_mapping.filter,
                                data=event.values)
                
                if not self.filter_service.apply_filter(event.values, table_mapping.filter):
                    self.logger.info("INSERT event filtered out", 
                                    operation_id=operation_id,
                                    table=event.table, 
                                    schema=event.schema,
                                    source_name=event.source_name,
                                    target_name=self.target_name,
                                    filter=table_mapping.filter)
                    return
                
                self.logger.debug("INSERT event passed filter", 
                                operation_id=operation_id,
                                filter=table_mapping.filter)
            
            # Apply transformations
            source_table = f"{event.schema}.{event.table}"
            self.logger.debug("Starting data transformation for INSERT", 
                            operation_id=operation_id,
                            source_table=source_table,
                            column_mapping=len(table_mapping.column_mapping))
            
            transformed_data = self.transform_service.apply_column_transforms(
                event.values, table_mapping.column_mapping, source_table
            )
            
            self.logger.debug("Data transformation completed for INSERT", 
                            operation_id=operation_id,
                            original_keys=list(event.values.keys()),
                            transformed_keys=list(transformed_data.keys()))
            
            # Build and execute UPSERT
            self.logger.debug("Building UPSERT SQL for INSERT", 
                            operation_id=operation_id,
                            target_table=target_table_name,
                            primary_key=table_mapping.primary_key)
            
            sql, values = SQLBuilder.build_upsert_sql(
                target_table_name,
                transformed_data,
                table_mapping.primary_key
            )
            
            self.logger.info("Executing UPSERT for INSERT", 
                            operation_id=operation_id,
                            sql=sql,
                            values_count=len(values),
                            target_name=self.target_name)
            
            result = self._execute_with_retry(
                self.database_service.execute_update, sql, values, self.target_name
            )
            
            self.logger.info("INSERT processed successfully", 
                            operation_id=operation_id,
                            original=event.values, 
                            transformed=transformed_data,
                            sql=sql,
                            affected_rows=result,
                            target_name=self.target_name)
        except Exception as e:
            self.logger.error("Error processing INSERT event", 
                            operation_id=operation_id,
                            error=str(e), 
                            table=event.table, 
                            schema=event.schema,
                            source_name=event.source_name,
                            target_name=self.target_name,
                            original_data=event.values)
    
    def _process_update_event(self, event: UpdateEvent, table_mapping, target_table_name: str) -> None:
        """Process UPDATE event"""
        import uuid
        operation_id = str(uuid.uuid4())[:8]
        
        try:
            self.logger.info("Processing UPDATE event", 
                            operation_id=operation_id,
                            event_id=event.event_id,
                            table=event.table, 
                            schema=event.schema,
                            source_name=event.source_name,
                            target_name=self.target_name,
                            timestamp=event.timestamp,
                            log_pos=event.log_pos,
                            server_id=event.server_id,
                            before_data=event.before_values,
                            after_data=event.after_values)
            
            # Apply filters if configured (check both before and after values)
            if table_mapping.filter:
                self.logger.debug("Applying filter to UPDATE event", 
                                operation_id=operation_id,
                                filter=table_mapping.filter,
                                before_data=event.before_values,
                                after_data=event.after_values)
                
                # Check if after_values pass the filter
                after_passes_filter = self.filter_service.apply_filter(event.after_values, table_mapping.filter)
                # Check if before_values passed the filter
                before_passed_filter = self.filter_service.apply_filter(event.before_values, table_mapping.filter)
                
                self.logger.debug("Filter results for UPDATE", 
                                operation_id=operation_id,
                                before_passed_filter=before_passed_filter,
                                after_passes_filter=after_passes_filter)
                
                if not after_passes_filter and not before_passed_filter:
                    # Both before and after don't pass filter, skip
                    self.logger.info("UPDATE event filtered out (both before and after)", 
                                    operation_id=operation_id,
                                    table=event.table, 
                                    schema=event.schema,
                                    source_name=event.source_name,
                                    target_name=self.target_name,
                                    filter=table_mapping.filter)
                    return
                elif not after_passes_filter and before_passed_filter:
                    # Record was previously included but now excluded, delete it
                    self.logger.info("UPDATE event: record now filtered out, deleting", 
                                    operation_id=operation_id,
                                    table=event.table, 
                                    schema=event.schema,
                                    source_name=event.source_name,
                                    target_name=self.target_name,
                                    filter=table_mapping.filter)
                    source_table = f"{event.schema}.{event.table}"
                    self._delete_filtered_record(event.before_values, table_mapping, target_table_name, source_table, operation_id)
                    return
                
                self.logger.debug("UPDATE event passed filter", 
                                operation_id=operation_id,
                                filter=table_mapping.filter)
            
            # Apply transformations to after_values
            source_table = f"{event.schema}.{event.table}"
            self.logger.debug("Starting data transformation for UPDATE", 
                            operation_id=operation_id,
                            source_table=source_table,
                            column_mapping=len(table_mapping.column_mapping))
            
            transformed_data = self.transform_service.apply_column_transforms(
                event.after_values, table_mapping.column_mapping, source_table
            )
            
            self.logger.debug("Data transformation completed for UPDATE", 
                            operation_id=operation_id,
                            before_keys=list(event.before_values.keys()),
                            after_keys=list(event.after_values.keys()),
                            transformed_keys=list(transformed_data.keys()))
            
            # Build and execute UPSERT
            self.logger.debug("Building UPSERT SQL for UPDATE", 
                            operation_id=operation_id,
                            target_table=target_table_name,
                            primary_key=table_mapping.primary_key)
            
            sql, values = SQLBuilder.build_upsert_sql(
                target_table_name,
                transformed_data,
                table_mapping.primary_key
            )
            
            self.logger.info("Executing UPSERT for UPDATE", 
                            operation_id=operation_id,
                            sql=sql,
                            values_count=len(values),
                            target_name=self.target_name)
            
            result = self._execute_with_retry(
                self.database_service.execute_update, sql, values, self.target_name
            )
            
            self.logger.info("UPDATE processed successfully", 
                            operation_id=operation_id,
                            before=event.before_values,
                            after=event.after_values, 
                            transformed=transformed_data,
                            sql=sql,
                            affected_rows=result,
                            target_name=self.target_name)
        except Exception as e:
            self.logger.error("Error processing UPDATE event", 
                            operation_id=operation_id,
                            error=str(e), 
                            table=event.table, 
                            schema=event.schema,
                            source_name=event.source_name,
                            target_name=self.target_name,
                            before_data=event.before_values,
                            after_data=event.after_values)
    
    def _process_delete_event(self, event: DeleteEvent, table_mapping, target_table_name: str) -> None:
        """Process DELETE event"""
        import uuid
        operation_id = str(uuid.uuid4())[:8]
        
        try:
            self.logger.info("Processing DELETE event", 
                            operation_id=operation_id,
                            event_id=event.event_id,
                            table=event.table, 
                            schema=event.schema,
                            source_name=event.source_name,
                            target_name=self.target_name,
                            timestamp=event.timestamp,
                            log_pos=event.log_pos,
                            server_id=event.server_id,
                            data=event.values)
            
            # Apply filters if configured
            if table_mapping.filter:
                self.logger.debug("Applying filter to DELETE event", 
                                operation_id=operation_id,
                                filter=table_mapping.filter,
                                data=event.values)
                
                if not self.filter_service.apply_filter(event.values, table_mapping.filter):
                    self.logger.info("DELETE event filtered out", 
                                    operation_id=operation_id,
                                    table=event.table, 
                                    schema=event.schema,
                                    source_name=event.source_name,
                                    target_name=self.target_name,
                                    filter=table_mapping.filter)
                    return
                
                self.logger.debug("DELETE event passed filter", 
                                operation_id=operation_id,
                                filter=table_mapping.filter)
            
            # Apply transformations to get primary key
            source_table = f"{event.schema}.{event.table}"
            self.logger.debug("Starting data transformation for DELETE", 
                            operation_id=operation_id,
                            source_table=source_table,
                            column_mapping=len(table_mapping.column_mapping))
            
            transformed_data = self.transform_service.apply_column_transforms(
                event.values, table_mapping.column_mapping, source_table
            )
            
            self.logger.debug("Data transformation completed for DELETE", 
                            operation_id=operation_id,
                            original_keys=list(event.values.keys()),
                            transformed_keys=list(transformed_data.keys()))
            
            # Build and execute DELETE
            self.logger.debug("Building DELETE SQL", 
                            operation_id=operation_id,
                            target_table=target_table_name,
                            primary_key=table_mapping.primary_key)
            
            sql, values = SQLBuilder.build_delete_sql(
                target_table_name,
                transformed_data,
                table_mapping.primary_key
            )
            
            self.logger.info("Executing DELETE", 
                            operation_id=operation_id,
                            sql=sql,
                            values_count=len(values),
                            target_name=self.target_name)
            
            result = self._execute_with_retry(
                self.database_service.execute_update, sql, values, self.target_name
            )
            
            self.logger.info("DELETE processed successfully", 
                            operation_id=operation_id,
                            data=event.values, 
                            transformed=transformed_data,
                            sql=sql,
                            affected_rows=result,
                            target_name=self.target_name)
        except Exception as e:
            self.logger.error("Error processing DELETE event", 
                            operation_id=operation_id,
                            error=str(e), 
                            table=event.table, 
                            schema=event.schema,
                            source_name=event.source_name,
                            target_name=self.target_name,
                            data=event.values)
    
    def _delete_filtered_record(self, values: dict, table_mapping, target_table_name: str, source_table: str, operation_id: str = None) -> None:
        """Delete a record that was previously included but now filtered out"""
        if not operation_id:
            import uuid
            operation_id = str(uuid.uuid4())[:8]
        
        try:
            self.logger.info("Deleting filtered record", 
                            operation_id=operation_id,
                            data=values,
                            target_table=target_table_name,
                            target_name=self.target_name)
            
            # Apply transformations to get primary key
            self.logger.debug("Starting data transformation for filtered record deletion", 
                            operation_id=operation_id,
                            source_table=source_table,
                            column_mapping=len(table_mapping.column_mapping))
            
            transformed_data = self.transform_service.apply_column_transforms(
                values, table_mapping.column_mapping, source_table
            )
            
            self.logger.debug("Data transformation completed for filtered record deletion", 
                            operation_id=operation_id,
                            original_keys=list(values.keys()),
                            transformed_keys=list(transformed_data.keys()))
            
            # Build and execute DELETE
            self.logger.debug("Building DELETE SQL for filtered record", 
                            operation_id=operation_id,
                            target_table=target_table_name,
                            primary_key=table_mapping.primary_key)
            
            sql, delete_values = SQLBuilder.build_delete_sql(
                target_table_name,
                transformed_data,
                table_mapping.primary_key
            )
            
            self.logger.info("Executing DELETE for filtered record", 
                            operation_id=operation_id,
                            sql=sql,
                            values_count=len(delete_values),
                            target_name=self.target_name)
            
            result = self._execute_with_retry(
                self.database_service.execute_update, sql, delete_values, self.target_name
            )
            
            self.logger.info("Filtered record deleted successfully", 
                            operation_id=operation_id,
                            data=values, 
                            transformed=transformed_data,
                            sql=sql,
                            affected_rows=result,
                            target_name=self.target_name)
        except Exception as e:
            self.logger.error("Error deleting filtered record", 
                            operation_id=operation_id,
                            error=str(e), 
                            data=values,
                            target_name=self.target_name)
    
    def _is_shutdown_requested(self) -> bool:
        """Check if shutdown is requested"""
        with self._shutdown_lock:
            return self._shutdown_requested
    
    def _log_queue_stats(self) -> None:
        """Log queue statistics for monitoring"""
        with self._stats_lock:
            stats = self._stats.copy()
        
        current_queue_size = self._event_queue.qsize()
        max_queue_size = self._event_queue.maxsize
        queue_usage_percent = (current_queue_size / max_queue_size * 100) if max_queue_size > 0 else 0
        
        self.logger.info("Target thread queue statistics", 
                        target_name=self.target_name,
                        queue_size=current_queue_size,
                        max_queue_size=max_queue_size,
                        queue_usage_percent=queue_usage_percent,
                        events_processed=stats['events_processed'],
                        errors_count=stats['errors_count'],
                        queue_overflow_count=stats['queue_overflow_count'],
                        inserts_processed=stats['inserts_processed'],
                        updates_processed=stats['updates_processed'],
                        deletes_processed=stats['deletes_processed'],
                        init_query_events_processed=stats['init_query_events_processed'])
    
    def _ensure_target_connection(self) -> bool:
        """Ensure target connection is available and reconnect if needed"""
        try:
            # Check if connection exists and is valid
            if not self.database_service.connection_exists(self.target_name):
                self.logger.warning("Target connection does not exist, attempting to create", 
                                  target_name=self.target_name)
                try:
                    self.database_service.connect(self.target_config, self.target_name)
                    self.logger.info("Target connection created successfully", 
                                   target_name=self.target_name)
                    return True
                except Exception as e:
                    self.logger.error("Failed to create target connection", 
                                    target_name=self.target_name, error=str(e))
                    return False
            
            # Check if connection is still valid and reconnect if needed
            if not self.database_service.reconnect_if_needed(self.target_name):
                self.logger.error("Failed to reconnect to target", target_name=self.target_name)
                return False
            
            return True
            
        except Exception as e:
            self.logger.error("Error ensuring target connection", 
                            target_name=self.target_name, error=str(e))
            return False
    
    def _execute_with_retry(self, operation_func, *args, **kwargs):
        """Execute database operation with retry logic"""
        max_retries = 3
        retry_delay = 1.0
        
        for attempt in range(max_retries):
            try:
                # Ensure connection is available before each attempt
                if not self._ensure_target_connection():
                    if attempt < max_retries - 1:
                        self.logger.warning("Connection not available, retrying", 
                                          target_name=self.target_name,
                                          attempt=attempt + 1,
                                          max_retries=max_retries)
                        time.sleep(retry_delay * (attempt + 1))
                        continue
                    else:
                        raise Exception("Target connection not available after all retries")
                
                # Execute the operation
                return operation_func(*args, **kwargs)
                
            except Exception as e:
                self.logger.warning("Database operation failed, retrying", 
                                  target_name=self.target_name,
                                  attempt=attempt + 1,
                                  max_retries=max_retries,
                                  error=str(e))
                
                if attempt < max_retries - 1:
                    time.sleep(retry_delay * (attempt + 1))
                    continue
                else:
                    raise e


class TargetThreadService:
    """Service for managing target threads"""
    
    def __init__(self, message_bus: MessageBus, database_service: DatabaseService,
                 transform_service: TransformService, filter_service: FilterService):
        self.message_bus = message_bus
        self.database_service = database_service
        self.transform_service = transform_service
        self.filter_service = filter_service
        self.logger = structlog.get_logger()
        
        # Thread management
        self._target_threads: Dict[str, TargetThread] = {}
        self._threads_lock = threading.RLock()
        
        # Shutdown flag
        self._shutdown_requested = False
        self._shutdown_lock = threading.Lock()
        
        self.logger.info("Target thread service initialized")
    
    def start_target(self, target_name: str, target_config: DatabaseConfig, config: ETLConfig) -> None:
        """Start a target thread"""
        with self._threads_lock:
            if target_name in self._target_threads:
                self.logger.warning("Target thread already exists", target_name=target_name)
                return
            
            target_thread = TargetThread(
                target_name=target_name,
                target_config=target_config,
                message_bus=self.message_bus,
                database_service=self.database_service,
                transform_service=self.transform_service,
                filter_service=self.filter_service,
                config=config
            )
            
            self._target_threads[target_name] = target_thread
            target_thread.start()
            
            self.logger.info("Target thread started", target_name=target_name)
    
    def stop_target(self, target_name: str) -> None:
        """Stop a specific target thread"""
        with self._threads_lock:
            if target_name not in self._target_threads:
                self.logger.warning("Target thread not found", target_name=target_name)
                return
            
            target_thread = self._target_threads[target_name]
            target_thread.stop()
            del self._target_threads[target_name]
            
            self.logger.info("Target thread stopped", target_name=target_name)
    
    def stop_all_targets(self) -> None:
        """Stop all target threads"""
        with self._shutdown_lock:
            self._shutdown_requested = True
        
        with self._threads_lock:
            target_names = list(self._target_threads.keys())
        
        for target_name in target_names:
            self.stop_target(target_name)
        
        self.logger.info("All target threads stopped")
    
    def get_target_stats(self, target_name: str) -> Optional[Dict[str, Any]]:
        """Get statistics for a specific target"""
        with self._threads_lock:
            if target_name not in self._target_threads:
                return None
            return self._target_threads[target_name].get_stats()
    
    def get_all_stats(self) -> Dict[str, Dict[str, Any]]:
        """Get statistics for all targets"""
        with self._threads_lock:
            return {name: thread.get_stats() for name, thread in self._target_threads.items()}
    
    def is_target_running(self, target_name: str) -> bool:
        """Check if a target is running"""
        with self._threads_lock:
            if target_name not in self._target_threads:
                return False
            return self._target_threads[target_name].is_running()
    
    def get_running_targets(self) -> List[str]:
        """Get list of running targets"""
        with self._threads_lock:
            return [name for name, thread in self._target_threads.items() if thread.is_running()]

