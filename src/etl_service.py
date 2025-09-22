"""
Main ETL service for MySQL Replication ETL
"""

from typing import Optional
import os
import structlog
import signal
import sys

from .exceptions import ETLException
from .models.config import ETLConfig
from .models.events import BinlogEvent, InsertEvent, UpdateEvent, DeleteEvent
from .services import ConfigService, DatabaseService, TransformService, ReplicationService, FilterService
from .services.thread_manager import ThreadManager
from .services.message_bus import MessageBus
from .utils import SQLBuilder, retry_on_connection_error, retry_on_transform_error


class ETLService:
    """Main ETL service orchestrating all operations"""
    
    def __init__(self):
        self.logger = structlog.get_logger()
        self.config_service = ConfigService()
        self.thread_manager: Optional[ThreadManager] = None
        self.config: Optional[ETLConfig] = None
        self.config_path: Optional[str] = None
        self._shutdown_requested = False
        
        # Core services for dependency injection
        self.message_bus = MessageBus(max_queue_size=10000)
        self.database_service = DatabaseService()
        self.transform_service = TransformService()
        self.filter_service = FilterService()
        
        # Setup signal handlers for graceful shutdown
        self._setup_signal_handlers()
    
    def _setup_signal_handlers(self) -> None:
        """Setup signal handlers for graceful shutdown"""
        def signal_handler(signum, frame):
            self.logger.info("Received signal, initiating shutdown", signal=signum)
            self.request_shutdown()
        
        signal.signal(signal.SIGINT, signal_handler)
        signal.signal(signal.SIGTERM, signal_handler)
    
    def request_shutdown(self) -> None:
        """Запрос на остановку сервиса"""
        self.logger.info("Shutdown requested")
        self._shutdown_requested = True
        if self.thread_manager:
            self.thread_manager.stop()
    
    def initialize(self, config_path: str) -> None:
        """Initialize ETL service with configuration"""
        try:
            # Store config path for later use
            self.config_path = config_path
            
            # Load configuration
            self.config = self.config_service.load_config(config_path)
            
            # Validate configuration
            if not self.config_service.validate_config(self.config):
                raise ETLException("Invalid configuration")
            
            # Initialize thread manager with dependency injection
            self.thread_manager = ThreadManager(
                message_bus=self.message_bus,
                database_service=self.database_service,
                transform_service=self.transform_service,
                filter_service=self.filter_service
            )
            
            self.logger.info("ETL service initialized", config_path=config_path)
            
        except Exception as e:
            self.logger.error("Failed to initialize ETL service", error=str(e))
            raise ETLException(f"Initialization failed: {e}")
    
    def test_connections(self) -> bool:
        """Test all source and target connections"""
        try:
            if not self.thread_manager:
                raise ETLException("Thread manager not initialized")
            
            database_service = self.thread_manager.database_service
            
            # Test all source connections
            for source_name, source_config in self.config.sources.items():
                source_ok = database_service.test_connection(source_config)
                if not source_ok:
                    self.logger.error("Source connection test failed", source_name=source_name)
                    return False
            
            # Test all target connections
            for target_name, target_config in self.config.targets.items():
                target_ok = database_service.test_connection(target_config)
                if not target_ok:
                    self.logger.error("Target connection test failed", target_name=target_name)
                    return False
            
            self.logger.info("All connections tested successfully")
            return True
            
        except Exception as e:
            self.logger.error("Connection test failed", error=str(e))
            return False
    
    def _establish_target_connections(self) -> None:
        """Establish target connections for init queries"""
        try:
            if not self.thread_manager:
                raise ETLException("Thread manager not initialized")
            
            database_service = self.thread_manager.database_service
            
            self.logger.info("Establishing target connections for init queries")
            
            # Connect to all target databases
            for target_name, target_config in self.config.targets.items():
                try:
                    database_service.connect(target_config, target_name)
                    self.logger.info("Target connection established", target_name=target_name)
                except Exception as e:
                    self.logger.error("Failed to establish target connection", 
                                    target_name=target_name, error=str(e))
                    raise ETLException(f"Failed to establish target connection '{target_name}': {e}")
            
            self.logger.info("All target connections established successfully")
            
        except Exception as e:
            self.logger.error("Error establishing target connections", error=str(e))
            raise
    
    
    
    def run_replication(self) -> None:
        """Run the replication process using multithreaded architecture"""
        try:
            if not self.thread_manager:
                raise ETLException("Thread manager not initialized")
            
            self.logger.info("Starting multithreaded replication process")
            
            # Start all services and threads (including init query threads)
            self.thread_manager.start(self.config, self.config_path)
            
            # Establish target connections for init queries
            self._establish_target_connections()
            
            self.logger.info("Replication process started successfully (init queries running in parallel)")
            
            # Wait for shutdown signal
            while not self._shutdown_requested and self.thread_manager.is_running():
                try:
                    # Check status periodically
                    stats = self.thread_manager.get_stats()
                    self.logger.debug("Service status", 
                                    status=stats.status.value,
                                    uptime=stats.uptime,
                                    events_processed=stats.total_events_processed,
                                    init_query_threads=stats.init_query_threads_count)
                    
                    # Sleep for a short time to avoid busy waiting
                    import time
                    time.sleep(1.0)
                    
                except KeyboardInterrupt:
                    self.logger.info("Replication stopped by user")
                    break
                except Exception as e:
                    self.logger.error("Error in main loop", error=str(e))
                    break
            
        except Exception as e:
            self.logger.error("Replication error", error=str(e))
            raise
        finally:
            self.cleanup()
    
    def execute_init_queries(self) -> None:
        """Execute init queries for empty target tables
        
        DEPRECATED: This method is kept for backward compatibility.
        Init queries are now executed in parallel threads via ThreadManager.
        Use the new parallel init query system instead.
        """
        try:
            if not self.thread_manager:
                raise ETLException("Thread manager not initialized")
            
            database_service = self.thread_manager.database_service
            transform_service = self.thread_manager.transform_service
            filter_service = self.thread_manager.filter_service
            
            self.logger.info("Checking for init queries to execute")
            
            for mapping_key, table_mapping in self.config.mapping.items():
                if not table_mapping.init_query:
                    continue
                
                # Get target information from mapping
                if table_mapping.target:
                    # New format: use target field
                    target_name = table_mapping.target
                    target_table_name = table_mapping.target_table
                else:
                    # Legacy format: parse target_table (should not happen in new configs)
                    self.logger.warning("Using legacy target table format, consider updating configuration", 
                                      mapping_key=mapping_key, 
                                      target_table=table_mapping.target_table)
                    target_name, target_table_name = self.config.parse_target_table(table_mapping.target_table)
                
                # Check if we should run init query based on configuration
                if table_mapping.init_if_table_empty:
                    # Check if target table is empty only if init_if_table_empty is True
                    try:
                        if not database_service.is_table_empty(target_table_name, target_name):
                            self.logger.debug("Target table not empty and init_if_table_empty=True, skipping init query", 
                                            mapping_key=mapping_key, 
                                            target_table=table_mapping.target_table)
                            continue
                    except Exception as e:
                        # If we can't check if table is empty, log warning and continue
                        self.logger.warning("Could not check if target table is empty, proceeding with init query", 
                                          mapping_key=mapping_key, 
                                          target_table=table_mapping.target_table, 
                                          error=str(e))
                else:
                    # init_if_table_empty=False, always run init query regardless of table state
                    self.logger.debug("init_if_table_empty=False, running init query regardless of table state", 
                                    mapping_key=mapping_key, 
                                    target_table=table_mapping.target_table)
                
                self.logger.info("Executing init query for empty table", 
                               mapping_key=mapping_key, 
                               target_table=table_mapping.target_table,
                               init_query=table_mapping.init_query)
                
                # Get source name from source_table field if available, otherwise from mapping key
                if table_mapping.source_table:
                    try:
                        source_name, _ = self.config.parse_source_table(table_mapping.source_table)
                    except Exception as e:
                        self.logger.warning("Invalid source_table format, using mapping key", 
                                          source_table=table_mapping.source_table, error=str(e))
                        # Fallback to mapping key
                        if '.' not in mapping_key:
                            self.logger.warning("Invalid mapping key format, skipping init query", 
                                              mapping_key=mapping_key)
                            continue
                        source_name = mapping_key.split('.')[0]
                else:
                    # Fallback to mapping key (format: source_name.table_name)
                    if '.' not in mapping_key:
                        self.logger.warning("Invalid mapping key format, skipping init query", 
                                          mapping_key=mapping_key)
                        continue
                    source_name = mapping_key.split('.')[0]
                
                if source_name not in self.config.sources:
                    self.logger.warning("Source not found in configuration, skipping init query", 
                                      source_name=source_name, mapping_key=mapping_key)
                    continue
                
                # Execute init query on source database
                source_config = self.config.get_source_config(source_name)
                source_connection_name = f"init_source_{source_name}"
                
                try:
                    # Connect to source database
                    database_service.connect(source_config, source_connection_name)
                    
                    # Execute init query
                    results, columns = database_service.execute_init_query(
                        table_mapping.init_query, source_connection_name
                    )
                    
                    self.logger.info("Init query executed successfully", 
                                   mapping_key=mapping_key, 
                                   rows_count=len(results))
                    
                    # Process each row from init query
                    for row_data in results:
                        # Convert row to dictionary using column names
                        row_dict = dict(zip(columns, row_data))
                        
                        # Apply filters if configured
                        if table_mapping.filter:
                            if not filter_service.apply_filter(row_dict, table_mapping.filter):
                                self.logger.debug("Init query row filtered out", 
                                                mapping_key=mapping_key, 
                                                row=row_dict)
                                continue
                        
                        # Apply transformations
                        source_table = mapping_key  # mapping_key is already in format "source_name.table_name"
                        transformed_data = transform_service.apply_column_transforms(
                            row_dict, table_mapping.column_mapping, source_table
                        )
                        
                        # Build and execute UPSERT
                        sql, values = SQLBuilder.build_upsert_sql(
                            target_table_name,
                            transformed_data,
                            table_mapping.primary_key
                        )
                        
                        try:
                            # Ensure target connection is available before executing
                            if not database_service.connection_exists(target_name):
                                self.logger.warning("Target connection not found, attempting to establish", 
                                                  target_name=target_name)
                                target_config = self.config.get_target_config(target_name)
                                database_service.connect(target_config, target_name)
                            
                            # Check and reconnect if needed
                            if not database_service.reconnect_if_needed(target_name):
                                self.logger.warning("Target connection not available, skipping init query row", 
                                                  target_name=target_name,
                                                  mapping_key=mapping_key)
                                continue
                            
                            database_service.execute_update(sql, values, target_name)
                            self.logger.debug("Init query row processed successfully", 
                                            mapping_key=mapping_key, 
                                            original=row_dict, 
                                            transformed=transformed_data)
                        except Exception as e:
                            # Log error but continue processing - data will be updated later
                            self.logger.warning("Error processing init query row (ignoring, will retry later)", 
                                            error=str(e), 
                                            mapping_key=mapping_key, 
                                            original=row_dict)
                    
                    self.logger.info("Init query processing completed", 
                                   mapping_key=mapping_key, 
                                   processed_rows=len(results))
                    
                except Exception as e:
                    # Log error but continue processing - data will be updated later
                    self.logger.warning("Error executing init query (ignoring, will retry later)", 
                                    mapping_key=mapping_key, 
                                    error=str(e))
                finally:
                    # Close source connection
                    database_service.close_connection(source_connection_name)
            
            self.logger.info("All init queries processed")
            
        except ETLException as e:
            # Re-raise ETLException (like thread manager not initialized)
            raise
        except Exception as e:
            # Log error but continue processing - data will be updated later
            self.logger.warning("Error processing init queries (continuing, individual queries will be handled separately)", error=str(e))
    
    def cleanup(self) -> None:
        """Cleanup resources"""
        try:
            self.logger.info("Starting cleanup process")
            
            # Устанавливаем флаг остановки
            self._shutdown_requested = True
            
            # Останавливаем thread manager
            if self.thread_manager:
                try:
                    self.thread_manager.stop()
                    self.logger.info("Thread manager stopped")
                except Exception as e:
                    self.logger.error("Error stopping thread manager", error=str(e))
            
            self.logger.info("Cleanup completed successfully")
        except Exception as e:
            self.logger.error("Cleanup error", error=str(e))
            # Force cleanup even if there are errors
            try:
                self.logger.info("Force cleanup completed")
            except Exception:
                pass
