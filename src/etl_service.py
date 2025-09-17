"""
Main ETL service for MySQL Replication ETL
"""

from typing import Optional
import os
import structlog

from .exceptions import ETLException
from .models.config import ETLConfig
from .models.events import BinlogEvent, InsertEvent, UpdateEvent, DeleteEvent
from .services import ConfigService, DatabaseService, TransformService, ReplicationService, FilterService
from .utils import SQLBuilder, retry_on_connection_error, retry_on_transform_error


class ETLService:
    """Main ETL service orchestrating all operations"""
    
    def __init__(self):
        self.logger = structlog.get_logger()
        self.config_service = ConfigService()
        self.database_service = DatabaseService()
        self.transform_service = TransformService()
        self.filter_service = FilterService()
        self.replication_service: Optional[ReplicationService] = None
        self.config: Optional[ETLConfig] = None
        self._shutdown_requested = False
    
    def request_shutdown(self) -> None:
        """Запрос на остановку сервиса"""
        self.logger.info("Shutdown requested")
        self._shutdown_requested = True
        if self.replication_service:
            self.replication_service.request_shutdown()
    
    def initialize(self, config_path: str) -> None:
        """Initialize ETL service with configuration"""
        try:
            # Load configuration
            self.config = self.config_service.load_config(config_path)
            
            # Validate configuration
            if not self.config_service.validate_config(self.config):
                raise ETLException("Invalid configuration")
            
            # Load transform module from config directory
            config_dir = os.path.dirname(os.path.abspath(config_path))
            self.logger.debug("Loading transform module", 
                            config_path=config_path, 
                            config_dir=config_dir)
            self.transform_service.load_transform_module(config_dir=config_dir)
            
            # Initialize replication service
            self.replication_service = ReplicationService(self.database_service)
            
            self.logger.info("ETL service initialized", config_path=config_path)
            
        except Exception as e:
            self.logger.error("Failed to initialize ETL service", error=str(e))
            raise ETLException(f"Initialization failed: {e}")
    
    @retry_on_connection_error(max_attempts=3)
    def connect_to_targets(self) -> None:
        """Connect to all target databases"""
        try:
            for target_name, target_config in self.config.targets.items():
                self.database_service.connect(target_config, target_name)
                self.logger.info("Connected to target database", target_name=target_name)
        except Exception as e:
            self.logger.error("Failed to connect to target databases", error=str(e))
            raise ETLException(f"Target connection failed: {e}")
    
    def test_connections(self) -> bool:
        """Test all source and target connections"""
        try:
            # Test all source connections
            for source_name, source_config in self.config.sources.items():
                source_ok = self.database_service.test_connection(source_config)
                if not source_ok:
                    self.logger.error("Source connection test failed", source_name=source_name)
                    return False
            
            # Test all target connections
            for target_name, target_config in self.config.targets.items():
                target_ok = self.database_service.test_connection(target_config)
                if not target_ok:
                    self.logger.error("Target connection test failed", target_name=target_name)
                    return False
            
            self.logger.info("All connections tested successfully")
            return True
            
        except Exception as e:
            self.logger.error("Connection test failed", error=str(e))
            return False
    
    def process_event(self, event: BinlogEvent) -> None:
        """Process a single binlog event"""
        try:
            # Get table mapping
            table_mapping = self.replication_service.get_table_mapping(
                self.config, event.schema, event.table, event.source_name
            )
            
            if not table_mapping:
                self.logger.debug("No mapping found for table", 
                                schema=event.schema, table=event.table, source_name=event.source_name)
                return
            
            # Parse target table to get target name and table name
            target_name, target_table_name = self.config.parse_target_table(table_mapping.target_table)
            
            # Process based on event type
            if isinstance(event, InsertEvent):
                self._process_insert_event(event, table_mapping, target_name, target_table_name)
            elif isinstance(event, UpdateEvent):
                self._process_update_event(event, table_mapping, target_name, target_table_name)
            elif isinstance(event, DeleteEvent):
                self._process_delete_event(event, table_mapping, target_name, target_table_name)
            else:
                self.logger.debug("Unhandled event type", 
                                event_type=type(event).__name__)
                
        except Exception as e:
            self.logger.error("Error processing event", 
                            error=str(e), 
                            schema=event.schema, 
                            table=event.table,
                            source_name=event.source_name)
            raise ETLException(f"Event processing failed: {e}")
    
    @retry_on_transform_error(max_attempts=2)
    def _process_insert_event(self, event: InsertEvent, table_mapping, target_name: str, target_table_name: str) -> None:
        """Process INSERT event"""
        self.logger.info("Processing INSERT event", 
                        table=event.table, 
                        schema=event.schema,
                        source_name=event.source_name,
                        target_name=target_name)
        
        # Apply filters if configured
        if table_mapping.filter:
            if not self.filter_service.apply_filter(event.values, table_mapping.filter):
                self.logger.debug("INSERT event filtered out", 
                                table=event.table, 
                                schema=event.schema,
                                source_name=event.source_name,
                                filter=table_mapping.filter)
                return
        
        # Apply transformations
        source_table = f"{event.schema}.{event.table}"
        transformed_data = self.transform_service.apply_column_transforms(
            event.values, table_mapping.column_mapping, source_table
        )
        
        # Build and execute UPSERT
        sql, values = SQLBuilder.build_upsert_sql(
            target_table_name,
            transformed_data,
            table_mapping.primary_key
        )
        
        self.database_service.execute_update(sql, values, target_name)
        self.logger.info("INSERT processed successfully", 
                        original=event.values, 
                        transformed=transformed_data,
                        target_name=target_name)
    
    @retry_on_transform_error(max_attempts=2)
    def _process_update_event(self, event: UpdateEvent, table_mapping, target_name: str, target_table_name: str) -> None:
        """Process UPDATE event"""
        self.logger.info("Processing UPDATE event", 
                        table=event.table, 
                        schema=event.schema,
                        source_name=event.source_name,
                        target_name=target_name)
        
        # Apply filters if configured (check both before and after values)
        if table_mapping.filter:
            # Check if after_values pass the filter
            after_passes_filter = self.filter_service.apply_filter(event.after_values, table_mapping.filter)
            # Check if before_values passed the filter
            before_passed_filter = self.filter_service.apply_filter(event.before_values, table_mapping.filter)
            
            if not after_passes_filter and not before_passed_filter:
                # Both before and after don't pass filter, skip
                self.logger.debug("UPDATE event filtered out (both before and after)", 
                                table=event.table, 
                                schema=event.schema,
                                source_name=event.source_name,
                                filter=table_mapping.filter)
                return
            elif not after_passes_filter and before_passed_filter:
                # Record was previously included but now excluded, delete it
                self.logger.debug("UPDATE event: record now filtered out, deleting", 
                                table=event.table, 
                                schema=event.schema,
                                source_name=event.source_name,
                                filter=table_mapping.filter)
                source_table = f"{event.schema}.{event.table}"
                self._delete_filtered_record(event.before_values, table_mapping, target_name, target_table_name, source_table)
                return
        
        # Apply transformations to after_values
        source_table = f"{event.schema}.{event.table}"
        transformed_data = self.transform_service.apply_column_transforms(
            event.after_values, table_mapping.column_mapping, source_table
        )
        
        # Build and execute UPSERT
        sql, values = SQLBuilder.build_upsert_sql(
            target_table_name,
            transformed_data,
            table_mapping.primary_key
        )
        
        self.database_service.execute_update(sql, values, target_name)
        self.logger.info("UPDATE processed successfully", 
                        before=event.before_values,
                        after=event.after_values, 
                        transformed=transformed_data,
                        target_name=target_name)
    
    def _process_delete_event(self, event: DeleteEvent, table_mapping, target_name: str, target_table_name: str) -> None:
        """Process DELETE event"""
        self.logger.info("Processing DELETE event", 
                        table=event.table, 
                        schema=event.schema,
                        source_name=event.source_name,
                        target_name=target_name)
        
        # Apply filters if configured
        if table_mapping.filter:
            if not self.filter_service.apply_filter(event.values, table_mapping.filter):
                self.logger.debug("DELETE event filtered out", 
                                table=event.table, 
                                schema=event.schema,
                                source_name=event.source_name,
                                filter=table_mapping.filter)
                return
        
        # Apply transformations to get primary key
        source_table = f"{event.schema}.{event.table}"
        transformed_data = self.transform_service.apply_column_transforms(
            event.values, table_mapping.column_mapping, source_table
        )
        
        # Build and execute DELETE
        sql, values = SQLBuilder.build_delete_sql(
            target_table_name,
            transformed_data,
            table_mapping.primary_key
        )
        
        self.database_service.execute_update(sql, values, target_name)
        self.logger.info("DELETE processed successfully", 
                        data=event.values, 
                        transformed=transformed_data,
                        target_name=target_name)
    
    def _delete_filtered_record(self, values: dict, table_mapping, target_name: str, target_table_name: str, source_table: str) -> None:
        """Delete a record that was previously included but now filtered out"""
        # Apply transformations to get primary key
        transformed_data = self.transform_service.apply_column_transforms(
            values, table_mapping.column_mapping, source_table
        )
        
        # Build and execute DELETE
        sql, values = SQLBuilder.build_delete_sql(
            target_table_name,
            transformed_data,
            table_mapping.primary_key
        )
        
        self.database_service.execute_update(sql, values, target_name)
        self.logger.info("Filtered record deleted successfully", 
                        data=values, 
                        transformed=transformed_data,
                        target_name=target_name)
    
    def run_replication(self) -> None:
        """Run the replication process"""
        try:
            # Connect to all targets
            self.connect_to_targets()
            
            # Execute init queries for empty target tables
            self.execute_init_queries()
            
            # Connect to replication streams for all sources
            for source_name, source_config in self.config.sources.items():
                # Get tables for this source from pipeline configuration
                tables = self.config.get_tables_for_source(source_name)
                self.logger.info("Tables configured for source", 
                               source_name=source_name, 
                               tables=[f"{schema}.{table}" for schema, table in tables])
                
                self.replication_service.connect_to_replication(
                    source_name, source_config, self.config.replication, tables
                )
                self.logger.info("Connected to replication stream", source_name=source_name)
            
            self.logger.info("Starting replication process")
            
            # Process events from all sources
            for source_name, event in self.replication_service.get_all_events():
                # Проверяем флаг остановки перед обработкой каждого события
                if self._shutdown_requested:
                    self.logger.info("Shutdown requested, stopping event processing")
                    break
                    
                self.process_event(event)
                
        except KeyboardInterrupt:
            self.logger.info("Replication stopped by user")
        except Exception as e:
            self.logger.error("Replication failed", error=str(e))
            raise ETLException(f"Replication failed: {e}")
        finally:
            self.cleanup()
    
    def execute_init_queries(self) -> None:
        """Execute init queries for empty target tables"""
        try:
            self.logger.info("Checking for init queries to execute")
            
            for mapping_key, table_mapping in self.config.mapping.items():
                if not table_mapping.init_query:
                    continue
                
                # Parse target table to get target name and table name
                target_name, target_table_name = self.config.parse_target_table(table_mapping.target_table)
                
                # Check if target table is empty
                if not self.database_service.is_table_empty(target_table_name, target_name):
                    self.logger.debug("Target table not empty, skipping init query", 
                                    mapping_key=mapping_key, 
                                    target_table=table_mapping.target_table)
                    continue
                
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
                    self.database_service.connect(source_config, source_connection_name)
                    
                    # Execute init query
                    results, columns = self.database_service.execute_init_query(
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
                            if not self.filter_service.apply_filter(row_dict, table_mapping.filter):
                                self.logger.debug("Init query row filtered out", 
                                                mapping_key=mapping_key, 
                                                row=row_dict)
                                continue
                        
                        # Apply transformations
                        source_table = mapping_key  # mapping_key is already in format "source_name.table_name"
                        transformed_data = self.transform_service.apply_column_transforms(
                            row_dict, table_mapping.column_mapping, source_table
                        )
                        
                        # Build and execute UPSERT
                        sql, values = SQLBuilder.build_upsert_sql(
                            target_table_name,
                            transformed_data,
                            table_mapping.primary_key
                        )
                        
                        self.database_service.execute_update(sql, values, target_name)
                        self.logger.debug("Init query row processed successfully", 
                                        mapping_key=mapping_key, 
                                        original=row_dict, 
                                        transformed=transformed_data)
                    
                    self.logger.info("Init query processing completed", 
                                   mapping_key=mapping_key, 
                                   processed_rows=len(results))
                    
                except Exception as e:
                    self.logger.error("Error executing init query", 
                                    mapping_key=mapping_key, 
                                    error=str(e))
                    raise ETLException(f"Init query execution failed for {mapping_key}: {e}")
                finally:
                    # Close source connection
                    self.database_service.close_connection(source_connection_name)
            
            self.logger.info("All init queries processed")
            
        except Exception as e:
            self.logger.error("Error processing init queries", error=str(e))
            raise ETLException(f"Init queries processing failed: {e}")
    
    def cleanup(self) -> None:
        """Cleanup resources"""
        try:
            self.logger.info("Starting cleanup process")
            
            # Устанавливаем флаг остановки
            self._shutdown_requested = True
            
            # Закрываем replication service
            if self.replication_service:
                try:
                    self.replication_service.close()
                    self.logger.info("Replication service closed")
                except Exception as e:
                    self.logger.error("Error closing replication service", error=str(e))
            
            # Закрываем все соединения с базой данных
            try:
                self.database_service.close_all_connections()
                self.logger.info("Database connections closed")
            except Exception as e:
                self.logger.error("Error closing database connections", error=str(e))
            
            self.logger.info("Cleanup completed successfully")
        except Exception as e:
            self.logger.error("Cleanup error", error=str(e))
