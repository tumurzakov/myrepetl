"""
Main ETL service for MySQL Replication ETL
"""

from typing import Optional
import structlog

from .exceptions import ETLException
from .models.config import ETLConfig
from .models.events import BinlogEvent, InsertEvent, UpdateEvent, DeleteEvent
from .services import ConfigService, DatabaseService, TransformService, ReplicationService
from .utils import SQLBuilder, retry_on_connection_error, retry_on_transform_error


class ETLService:
    """Main ETL service orchestrating all operations"""
    
    def __init__(self):
        self.logger = structlog.get_logger()
        self.config_service = ConfigService()
        self.database_service = DatabaseService()
        self.transform_service = TransformService()
        self.replication_service: Optional[ReplicationService] = None
        self.config: Optional[ETLConfig] = None
    
    def initialize(self, config_path: str) -> None:
        """Initialize ETL service with configuration"""
        try:
            # Load configuration
            self.config = self.config_service.load_config(config_path)
            
            # Validate configuration
            if not self.config_service.validate_config(self.config):
                raise ETLException("Invalid configuration")
            
            # Load transform module
            self.transform_service.load_transform_module()
            
            # Initialize replication service
            self.replication_service = ReplicationService(self.database_service)
            
            self.logger.info("ETL service initialized", config_path=config_path)
            
        except Exception as e:
            self.logger.error("Failed to initialize ETL service", error=str(e))
            raise ETLException(f"Initialization failed: {e}")
    
    @retry_on_connection_error(max_attempts=3)
    def connect_to_target(self) -> None:
        """Connect to target database"""
        try:
            self.database_service.connect(self.config.target, "target")
            self.logger.info("Connected to target database")
        except Exception as e:
            self.logger.error("Failed to connect to target database", error=str(e))
            raise ETLException(f"Target connection failed: {e}")
    
    def test_connections(self) -> bool:
        """Test both source and target connections"""
        try:
            # Test source connection
            source_ok = self.database_service.test_connection(self.config.source)
            if not source_ok:
                self.logger.error("Source connection test failed")
                return False
            
            # Test target connection
            target_ok = self.database_service.test_connection(self.config.target)
            if not target_ok:
                self.logger.error("Target connection test failed")
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
                self.config, event.schema, event.table
            )
            
            if not table_mapping:
                self.logger.debug("No mapping found for table", 
                                schema=event.schema, table=event.table)
                return
            
            # Process based on event type
            if isinstance(event, InsertEvent):
                self._process_insert_event(event, table_mapping)
            elif isinstance(event, UpdateEvent):
                self._process_update_event(event, table_mapping)
            elif isinstance(event, DeleteEvent):
                self._process_delete_event(event, table_mapping)
            else:
                self.logger.debug("Unhandled event type", 
                                event_type=type(event).__name__)
                
        except Exception as e:
            self.logger.error("Error processing event", 
                            error=str(e), 
                            schema=event.schema, 
                            table=event.table)
            raise ETLException(f"Event processing failed: {e}")
    
    @retry_on_transform_error(max_attempts=2)
    def _process_insert_event(self, event: InsertEvent, table_mapping) -> None:
        """Process INSERT event"""
        self.logger.info("Processing INSERT event", 
                        table=event.table, 
                        schema=event.schema)
        
        # Apply transformations
        transformed_data = self.transform_service.apply_column_transforms(
            event.values, table_mapping.column_mapping
        )
        
        # Build and execute UPSERT
        sql, values = SQLBuilder.build_upsert_sql(
            table_mapping.target_table,
            transformed_data,
            table_mapping.primary_key
        )
        
        self.database_service.execute_update(sql, values, "target")
        self.logger.info("INSERT processed successfully", 
                        original=event.values, 
                        transformed=transformed_data)
    
    @retry_on_transform_error(max_attempts=2)
    def _process_update_event(self, event: UpdateEvent, table_mapping) -> None:
        """Process UPDATE event"""
        self.logger.info("Processing UPDATE event", 
                        table=event.table, 
                        schema=event.schema)
        
        # Apply transformations to after_values
        transformed_data = self.transform_service.apply_column_transforms(
            event.after_values, table_mapping.column_mapping
        )
        
        # Build and execute UPSERT
        sql, values = SQLBuilder.build_upsert_sql(
            table_mapping.target_table,
            transformed_data,
            table_mapping.primary_key
        )
        
        self.database_service.execute_update(sql, values, "target")
        self.logger.info("UPDATE processed successfully", 
                        before=event.before_values,
                        after=event.after_values, 
                        transformed=transformed_data)
    
    def _process_delete_event(self, event: DeleteEvent, table_mapping) -> None:
        """Process DELETE event"""
        self.logger.info("Processing DELETE event", 
                        table=event.table, 
                        schema=event.schema)
        
        # Apply transformations to get primary key
        transformed_data = self.transform_service.apply_column_transforms(
            event.values, table_mapping.column_mapping
        )
        
        # Build and execute DELETE
        sql, values = SQLBuilder.build_delete_sql(
            table_mapping.target_table,
            transformed_data,
            table_mapping.primary_key
        )
        
        self.database_service.execute_update(sql, values, "target")
        self.logger.info("DELETE processed successfully", 
                        data=event.values, 
                        transformed=transformed_data)
    
    def run_replication(self) -> None:
        """Run the replication process"""
        try:
            # Connect to target
            self.connect_to_target()
            
            # Connect to replication stream
            stream = self.replication_service.connect_to_replication(
                self.config.source, self.config.replication
            )
            
            self.logger.info("Starting replication process")
            
            # Process events
            for event in self.replication_service.get_events():
                self.process_event(event)
                
        except KeyboardInterrupt:
            self.logger.info("Replication stopped by user")
        except Exception as e:
            self.logger.error("Replication failed", error=str(e))
            raise ETLException(f"Replication failed: {e}")
        finally:
            self.cleanup()
    
    def cleanup(self) -> None:
        """Cleanup resources"""
        try:
            if self.replication_service:
                self.replication_service.close()
            self.database_service.close_all_connections()
            self.logger.info("Cleanup completed")
        except Exception as e:
            self.logger.error("Cleanup error", error=str(e))
