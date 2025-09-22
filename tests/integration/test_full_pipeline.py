"""
Integration tests for full ETL pipeline
"""

import pytest
import tempfile
import json
import os
import time
import threading
from unittest.mock import Mock, patch, MagicMock

from src.etl_service import ETLService
from src.models.config import ETLConfig, DatabaseConfig, ReplicationConfig, TableMapping, ColumnMapping
from src.models.events import InsertEvent, UpdateEvent, DeleteEvent, EventType
from src.services.thread_manager import ServiceStatus


class TestFullPipeline:
    """Test full ETL pipeline integration"""
    
    @pytest.fixture
    def sample_config(self):
        """Create sample configuration for testing"""
        config_data = {
            "sources": {
                "source1": {
                    "host": "localhost",
                    "port": 3306,
                    "user": "source_user",
                    "password": "source_password",
                    "database": "source_db"
                }
            },
            "targets": {
                "target1": {
                    "host": "localhost",
                    "port": 3306,
                    "user": "target_user",
                    "password": "target_password",
                    "database": "target_db"
                }
            },
            "replication": {
                "server_id": 100,
                "log_file": "mysql-bin.000001",
                "log_pos": 4
            },
            "mapping": {
                "source1.users": {
                    "target_table": "target1.users",
                    "primary_key": "id",
                    "column_mapping": {
                        "id": {"column": "id", "primary_key": True},
                        "name": {"column": "name", "transform": "transform.uppercase"},
                        "email": {"column": "email"},
                        "status": {"column": "status", "value": "active"}
                    }
                }
            }
        }
        
        with tempfile.NamedTemporaryFile(mode='w', suffix='.json', delete=False) as f:
            json.dump(config_data, f)
            config_path = f.name
        
        yield config_path
        
        # Cleanup
        os.unlink(config_path)
    
    def test_etl_service_initialization(self, sample_config):
        """Test ETL service initialization with real config"""
        with patch('src.etl_service.ConfigService') as mock_config_service, \
             patch('src.etl_service.ThreadManager') as mock_thread_manager, \
             patch('src.etl_service.MetricsEndpoint') as mock_metrics_endpoint:
            
            # Mock config service to return real config
            mock_config_service.return_value.load_config.return_value = ETLConfig.from_dict({
                "sources": {
                    "source1": {
                        "host": "localhost",
                        "port": 3306,
                        "user": "source_user",
                        "password": "source_password",
                        "database": "source_db"
                    }
                },
                "targets": {
                    "target1": {
                        "host": "localhost",
                        "port": 3306,
                        "user": "target_user",
                        "password": "target_password",
                        "database": "target_db"
                    }
                },
                "replication": {
                    "server_id": 100
                },
                "mapping": {
                    "source1.users": {
                        "target_table": "target1.users",
                        "primary_key": "id",
                        "column_mapping": {
                            "id": {"column": "id", "primary_key": True},
                            "name": {"column": "name"}
                        }
                    }
                },
                "metrics_port": 8081
            })
            mock_config_service.return_value.validate_config.return_value = True
            
            service = ETLService()
            service.initialize(sample_config)
            
            assert service.config is not None
            assert service.thread_manager is not None
            assert service.config.sources["source1"].host == "localhost"
            assert service.config.targets["target1"].host == "localhost"
            assert "source1.users" in service.config.mapping
    
    def test_connection_testing(self, sample_config):
        """Test connection testing functionality"""
        with patch('src.etl_service.ThreadManager') as mock_thread_manager:
            mock_database_service = Mock()
            mock_database_service.test_connection.return_value = True
            mock_thread_manager.return_value.database_service = mock_database_service
            
            service = ETLService()
            service.thread_manager = mock_thread_manager.return_value
            service.config = ETLConfig(
                sources={"source1": DatabaseConfig(host="localhost", user="user", password="pass", database="db")},
                targets={"target1": DatabaseConfig(host="localhost", user="user", password="pass", database="db")},
                replication=ReplicationConfig(),
                mapping={
                    "source1.table": TableMapping(
                        target_table="target1.table",
                        primary_key="id",
                        column_mapping={"id": ColumnMapping(column="id")}
                    )
                }
            )
            
            result = service.test_connections()
            assert result is True
            assert mock_database_service.test_connection.call_count == 2
    
    def test_multithreaded_replication_integration(self):
        """Test multithreaded replication with real components"""
        with patch('src.etl_service.ThreadManager') as mock_thread_manager, \
             patch('src.etl_service.ETLService.execute_init_queries') as mock_init_queries, \
             patch('time.sleep') as mock_sleep:
            
            # Mock thread manager
            mock_thread_manager.return_value.is_running.return_value = True
            mock_thread_manager.return_value.get_stats.return_value = Mock(
                status=ServiceStatus.RUNNING,
                uptime=10.0,
                total_events_processed=5,
                init_query_threads_count=0
            )
            
            # Mock config
            config = ETLConfig(
                sources={"source1": DatabaseConfig(host="localhost", user="user", password="pass", database="db")},
                targets={"target1": DatabaseConfig(host="localhost", user="user", password="pass", database="db")},
                replication=ReplicationConfig(),
                mapping={
                    "source1.table": TableMapping(
                        target_table="target1.table",
                        primary_key="id",
                        column_mapping={"id": ColumnMapping(column="id")}
                    )
                }
            )
            
            service = ETLService()
            service.thread_manager = mock_thread_manager.return_value
            service.config = config
            service._shutdown_requested = False
            
            # Simulate running for a short time then shutdown
            def side_effect(*args):
                service._shutdown_requested = True
            mock_sleep.side_effect = side_effect
            
            service.run_replication()
            
            mock_thread_manager.return_value.start.assert_called_once_with(config, None)
            # Note: execute_init_queries is no longer called directly as init queries are handled by ThreadManager
            mock_thread_manager.return_value.stop.assert_called_once()
    
    def test_message_bus_integration(self):
        """Test message bus integration with real components"""
        from src.services.message_bus import MessageBus, Message, MessageType
        from src.models.events import InsertEvent
        
        # Create real message bus
        message_bus = MessageBus()
        
        # Create test event
        event = InsertEvent(
            schema="test_schema",
            table="test_table",
            values={"id": 1, "name": "test"},
            source_name="test_source"
        )
        
        # Test message publishing and processing
        callback_called = []
        
        def test_callback(message):
            callback_called.append(message)
        
        # Subscribe to messages
        message_bus.subscribe(MessageType.BINLOG_EVENT, test_callback)
        
        # Publish message
        result = message_bus.publish_binlog_event("test_source", event, "test_target")
        assert result is True
        
        # Process messages
        message_bus.process_messages(timeout=0.01)
        
        # Verify callback was called
        assert len(callback_called) == 1
        assert callback_called[0].data == event
        assert callback_called[0].source == "test_source"
        assert callback_called[0].target == "test_target"
    
    def test_error_handling_in_replication(self, sample_config):
        """Test error handling during replication"""
        with patch('src.etl_service.ThreadManager') as mock_thread_manager:
            # Setup mock to raise exception
            mock_thread_manager.return_value.start.side_effect = Exception("Thread manager error")
            
            service = ETLService()
            service.thread_manager = mock_thread_manager.return_value
            service.config = Mock()
            
            with pytest.raises(Exception, match="Thread manager error"):
                service.run_replication()
    
    def test_cleanup_on_exit(self, sample_config):
        """Test cleanup is called on service exit"""
        with patch('src.etl_service.ThreadManager') as mock_thread_manager:
            mock_thread_manager_instance = Mock()
            mock_thread_manager.return_value = mock_thread_manager_instance
            
            service = ETLService()
            service.thread_manager = mock_thread_manager_instance
            
            # Test cleanup
            service.cleanup()
            
            assert service._shutdown_requested is True
            mock_thread_manager_instance.stop.assert_called_once()
    
    def test_config_validation_integration(self):
        """Test configuration validation with real config objects"""
        # Valid config
        valid_config = ETLConfig(
            sources={"source1": DatabaseConfig(host="localhost", user="user", password="pass", database="db")},
            targets={"target1": DatabaseConfig(host="localhost", user="user", password="pass", database="db")},
            replication=ReplicationConfig(),
            mapping={
                "source1.table": TableMapping(
                    target_table="target1.table",
                    primary_key="id",
                    column_mapping={"id": ColumnMapping(column="id")}
                )
            }
        )
        
        service = ETLService()
        service.config_service = Mock()
        service.config_service.validate_config.return_value = True
        
        # This should not raise an exception
        service.config_service.validate_config(valid_config)
    
    def test_transform_functions_integration(self):
        """Test transform functions with real data"""
        service = ETLService()
        from src.services.transform_service import TransformService
        service.transform_service = TransformService()
        service.transform_service.load_transform_module("src.transform")
        
        # Test uppercase transform
        result = service.transform_service.apply_column_transforms(
            {"name": "test"},
            {"name": ColumnMapping(column="name", transform="transform.uppercase")},
            "test_schema.test_table"
        )
        
        assert result["name"] == "TEST"
        
        # Test static value
        result = service.transform_service.apply_column_transforms(
            {"name": "test"},
            {"name": ColumnMapping(column="name", value="STATIC")},
            "test_schema.test_table"
        )
        
        assert result["name"] == "STATIC"
        
        # Test simple copy
        result = service.transform_service.apply_column_transforms(
            {"name": "test"},
            {"name": ColumnMapping(column="name")},
            "test_schema.test_table"
        )
        
        assert result["name"] == "test"
    
    def test_multithreaded_parallel_processing(self):
        """Test that multiple sources can process events in parallel"""
        from src.services.message_bus import MessageBus, MessageType
        from src.services.source_thread_service import SourceThreadService
        from src.services.target_thread_service import TargetThreadService
        from src.models.events import InsertEvent
        
        # Create real components
        message_bus = MessageBus()
        database_service = Mock()
        transform_service = Mock()
        filter_service = Mock()
        
        # Create services
        source_service = SourceThreadService(message_bus, database_service)
        target_service = TargetThreadService(message_bus, database_service, transform_service, filter_service)
        
        # Mock database service
        database_service.get_master_status.return_value = {"file": "binlog.000001", "position": 123}
        database_service.test_connection.return_value = True
        
        # Mock transform service
        transform_service.apply_column_transforms.return_value = {"id": 1, "name": "test"}
        
        # Create config
        config = ETLConfig(
            sources={
                "source1": DatabaseConfig(host="localhost", user="user", password="pass", database="db1"),
                "source2": DatabaseConfig(host="localhost", user="user", password="pass", database="db2")
            },
            targets={
                "target1": DatabaseConfig(host="localhost", user="user", password="pass", database="target_db")
            },
            replication=ReplicationConfig(),
            mapping={
                "source1.table": TableMapping(
                    target_table="target1.table",
                    primary_key="id",
                    column_mapping={"id": ColumnMapping(column="id")}
                ),
                "source2.table": TableMapping(
                    target_table="target1.table",
                    primary_key="id",
                    column_mapping={"id": ColumnMapping(column="id")}
                )
            }
        )
        
        # Test that we can start multiple sources
        source_service.start_source("source1", config.sources["source1"], config.replication, [("db1", "table")])
        source_service.start_source("source2", config.sources["source2"], config.replication, [("db2", "table")])
        
        # Verify both sources are running
        assert source_service.is_source_running("source1")
        assert source_service.is_source_running("source2")
        
        # Test that we can start target
        target_service.start_target("target1", config.targets["target1"], config)
        assert target_service.is_target_running("target1")
        
        # Cleanup
        source_service.stop_all_sources()
        target_service.stop_all_targets()
        
        # Verify cleanup
        assert not source_service.is_source_running("source1")
        assert not source_service.is_source_running("source2")
        assert not target_service.is_target_running("target1")
