"""
Integration tests for full ETL pipeline
"""

import pytest
import tempfile
import json
import os
from unittest.mock import Mock, patch, MagicMock

from src.etl_service import ETLService
from src.models.config import ETLConfig, DatabaseConfig, ReplicationConfig, TableMapping, ColumnMapping
from src.models.events import InsertEvent, UpdateEvent, DeleteEvent, EventType


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
             patch('src.etl_service.TransformService') as mock_transform_service, \
             patch('src.etl_service.ReplicationService') as mock_replication_service:
            
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
                }
            })
            mock_config_service.return_value.validate_config.return_value = True
            
            service = ETLService()
            service.initialize(sample_config)
            
            assert service.config is not None
            assert service.config.sources["source1"].host == "localhost"
            assert service.config.targets["target1"].host == "localhost"
            assert "source1.users" in service.config.mapping
    
    def test_connection_testing(self, sample_config):
        """Test connection testing functionality"""
        with patch('src.etl_service.DatabaseService') as mock_db_service:
            mock_db_service.return_value.test_connection.return_value = True
            
            service = ETLService()
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
            assert mock_db_service.return_value.test_connection.call_count == 2
    
    def test_event_processing_integration(self):
        """Test event processing with real transform functions"""
        with patch('src.etl_service.ReplicationService') as mock_replication_service, \
             patch('src.etl_service.DatabaseService') as mock_db_service, \
             patch('src.etl_service.SQLBuilder') as mock_sql_builder:
            
            # Setup mocks
            mock_table_mapping = TableMapping(
                target_table="target1.users",
                primary_key="id",
                column_mapping={
                    "id": ColumnMapping(column="id", primary_key=True),
                    "name": ColumnMapping(column="name", transform="transform.uppercase"),
                    "email": ColumnMapping(column="email"),
                    "status": ColumnMapping(column="status", value="active")
                }
            )
            
            mock_replication_service.return_value.get_table_mapping.return_value = mock_table_mapping
            mock_sql_builder.build_upsert_sql.return_value = ("INSERT SQL", [1, "TEST", "test@example.com", "active"])
            
            # Create service
            service = ETLService()
            service.replication_service = mock_replication_service.return_value
            service.database_service = mock_db_service.return_value
            service.config = Mock()
            service.config.parse_target_table.return_value = ("target1", "users")
            
            # Load transform module
            service.transform_service.load_transform_module("src.transform")
            
            # Create test event
            event = InsertEvent(
                schema="source_db",
                table="users",
                values={"id": 1, "name": "test", "email": "test@example.com"},
                source_name="source1"
            )
            
            # Process event
            service.process_event(event)
            
            # Verify transform was applied
            mock_sql_builder.build_upsert_sql.assert_called_once()
            call_args = mock_sql_builder.build_upsert_sql.call_args
            transformed_data = call_args[0][1]  # Second argument is data
            
            assert transformed_data["id"] == 1
            assert transformed_data["name"] == "TEST"  # Uppercase transform applied
            assert transformed_data["email"] == "test@example.com"
            assert transformed_data["status"] == "active"  # Static value applied
    
    def test_full_replication_cycle(self, sample_config):
        """Test full replication cycle with mocked components"""
        with patch('src.etl_service.ETLService.connect_to_targets') as mock_connect, \
             patch('src.etl_service.ReplicationService') as mock_replication_service, \
             patch('src.etl_service.TransformService') as mock_transform_service, \
             patch('src.etl_service.DatabaseService') as mock_db_service, \
             patch('src.etl_service.SQLBuilder') as mock_sql_builder:
            
            # Setup mocks
            mock_stream = Mock()
            mock_event = InsertEvent(
                schema="source_db",
                table="users",
                values={"id": 1, "name": "test", "email": "test@example.com"},
                source_name="source1"
            )
            mock_stream.__iter__ = Mock(return_value=iter([mock_event]))
            
            mock_replication_service.return_value.connect_to_replication.return_value = mock_stream
            mock_replication_service.return_value.get_all_events.return_value = [("source1", mock_event)]
            mock_replication_service.return_value.get_table_mapping.return_value = TableMapping(
                target_table="target1.users",
                primary_key="id",
                column_mapping={
                    "id": ColumnMapping(column="id", primary_key=True),
                    "name": ColumnMapping(column="name"),
                    "email": ColumnMapping(column="email")
                }
            )
            
            mock_transform_service.return_value.apply_column_transforms.return_value = {
                "id": 1, "name": "test", "email": "test@example.com"
            }
            mock_sql_builder.build_upsert_sql.return_value = ("INSERT SQL", [1, "test", "test@example.com"])
            
            # Create and run service
            service = ETLService()
            service.replication_service = mock_replication_service.return_value
            service.transform_service = mock_transform_service.return_value
            service.database_service = mock_db_service.return_value
            service.config = Mock()
            service.config.sources = {"source1": Mock()}
            service.config.replication = Mock()
            service.config.parse_target_table.return_value = ("target1", "users")
            
            service.run_replication()
            
            # Verify all components were called
            mock_connect.assert_called_once()
            mock_replication_service.return_value.connect_to_replication.assert_called_once()
            mock_transform_service.return_value.apply_column_transforms.assert_called_once()
            mock_sql_builder.build_upsert_sql.assert_called_once()
            mock_db_service.return_value.execute_update.assert_called_once()
    
    def test_error_handling_in_replication(self, sample_config):
        """Test error handling during replication"""
        with patch('src.etl_service.ETLService.connect_to_targets') as mock_connect, \
             patch('src.etl_service.ReplicationService') as mock_replication_service:
            
            # Setup mock to raise exception
            mock_connect.side_effect = Exception("Connection failed")
            
            service = ETLService()
            service.config = Mock()
            
            with pytest.raises(Exception, match="Replication failed"):
                service.run_replication()
    
    def test_cleanup_on_exit(self, sample_config):
        """Test cleanup is called on service exit"""
        with patch('src.etl_service.ReplicationService') as mock_replication_service, \
             patch('src.etl_service.DatabaseService') as mock_db_service:
            
            mock_replication = Mock()
            mock_db = Mock()
            
            service = ETLService()
            service.replication_service = mock_replication
            service.database_service = mock_db
            
            # Test cleanup
            service.cleanup()
            
            mock_replication.close.assert_called_once()
            mock_db.close_all_connections.assert_called_once()
    
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
        service.transform_service.load_transform_module("src.transform")
        
        # Test uppercase transform
        result = service.transform_service.apply_column_transforms(
            {"name": "test"},
            {"name": ColumnMapping(column="name", transform="transform.uppercase")}
        )
        
        assert result["name"] == "TEST"
        
        # Test static value
        result = service.transform_service.apply_column_transforms(
            {"name": "test"},
            {"name": ColumnMapping(column="name", value="STATIC")}
        )
        
        assert result["name"] == "STATIC"
        
        # Test simple copy
        result = service.transform_service.apply_column_transforms(
            {"name": "test"},
            {"name": ColumnMapping(column="name")}
        )
        
        assert result["name"] == "test"
