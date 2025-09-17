"""
Unit tests for ETL service
"""

import pytest
from unittest.mock import Mock, patch, MagicMock

from src.etl_service import ETLService
from src.models.config import ETLConfig, DatabaseConfig
from src.models.events import InsertEvent, UpdateEvent, DeleteEvent, EventType
from src.exceptions import ETLException


class TestETLService:
    """Test ETLService"""
    
    def test_initialize_success(self):
        """Test successful ETL service initialization"""
        with patch('src.etl_service.ConfigService') as mock_config_service, \
             patch('src.etl_service.TransformService') as mock_transform_service, \
             patch('src.etl_service.ReplicationService') as mock_replication_service:
            
            mock_config = Mock()
            mock_config_service.return_value.load_config.return_value = mock_config
            mock_config_service.return_value.validate_config.return_value = True
            
            service = ETLService()
            service.initialize("config.json")
            
            assert service.config == mock_config
            mock_config_service.return_value.load_config.assert_called_once_with("config.json")
            mock_config_service.return_value.validate_config.assert_called_once_with(mock_config)
            mock_transform_service.return_value.load_transform_module.assert_called_once()
    
    def test_initialize_invalid_config(self):
        """Test ETL service initialization with invalid config"""
        with patch('src.etl_service.ConfigService') as mock_config_service:
            mock_config = Mock()
            mock_config_service.return_value.load_config.return_value = mock_config
            mock_config_service.return_value.validate_config.return_value = False
            
            service = ETLService()
            
            with pytest.raises(ETLException, match="Invalid configuration"):
                service.initialize("config.json")
    
    def test_initialize_load_config_error(self):
        """Test ETL service initialization with config load error"""
        with patch('src.etl_service.ConfigService') as mock_config_service:
            mock_config_service.return_value.load_config.side_effect = Exception("Config error")
            
            service = ETLService()
            
            with pytest.raises(ETLException, match="Initialization failed"):
                service.initialize("config.json")
    
    def test_connect_to_targets_success(self):
        """Test successful target connections"""
        with patch('src.etl_service.DatabaseService') as mock_db_service:
            mock_config = Mock()
            mock_config.targets = {
                'target1': Mock(),
                'target2': Mock()
            }
            service = ETLService()
            service.config = mock_config
            
            service.connect_to_targets()
            
            assert mock_db_service.return_value.connect.call_count == 2
    
    def test_connect_to_targets_failure(self):
        """Test failed target connections"""
        with patch('src.etl_service.DatabaseService') as mock_db_service:
            mock_db_service.return_value.connect.side_effect = Exception("Connection failed")
            mock_config = Mock()
            mock_config.targets = {'target1': Mock()}
            service = ETLService()
            service.config = mock_config
            
            with pytest.raises(ETLException, match="Target connection failed"):
                service.connect_to_targets()
    
    def test_test_connections_success(self):
        """Test successful connection testing"""
        with patch('src.etl_service.DatabaseService') as mock_db_service:
            mock_db_service.return_value.test_connection.return_value = True
            mock_config = Mock()
            mock_config.sources = {'source1': Mock(), 'source2': Mock()}
            mock_config.targets = {'target1': Mock(), 'target2': Mock()}
            service = ETLService()
            service.config = mock_config
            
            result = service.test_connections()
            
            assert result is True
            assert mock_db_service.return_value.test_connection.call_count == 4
    
    def test_test_connections_failure(self):
        """Test failed connection testing"""
        with patch('src.etl_service.DatabaseService') as mock_db_service:
            mock_db_service.return_value.test_connection.return_value = False
            mock_config = Mock()
            mock_config.sources = {'source1': Mock()}
            mock_config.targets = {'target1': Mock()}
            service = ETLService()
            service.config = mock_config
            
            result = service.test_connections()
            
            assert result is False
    
    def test_process_event_insert(self):
        """Test processing INSERT event"""
        with patch('src.etl_service.ReplicationService') as mock_replication_service, \
             patch('src.etl_service.TransformService') as mock_transform_service, \
             patch('src.etl_service.DatabaseService') as mock_db_service, \
             patch('src.etl_service.SQLBuilder') as mock_sql_builder:
            
            # Setup mocks
            mock_table_mapping = Mock()
            mock_table_mapping.target_table = "target1.users"
            mock_table_mapping.primary_key = "id"
            mock_table_mapping.column_mapping = {"id": Mock(), "name": Mock()}
            mock_table_mapping.filter = None  # No filter for this test
            
            mock_config = Mock()
            mock_config.parse_target_table.return_value = ("target1", "users")
            
            mock_replication_service.return_value.get_table_mapping.return_value = mock_table_mapping
            mock_transform_service.return_value.apply_column_transforms.return_value = {"id": 1, "name": "TEST"}
            mock_sql_builder.build_upsert_sql.return_value = ("INSERT SQL", [1, "TEST"])
            
            # Create service and event
            service = ETLService()
            service.replication_service = mock_replication_service.return_value
            service.transform_service = mock_transform_service.return_value
            service.database_service = mock_db_service.return_value
            service.filter_service = Mock()
            service.config = mock_config
            
            event = InsertEvent(
                schema="test_schema",
                table="test_table",
                values={"id": 1, "name": "test"},
                source_name="source1"
            )
            
            # Process event
            service.process_event(event)
            
            # Verify calls
            mock_replication_service.return_value.get_table_mapping.assert_called_once()
            mock_config.parse_target_table.assert_called_once_with("target1.users")
            mock_transform_service.return_value.apply_column_transforms.assert_called_once()
            mock_sql_builder.build_upsert_sql.assert_called_once_with("users", {"id": 1, "name": "TEST"}, "id")
            mock_db_service.return_value.execute_update.assert_called_once_with("INSERT SQL", [1, "TEST"], "target1")
    
    def test_process_event_update(self):
        """Test processing UPDATE event"""
        with patch('src.etl_service.ReplicationService') as mock_replication_service, \
             patch('src.etl_service.TransformService') as mock_transform_service, \
             patch('src.etl_service.DatabaseService') as mock_db_service, \
             patch('src.etl_service.SQLBuilder') as mock_sql_builder:
            
            # Setup mocks
            mock_table_mapping = Mock()
            mock_table_mapping.target_table = "target1.users"
            mock_table_mapping.primary_key = "id"
            mock_table_mapping.column_mapping = {"id": Mock(), "name": Mock()}
            mock_table_mapping.filter = None  # No filter for this test
            
            mock_config = Mock()
            mock_config.parse_target_table.return_value = ("target1", "users")
            
            mock_replication_service.return_value.get_table_mapping.return_value = mock_table_mapping
            mock_transform_service.return_value.apply_column_transforms.return_value = {"id": 1, "name": "UPDATED"}
            mock_sql_builder.build_upsert_sql.return_value = ("UPDATE SQL", [1, "UPDATED"])
            
            # Create service and event
            service = ETLService()
            service.replication_service = mock_replication_service.return_value
            service.transform_service = mock_transform_service.return_value
            service.database_service = mock_db_service.return_value
            service.filter_service = Mock()
            service.config = mock_config
            
            event = UpdateEvent(
                schema="test_schema",
                table="test_table",
                before_values={"id": 1, "name": "old"},
                after_values={"id": 1, "name": "new"},
                source_name="source1"
            )
            
            # Process event
            service.process_event(event)
            
            # Verify calls
            mock_replication_service.return_value.get_table_mapping.assert_called_once()
            mock_config.parse_target_table.assert_called_once_with("target1.users")
            mock_transform_service.return_value.apply_column_transforms.assert_called_once_with(
                {"id": 1, "name": "new"}, mock_table_mapping.column_mapping, "test_schema.test_table"
            )
            mock_sql_builder.build_upsert_sql.assert_called_once_with("users", {"id": 1, "name": "UPDATED"}, "id")
            mock_db_service.return_value.execute_update.assert_called_once_with("UPDATE SQL", [1, "UPDATED"], "target1")
    
    def test_process_event_delete(self):
        """Test processing DELETE event"""
        with patch('src.etl_service.ReplicationService') as mock_replication_service, \
             patch('src.etl_service.TransformService') as mock_transform_service, \
             patch('src.etl_service.DatabaseService') as mock_db_service, \
             patch('src.etl_service.SQLBuilder') as mock_sql_builder:
            
            # Setup mocks
            mock_table_mapping = Mock()
            mock_table_mapping.target_table = "target1.users"
            mock_table_mapping.primary_key = "id"
            mock_table_mapping.column_mapping = {"id": Mock()}
            mock_table_mapping.filter = None  # No filter for this test
            
            mock_config = Mock()
            mock_config.parse_target_table.return_value = ("target1", "users")
            
            mock_replication_service.return_value.get_table_mapping.return_value = mock_table_mapping
            mock_transform_service.return_value.apply_column_transforms.return_value = {"id": 1}
            mock_sql_builder.build_delete_sql.return_value = ("DELETE SQL", [1])
            
            # Create service and event
            service = ETLService()
            service.replication_service = mock_replication_service.return_value
            service.transform_service = mock_transform_service.return_value
            service.database_service = mock_db_service.return_value
            service.filter_service = Mock()
            service.config = mock_config
            
            event = DeleteEvent(
                schema="test_schema",
                table="test_table",
                values={"id": 1, "name": "test"},
                source_name="source1"
            )
            
            # Process event
            service.process_event(event)
            
            # Verify calls
            mock_replication_service.return_value.get_table_mapping.assert_called_once()
            mock_config.parse_target_table.assert_called_once_with("target1.users")
            mock_transform_service.return_value.apply_column_transforms.assert_called_once()
            mock_sql_builder.build_delete_sql.assert_called_once_with("users", {"id": 1}, "id")
            mock_db_service.return_value.execute_update.assert_called_once_with("DELETE SQL", [1], "target1")
    
    def test_process_event_no_mapping(self):
        """Test processing event with no table mapping"""
        with patch('src.etl_service.ReplicationService') as mock_replication_service:
            mock_replication_service.return_value.get_table_mapping.return_value = None
            
            service = ETLService()
            service.replication_service = mock_replication_service.return_value
            service.config = Mock()
            
            event = InsertEvent(
                schema="test_schema",
                table="test_table",
                values={"id": 1, "name": "test"}
            )
            
            # Process event (should not raise exception)
            service.process_event(event)
            
            mock_replication_service.return_value.get_table_mapping.assert_called_once()
    
    def test_process_event_error(self):
        """Test processing event with error"""
        with patch('src.etl_service.ReplicationService') as mock_replication_service:
            mock_replication_service.return_value.get_table_mapping.side_effect = Exception("Mapping error")
            
            service = ETLService()
            service.replication_service = mock_replication_service.return_value
            service.config = Mock()
            
            event = InsertEvent(
                schema="test_schema",
                table="test_table",
                values={"id": 1, "name": "test"}
            )
            
            with pytest.raises(ETLException, match="Event processing failed"):
                service.process_event(event)
    
    def test_run_replication_success(self):
        """Test successful replication run"""
        with patch('src.etl_service.ETLService.connect_to_targets') as mock_connect, \
             patch('src.etl_service.ReplicationService') as mock_replication_service:
            
            mock_stream = Mock()
            mock_event = Mock()
            mock_stream.__iter__ = Mock(return_value=iter([mock_event]))
            mock_replication_service.return_value.connect_to_replication.return_value = mock_stream
            mock_replication_service.return_value.get_all_events.return_value = [("source1", mock_event)]
            
            mock_config = Mock()
            mock_config.sources = {'source1': Mock(), 'source2': Mock()}
            mock_config.replication = Mock()
            mock_config.mapping = {}
            
            service = ETLService()
            service.replication_service = mock_replication_service.return_value
            service.database_service = Mock()
            service.transform_service = Mock()
            service.filter_service = Mock()
            service.config = mock_config
            
            with patch.object(service, 'process_event') as mock_process:
                service.run_replication()
                
                mock_connect.assert_called_once()
                assert mock_replication_service.return_value.connect_to_replication.call_count == 2
                mock_process.assert_called_once_with(mock_event)
    
    def test_run_replication_keyboard_interrupt(self):
        """Test replication run with keyboard interrupt"""
        with patch('src.etl_service.ETLService.connect_to_targets') as mock_connect, \
             patch('src.etl_service.ReplicationService') as mock_replication_service:
            
            mock_stream = Mock()
            mock_stream.__iter__ = Mock(side_effect=KeyboardInterrupt())
            mock_replication_service.return_value.connect_to_replication.return_value = mock_stream
            mock_replication_service.return_value.get_all_events.return_value = iter([])
            
            mock_config = Mock()
            mock_config.sources = {'source1': Mock()}
            mock_config.replication = Mock()
            mock_config.mapping = {}
            
            service = ETLService()
            service.replication_service = mock_replication_service.return_value
            service.database_service = Mock()
            service.transform_service = Mock()
            service.filter_service = Mock()
            service.config = mock_config
            
            # Should not raise exception
            service.run_replication()
    
    def test_run_replication_error(self):
        """Test replication run with error"""
        with patch('src.etl_service.ETLService.connect_to_targets') as mock_connect:
            mock_connect.side_effect = Exception("Connection error")
            
            service = ETLService()
            service.config = Mock()
            
            with pytest.raises(ETLException, match="Replication failed"):
                service.run_replication()
    
    def test_cleanup(self):
        """Test cleanup method"""
        with patch('src.etl_service.ReplicationService') as mock_replication_service, \
             patch('src.etl_service.DatabaseService') as mock_db_service:
            
            mock_replication = Mock()
            mock_db = Mock()
            
            service = ETLService()
            service.replication_service = mock_replication
            service.database_service = mock_db
            
            service.cleanup()
            
            mock_replication.close.assert_called_once()
            mock_db.close_all_connections.assert_called_once()
