"""
Unit tests for ETL service
"""

import pytest
import threading
import time
from unittest.mock import Mock, patch, MagicMock

from src.etl_service import ETLService
from src.models.config import ETLConfig, DatabaseConfig
from src.models.events import InsertEvent, UpdateEvent, DeleteEvent, EventType
from src.exceptions import ETLException
from src.services.thread_manager import ServiceStatus


class TestETLService:
    """Test ETLService"""
    
    def test_initialize_success(self):
        """Test successful ETL service initialization"""
        with patch('src.etl_service.ConfigService') as mock_config_service, \
             patch('src.etl_service.ThreadManager') as mock_thread_manager:
            
            mock_config = Mock()
            mock_config_service.return_value.load_config.return_value = mock_config
            mock_config_service.return_value.validate_config.return_value = True
            
            service = ETLService()
            service.initialize("config.json")
            
            assert service.config == mock_config
            assert service.thread_manager is not None
            mock_config_service.return_value.load_config.assert_called_once_with("config.json")
            mock_config_service.return_value.validate_config.assert_called_once_with(mock_config)
            mock_thread_manager.assert_called_once()
    
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
    
    def test_signal_handlers_setup(self):
        """Test signal handlers are properly set up"""
        with patch('signal.signal') as mock_signal:
            service = ETLService()
            assert mock_signal.call_count == 2  # SIGINT and SIGTERM
    
    def test_test_connections_success(self):
        """Test successful connection testing"""
        with patch('src.etl_service.ThreadManager') as mock_thread_manager:
            mock_database_service = Mock()
            mock_database_service.test_connection.return_value = True
            mock_thread_manager.return_value.database_service = mock_database_service
            
            mock_config = Mock()
            mock_config.sources = {'source1': Mock(), 'source2': Mock()}
            mock_config.targets = {'target1': Mock(), 'target2': Mock()}
            
            service = ETLService()
            service.thread_manager = mock_thread_manager.return_value
            service.config = mock_config
            
            result = service.test_connections()
            
            assert result is True
            assert mock_database_service.test_connection.call_count == 4
    
    def test_test_connections_failure(self):
        """Test failed connection testing"""
        with patch('src.etl_service.ThreadManager') as mock_thread_manager:
            mock_database_service = Mock()
            mock_database_service.test_connection.return_value = False
            mock_thread_manager.return_value.database_service = mock_database_service
            
            mock_config = Mock()
            mock_config.sources = {'source1': Mock()}
            mock_config.targets = {'target1': Mock()}
            
            service = ETLService()
            service.thread_manager = mock_thread_manager.return_value
            service.config = mock_config
            
            result = service.test_connections()
            
            assert result is False
    
    def test_request_shutdown(self):
        """Test shutdown request"""
        with patch('src.etl_service.ThreadManager') as mock_thread_manager:
            service = ETLService()
            service.thread_manager = mock_thread_manager.return_value
            
            service.request_shutdown()
            
            assert service._shutdown_requested is True
            mock_thread_manager.return_value.stop.assert_called_once()
    
    def test_run_replication_success(self):
        """Test successful replication run"""
        with patch('src.etl_service.ThreadManager') as mock_thread_manager, \
             patch('src.etl_service.ETLService.execute_init_queries') as mock_init_queries, \
             patch('time.sleep') as mock_sleep:
            
            # Mock thread manager
            mock_thread_manager.return_value.is_running.return_value = True
            mock_thread_manager.return_value.get_stats.return_value = Mock(
                status=ServiceStatus.RUNNING,
                uptime=10.0,
                total_events_processed=5
            )
            
            # Mock config
            mock_config = Mock()
            mock_config.sources = {'source1': Mock(), 'source2': Mock()}
            mock_config.targets = {'target1': Mock()}
            mock_config.mapping = {}
            
            service = ETLService()
            service.thread_manager = mock_thread_manager.return_value
            service.config = mock_config
            service._shutdown_requested = False
            
            # Simulate running for a short time then shutdown
            def side_effect():
                service._shutdown_requested = True
            mock_sleep.side_effect = side_effect
            
            service.run_replication()
            
            mock_thread_manager.return_value.start.assert_called_once_with(mock_config)
            mock_init_queries.assert_called_once()
            mock_thread_manager.return_value.stop.assert_called_once()
    
    def test_run_replication_keyboard_interrupt(self):
        """Test replication run with keyboard interrupt"""
        with patch('src.etl_service.ThreadManager') as mock_thread_manager, \
             patch('src.etl_service.ETLService.execute_init_queries') as mock_init_queries, \
             patch('time.sleep') as mock_sleep:
            
            # Mock thread manager
            mock_thread_manager.return_value.is_running.return_value = True
            mock_thread_manager.return_value.get_stats.return_value = Mock(
                status=ServiceStatus.RUNNING,
                uptime=10.0,
                total_events_processed=5
            )
            
            # Mock config
            mock_config = Mock()
            mock_config.sources = {'source1': Mock()}
            mock_config.targets = {'target1': Mock()}
            mock_config.mapping = {}
            
            service = ETLService()
            service.thread_manager = mock_thread_manager.return_value
            service.config = mock_config
            service._shutdown_requested = False
            
            # Simulate keyboard interrupt
            def side_effect():
                raise KeyboardInterrupt()
            mock_sleep.side_effect = side_effect
            
            # Should not raise exception
            service.run_replication()
            
            mock_thread_manager.return_value.start.assert_called_once_with(mock_config)
            mock_init_queries.assert_called_once()
    
    def test_run_replication_error(self):
        """Test replication run with error"""
        with patch('src.etl_service.ThreadManager') as mock_thread_manager:
            mock_thread_manager.return_value.start.side_effect = Exception("Thread manager error")
            
            service = ETLService()
            service.thread_manager = mock_thread_manager.return_value
            service.config = Mock()
            
            with pytest.raises(Exception, match="Thread manager error"):
                service.run_replication()
    
    def test_cleanup(self):
        """Test cleanup method"""
        with patch('src.etl_service.ThreadManager') as mock_thread_manager:
            service = ETLService()
            service.thread_manager = mock_thread_manager.return_value
            
            service.cleanup()
            
            assert service._shutdown_requested is True
            mock_thread_manager.return_value.stop.assert_called_once()
    
    def test_execute_init_queries_success(self):
        """Test successful init queries execution"""
        with patch('src.etl_service.ThreadManager') as mock_thread_manager:
            # Mock services
            mock_database_service = Mock()
            mock_transform_service = Mock()
            mock_filter_service = Mock()
            mock_database_service.is_table_empty.return_value = True
            mock_database_service.execute_init_query.return_value = ([(1, "test")], ["id", "name"])
            
            mock_thread_manager.return_value.database_service = mock_database_service
            mock_thread_manager.return_value.transform_service = mock_transform_service
            mock_thread_manager.return_value.filter_service = mock_filter_service
            
            # Mock config
            mock_config = Mock()
            mock_mapping = Mock()
            mock_mapping.init_query = "SELECT * FROM users"
            mock_mapping.target_table = "target1.users"
            mock_mapping.source_table = "source1.users"
            mock_mapping.column_mapping = {"id": Mock(), "name": Mock()}
            mock_mapping.primary_key = "id"
            mock_mapping.filter = None
            
            mock_config.mapping = {
                "source1.users": mock_mapping
            }
            mock_config.sources = {"source1": Mock()}
            mock_config.parse_target_table.return_value = ("target1", "users")
            mock_config.parse_source_table.return_value = ("source1", "users")
            mock_config.get_source_config.return_value = Mock()
            
            service = ETLService()
            service.thread_manager = mock_thread_manager.return_value
            service.config = mock_config
            
            service.execute_init_queries()
            
            mock_database_service.is_table_empty.assert_called_once()
            mock_database_service.execute_init_query.assert_called_once()
    
    def test_execute_init_queries_no_thread_manager(self):
        """Test init queries execution without thread manager"""
        service = ETLService()
        service.thread_manager = None
        
        with pytest.raises(ETLException, match="Thread manager not initialized"):
            service.execute_init_queries()
