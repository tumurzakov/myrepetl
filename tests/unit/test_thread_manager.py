"""
Unit tests for ThreadManager
"""

import pytest
import threading
import time
from unittest.mock import Mock, patch, MagicMock

from src.services.thread_manager import ThreadManager, ServiceStatus, ServiceStats
from src.models.config import ETLConfig, DatabaseConfig


class TestThreadManager:
    """Test ThreadManager"""
    
    def _create_manager(self):
        """Helper method to create ThreadManager with mocked dependencies"""
        message_bus = Mock()
        database_service = Mock()
        transform_service = Mock()
        filter_service = Mock()
        
        return ThreadManager(
            message_bus=message_bus,
            database_service=database_service,
            transform_service=transform_service,
            filter_service=filter_service
        )
    
    def test_thread_manager_initialization(self):
        """Test thread manager initialization"""
        manager = self._create_manager()
        
        assert manager.message_bus is not None
        assert manager.database_service is not None
        assert manager.transform_service is not None
        assert manager.filter_service is not None
        assert manager.source_thread_service is not None
        assert manager.target_thread_service is not None
        assert manager.get_status() == ServiceStatus.STOPPED
        assert not manager._shutdown_requested
    
    def test_start_success(self):
        """Test successful start"""
        with patch('src.services.thread_manager.ThreadManager._load_transform_module') as mock_load_transform, \
             patch('src.services.thread_manager.ThreadManager._start_message_bus_processing') as mock_start_bus, \
             patch('src.services.thread_manager.ThreadManager._start_target_threads') as mock_start_targets, \
             patch('src.services.thread_manager.ThreadManager._start_source_threads') as mock_start_sources, \
             patch('src.services.thread_manager.ThreadManager._start_monitoring') as mock_start_monitoring:
            
            manager = self._create_manager()
            config = Mock(spec=ETLConfig)
            
            manager.start(config)
            
            assert manager.get_status() == ServiceStatus.RUNNING
            mock_load_transform.assert_called_once_with(config, None)
            mock_start_bus.assert_called_once()
            mock_start_targets.assert_called_once_with(config)
            mock_start_sources.assert_called_once_with(config)
            mock_start_monitoring.assert_called_once()
    
    def test_start_error(self):
        """Test start with error"""
        with patch('src.services.thread_manager.ThreadManager._load_transform_module') as mock_load_transform:
            mock_load_transform.side_effect = Exception("Load error")
            
            manager = self._create_manager()
            config = Mock(spec=ETLConfig)
            
            with pytest.raises(Exception, match="Load error"):
                manager.start(config)
            
            assert manager.get_status() == ServiceStatus.ERROR
    
    def test_stop_success(self):
        """Test successful stop"""
        with patch('src.services.thread_manager.ThreadManager._stop_message_bus_processing') as mock_stop_bus, \
             patch('src.services.thread_manager.ThreadManager._stop_monitoring') as mock_stop_monitoring:
            
            manager = self._create_manager()
            manager._status = ServiceStatus.RUNNING
            
            manager.stop()
            
            assert manager.get_status() == ServiceStatus.STOPPED
            assert manager._shutdown_requested is True
            mock_stop_bus.assert_called_once()
            mock_stop_monitoring.assert_called_once()
    
    def test_stop_already_stopped(self):
        """Test stopping already stopped service"""
        manager = self._create_manager()
        manager._status = ServiceStatus.STOPPED
        
        # Should not raise exception
        manager.stop()
        
        assert manager.get_status() == ServiceStatus.STOPPED
    
    def test_get_status(self):
        """Test getting service status"""
        manager = self._create_manager()
        
        assert manager.get_status() == ServiceStatus.STOPPED
        
        manager._status = ServiceStatus.RUNNING
        assert manager.get_status() == ServiceStatus.RUNNING
    
    def test_get_stats(self):
        """Test getting service statistics"""
        manager = self._create_manager()
        with patch.object(manager, 'source_thread_service') as mock_source_service, \
             patch.object(manager, 'target_thread_service') as mock_target_service, \
             patch.object(manager, 'message_bus') as mock_message_bus:
            
            # Mock statistics
            mock_source_service.get_all_stats.return_value = {
                "source1": {"events_processed": 10, "errors_count": 0}
            }
            mock_target_service.get_all_stats.return_value = {
                "target1": {"events_processed": 8, "errors_count": 0}
            }
            mock_message_bus.get_stats.return_value = {
                "messages_sent": 20,
                "messages_processed": 18,
                "messages_dropped": 0
            }
            
            manager._start_time = time.time() - 10.0  # 10 seconds ago
            manager._status = ServiceStatus.RUNNING
            
            stats = manager.get_stats()
            
            assert isinstance(stats, ServiceStats)
            assert stats.status == ServiceStatus.RUNNING
            assert stats.uptime >= 10.0
            assert stats.sources_count == 1
            assert stats.targets_count == 1
            assert stats.total_events_processed == 18  # 10 + 8
            assert stats.total_errors == 0
            assert stats.message_bus_stats["messages_sent"] == 20
    
    def test_is_running(self):
        """Test checking if service is running"""
        manager = self._create_manager()
        
        assert not manager.is_running()
        
        manager._status = ServiceStatus.RUNNING
        assert manager.is_running()
        
        manager._status = ServiceStatus.STOPPING
        assert not manager.is_running()
    
    def test_wait_for_completion_success(self):
        """Test waiting for completion successfully"""
        manager = self._create_manager()
        manager._status = ServiceStatus.RUNNING
        
        # Simulate status change after short delay
        def change_status():
            time.sleep(0.1)
            manager._status = ServiceStatus.STOPPED
        
        thread = threading.Thread(target=change_status)
        thread.start()
        
        result = manager.wait_for_completion(timeout=1.0)
        
        assert result is True
        thread.join()
    
    def test_wait_for_completion_timeout(self):
        """Test waiting for completion with timeout"""
        manager = self._create_manager()
        manager._status = ServiceStatus.RUNNING
        
        result = manager.wait_for_completion(timeout=0.1)
        
        assert result is False
    
    def test_start_message_bus_processing(self):
        """Test starting message bus processing"""
        with patch('threading.Thread') as mock_thread_class:
            mock_thread = Mock()
            mock_thread_class.return_value = mock_thread
            
            manager = self._create_manager()
            manager._start_message_bus_processing()
            
            mock_thread_class.assert_called_once()
            mock_thread.start.assert_called_once()
            assert manager._message_bus_thread == mock_thread
    
    def test_stop_message_bus_processing(self):
        """Test stopping message bus processing"""
        with patch('threading.Thread') as mock_thread_class:
            mock_thread = Mock()
            mock_thread.is_alive.return_value = True
            mock_thread_class.return_value = mock_thread
            
            manager = self._create_manager()
            manager._message_bus_thread = mock_thread
            manager._stop_message_bus_processing()
            
            mock_thread.join.assert_called_once_with(timeout=5.0)
    
    def test_start_source_threads(self):
        """Test starting source threads"""
        manager = self._create_manager()
        with patch.object(manager, 'source_thread_service') as mock_source_service:
            config = Mock(spec=ETLConfig)
            config.sources = {
                "source1": Mock(spec=DatabaseConfig),
                "source2": Mock(spec=DatabaseConfig)
            }
            config.replication = Mock()
            config.get_tables_for_source.return_value = [("schema1", "table1")]
            
            manager._start_source_threads(config)
            
            assert mock_source_service.start_source.call_count == 2
    
    def test_start_target_threads(self):
        """Test starting target threads"""
        manager = self._create_manager()
        with patch.object(manager, 'target_thread_service') as mock_target_service:
            config = Mock(spec=ETLConfig)
            config.targets = {
                "target1": Mock(spec=DatabaseConfig),
                "target2": Mock(spec=DatabaseConfig)
            }
            
            manager._start_target_threads(config)
            
            assert mock_target_service.start_target.call_count == 2
    
    def test_start_monitoring(self):
        """Test starting monitoring"""
        with patch('threading.Thread') as mock_thread_class:
            mock_thread = Mock()
            mock_thread_class.return_value = mock_thread
            
            manager = self._create_manager()
            manager._start_monitoring()
            
            mock_thread_class.assert_called_once()
            mock_thread.start.assert_called_once()
            assert manager._monitoring_thread == mock_thread
    
    def test_stop_monitoring(self):
        """Test stopping monitoring"""
        with patch('threading.Thread') as mock_thread_class:
            mock_thread = Mock()
            mock_thread.is_alive.return_value = True
            mock_thread_class.return_value = mock_thread
            
            manager = self._create_manager()
            manager._monitoring_thread = mock_thread
            manager._stop_monitoring()
            
            mock_thread.join.assert_called_once_with(timeout=5.0)
    
    def test_message_bus_worker(self):
        """Test message bus worker thread"""
        manager = self._create_manager()
        with patch.object(manager, 'message_bus') as mock_message_bus:
            
            # Simulate shutdown after processing
            def side_effect():
                manager._shutdown_requested = True
            
            mock_message_bus.process_messages.side_effect = side_effect
            
            manager._message_bus_worker()
            
            mock_message_bus.process_messages.assert_called_once()
    
    def test_monitoring_worker(self):
        """Test monitoring worker thread"""
        with patch('src.services.thread_manager.ThreadManager.get_stats') as mock_get_stats:
            mock_get_stats.return_value = Mock(
                status=ServiceStatus.RUNNING,
                uptime=10.0,
                sources_count=2,
                targets_count=1,
                total_events_processed=100,
                total_errors=0
            )
            
            manager = self._create_manager()
            manager._monitoring_interval = 0.1  # Short interval for testing
            
            # Simulate shutdown after one iteration
            def side_effect():
                manager._shutdown_requested = True
            
            with patch('time.sleep', side_effect=side_effect):
                manager._monitoring_worker()
            
            mock_get_stats.assert_called()
    
    def test_is_shutdown_requested(self):
        """Test checking if shutdown is requested"""
        manager = self._create_manager()
        
        assert not manager._is_shutdown_requested()
        
        manager._shutdown_requested = True
        assert manager._is_shutdown_requested()
    
    def test_load_transform_module(self):
        """Test loading transform module"""
        manager = self._create_manager()
        config = Mock(spec=ETLConfig)
        
        # Mock the transform service to avoid actual module loading
        with patch.object(manager.transform_service, 'load_transform_module') as mock_load:
            # Should not raise exception
            manager._load_transform_module(config)
            mock_load.assert_called_once_with("transform", None)
    
    def test_service_stats_creation(self):
        """Test ServiceStats creation"""
        stats = ServiceStats(
            status=ServiceStatus.RUNNING,
            uptime=10.0,
            sources_count=2,
            targets_count=1,
            total_events_processed=100,
            total_errors=0,
            message_bus_stats={"messages_sent": 50},
            source_stats={"source1": {"events_processed": 50}},
            target_stats={"target1": {"events_processed": 50}}
        )
        
        assert stats.status == ServiceStatus.RUNNING
        assert stats.uptime == 10.0
        assert stats.sources_count == 2
        assert stats.targets_count == 1
        assert stats.total_events_processed == 100
        assert stats.total_errors == 0
        assert stats.message_bus_stats["messages_sent"] == 50
        assert stats.source_stats["source1"]["events_processed"] == 50
        assert stats.target_stats["target1"]["events_processed"] == 50

