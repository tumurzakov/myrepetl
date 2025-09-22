"""
Unit tests for TargetThreadService
"""

import pytest
import threading
import time
from unittest.mock import Mock, patch, MagicMock

from src.services.target_thread_service import TargetThread, TargetThreadService
from src.models.config import DatabaseConfig, ETLConfig
from src.models.events import InsertEvent, EventType
from src.services.message_bus import Message, MessageType


class TestTargetThread:
    """Test TargetThread"""
    
    def test_target_thread_initialization(self):
        """Test target thread initialization"""
        target_config = Mock(spec=DatabaseConfig)
        target_config.batch_size = 100
        target_config.batch_flush_interval = 5.0
        message_bus = Mock()
        database_service = Mock()
        transform_service = Mock()
        filter_service = Mock()
        config = Mock(spec=ETLConfig)
        
        thread = TargetThread(
            target_name="test_target",
            target_config=target_config,
            message_bus=message_bus,
            database_service=database_service,
            transform_service=transform_service,
            filter_service=filter_service,
            config=config
        )
        
        assert thread.target_name == "test_target"
        assert thread.target_config == target_config
        assert thread.message_bus == message_bus
        assert thread.database_service == database_service
        assert thread.transform_service == transform_service
        assert thread.filter_service == filter_service
        assert thread.config == config
        assert not thread.is_running()
    
    def test_start_stop_thread(self):
        """Test starting and stopping thread"""
        target_config = Mock(spec=DatabaseConfig)
        target_config.batch_size = 100
        target_config.batch_flush_interval = 5.0
        message_bus = Mock()
        database_service = Mock()
        transform_service = Mock()
        filter_service = Mock()
        config = Mock(spec=ETLConfig)
        
        thread = TargetThread(
            target_name="test_target",
            target_config=target_config,
            message_bus=message_bus,
            database_service=database_service,
            transform_service=transform_service,
            filter_service=filter_service,
            config=config
        )
        
        # Start thread
        thread.start()
        time.sleep(0.1)  # Give thread time to start
        
        assert thread.is_running()
        
        # Stop thread
        thread.stop()
        time.sleep(0.1)  # Give thread time to stop
        
        assert not thread.is_running()
    
    def test_get_stats(self):
        """Test getting thread statistics"""
        target_config = Mock(spec=DatabaseConfig)
        message_bus = Mock()
        database_service = Mock()
        transform_service = Mock()
        filter_service = Mock()
        config = Mock(spec=ETLConfig)
        
        thread = TargetThread(
            target_name="test_target",
            target_config=target_config,
            message_bus=message_bus,
            database_service=database_service,
            transform_service=transform_service,
            filter_service=filter_service,
            config=config
        )
        
        stats = thread.get_stats()
        
        assert 'events_processed' in stats
        assert 'inserts_processed' in stats
        assert 'updates_processed' in stats
        assert 'deletes_processed' in stats
        assert 'errors_count' in stats
        assert 'last_event_time' in stats
        assert 'is_running' in stats
        assert 'queue_size' in stats
        assert stats['is_running'] is False
    
    def test_handle_binlog_event_message(self):
        """Test handling binlog event message"""
        target_config = Mock(spec=DatabaseConfig)
        message_bus = Mock()
        database_service = Mock()
        transform_service = Mock()
        filter_service = Mock()
        config = Mock(spec=ETLConfig)
        
        thread = TargetThread(
            target_name="test_target",
            target_config=target_config,
            message_bus=message_bus,
            database_service=database_service,
            transform_service=transform_service,
            filter_service=filter_service,
            config=config
        )
        
        # Create test event
        event = InsertEvent(
            schema="test_schema",
            table="test_table",
            values={"id": 1, "name": "test"},
            source_name="test_source"
        )
        
        # Create message
        message = Message(
            message_type=MessageType.BINLOG_EVENT,
            source="test_source",
            target="test_target",
            data=event
        )
        
        # Handle message
        thread._handle_binlog_event(message)
        
        # Check that event was added to queue
        assert thread._event_queue.qsize() == 1
    
    def test_handle_binlog_event_message_wrong_target(self):
        """Test handling binlog event message for different target"""
        target_config = Mock(spec=DatabaseConfig)
        message_bus = Mock()
        database_service = Mock()
        transform_service = Mock()
        filter_service = Mock()
        config = Mock(spec=ETLConfig)
        
        thread = TargetThread(
            target_name="test_target",
            target_config=target_config,
            message_bus=message_bus,
            database_service=database_service,
            transform_service=transform_service,
            filter_service=filter_service,
            config=config
        )
        
        # Create test event
        event = InsertEvent(
            schema="test_schema",
            table="test_table",
            values={"id": 1, "name": "test"},
            source_name="test_source"
        )
        
        # Create message for different target
        message = Message(
            message_type=MessageType.BINLOG_EVENT,
            source="test_source",
            target="other_target",
            data=event
        )
        
        # Handle message
        thread._handle_binlog_event(message)
        
        # Check that event was not added to queue
        assert thread._event_queue.qsize() == 0
    
    def test_handle_shutdown_message(self):
        """Test handling shutdown message"""
        target_config = Mock(spec=DatabaseConfig)
        message_bus = Mock()
        database_service = Mock()
        transform_service = Mock()
        filter_service = Mock()
        config = Mock(spec=ETLConfig)
        
        thread = TargetThread(
            target_name="test_target",
            target_config=target_config,
            message_bus=message_bus,
            database_service=database_service,
            transform_service=transform_service,
            filter_service=filter_service,
            config=config
        )
        
        # Create shutdown message
        message = Message(
            message_type=MessageType.SHUTDOWN,
            source="test_source",
            data="Shutdown requested"
        )
        
        # Handle message
        thread._handle_shutdown(message)
        
        # Check that shutdown was requested
        assert thread._shutdown_requested is True


class TestTargetThreadService:
    """Test TargetThreadService"""
    
    def test_target_thread_service_initialization(self):
        """Test target thread service initialization"""
        message_bus = Mock()
        database_service = Mock()
        transform_service = Mock()
        filter_service = Mock()
        
        service = TargetThreadService(
            message_bus, database_service, transform_service, filter_service
        )
        
        assert service.message_bus == message_bus
        assert service.database_service == database_service
        assert service.transform_service == transform_service
        assert service.filter_service == filter_service
        assert len(service._target_threads) == 0
    
    def test_start_target(self):
        """Test starting a target thread"""
        with patch('src.services.target_thread_service.TargetThread') as mock_target_thread_class:
            mock_thread = Mock()
            mock_target_thread_class.return_value = mock_thread
            
            message_bus = Mock()
            database_service = Mock()
            transform_service = Mock()
            filter_service = Mock()
            service = TargetThreadService(
                message_bus, database_service, transform_service, filter_service
            )
            
            target_config = Mock(spec=DatabaseConfig)
            config = Mock(spec=ETLConfig)
            
            service.start_target("test_target", target_config, config)
            
            assert "test_target" in service._target_threads
            mock_target_thread_class.assert_called_once_with(
                target_name="test_target",
                target_config=target_config,
                message_bus=message_bus,
                database_service=database_service,
                transform_service=transform_service,
                filter_service=filter_service,
                config=config,
                metrics_service=None
            )
            mock_thread.start.assert_called_once()
    
    def test_start_target_already_exists(self):
        """Test starting a target thread that already exists"""
        with patch('src.services.target_thread_service.TargetThread') as mock_target_thread_class:
            mock_thread = Mock()
            mock_target_thread_class.return_value = mock_thread
            
            message_bus = Mock()
            database_service = Mock()
            transform_service = Mock()
            filter_service = Mock()
            service = TargetThreadService(
                message_bus, database_service, transform_service, filter_service
            )
            
            # Add existing thread
            service._target_threads["test_target"] = mock_thread
            
            target_config = Mock(spec=DatabaseConfig)
            config = Mock(spec=ETLConfig)
            
            # Should not create new thread
            service.start_target("test_target", target_config, config)
            
            # Should not call start again
            assert mock_thread.start.call_count == 0
    
    def test_stop_target(self):
        """Test stopping a target thread"""
        with patch('src.services.target_thread_service.TargetThread') as mock_target_thread_class:
            mock_thread = Mock()
            mock_target_thread_class.return_value = mock_thread
            
            message_bus = Mock()
            database_service = Mock()
            transform_service = Mock()
            filter_service = Mock()
            service = TargetThreadService(
                message_bus, database_service, transform_service, filter_service
            )
            
            # Add thread
            service._target_threads["test_target"] = mock_thread
            
            service.stop_target("test_target")
            
            mock_thread.stop.assert_called_once()
            assert "test_target" not in service._target_threads
    
    def test_stop_target_not_found(self):
        """Test stopping a target thread that doesn't exist"""
        message_bus = Mock()
        database_service = Mock()
        transform_service = Mock()
        filter_service = Mock()
        service = TargetThreadService(
            message_bus, database_service, transform_service, filter_service
        )
        
        # Should not raise exception
        service.stop_target("nonexistent_target")
    
    def test_stop_all_targets(self):
        """Test stopping all target threads"""
        with patch('src.services.target_thread_service.TargetThread') as mock_target_thread_class:
            mock_thread1 = Mock()
            mock_thread2 = Mock()
            
            message_bus = Mock()
            database_service = Mock()
            transform_service = Mock()
            filter_service = Mock()
            service = TargetThreadService(
                message_bus, database_service, transform_service, filter_service
            )
            
            # Add threads
            service._target_threads["target1"] = mock_thread1
            service._target_threads["target2"] = mock_thread2
            
            service.stop_all_targets()
            
            mock_thread1.stop.assert_called_once()
            mock_thread2.stop.assert_called_once()
            assert len(service._target_threads) == 0
    
    def test_get_target_stats(self):
        """Test getting target statistics"""
        with patch('src.services.target_thread_service.TargetThread') as mock_target_thread_class:
            mock_thread = Mock()
            mock_thread.get_stats.return_value = {"events_processed": 10, "is_running": True}
            
            message_bus = Mock()
            database_service = Mock()
            transform_service = Mock()
            filter_service = Mock()
            service = TargetThreadService(
                message_bus, database_service, transform_service, filter_service
            )
            
            # Add thread
            service._target_threads["test_target"] = mock_thread
            
            stats = service.get_target_stats("test_target")
            
            assert stats == {"events_processed": 10, "is_running": True}
            mock_thread.get_stats.assert_called_once()
    
    def test_get_target_stats_not_found(self):
        """Test getting statistics for non-existent target"""
        message_bus = Mock()
        database_service = Mock()
        transform_service = Mock()
        filter_service = Mock()
        service = TargetThreadService(
            message_bus, database_service, transform_service, filter_service
        )
        
        stats = service.get_target_stats("nonexistent_target")
        
        assert stats is None
    
    def test_get_all_stats(self):
        """Test getting statistics for all targets"""
        with patch('src.services.target_thread_service.TargetThread') as mock_target_thread_class:
            mock_thread1 = Mock()
            mock_thread1.get_stats.return_value = {"events_processed": 10}
            mock_thread2 = Mock()
            mock_thread2.get_stats.return_value = {"events_processed": 20}
            
            message_bus = Mock()
            database_service = Mock()
            transform_service = Mock()
            filter_service = Mock()
            service = TargetThreadService(
                message_bus, database_service, transform_service, filter_service
            )
            
            # Add threads
            service._target_threads["target1"] = mock_thread1
            service._target_threads["target2"] = mock_thread2
            
            stats = service.get_all_stats()
            
            assert "target1" in stats
            assert "target2" in stats
            assert stats["target1"]["events_processed"] == 10
            assert stats["target2"]["events_processed"] == 20
    
    def test_is_target_running(self):
        """Test checking if target is running"""
        with patch('src.services.target_thread_service.TargetThread') as mock_target_thread_class:
            mock_thread = Mock()
            mock_thread.is_running.return_value = True
            
            message_bus = Mock()
            database_service = Mock()
            transform_service = Mock()
            filter_service = Mock()
            service = TargetThreadService(
                message_bus, database_service, transform_service, filter_service
            )
            
            # Add thread
            service._target_threads["test_target"] = mock_thread
            
            assert service.is_target_running("test_target") is True
            assert service.is_target_running("nonexistent_target") is False
    
    def test_get_running_targets(self):
        """Test getting list of running targets"""
        with patch('src.services.target_thread_service.TargetThread') as mock_target_thread_class:
            mock_thread1 = Mock()
            mock_thread1.is_running.return_value = True
            mock_thread2 = Mock()
            mock_thread2.is_running.return_value = False
            
            message_bus = Mock()
            database_service = Mock()
            transform_service = Mock()
            filter_service = Mock()
            service = TargetThreadService(
                message_bus, database_service, transform_service, filter_service
            )
            
            # Add threads
            service._target_threads["target1"] = mock_thread1
            service._target_threads["target2"] = mock_thread2
            
            running_targets = service.get_running_targets()
            
            assert "target1" in running_targets
            assert "target2" not in running_targets
    
    def test_ensure_target_connection_success(self):
        """Test successful target connection ensuring"""
        target_config = Mock(spec=DatabaseConfig)
        message_bus = Mock()
        database_service = Mock()
        database_service.connection_exists.return_value = True
        database_service.reconnect_if_needed.return_value = True
        transform_service = Mock()
        filter_service = Mock()
        config = Mock(spec=ETLConfig)
        
        thread = TargetThread(
            target_name="test_target",
            target_config=target_config,
            message_bus=message_bus,
            database_service=database_service,
            transform_service=transform_service,
            filter_service=filter_service,
            config=config
        )
        
        result = thread._ensure_target_connection()
        
        assert result is True
        database_service.connection_exists.assert_called_once_with("test_target")
        database_service.reconnect_if_needed.assert_called_once_with("test_target")
    
    def test_ensure_target_connection_create_new(self):
        """Test creating new target connection"""
        target_config = Mock(spec=DatabaseConfig)
        message_bus = Mock()
        database_service = Mock()
        database_service.connection_exists.return_value = False
        database_service.connect.return_value = Mock()
        transform_service = Mock()
        filter_service = Mock()
        config = Mock(spec=ETLConfig)
        
        thread = TargetThread(
            target_name="test_target",
            target_config=target_config,
            message_bus=message_bus,
            database_service=database_service,
            transform_service=transform_service,
            filter_service=filter_service,
            config=config
        )
        
        result = thread._ensure_target_connection()
        
        assert result is True
        database_service.connection_exists.assert_called_once_with("test_target")
        database_service.connect.assert_called_once_with(target_config, "test_target")
        # reconnect_if_needed is not called when creating new connection
    
    def test_ensure_target_connection_failure(self):
        """Test target connection ensuring failure"""
        target_config = Mock(spec=DatabaseConfig)
        message_bus = Mock()
        database_service = Mock()
        database_service.connection_exists.return_value = False
        database_service.connect.side_effect = Exception("Connection failed")
        transform_service = Mock()
        filter_service = Mock()
        config = Mock(spec=ETLConfig)
        
        thread = TargetThread(
            target_name="test_target",
            target_config=target_config,
            message_bus=message_bus,
            database_service=database_service,
            transform_service=transform_service,
            filter_service=filter_service,
            config=config
        )
        
        result = thread._ensure_target_connection()
        
        assert result is False
        database_service.connection_exists.assert_called_once_with("test_target")
        database_service.connect.assert_called_once_with(target_config, "test_target")
    
    def test_execute_with_retry_success(self):
        """Test successful execution with retry"""
        target_config = Mock(spec=DatabaseConfig)
        message_bus = Mock()
        database_service = Mock()
        database_service.connection_exists.return_value = True
        database_service.reconnect_if_needed.return_value = True
        transform_service = Mock()
        filter_service = Mock()
        config = Mock(spec=ETLConfig)
        
        thread = TargetThread(
            target_name="test_target",
            target_config=target_config,
            message_bus=message_bus,
            database_service=database_service,
            transform_service=transform_service,
            filter_service=filter_service,
            config=config
        )
        
        mock_operation = Mock(return_value="success")
        
        result = thread._execute_with_retry(mock_operation, "arg1", "arg2", kwarg1="value1")
        
        assert result == "success"
        mock_operation.assert_called_once_with("arg1", "arg2", kwarg1="value1")
    
    def test_execute_with_retry_connection_failure(self):
        """Test execution with retry when connection fails"""
        target_config = Mock(spec=DatabaseConfig)
        message_bus = Mock()
        database_service = Mock()
        database_service.connection_exists.return_value = False
        database_service.connect.side_effect = Exception("Connection failed")
        transform_service = Mock()
        filter_service = Mock()
        config = Mock(spec=ETLConfig)
        
        thread = TargetThread(
            target_name="test_target",
            target_config=target_config,
            message_bus=message_bus,
            database_service=database_service,
            transform_service=transform_service,
            filter_service=filter_service,
            config=config
        )
        
        mock_operation = Mock()
        
        with pytest.raises(Exception, match="Target connection not available after all retries"):
            thread._execute_with_retry(mock_operation, "arg1")
        
        # Should have tried 3 times
        assert database_service.connection_exists.call_count == 3
        assert database_service.connect.call_count == 3
    
    def test_execute_with_retry_operation_failure(self):
        """Test execution with retry when operation fails"""
        import pymysql.err
        
        target_config = Mock(spec=DatabaseConfig)
        message_bus = Mock()
        database_service = Mock()
        database_service.connection_exists.return_value = True
        database_service.reconnect_if_needed.return_value = True
        transform_service = Mock()
        filter_service = Mock()
        config = Mock(spec=ETLConfig)
        
        thread = TargetThread(
            target_name="test_target",
            target_config=target_config,
            message_bus=message_bus,
            database_service=database_service,
            transform_service=transform_service,
            filter_service=filter_service,
            config=config
        )
        
        # Test with MySQL-specific error (retryable)
        mock_operation = Mock(side_effect=pymysql.err.OperationalError(2006, "MySQL server has gone away"))
        
        with pytest.raises(pymysql.err.OperationalError):
            thread._execute_with_retry(mock_operation, "arg1")
        
        # Should have tried 3 times for MySQL errors
        assert mock_operation.call_count == 3
        
        # Test with generic exception (non-retryable)
        mock_operation2 = Mock(side_effect=Exception("Operation failed"))
        
        with pytest.raises(Exception, match="Operation failed"):
            thread._execute_with_retry(mock_operation2, "arg1")
        
        # Should have tried only 1 time for generic exceptions
        assert mock_operation2.call_count == 1
    
    def test_batch_processing_initialization(self):
        """Test batch processing initialization"""
        target_config = Mock(spec=DatabaseConfig)
        target_config.batch_size = 50
        target_config.batch_flush_interval = 3.0
        message_bus = Mock()
        database_service = Mock()
        transform_service = Mock()
        filter_service = Mock()
        config = Mock(spec=ETLConfig)
        
        thread = TargetThread(
            target_name="test_target",
            target_config=target_config,
            message_bus=message_bus,
            database_service=database_service,
            transform_service=transform_service,
            filter_service=filter_service,
            config=config
        )
        
        assert thread._batch_size == 50
        assert thread._batch_flush_interval == 3.0
        assert thread._batch_accumulator == {}
    
    def test_get_table_key(self):
        """Test getting table key for batching"""
        target_config = Mock(spec=DatabaseConfig)
        target_config.batch_size = 100
        target_config.batch_flush_interval = 5.0
        message_bus = Mock()
        database_service = Mock()
        transform_service = Mock()
        filter_service = Mock()
        config = Mock(spec=ETLConfig)
        
        thread = TargetThread(
            target_name="test_target",
            target_config=target_config,
            message_bus=message_bus,
            database_service=database_service,
            transform_service=transform_service,
            filter_service=filter_service,
            config=config
        )
        
        event = InsertEvent(
            schema="test_schema",
            table="test_table",
            values={"id": 1, "name": "test"},
            source_name="test_source"
        )
        
        table_key = thread._get_table_key(event)
        assert table_key == "test_source.test_schema.test_table"
    
    def test_add_to_batch_insert_event(self):
        """Test adding INSERT event to batch"""
        target_config = Mock(spec=DatabaseConfig)
        target_config.batch_size = 2  # Small batch size for testing
        target_config.batch_flush_interval = 5.0
        message_bus = Mock()
        database_service = Mock()
        transform_service = Mock()
        filter_service = Mock()
        config = Mock(spec=ETLConfig)
        
        thread = TargetThread(
            target_name="test_target",
            target_config=target_config,
            message_bus=message_bus,
            database_service=database_service,
            transform_service=transform_service,
            filter_service=filter_service,
            config=config
        )
        
        event = InsertEvent(
            schema="test_schema",
            table="test_table",
            values={"id": 1, "name": "test"},
            source_name="test_source"
        )
        
        # Add first event
        result = thread._add_to_batch(event)
        assert result is True
        assert len(thread._batch_accumulator) == 1
        table_key = thread._get_table_key(event)
        assert len(thread._batch_accumulator[table_key]) == 1
        
        # Add second event (should trigger batch processing)
        event2 = InsertEvent(
            schema="test_schema",
            table="test_table",
            values={"id": 2, "name": "test2"},
            source_name="test_source"
        )
        
        with patch.object(thread, '_process_batch_events') as mock_process_batch:
            result = thread._add_to_batch(event2)
            assert result is True
            # Batch should be processed and cleared
            assert len(thread._batch_accumulator[table_key]) == 0
            mock_process_batch.assert_called_once()
    
    def test_add_to_batch_delete_event(self):
        """Test that DELETE events are not batched"""
        target_config = Mock(spec=DatabaseConfig)
        target_config.batch_size = 100
        target_config.batch_flush_interval = 5.0
        message_bus = Mock()
        database_service = Mock()
        transform_service = Mock()
        filter_service = Mock()
        config = Mock(spec=ETLConfig)
        
        thread = TargetThread(
            target_name="test_target",
            target_config=target_config,
            message_bus=message_bus,
            database_service=database_service,
            transform_service=transform_service,
            filter_service=filter_service,
            config=config
        )
        
        from src.models.events import DeleteEvent
        event = DeleteEvent(
            schema="test_schema",
            table="test_table",
            values={"id": 1, "name": "test"},
            source_name="test_source"
        )
        
        result = thread._add_to_batch(event)
        assert result is False
        assert len(thread._batch_accumulator) == 0
    
    def test_should_flush_batches_time_interval(self):
        """Test batch flushing based on time interval"""
        target_config = Mock(spec=DatabaseConfig)
        target_config.batch_size = 100
        target_config.batch_flush_interval = 2.0
        message_bus = Mock()
        database_service = Mock()
        transform_service = Mock()
        filter_service = Mock()
        config = Mock(spec=ETLConfig)
        
        thread = TargetThread(
            target_name="test_target",
            target_config=target_config,
            message_bus=message_bus,
            database_service=database_service,
            transform_service=transform_service,
            filter_service=filter_service,
            config=config
        )
        
        # Set last flush time to 3 seconds ago (exceeds 2.0 second interval)
        thread._last_batch_flush = time.time() - 3.0
        
        result = thread._should_flush_batches()
        assert result is True
    
    def test_should_not_flush_batches_before_time_interval(self):
        """Test that batches are not flushed before time interval"""
        target_config = Mock(spec=DatabaseConfig)
        target_config.batch_size = 100
        target_config.batch_flush_interval = 5.0
        message_bus = Mock()
        database_service = Mock()
        transform_service = Mock()
        filter_service = Mock()
        config = Mock(spec=ETLConfig)
        
        thread = TargetThread(
            target_name="test_target",
            target_config=target_config,
            message_bus=message_bus,
            database_service=database_service,
            transform_service=transform_service,
            filter_service=filter_service,
            config=config
        )
        
        # Set last flush time to 2 seconds ago (less than 5.0 second interval)
        thread._last_batch_flush = time.time() - 2.0
        
        result = thread._should_flush_batches()
        assert result is False
    
    def test_should_flush_batches_size_limit(self):
        """Test batch flushing based on size limit"""
        target_config = Mock(spec=DatabaseConfig)
        target_config.batch_size = 2
        target_config.batch_flush_interval = 5.0
        message_bus = Mock()
        database_service = Mock()
        transform_service = Mock()
        filter_service = Mock()
        config = Mock(spec=ETLConfig)
        
        thread = TargetThread(
            target_name="test_target",
            target_config=target_config,
            message_bus=message_bus,
            database_service=database_service,
            transform_service=transform_service,
            filter_service=filter_service,
            config=config
        )
        
        # Add events to batch manually (bypassing _add_to_batch to avoid auto-processing)
        event1 = InsertEvent(
            schema="test_schema",
            table="test_table",
            values={"id": 1, "name": "test1"},
            source_name="test_source"
        )
        event2 = InsertEvent(
            schema="test_schema",
            table="test_table",
            values={"id": 2, "name": "test2"},
            source_name="test_source"
        )
        
        table_key = thread._get_table_key(event1)
        with thread._batch_lock:
            thread._batch_accumulator[table_key] = [event1, event2]
        
        result = thread._should_flush_batches()
        assert result is True
    
    def test_flush_all_batches(self):
        """Test flushing all batches"""
        target_config = Mock(spec=DatabaseConfig)
        target_config.batch_size = 100
        target_config.batch_flush_interval = 5.0
        message_bus = Mock()
        database_service = Mock()
        transform_service = Mock()
        filter_service = Mock()
        config = Mock(spec=ETLConfig)
        
        thread = TargetThread(
            target_name="test_target",
            target_config=target_config,
            message_bus=message_bus,
            database_service=database_service,
            transform_service=transform_service,
            filter_service=filter_service,
            config=config
        )
        
        # Add some events to batch
        event1 = InsertEvent(
            schema="test_schema",
            table="test_table",
            values={"id": 1, "name": "test1"},
            source_name="test_source"
        )
        event2 = InsertEvent(
            schema="test_schema",
            table="test_table",
            values={"id": 2, "name": "test2"},
            source_name="test_source"
        )
        
        thread._add_to_batch(event1)
        thread._add_to_batch(event2)
        
        with patch.object(thread, '_process_batch_events') as mock_process_batch:
            thread._flush_all_batches()
            
            # Should process batches and clear accumulator
            assert len(thread._batch_accumulator) == 0
            mock_process_batch.assert_called_once()
    
    def test_batch_statistics(self):
        """Test batch statistics tracking"""
        target_config = Mock(spec=DatabaseConfig)
        target_config.batch_size = 100
        target_config.batch_flush_interval = 5.0
        message_bus = Mock()
        database_service = Mock()
        transform_service = Mock()
        filter_service = Mock()
        config = Mock(spec=ETLConfig)
        
        thread = TargetThread(
            target_name="test_target",
            target_config=target_config,
            message_bus=message_bus,
            database_service=database_service,
            transform_service=transform_service,
            filter_service=filter_service,
            config=config
        )
        
        stats = thread.get_stats()
        
        assert 'batch_operations' in stats
        assert 'batch_records_processed' in stats
        assert stats['batch_operations'] == 0
        assert stats['batch_records_processed'] == 0

    def test_init_batch_processing_initialization(self):
        """Test init batch processing attributes are correctly initialized"""
        target_config = Mock(spec=DatabaseConfig)
        target_config.batch_size = 50
        target_config.batch_flush_interval = 3.0
        
        message_bus = Mock()
        database_service = Mock()
        transform_service = Mock()
        filter_service = Mock()
        config = Mock(spec=ETLConfig)
        
        thread = TargetThread(
            target_name="test_target",
            target_config=target_config,
            message_bus=message_bus,
            database_service=database_service,
            transform_service=transform_service,
            filter_service=filter_service,
            config=config
        )
        
        # Check init batch attributes
        assert hasattr(thread, '_init_batch_accumulator')
        assert hasattr(thread, '_init_batch_lock')
        assert hasattr(thread, '_last_init_batch_flush')
        assert thread._init_batch_accumulator == {}
        assert isinstance(thread._init_batch_lock, type(threading.Lock()))
        assert isinstance(thread._last_init_batch_flush, float)
        
        # Check init batch statistics
        stats = thread.get_stats()
        assert 'init_batch_operations' in stats
        assert 'init_batch_records_processed' in stats
        assert stats['init_batch_operations'] == 0
        assert stats['init_batch_records_processed'] == 0

    def test_get_init_table_key(self):
        """Test init table key generation"""
        target_config = Mock(spec=DatabaseConfig)
        target_config.batch_size = 100
        target_config.batch_flush_interval = 5.0
        
        message_bus = Mock()
        database_service = Mock()
        transform_service = Mock()
        filter_service = Mock()
        config = Mock(spec=ETLConfig)
        
        thread = TargetThread(
            target_name="test_target",
            target_config=target_config,
            message_bus=message_bus,
            database_service=database_service,
            transform_service=transform_service,
            filter_service=filter_service,
            config=config
        )
        
        # Create mock init event
        from src.models.events import InitQueryEvent
        init_event = InitQueryEvent(
            mapping_key="source1.table1",
            source_name="source1",
            target_name="target1",
            target_table="target_table1",
            row_data={"id": 1, "name": "test"},
            columns=["id", "name"],
            init_query="SELECT * FROM table1",
            primary_key="id",
            column_mapping={"id": "id", "name": "name"}
        )
        
        table_key = thread._get_init_table_key(init_event)
        assert table_key == "source1.target_table1"

    def test_add_to_init_batch(self):
        """Test adding init events to batch"""
        target_config = Mock(spec=DatabaseConfig)
        target_config.batch_size = 2  # Small batch size for testing
        target_config.batch_flush_interval = 5.0
        
        message_bus = Mock()
        database_service = Mock()
        transform_service = Mock()
        filter_service = Mock()
        config = Mock(spec=ETLConfig)
        
        thread = TargetThread(
            target_name="test_target",
            target_config=target_config,
            message_bus=message_bus,
            database_service=database_service,
            transform_service=transform_service,
            filter_service=filter_service,
            config=config
        )
        
        # Create mock init event
        from src.models.events import InitQueryEvent
        init_event = InitQueryEvent(
            mapping_key="source1.table1",
            source_name="source1",
            target_name="target1",
            target_table="target_table1",
            row_data={"id": 1, "name": "test"},
            columns=["id", "name"],
            init_query="SELECT * FROM table1",
            primary_key="id",
            column_mapping={"id": "id", "name": "name"}
        )
        
        # Mock the batch processing method
        with patch.object(thread, '_process_init_batch_events') as mock_process:
            # Add first event
            result = thread._add_to_init_batch(init_event)
            assert result is True
            assert len(thread._init_batch_accumulator["source1.target_table1"]) == 1
            
            # Add second event (should trigger batch processing)
            result = thread._add_to_init_batch(init_event)
            assert result is True
            assert mock_process.called
            assert len(thread._init_batch_accumulator["source1.target_table1"]) == 0

    def test_should_flush_init_batches_time_interval(self):
        """Test init batch flushing based on time interval"""
        target_config = Mock(spec=DatabaseConfig)
        target_config.batch_size = 100
        target_config.batch_flush_interval = 1.0  # 1 second
        
        message_bus = Mock()
        database_service = Mock()
        transform_service = Mock()
        filter_service = Mock()
        config = Mock(spec=ETLConfig)
        
        thread = TargetThread(
            target_name="test_target",
            target_config=target_config,
            message_bus=message_bus,
            database_service=database_service,
            transform_service=transform_service,
            filter_service=filter_service,
            config=config
        )
        
        # Set last flush time to 2 seconds ago
        thread._last_init_batch_flush = time.time() - 2.0
        
        # Should flush due to time interval
        assert thread._should_flush_init_batches() is True

    def test_should_not_flush_init_batches_before_time_interval(self):
        """Test init batch doesn't flush before time interval"""
        target_config = Mock(spec=DatabaseConfig)
        target_config.batch_size = 100
        target_config.batch_flush_interval = 5.0
        
        message_bus = Mock()
        database_service = Mock()
        transform_service = Mock()
        filter_service = Mock()
        config = Mock(spec=ETLConfig)
        
        thread = TargetThread(
            target_name="test_target",
            target_config=target_config,
            message_bus=message_bus,
            database_service=database_service,
            transform_service=transform_service,
            filter_service=filter_service,
            config=config
        )
        
        # Set last flush time to 1 second ago
        thread._last_init_batch_flush = time.time() - 1.0
        
        # Should not flush before time interval
        assert thread._should_flush_init_batches() is False

    def test_should_flush_init_batches_size_limit(self):
        """Test init batch flushing based on size limit"""
        target_config = Mock(spec=DatabaseConfig)
        target_config.batch_size = 2
        target_config.batch_flush_interval = 5.0
        
        message_bus = Mock()
        database_service = Mock()
        transform_service = Mock()
        filter_service = Mock()
        config = Mock(spec=ETLConfig)
        
        thread = TargetThread(
            target_name="test_target",
            target_config=target_config,
            message_bus=message_bus,
            database_service=database_service,
            transform_service=transform_service,
            filter_service=filter_service,
            config=config
        )
        
        # Manually populate init batch accumulator to avoid auto-processing
        from src.models.events import InitQueryEvent
        init_event = InitQueryEvent(
            mapping_key="source1.table1",
            source_name="source1",
            target_name="target1",
            target_table="target_table1",
            row_data={"id": 1, "name": "test"},
            columns=["id", "name"],
            init_query="SELECT * FROM table1",
            primary_key="id",
            column_mapping={"id": "id", "name": "name"}
        )
        
        thread._init_batch_accumulator["source1.target_table1"] = [init_event, init_event]
        
        # Should flush due to size limit
        assert thread._should_flush_init_batches() is True

    def test_flush_all_init_batches(self):
        """Test flushing all accumulated init batches"""
        target_config = Mock(spec=DatabaseConfig)
        target_config.batch_size = 100
        target_config.batch_flush_interval = 5.0
        
        message_bus = Mock()
        database_service = Mock()
        transform_service = Mock()
        filter_service = Mock()
        config = Mock(spec=ETLConfig)
        
        thread = TargetThread(
            target_name="test_target",
            target_config=target_config,
            message_bus=message_bus,
            database_service=database_service,
            transform_service=transform_service,
            filter_service=filter_service,
            config=config
        )
        
        # Mock the batch processing method
        with patch.object(thread, '_process_init_batch_events') as mock_process:
            # Add some events to init batch accumulator
            from src.models.events import InitQueryEvent
            init_event = InitQueryEvent(
                mapping_key="source1.table1",
                source_name="source1",
                target_name="target1",
                target_table="target_table1",
                row_data={"id": 1, "name": "test"},
                columns=["id", "name"],
                init_query="SELECT * FROM table1",
                primary_key="id",
                column_mapping={"id": "id", "name": "name"}
            )
            
            thread._init_batch_accumulator["source1.target_table1"] = [init_event, init_event]
            
            # Flush all batches
            thread._flush_all_init_batches()
            
            # Check that batch processing was called
            assert mock_process.called
            assert len(thread._init_batch_accumulator) == 0

    def test_init_batch_statistics(self):
        """Test init batch statistics are tracked correctly"""
        target_config = Mock(spec=DatabaseConfig)
        target_config.batch_size = 100
        target_config.batch_flush_interval = 5.0
        
        message_bus = Mock()
        database_service = Mock()
        transform_service = Mock()
        filter_service = Mock()
        config = Mock(spec=ETLConfig)
        
        thread = TargetThread(
            target_name="test_target",
            target_config=target_config,
            message_bus=message_bus,
            database_service=database_service,
            transform_service=transform_service,
            filter_service=filter_service,
            config=config
        )
        
        # Check initial statistics
        stats = thread.get_stats()
        assert stats['init_batch_operations'] == 0
        assert stats['init_batch_records_processed'] == 0
        
        # Manually update statistics (simulating batch processing)
        with thread._stats_lock:
            thread._stats['init_batch_operations'] = 5
            thread._stats['init_batch_records_processed'] = 100
        
        stats = thread.get_stats()
        assert stats['init_batch_operations'] == 5
        assert stats['init_batch_records_processed'] == 100

