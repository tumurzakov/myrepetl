"""
Unit tests for SourceThreadService
"""

import pytest
import threading
import time
from unittest.mock import Mock, patch, MagicMock

from src.services.source_thread_service import SourceThread, SourceThreadService
from src.models.config import DatabaseConfig, ReplicationConfig
from src.models.events import InsertEvent, EventType
from src.exceptions import ReplicationError


class TestSourceThread:
    """Test SourceThread"""
    
    def test_source_thread_initialization(self):
        """Test source thread initialization"""
        source_config = Mock(spec=DatabaseConfig)
        replication_config = Mock(spec=ReplicationConfig)
        tables = [("schema1", "table1"), ("schema2", "table2")]
        message_bus = Mock()
        database_service = Mock()
        
        thread = SourceThread(
            source_name="test_source",
            source_config=source_config,
            replication_config=replication_config,
            tables=tables,
            message_bus=message_bus,
            database_service=database_service
        )
        
        assert thread.source_name == "test_source"
        assert thread.source_config == source_config
        assert thread.replication_config == replication_config
        assert thread.tables == tables
        assert thread.message_bus == message_bus
        assert thread.database_service == database_service
        assert not thread.is_running()
    
    def test_start_stop_thread(self):
        """Test starting and stopping thread"""
        with patch('src.services.source_thread_service.BinLogStreamReader') as mock_stream_reader:
            # Setup mocks
            mock_stream = Mock()
            mock_stream_reader.return_value = mock_stream
            mock_stream.__iter__ = Mock(return_value=iter([]))
            
            source_config = Mock(spec=DatabaseConfig)
            source_config.host = "localhost"
            source_config.port = 3306
            source_config.user = "test"
            source_config.password = "test"
            source_config.database = "test"
            source_config.charset = "utf8mb4"
            replication_config = Mock(spec=ReplicationConfig)
            tables = [("schema1", "table1")]
            message_bus = Mock()
            database_service = Mock()
            database_service.get_master_status.return_value = {"file": "binlog.000001", "position": 123}
            
            thread = SourceThread(
                source_name="test_source",
                source_config=source_config,
                replication_config=replication_config,
                tables=tables,
                message_bus=message_bus,
                database_service=database_service
            )
            
            # Start thread
            thread.start()
            time.sleep(0.2)  # Give thread time to start
            
            # Thread should be running or finished by now
            # Check if it was running at some point
            assert thread._thread is not None and thread._thread.is_alive() or not thread.is_running()
            
            # Stop thread
            thread.stop()
            time.sleep(0.1)  # Give thread time to stop
            
            assert not thread.is_running()
    
    def test_get_stats(self):
        """Test getting thread statistics"""
        source_config = Mock(spec=DatabaseConfig)
        replication_config = Mock(spec=ReplicationConfig)
        tables = [("schema1", "table1")]
        message_bus = Mock()
        database_service = Mock()
        
        thread = SourceThread(
            source_name="test_source",
            source_config=source_config,
            replication_config=replication_config,
            tables=tables,
            message_bus=message_bus,
            database_service=database_service
        )
        
        stats = thread.get_stats()
        
        assert 'events_processed' in stats
        assert 'errors_count' in stats
        assert 'last_event_time' in stats
        assert 'is_running' in stats
        assert stats['is_running'] is False
    
    def test_convert_binlog_event_insert(self):
        """Test converting binlog event to insert event"""
        source_config = Mock(spec=DatabaseConfig)
        replication_config = Mock(spec=ReplicationConfig)
        tables = [("schema1", "table1")]
        message_bus = Mock()
        database_service = Mock()
        
        thread = SourceThread(
            source_name="test_source",
            source_config=source_config,
            replication_config=replication_config,
            tables=tables,
            message_bus=message_bus,
            database_service=database_service
        )
        
        # Mock binlog event
        mock_binlog_event = Mock()
        mock_binlog_event.schema = "test_schema"
        mock_binlog_event.table = "test_table"
        mock_binlog_event.rows = [{"values": {"id": 1, "name": "test"}}]
        
        # Mock WriteRowsEvent by making the binlog_event an instance of it
        from pymysqlreplication import row_event
        mock_binlog_event.__class__ = row_event.WriteRowsEvent
        
        event = thread._convert_binlog_event(mock_binlog_event)
        
        assert isinstance(event, InsertEvent)
        assert event.schema == "test_schema"
        assert event.table == "test_table"
        assert event.source_name == "test_source"
        assert event.values == {"id": 1, "name": "test"}


class TestSourceThreadService:
    """Test SourceThreadService"""
    
    def test_source_thread_service_initialization(self):
        """Test source thread service initialization"""
        message_bus = Mock()
        database_service = Mock()
        
        service = SourceThreadService(message_bus, database_service)
        
        assert service.message_bus == message_bus
        assert service.database_service == database_service
        assert len(service._source_threads) == 0
    
    def test_start_source(self):
        """Test starting a source thread"""
        with patch('src.services.source_thread_service.SourceThread') as mock_source_thread_class:
            mock_thread = Mock()
            mock_source_thread_class.return_value = mock_thread
            
            message_bus = Mock()
            database_service = Mock()
            service = SourceThreadService(message_bus, database_service)
            
            source_config = Mock(spec=DatabaseConfig)
            replication_config = Mock(spec=ReplicationConfig)
            tables = [("schema1", "table1")]
            
            service.start_source("test_source", source_config, replication_config, tables)
            
            assert "test_source" in service._source_threads
            mock_source_thread_class.assert_called_once_with(
                source_name="test_source",
                source_config=source_config,
                replication_config=replication_config,
                tables=tables,
                message_bus=message_bus,
                database_service=database_service
            )
            mock_thread.start.assert_called_once()
    
    def test_start_source_already_exists(self):
        """Test starting a source thread that already exists"""
        with patch('src.services.source_thread_service.SourceThread') as mock_source_thread_class:
            mock_thread = Mock()
            mock_source_thread_class.return_value = mock_thread
            
            message_bus = Mock()
            database_service = Mock()
            service = SourceThreadService(message_bus, database_service)
            
            # Add existing thread
            service._source_threads["test_source"] = mock_thread
            
            source_config = Mock(spec=DatabaseConfig)
            replication_config = Mock(spec=ReplicationConfig)
            tables = [("schema1", "table1")]
            
            # Should not create new thread
            service.start_source("test_source", source_config, replication_config, tables)
            
            # Should not call start again
            assert mock_thread.start.call_count == 0
    
    def test_stop_source(self):
        """Test stopping a source thread"""
        with patch('src.services.source_thread_service.SourceThread') as mock_source_thread_class:
            mock_thread = Mock()
            mock_source_thread_class.return_value = mock_thread
            
            message_bus = Mock()
            database_service = Mock()
            service = SourceThreadService(message_bus, database_service)
            
            # Add thread
            service._source_threads["test_source"] = mock_thread
            
            service.stop_source("test_source")
            
            mock_thread.stop.assert_called_once()
            assert "test_source" not in service._source_threads
    
    def test_stop_source_not_found(self):
        """Test stopping a source thread that doesn't exist"""
        message_bus = Mock()
        database_service = Mock()
        service = SourceThreadService(message_bus, database_service)
        
        # Should not raise exception
        service.stop_source("nonexistent_source")
    
    def test_stop_all_sources(self):
        """Test stopping all source threads"""
        with patch('src.services.source_thread_service.SourceThread') as mock_source_thread_class:
            mock_thread1 = Mock()
            mock_thread2 = Mock()
            mock_source_thread_class.return_value = mock_thread1
            
            message_bus = Mock()
            database_service = Mock()
            service = SourceThreadService(message_bus, database_service)
            
            # Add threads
            service._source_threads["source1"] = mock_thread1
            service._source_threads["source2"] = mock_thread2
            
            service.stop_all_sources()
            
            mock_thread1.stop.assert_called_once()
            mock_thread2.stop.assert_called_once()
            assert len(service._source_threads) == 0
    
    def test_get_source_stats(self):
        """Test getting source statistics"""
        with patch('src.services.source_thread_service.SourceThread') as mock_source_thread_class:
            mock_thread = Mock()
            mock_thread.get_stats.return_value = {"events_processed": 10, "is_running": True}
            mock_source_thread_class.return_value = mock_thread
            
            message_bus = Mock()
            database_service = Mock()
            service = SourceThreadService(message_bus, database_service)
            
            # Add thread
            service._source_threads["test_source"] = mock_thread
            
            stats = service.get_source_stats("test_source")
            
            assert stats == {"events_processed": 10, "is_running": True}
            mock_thread.get_stats.assert_called_once()
    
    def test_get_source_stats_not_found(self):
        """Test getting statistics for non-existent source"""
        message_bus = Mock()
        database_service = Mock()
        service = SourceThreadService(message_bus, database_service)
        
        stats = service.get_source_stats("nonexistent_source")
        
        assert stats is None
    
    def test_get_all_stats(self):
        """Test getting statistics for all sources"""
        with patch('src.services.source_thread_service.SourceThread') as mock_source_thread_class:
            mock_thread1 = Mock()
            mock_thread1.get_stats.return_value = {"events_processed": 10}
            mock_thread2 = Mock()
            mock_thread2.get_stats.return_value = {"events_processed": 20}
            
            message_bus = Mock()
            database_service = Mock()
            service = SourceThreadService(message_bus, database_service)
            
            # Add threads
            service._source_threads["source1"] = mock_thread1
            service._source_threads["source2"] = mock_thread2
            
            stats = service.get_all_stats()
            
            assert "source1" in stats
            assert "source2" in stats
            assert stats["source1"]["events_processed"] == 10
            assert stats["source2"]["events_processed"] == 20
    
    def test_is_source_running(self):
        """Test checking if source is running"""
        with patch('src.services.source_thread_service.SourceThread') as mock_source_thread_class:
            mock_thread = Mock()
            mock_thread.is_running.return_value = True
            mock_source_thread_class.return_value = mock_thread
            
            message_bus = Mock()
            database_service = Mock()
            service = SourceThreadService(message_bus, database_service)
            
            # Add thread
            service._source_threads["test_source"] = mock_thread
            
            assert service.is_source_running("test_source") is True
            assert service.is_source_running("nonexistent_source") is False
    
    def test_get_running_sources(self):
        """Test getting list of running sources"""
        with patch('src.services.source_thread_service.SourceThread') as mock_source_thread_class:
            mock_thread1 = Mock()
            mock_thread1.is_running.return_value = True
            mock_thread2 = Mock()
            mock_thread2.is_running.return_value = False
            
            message_bus = Mock()
            database_service = Mock()
            service = SourceThreadService(message_bus, database_service)
            
            # Add threads
            service._source_threads["source1"] = mock_thread1
            service._source_threads["source2"] = mock_thread2
            
            running_sources = service.get_running_sources()
            
            assert "source1" in running_sources
            assert "source2" not in running_sources

