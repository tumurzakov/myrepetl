"""
Unit tests for MessageBus
"""

import pytest
import threading
import time
from unittest.mock import Mock, MagicMock

from src.services.message_bus import MessageBus, Message, MessageType


class TestMessageBus:
    """Test MessageBus"""
    
    def test_message_creation(self):
        """Test message creation"""
        message = Message(
            message_type=MessageType.BINLOG_EVENT,
            source="test_source",
            target="test_target",
            data={"test": "data"}
        )
        
        assert message.message_type == MessageType.BINLOG_EVENT
        assert message.source == "test_source"
        assert message.target == "test_target"
        assert message.data == {"test": "data"}
        assert message.timestamp is not None
        assert message.message_id is not None
    
    def test_message_bus_initialization(self):
        """Test message bus initialization"""
        bus = MessageBus(max_queue_size=1000)
        
        assert bus.max_queue_size == 1000
        assert bus.get_queue_size() == 0
        assert bus.is_empty() is True
        assert not bus._shutdown_requested
    
    def test_subscribe_unsubscribe(self):
        """Test subscription and unsubscription"""
        bus = MessageBus()
        callback = Mock()
        
        # Subscribe
        bus.subscribe(MessageType.BINLOG_EVENT, callback)
        assert MessageType.BINLOG_EVENT in bus._subscribers
        assert callback in bus._subscribers[MessageType.BINLOG_EVENT]
        
        # Unsubscribe
        bus.unsubscribe(MessageType.BINLOG_EVENT, callback)
        assert callback not in bus._subscribers[MessageType.BINLOG_EVENT]
    
    def test_publish_message_success(self):
        """Test successful message publishing"""
        bus = MessageBus()
        message = Message(
            message_type=MessageType.BINLOG_EVENT,
            source="test_source",
            data={"test": "data"}
        )
        
        result = bus.publish(message)
        
        assert result is True
        assert bus.get_queue_size() == 1
        assert not bus.is_empty()
    
    def test_publish_message_queue_full(self):
        """Test message publishing when queue is full"""
        bus = MessageBus(max_queue_size=1)
        
        # Fill the queue
        message1 = Message(MessageType.BINLOG_EVENT, "source1", data="data1")
        bus.publish(message1)
        
        # Try to publish another message
        message2 = Message(MessageType.BINLOG_EVENT, "source2", data="data2")
        result = bus.publish(message2)
        
        assert result is False
        assert bus.get_queue_size() == 1
    
    def test_publish_message_shutdown(self):
        """Test message publishing when shutdown is requested"""
        bus = MessageBus()
        bus.request_shutdown()
        
        message = Message(MessageType.BINLOG_EVENT, "test_source", data="data")
        result = bus.publish(message)
        
        assert result is False
    
    def test_publish_binlog_event(self):
        """Test publishing binlog event message"""
        bus = MessageBus()
        event = Mock()
        
        result = bus.publish_binlog_event("source1", event, "target1")
        
        assert result is True
        assert bus.get_queue_size() == 1
    
    def test_publish_shutdown(self):
        """Test publishing shutdown message"""
        bus = MessageBus()
        
        result = bus.publish_shutdown("test_source")
        
        assert result is True
        assert bus.get_queue_size() == 1
    
    def test_publish_error(self):
        """Test publishing error message"""
        bus = MessageBus()
        error = Exception("Test error")
        
        result = bus.publish_error("test_source", error, "target1")
        
        assert result is True
        assert bus.get_queue_size() == 1
    
    def test_publish_heartbeat(self):
        """Test publishing heartbeat message"""
        bus = MessageBus()
        
        result = bus.publish_heartbeat("test_source")
        
        assert result is True
        assert bus.get_queue_size() == 1
    
    def test_process_messages_with_subscriber(self):
        """Test message processing with subscriber"""
        bus = MessageBus()
        callback = Mock()
        
        # Subscribe to messages
        bus.subscribe(MessageType.BINLOG_EVENT, callback)
        
        # Publish a message
        message = Message(MessageType.BINLOG_EVENT, "test_source", data="test_data")
        bus.publish(message)
        
        # Process messages
        bus.process_messages(timeout=0.01)
        
        # Verify callback was called
        callback.assert_called_once()
        args = callback.call_args[0]
        assert args[0].message_type == MessageType.BINLOG_EVENT
        assert args[0].source == "test_source"
        assert args[0].data == "test_data"
    
    def test_process_messages_no_subscribers(self):
        """Test message processing with no subscribers"""
        bus = MessageBus()
        
        # Publish a message
        message = Message(MessageType.BINLOG_EVENT, "test_source", data="test_data")
        bus.publish(message)
        
        # Process messages (should not raise exception)
        bus.process_messages(timeout=0.01)
        
        # Message should still be processed
        assert bus.get_queue_size() == 0
    
    def test_process_messages_subscriber_error(self):
        """Test message processing with subscriber error"""
        bus = MessageBus()
        callback = Mock(side_effect=Exception("Callback error"))
        
        # Subscribe to messages
        bus.subscribe(MessageType.BINLOG_EVENT, callback)
        
        # Publish a message
        message = Message(MessageType.BINLOG_EVENT, "test_source", data="test_data")
        bus.publish(message)
        
        # Process messages (should not raise exception)
        bus.process_messages(timeout=0.01)
        
        # Callback should still be called
        callback.assert_called_once()
    
    def test_process_messages_timeout(self):
        """Test message processing with timeout"""
        bus = MessageBus()
        
        # Process messages with timeout (no messages in queue)
        start_time = time.time()
        bus.process_messages(timeout=0.01)  # Very short timeout
        end_time = time.time()
        
        # Should complete quickly (no infinite loop)
        assert end_time - start_time < 1.0  # Should complete in less than 1 second
    
    def test_request_shutdown(self):
        """Test shutdown request"""
        bus = MessageBus()
        
        bus.request_shutdown()
        
        assert bus._shutdown_requested is True
        assert bus.get_queue_size() == 1  # Shutdown message published
    
    def test_get_stats(self):
        """Test getting statistics"""
        bus = MessageBus()
        
        # Publish some messages
        bus.publish(Message(MessageType.BINLOG_EVENT, "source1", data="data1"))
        bus.publish(Message(MessageType.BINLOG_EVENT, "source2", data="data2"))
        
        stats = bus.get_stats()
        
        assert stats['messages_sent'] == 2
        assert stats['messages_processed'] == 0  # Not processed yet
        assert stats['messages_dropped'] == 0
        assert stats['subscribers_count'] == 0
    
    def test_clear(self):
        """Test clearing message queue"""
        bus = MessageBus()
        
        # Publish some messages
        bus.publish(Message(MessageType.BINLOG_EVENT, "source1", data="data1"))
        bus.publish(Message(MessageType.BINLOG_EVENT, "source2", data="data2"))
        
        assert bus.get_queue_size() == 2
        
        # Clear queue
        bus.clear()
        
        assert bus.get_queue_size() == 0
        assert bus.is_empty() is True
    
    def test_join(self):
        """Test joining message queue"""
        bus = MessageBus()
        
        # Publish a message
        bus.publish(Message(MessageType.BINLOG_EVENT, "source1", data="data1"))
        
        # Process the message
        bus.process_messages(timeout=0.1)
        
        # Join should complete immediately
        bus.join(timeout=0.1)
    
    def test_multiple_subscribers(self):
        """Test multiple subscribers for same message type"""
        bus = MessageBus()
        callback1 = Mock()
        callback2 = Mock()
        
        # Subscribe both callbacks
        bus.subscribe(MessageType.BINLOG_EVENT, callback1)
        bus.subscribe(MessageType.BINLOG_EVENT, callback2)
        
        # Publish a message
        message = Message(MessageType.BINLOG_EVENT, "test_source", data="test_data")
        bus.publish(message)
        
        # Process messages
        bus.process_messages(timeout=0.1)
        
        # Both callbacks should be called
        callback1.assert_called_once()
        callback2.assert_called_once()
    
    def test_different_message_types(self):
        """Test different message types with different subscribers"""
        bus = MessageBus()
        binlog_callback = Mock()
        error_callback = Mock()
        
        # Subscribe to different message types
        bus.subscribe(MessageType.BINLOG_EVENT, binlog_callback)
        bus.subscribe(MessageType.ERROR, error_callback)
        
        # Publish different message types
        binlog_message = Message(MessageType.BINLOG_EVENT, "source1", data="binlog_data")
        error_message = Message(MessageType.ERROR, "source2", data="error_data")
        
        bus.publish(binlog_message)
        bus.publish(error_message)
        
        # Process messages
        bus.process_messages(timeout=0.1)
        
        # Only relevant callbacks should be called
        binlog_callback.assert_called_once()
        error_callback.assert_called_once()
        
        # Verify correct messages
        binlog_args = binlog_callback.call_args[0]
        assert binlog_args[0].message_type == MessageType.BINLOG_EVENT
        
        error_args = error_callback.call_args[0]
        assert error_args[0].message_type == MessageType.ERROR
