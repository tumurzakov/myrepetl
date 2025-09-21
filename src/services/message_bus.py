"""
Message bus for inter-thread communication in MySQL Replication ETL
"""

import queue
import threading
import time
from typing import Any, Dict, Optional, Callable, List
from dataclasses import dataclass
from enum import Enum
import structlog

from ..models.events import BinlogEvent


class MessageType(Enum):
    """Types of messages in the bus"""
    BINLOG_EVENT = "binlog_event"
    SHUTDOWN = "shutdown"
    ERROR = "error"
    HEARTBEAT = "heartbeat"
    STATUS_UPDATE = "status_update"


@dataclass
class Message:
    """Message structure for the bus"""
    message_type: MessageType
    source: str
    target: Optional[str] = None
    data: Any = None
    timestamp: float = None
    message_id: str = None
    
    def __post_init__(self):
        if self.timestamp is None:
            self.timestamp = time.time()
        if self.message_id is None:
            self.message_id = f"{self.source}_{int(self.timestamp * 1000)}"


class MessageBus:
    """Thread-safe message bus for inter-thread communication"""
    
    def __init__(self, max_queue_size: int = 10000):
        self.logger = structlog.get_logger()
        self.max_queue_size = max_queue_size
        
        # Main message queue
        self._message_queue = queue.Queue(maxsize=max_queue_size)
        
        # Subscriber management
        self._subscribers: Dict[MessageType, List[Callable]] = {}
        self._subscriber_lock = threading.RLock()
        
        # Statistics
        self._stats = {
            'messages_sent': 0,
            'messages_processed': 0,
            'messages_dropped': 0,
            'subscribers_count': 0
        }
        self._stats_lock = threading.Lock()
        
        # Shutdown flag
        self._shutdown_requested = False
        self._shutdown_lock = threading.Lock()
        
        self.logger.info("Message bus initialized", max_queue_size=max_queue_size)
    
    def subscribe(self, message_type: MessageType, callback: Callable[[Message], None]) -> None:
        """Subscribe to specific message type"""
        with self._subscriber_lock:
            if message_type not in self._subscribers:
                self._subscribers[message_type] = []
            self._subscribers[message_type].append(callback)
            
            with self._stats_lock:
                self._stats['subscribers_count'] = sum(len(subs) for subs in self._subscribers.values())
        
        callback_name = getattr(callback, '__name__', str(callback))
        self.logger.debug("Subscribed to message type", 
                         message_type=message_type.value,
                         callback=callback_name)
    
    def unsubscribe(self, message_type: MessageType, callback: Callable[[Message], None]) -> None:
        """Unsubscribe from specific message type"""
        with self._subscriber_lock:
            if message_type in self._subscribers:
                try:
                    self._subscribers[message_type].remove(callback)
                    with self._stats_lock:
                        self._stats['subscribers_count'] = sum(len(subs) for subs in self._subscribers.values())
                    callback_name = getattr(callback, '__name__', str(callback))
                    self.logger.debug("Unsubscribed from message type", 
                                     message_type=message_type.value,
                                     callback=callback_name)
                except ValueError:
                    callback_name = getattr(callback, '__name__', str(callback))
                    self.logger.warning("Callback not found in subscribers", 
                                       message_type=message_type.value,
                                       callback=callback_name)
    
    def publish(self, message: Message) -> bool:
        """Publish message to the bus"""
        with self._shutdown_lock:
            if self._shutdown_requested:
                self.logger.debug("Message bus is shutting down, dropping message", 
                                 message_type=message.message_type.value,
                                 source=message.source)
                return False
        
        try:
            self._message_queue.put_nowait(message)
            with self._stats_lock:
                self._stats['messages_sent'] += 1
            
            self.logger.debug("Message published", 
                             message_type=message.message_type.value,
                             source=message.source,
                             target=message.target)
            return True
            
        except queue.Full:
            with self._stats_lock:
                self._stats['messages_dropped'] += 1
            
            self.logger.warning("Message queue is full, dropping message", 
                               message_type=message.message_type.value,
                               source=message.source,
                               queue_size=self._message_queue.qsize())
            return False
    
    def publish_binlog_event(self, source: str, event: BinlogEvent, target: str = None) -> bool:
        """Publish binlog event message"""
        message = Message(
            message_type=MessageType.BINLOG_EVENT,
            source=source,
            target=target,
            data=event
        )
        return self.publish(message)
    
    def publish_shutdown(self, source: str) -> bool:
        """Publish shutdown message"""
        message = Message(
            message_type=MessageType.SHUTDOWN,
            source=source,
            data="Shutdown requested"
        )
        return self.publish(message)
    
    def publish_error(self, source: str, error: Exception, target: str = None) -> bool:
        """Publish error message"""
        message = Message(
            message_type=MessageType.ERROR,
            source=source,
            target=target,
            data=str(error)
        )
        return self.publish(message)
    
    def publish_heartbeat(self, source: str) -> bool:
        """Publish heartbeat message"""
        message = Message(
            message_type=MessageType.HEARTBEAT,
            source=source,
            data=time.time()
        )
        return self.publish(message)
    
    def process_messages(self, timeout: float = 1.0) -> None:
        """Process messages from the queue"""
        try:
            # Process messages until timeout or shutdown
            start_time = time.time()
            while not self._is_shutdown_requested():
                try:
                    # Calculate remaining timeout
                    elapsed = time.time() - start_time
                    remaining_timeout = max(0, timeout - elapsed)
                    
                    if remaining_timeout <= 0:
                        # Timeout reached, exit
                        break
                    
                    # Get message with remaining timeout
                    message = self._message_queue.get(timeout=remaining_timeout)
                    
                    # Process message
                    self._process_message(message)
                    
                    # Mark task as done
                    self._message_queue.task_done()
                    
                    with self._stats_lock:
                        self._stats['messages_processed'] += 1
                        
                except queue.Empty:
                    # Timeout reached, exit
                    break
                    
        except Exception as e:
            self.logger.error("Error processing messages", error=str(e))
            raise
    
    def _process_message(self, message: Message) -> None:
        """Process a single message"""
        with self._subscriber_lock:
            subscribers = self._subscribers.get(message.message_type, [])
        
        if not subscribers:
            self.logger.debug("No subscribers for message type", 
                             message_type=message.message_type.value)
            return
        
        # Call all subscribers
        for callback in subscribers:
            try:
                callback(message)
            except Exception as e:
                callback_name = getattr(callback, '__name__', str(callback))
                self.logger.error("Error in message subscriber", 
                                 callback=callback_name,
                                 message_type=message.message_type.value,
                                 error=str(e))
    
    def _is_shutdown_requested(self) -> bool:
        """Check if shutdown is requested"""
        with self._shutdown_lock:
            return self._shutdown_requested
    
    def request_shutdown(self) -> None:
        """Request shutdown of the message bus"""
        # Publish shutdown message to wake up any waiting threads
        self.publish_shutdown("message_bus")
        
        with self._shutdown_lock:
            self._shutdown_requested = True
        
        self.logger.info("Message bus shutdown requested")
    
    def get_stats(self) -> Dict[str, Any]:
        """Get message bus statistics"""
        with self._stats_lock:
            return self._stats.copy()
    
    def get_queue_size(self) -> int:
        """Get current queue size"""
        return self._message_queue.qsize()
    
    def is_empty(self) -> bool:
        """Check if message queue is empty"""
        return self._message_queue.empty()
    
    def join(self, timeout: Optional[float] = None) -> None:
        """Wait for all messages to be processed"""
        self._message_queue.join()
    
    def clear(self) -> None:
        """Clear all messages from the queue"""
        while not self._message_queue.empty():
            try:
                self._message_queue.get_nowait()
            except queue.Empty:
                break
        
        self.logger.info("Message queue cleared")
