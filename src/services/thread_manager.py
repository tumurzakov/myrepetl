"""
Thread manager for MySQL Replication ETL
Manages lifecycle and synchronization of all threads
"""

import threading
import time
from typing import Dict, Any, Optional, List
from dataclasses import dataclass
from enum import Enum

from .message_bus import MessageBus, MessageType, Message
from .source_thread_service import SourceThreadService
from .target_thread_service import TargetThreadService
from .database_service import DatabaseService
from .transform_service import TransformService
from .filter_service import FilterService
from ..models.config import ETLConfig
import structlog


class ServiceStatus(Enum):
    """Service status enumeration"""
    STOPPED = "stopped"
    STARTING = "starting"
    RUNNING = "running"
    STOPPING = "stopping"
    ERROR = "error"


@dataclass
class ServiceStats:
    """Service statistics"""
    status: ServiceStatus
    uptime: float
    sources_count: int
    targets_count: int
    total_events_processed: int
    total_errors: int
    message_bus_stats: Dict[str, Any]
    source_stats: Dict[str, Dict[str, Any]]
    target_stats: Dict[str, Dict[str, Any]]


class ThreadManager:
    """Manages all threads and their lifecycle"""
    
    def __init__(self):
        self.logger = structlog.get_logger()
        
        # Core services
        self.message_bus = MessageBus(max_queue_size=10000)
        self.database_service = DatabaseService()
        self.transform_service = TransformService()
        self.filter_service = FilterService()
        
        # Thread services
        self.source_thread_service = SourceThreadService(self.message_bus, self.database_service)
        self.target_thread_service = TargetThreadService(
            self.message_bus, self.database_service, 
            self.transform_service, self.filter_service
        )
        
        # State management
        self._status = ServiceStatus.STOPPED
        self._status_lock = threading.RLock()
        self._start_time: Optional[float] = None
        self._shutdown_requested = False
        self._shutdown_lock = threading.Lock()
        
        # Message bus processing thread
        self._message_bus_thread: Optional[threading.Thread] = None
        self._message_bus_thread_lock = threading.Lock()
        
        # Monitoring thread
        self._monitoring_thread: Optional[threading.Thread] = None
        self._monitoring_thread_lock = threading.Lock()
        self._monitoring_interval = 30.0  # seconds
        
        self.logger.info("Thread manager initialized")
    
    def start(self, config: ETLConfig) -> None:
        """Start all services and threads"""
        with self._status_lock:
            if self._status != ServiceStatus.STOPPED:
                self.logger.warning("Services already started or starting", status=self._status.value)
                return
            
            self._status = ServiceStatus.STARTING
            self._start_time = time.time()
        
        try:
            self.logger.info("Starting ETL services")
            
            # Load transform module
            self._load_transform_module(config)
            
            # Start message bus processing
            self._start_message_bus_processing()
            
            # Start target threads first (they need to be ready to receive events)
            self._start_target_threads(config)
            
            # Start source threads
            self._start_source_threads(config)
            
            # Start monitoring
            self._start_monitoring()
            
            with self._status_lock:
                self._status = ServiceStatus.RUNNING
            
            self.logger.info("All ETL services started successfully")
            
        except Exception as e:
            with self._status_lock:
                self._status = ServiceStatus.ERROR
            
            self.logger.error("Failed to start ETL services", error=str(e))
            # Don't call stop() here to preserve ERROR status
            self._cleanup_resources()
            raise
    
    def _cleanup_resources(self) -> None:
        """Cleanup resources without changing status"""
        try:
            # Stop source threads
            self.source_thread_service.stop_all_sources()
            
            # Stop target threads
            self.target_thread_service.stop_all_targets()
            
            # Stop message bus processing
            self._stop_message_bus_processing()
            
            # Stop monitoring
            self._stop_monitoring()
            
            self.logger.info("All ETL services stopped successfully")
            
        except Exception as e:
            self.logger.error("Error during cleanup", error=str(e))
    
    def stop(self) -> None:
        """Stop all services and threads"""
        with self._shutdown_lock:
            if self._shutdown_requested:
                self.logger.warning("Shutdown already requested")
                return
            self._shutdown_requested = True
        
        with self._status_lock:
            if self._status == ServiceStatus.STOPPED:
                return
            self._status = ServiceStatus.STOPPING
        
        self.logger.info("Stopping ETL services")
        
        try:
            # Stop source threads
            self.source_thread_service.stop_all_sources()
            
            # Stop target threads
            self.target_thread_service.stop_all_targets()
            
            # Publish shutdown message to wake up any waiting threads
            self.message_bus.publish_shutdown("thread_manager")
            
            # Stop message bus processing
            self._stop_message_bus_processing()
            
            # Stop monitoring
            self._stop_monitoring()
            
            # Close database connections
            self.database_service.close_all_connections()
            
            with self._status_lock:
                self._status = ServiceStatus.STOPPED
            
            self.logger.info("All ETL services stopped successfully")
            
        except Exception as e:
            self.logger.error("Error stopping ETL services", error=str(e))
            with self._status_lock:
                self._status = ServiceStatus.ERROR
    
    def get_status(self) -> ServiceStatus:
        """Get current service status"""
        with self._status_lock:
            return self._status
    
    def get_stats(self) -> ServiceStats:
        """Get comprehensive service statistics"""
        uptime = 0.0
        if self._start_time:
            uptime = time.time() - self._start_time
        
        source_stats = self.source_thread_service.get_all_stats()
        target_stats = self.target_thread_service.get_all_stats()
        
        # Calculate totals
        total_events_processed = sum(
            stats.get('events_processed', 0) for stats in source_stats.values()
        ) + sum(
            stats.get('events_processed', 0) for stats in target_stats.values()
        )
        
        total_errors = sum(
            stats.get('errors_count', 0) for stats in source_stats.values()
        ) + sum(
            stats.get('errors_count', 0) for stats in target_stats.values()
        )
        
        return ServiceStats(
            status=self.get_status(),
            uptime=uptime,
            sources_count=len(source_stats),
            targets_count=len(target_stats),
            total_events_processed=total_events_processed,
            total_errors=total_errors,
            message_bus_stats=self.message_bus.get_stats(),
            source_stats=source_stats,
            target_stats=target_stats
        )
    
    def is_running(self) -> bool:
        """Check if services are running"""
        return self.get_status() == ServiceStatus.RUNNING
    
    def wait_for_completion(self, timeout: Optional[float] = None) -> bool:
        """Wait for all threads to complete"""
        start_time = time.time()
        
        while self.is_running():
            if timeout and (time.time() - start_time) > timeout:
                self.logger.warning("Timeout waiting for completion", timeout=timeout)
                return False
            
            time.sleep(0.1)
        
        return True
    
    def _load_transform_module(self, config: ETLConfig) -> None:
        """Load transform module from config"""
        try:
            # This would need to be implemented based on your config structure
            # For now, we'll assume the transform service is already configured
            self.logger.info("Transform module loaded")
        except Exception as e:
            self.logger.error("Failed to load transform module", error=str(e))
            raise
    
    def _start_message_bus_processing(self) -> None:
        """Start message bus processing thread"""
        with self._message_bus_thread_lock:
            if self._message_bus_thread and self._message_bus_thread.is_alive():
                return
            
            self._message_bus_thread = threading.Thread(
                target=self._message_bus_worker, 
                name="message_bus_worker"
            )
            self._message_bus_thread.daemon = True
            self._message_bus_thread.start()
            
            self.logger.info("Message bus processing thread started")
    
    def _stop_message_bus_processing(self) -> None:
        """Stop message bus processing thread"""
        with self._message_bus_thread_lock:
            if self._message_bus_thread and self._message_bus_thread.is_alive():
                # The thread will stop when shutdown is requested
                self._message_bus_thread.join(timeout=5.0)
                if self._message_bus_thread.is_alive():
                    self.logger.warning("Message bus thread did not stop gracefully")
            
            self.logger.info("Message bus processing thread stopped")
    
    def _message_bus_worker(self) -> None:
        """Message bus processing worker thread"""
        try:
            while not self._is_shutdown_requested():
                self.message_bus.process_messages(timeout=1.0)
        except Exception as e:
            self.logger.error("Error in message bus worker", error=str(e))
    
    def _start_source_threads(self, config: ETLConfig) -> None:
        """Start all source threads"""
        for source_name, source_config in config.sources.items():
            try:
                # Get tables for this source from pipeline configuration
                tables = config.get_tables_for_source(source_name)
                self.logger.info("Starting source thread", 
                               source_name=source_name, 
                               tables=[f"{schema}.{table}" for schema, table in tables])
                
                self.source_thread_service.start_source(
                    source_name, source_config, config.replication, tables
                )
                
            except Exception as e:
                self.logger.error("Failed to start source thread", 
                                source_name=source_name, error=str(e))
                raise
    
    def _start_target_threads(self, config: ETLConfig) -> None:
        """Start all target threads"""
        for target_name, target_config in config.targets.items():
            try:
                self.logger.info("Starting target thread", target_name=target_name)
                
                self.target_thread_service.start_target(
                    target_name, target_config, config
                )
                
            except Exception as e:
                self.logger.error("Failed to start target thread", 
                                target_name=target_name, error=str(e))
                raise
    
    def _start_monitoring(self) -> None:
        """Start monitoring thread"""
        with self._monitoring_thread_lock:
            if self._monitoring_thread and self._monitoring_thread.is_alive():
                return
            
            self._monitoring_thread = threading.Thread(
                target=self._monitoring_worker, 
                name="monitoring_worker"
            )
            self._monitoring_thread.daemon = True
            self._monitoring_thread.start()
            
            self.logger.info("Monitoring thread started")
    
    def _stop_monitoring(self) -> None:
        """Stop monitoring thread"""
        with self._monitoring_thread_lock:
            if self._monitoring_thread and self._monitoring_thread.is_alive():
                # The thread will stop when shutdown is requested
                self._monitoring_thread.join(timeout=5.0)
                if self._monitoring_thread.is_alive():
                    self.logger.warning("Monitoring thread did not stop gracefully")
            
            self.logger.info("Monitoring thread stopped")
    
    def _monitoring_worker(self) -> None:
        """Monitoring worker thread"""
        try:
            while not self._is_shutdown_requested():
                try:
                    # Log statistics
                    stats = self.get_stats()
                    self.logger.info("Service statistics", 
                                   status=stats.status.value,
                                   uptime=stats.uptime,
                                   sources_count=stats.sources_count,
                                   targets_count=stats.targets_count,
                                   total_events=stats.total_events_processed,
                                   total_errors=stats.total_errors,
                                   message_bus_queue_size=self.message_bus.get_queue_size())
                    
                    # Check for errors
                    if stats.total_errors > 0:
                        self.logger.warning("Errors detected in service", 
                                          total_errors=stats.total_errors)
                    
                    # Sleep for monitoring interval
                    time.sleep(self._monitoring_interval)
                    
                except Exception as e:
                    self.logger.error("Error in monitoring worker", error=str(e))
                    time.sleep(self._monitoring_interval)
                    
        except Exception as e:
            self.logger.error("Fatal error in monitoring worker", error=str(e))
    
    def _is_shutdown_requested(self) -> bool:
        """Check if shutdown is requested"""
        with self._shutdown_lock:
            return self._shutdown_requested

