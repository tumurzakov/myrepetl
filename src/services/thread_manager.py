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
from .init_query_thread_service import InitQueryThreadService
from .database_service import DatabaseService
from .transform_service import TransformService
from .filter_service import FilterService
from .metrics_service import MetricsService
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
    init_query_threads_count: int
    total_events_processed: int
    total_errors: int
    message_bus_stats: Dict[str, Any]
    source_stats: Dict[str, Dict[str, Any]]
    target_stats: Dict[str, Dict[str, Any]]
    init_query_stats: Dict[str, Dict[str, Any]]


class ThreadManager:
    """Manages all threads and their lifecycle"""
    
    def __init__(self, message_bus: MessageBus, database_service: DatabaseService, 
                 transform_service: TransformService, filter_service: FilterService,
                 metrics_service: Optional[MetricsService] = None):
        self.logger = structlog.get_logger()
        
        # Core services (injected dependencies)
        self.message_bus = message_bus
        self.database_service = database_service
        self.transform_service = transform_service
        self.filter_service = filter_service
        self.metrics_service = metrics_service
        
        # Thread services
        self.source_thread_service = SourceThreadService(self.message_bus, self.database_service, self.metrics_service)
        self.target_thread_service = TargetThreadService(
            self.message_bus, self.database_service, 
            self.transform_service, self.filter_service, self.metrics_service
        )
        self.init_query_thread_service = InitQueryThreadService(self.message_bus, self.database_service, self.metrics_service)
        
        # State management
        self._status = ServiceStatus.STOPPED
        self._status_lock = threading.RLock()
        self._start_time: Optional[float] = None
        self._shutdown_requested = False
        self._shutdown_lock = threading.Lock()
        
        # Configuration storage for health monitoring
        self._current_config: Optional[ETLConfig] = None
        self._config_lock = threading.RLock()
        
        # Message bus processing thread
        self._message_bus_thread: Optional[threading.Thread] = None
        self._message_bus_thread_lock = threading.Lock()
        
        # Monitoring thread
        self._monitoring_thread: Optional[threading.Thread] = None
        self._monitoring_thread_lock = threading.Lock()
        self._monitoring_interval = 30.0  # seconds
        self._init_check_interval = 10.0  # Check init threads every 10 seconds
        self._last_init_check_time = 0.0
        
        self.logger.info("Thread manager initialized")
    
    def start(self, config: ETLConfig, config_path: str = None) -> None:
        """Start all services and threads"""
        with self._status_lock:
            if self._status != ServiceStatus.STOPPED:
                self.logger.warning("Services already started or starting", status=self._status.value)
                return
            
            self._status = ServiceStatus.STARTING
            self._start_time = time.time()
        
        try:
            self.logger.info("Starting ETL services")
            
            # Store configuration for health monitoring
            with self._config_lock:
                self._current_config = config
            
            # Load transform module
            self._load_transform_module(config, config_path)
            
            # Start message bus processing
            self._start_message_bus_processing()
            
            # Start target threads first (they need to be ready to receive events)
            self._start_target_threads(config)
            
            # Start init query threads (they run in parallel with replication)
            self._start_init_query_threads(config)
            
            # Check if replication should be paused during init
            if config.replication.pause_replication_during_init:
                self.logger.info("Replication paused during init queries - will start after init completion")
                # Don't start source threads yet - they will be started after init queries complete
            else:
                # Start source threads immediately (original behavior)
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
            
            # Stop init query threads
            self.init_query_thread_service.stop_all_init_query_threads()
            
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
            try:
                self.source_thread_service.stop_all_sources()
            except Exception as e:
                self.logger.error("Error stopping source threads", error=str(e))
            
            # Stop init query threads
            try:
                self.init_query_thread_service.stop_all_init_query_threads()
            except Exception as e:
                self.logger.error("Error stopping init query threads", error=str(e))
            
            # Stop target threads
            try:
                self.target_thread_service.stop_all_targets()
            except Exception as e:
                self.logger.error("Error stopping target threads", error=str(e))
            
            # Publish shutdown message to wake up any waiting threads
            try:
                self.message_bus.publish_shutdown("thread_manager")
            except Exception as e:
                self.logger.error("Error publishing shutdown message", error=str(e))
            
            # Stop message bus processing
            try:
                self._stop_message_bus_processing()
            except Exception as e:
                self.logger.error("Error stopping message bus processing", error=str(e))
            
            # Stop monitoring
            try:
                self._stop_monitoring()
            except Exception as e:
                self.logger.error("Error stopping monitoring", error=str(e))
            
            # Close database connections
            try:
                self.database_service.close_all_connections()
            except Exception as e:
                self.logger.error("Error closing database connections", error=str(e))
            
            with self._status_lock:
                self._status = ServiceStatus.STOPPED
            
            # Clear configuration
            with self._config_lock:
                self._current_config = None
            
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
        init_query_stats = self.init_query_thread_service.get_all_stats()
        
        # Calculate totals
        total_events_processed = sum(
            stats.get('events_processed', 0) for stats in source_stats.values()
        ) + sum(
            stats.get('events_processed', 0) for stats in target_stats.values()
        ) + sum(
            stats.get('rows_processed', 0) for stats in init_query_stats.values()
        )
        
        total_errors = sum(
            stats.get('errors_count', 0) for stats in source_stats.values()
        ) + sum(
            stats.get('errors_count', 0) for stats in target_stats.values()
        ) + sum(
            stats.get('errors_count', 0) for stats in init_query_stats.values()
        )
        
        return ServiceStats(
            status=self.get_status(),
            uptime=uptime,
            sources_count=len(source_stats),
            targets_count=len(target_stats),
            init_query_threads_count=len(init_query_stats),
            total_events_processed=total_events_processed,
            total_errors=total_errors,
            message_bus_stats=self.message_bus.get_stats(),
            source_stats=source_stats,
            target_stats=target_stats,
            init_query_stats=init_query_stats
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
    
    def _load_transform_module(self, config: ETLConfig, config_path: str = None) -> None:
        """Load transform module from config"""
        try:
            # Get config directory from config path
            config_dir = None
            if config_path:
                import os
                config_dir = os.path.dirname(os.path.abspath(config_path))
                self.logger.debug("Config directory determined", config_dir=config_dir)
            
            # Load transform module
            self.transform_service.load_transform_module("transform", config_dir)
            self.logger.info("Transform module loaded successfully", config_dir=config_dir)
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
                # Small sleep to prevent busy loop when no messages
                time.sleep(0.01)
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
    
    def _start_init_query_threads(self, config: ETLConfig) -> None:
        """Start all init query threads"""
        try:
            self.logger.info("Starting init query threads")
            self.init_query_thread_service.start_init_query_threads(config)
            self.logger.info("All init query threads started successfully")
        except Exception as e:
            self.logger.error("Failed to start init query threads", error=str(e))
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
                    
                    # Check source thread health and restart if needed
                    self._check_source_thread_health()
                    
                    # Check target connection health
                    self._check_target_connection_health()
                    
                    # Check init query threads and resume if needed (more frequently)
                    current_time = time.time()
                    if current_time - self._last_init_check_time >= self._init_check_interval:
                        self._check_init_query_thread_health()
                        self._last_init_check_time = current_time
                    
                    # Check if replication should be started after init completion
                    self._check_replication_start_after_init()
                    
                    # Sleep for monitoring interval
                    time.sleep(self._monitoring_interval)
                    
                except Exception as e:
                    self.logger.error("Error in monitoring loop iteration", error=str(e))
                    time.sleep(self._monitoring_interval)
                    
        except Exception as e:
            self.logger.error("Fatal error in monitoring worker", error=str(e))
    
    def _check_init_query_thread_health(self) -> None:
        """Check init query thread health and resume if needed"""
        try:
            with self._config_lock:
                config = self._current_config
            
            if not config:
                return
            
            # Get incomplete threads
            incomplete_threads = self.init_query_thread_service.get_incomplete_threads()
            
            # Log detailed thread status for debugging
            all_stats = self.init_query_thread_service.get_all_stats()
            running_count = self.init_query_thread_service.get_active_threads_count()
            completed_count = self.init_query_thread_service.get_completed_threads_count()
            
            self.logger.info("Init query threads status", 
                            total_threads=len(all_stats),
                            running_count=running_count,
                            completed_count=completed_count,
                            incomplete_count=len(incomplete_threads),
                            incomplete_threads=incomplete_threads)
            
            if incomplete_threads:
                self.logger.info("Found incomplete init query threads, attempting to resume", 
                               incomplete_count=len(incomplete_threads),
                               incomplete_threads=incomplete_threads)
                
                # Check message bus queue size before resuming
                queue_size = self.message_bus.get_queue_size()
                queue_usage_percent = (queue_size / self.message_bus.max_queue_size) * 100
                
                # Resume threads if queue is not critically full
                # Use a higher threshold (80%) to be more aggressive about resuming threads
                if queue_usage_percent < 80:  # Resume if queue is less than 80% full
                    for mapping_key in incomplete_threads:
                        try:
                            success = self.init_query_thread_service.resume_init_query_thread(mapping_key, config)
                            if success:
                                self.logger.info("Successfully resumed init query thread", mapping_key=mapping_key)
                            else:
                                self.logger.warning("Failed to resume init query thread", mapping_key=mapping_key)
                        except Exception as e:
                            self.logger.error("Error resuming init query thread", 
                                            mapping_key=mapping_key, error=str(e))
                else:
                    self.logger.warning("Message bus queue too full to resume init query threads", 
                                      queue_usage_percent=queue_usage_percent,
                                      queue_size=queue_size,
                                      max_queue_size=self.message_bus.max_queue_size)
        
        except Exception as e:
            self.logger.error("Error checking init query thread health", error=str(e))
    
    def _check_source_thread_health(self) -> None:
        """Check source thread health and restart failed threads"""
        try:
            with self._config_lock:
                if not self._current_config:
                    return
            
                config = self._current_config
            
            # Get current source thread stats
            source_stats = self.source_thread_service.get_all_stats()
            
            for source_name, stats in source_stats.items():
                # Check if thread is not running but should be
                if not stats.get('is_running', False):
                    self.logger.warning("Source thread not running, attempting to restart", 
                                      source_name=source_name)
                    
                    try:
                        # Get source configuration
                        if source_name not in config.sources:
                            self.logger.error("Source configuration not found for restart", 
                                            source_name=source_name)
                            continue
                        
                        source_config = config.sources[source_name]
                        tables = config.get_tables_for_source(source_name)
                        
                        # Stop the failed thread if it still exists
                        self.source_thread_service.stop_source(source_name)
                        
                        # Wait a moment before restarting
                        time.sleep(2.0)
                        
                        # Restart the source thread
                        self.source_thread_service.start_source(
                            source_name, source_config, config.replication, tables
                        )
                        
                        self.logger.info("Source thread restarted successfully", 
                                       source_name=source_name)
                        
                    except Exception as e:
                        self.logger.error("Failed to restart source thread", 
                                        source_name=source_name, error=str(e))
                
                # Check for high error rates
                errors_count = stats.get('errors_count', 0)
                events_processed = stats.get('events_processed', 0)
                
                if events_processed > 0 and errors_count > 0:
                    error_rate = errors_count / events_processed
                    if error_rate > 0.1:  # More than 10% error rate
                        self.logger.warning("High error rate detected in source thread", 
                                          source_name=source_name, 
                                          error_rate=error_rate,
                                          errors_count=errors_count,
                                          events_processed=events_processed)
        
        except Exception as e:
            self.logger.error("Error checking source thread health", error=str(e))
    
    def _check_target_connection_health(self) -> None:
        """Check target connection health and manage source threads accordingly"""
        try:
            with self._config_lock:
                if not self._current_config:
                    return
            
            config = self._current_config
            
            # Check all target connections
            target_connections_healthy = True
            failed_targets = []
            
            for target_name, target_config in config.targets.items():
                try:
                    # Check connection status
                    status = self.database_service.get_connection_status(target_name)
                    
                    if not status['exists'] or not status['is_connected']:
                        self.logger.warning("Target connection unhealthy", 
                                          target_name=target_name,
                                          status=status)
                        target_connections_healthy = False
                        failed_targets.append(target_name)
                        
                        # Try to reconnect
                        try:
                            if self.database_service.reconnect_if_needed(target_name):
                                self.logger.info("Target connection restored", target_name=target_name)
                                target_connections_healthy = True
                                failed_targets.remove(target_name)
                            else:
                                self.logger.error("Failed to restore target connection", 
                                                target_name=target_name)
                        except Exception as e:
                            self.logger.error("Error reconnecting to target", 
                                            target_name=target_name, error=str(e))
                    
                except Exception as e:
                    self.logger.error("Error checking target connection health", 
                                    target_name=target_name, error=str(e))
                    target_connections_healthy = False
                    failed_targets.append(target_name)
            
            # If any target connections are unhealthy, pause source threads
            if not target_connections_healthy:
                self.logger.warning("Target connections unhealthy, pausing source threads", 
                                  failed_targets=failed_targets)
                self._pause_source_threads()
            else:
                # All target connections are healthy, resume source threads
                self._resume_source_threads()
        
        except Exception as e:
            self.logger.error("Error checking target connection health", error=str(e))
    
    def _pause_source_threads(self) -> None:
        """Pause source threads when target connections are unhealthy"""
        try:
            # Get all source threads
            source_stats = self.source_thread_service.get_all_stats()
            
            for source_name, stats in source_stats.items():
                if stats.get('is_running', False):
                    self.logger.info("Pausing source thread due to target connection issues", 
                                   source_name=source_name)
                    # Note: We don't actually stop the threads, just log the intention
                    # The target threads will handle connection failures gracefully
                    # and skip processing events when connections are unavailable
        
        except Exception as e:
            self.logger.error("Error pausing source threads", error=str(e))
    
    def _resume_source_threads(self) -> None:
        """Resume source threads when target connections are healthy"""
        try:
            # Get all source threads
            source_stats = self.source_thread_service.get_all_stats()
            
            for source_name, stats in source_stats.items():
                if stats.get('is_running', False):
                    self.logger.debug("Source thread is running normally", 
                                    source_name=source_name)
                    # Source threads are already running, no action needed
        
        except Exception as e:
            self.logger.error("Error resuming source threads", error=str(e))
    
    def _check_replication_start_after_init(self) -> None:
        """Check if replication should be started after init queries complete"""
        try:
            with self._config_lock:
                config = self._current_config
            
            if not config:
                return
            
            # Only check if pause_replication_during_init is enabled
            if not config.replication.pause_replication_during_init:
                return
            
            # Check if any source threads are already running
            source_stats = self.source_thread_service.get_all_stats()
            if source_stats:
                # Source threads are already running, no need to start them
                return
            
            # Check if all init query threads are completed by waiting with short timeout
            # This is more reliable than checking status flags
            all_completed = self.init_query_thread_service.wait_for_all_threads(timeout=0.1)
            
            self.logger.debug("Checking replication start condition",
                            pause_replication_during_init=config.replication.pause_replication_during_init,
                            source_threads_running=bool(source_stats),
                            all_init_completed=all_completed)
            
            if not all_completed:
                # Some init queries are still running, wait
                return
            
            # All init queries are completed, start source threads
            self.logger.info("All init queries completed, starting replication threads")
            
            try:
                self._start_source_threads(config)
                self.logger.info("Replication threads started successfully after init completion")
            except Exception as e:
                self.logger.error("Failed to start replication threads after init completion", error=str(e))
        
        except Exception as e:
            self.logger.error("Error checking replication start after init", error=str(e))
    
    def wait_for_init_completion(self, timeout: Optional[float] = None) -> bool:
        """Wait for all init query threads to complete
        
        Args:
            timeout: Maximum time to wait in seconds. If None, wait indefinitely.
            
        Returns:
            True if all init threads completed, False if timeout occurred
        """
        self.logger.info("Waiting for all init query threads to complete", timeout=timeout)
        return self.init_query_thread_service.wait_for_all_threads(timeout=timeout)
    
    def _is_shutdown_requested(self) -> bool:
        """Check if shutdown is requested"""
        with self._shutdown_lock:
            return self._shutdown_requested

