"""
Metrics service for Prometheus monitoring
Provides comprehensive metrics for ETL pipeline monitoring
"""

import time
from typing import Dict, Any, Optional
from prometheus_client import (
    Counter, Histogram, Gauge, Info, 
    CollectorRegistry, generate_latest, 
    CONTENT_TYPE_LATEST
)
import structlog


class MetricsService:
    """Service for managing Prometheus metrics"""
    
    def __init__(self, registry: Optional[CollectorRegistry] = None):
        self.logger = structlog.get_logger()
        self.registry = registry or CollectorRegistry()
        self.thread_manager = None  # Will be set later
        self.database_service = None  # Will be set later
        
        # Initialize all metrics
        self._init_metrics()
        
        self.logger.info("Metrics service initialized")
    
    def set_thread_manager(self, thread_manager) -> None:
        """Set thread manager reference for health checks"""
        self.thread_manager = thread_manager
    
    def set_database_service(self, database_service) -> None:
        """Set database service reference for health checks"""
        self.database_service = database_service
    
    def _init_metrics(self) -> None:
        """Initialize all Prometheus metrics"""
        
        # === INIT QUERY METRICS ===
        # Количество init записей отправлено из каждой таблицы source
        self.init_records_sent_total = Counter(
            'etl_init_records_sent_total',
            'Total number of init records sent from source tables',
            ['source_name', 'table_name', 'mapping_key'],
            registry=self.registry
        )
        
        # Размер батчей init запросов
        self.init_batch_size = Histogram(
            'etl_init_batch_size',
            'Size of init query batches',
            ['source_name', 'table_name'],
            buckets=[1, 5, 10, 25, 50, 100, 250, 500, 1000, 2500, 5000, 10000],
            registry=self.registry
        )
        
        # Время выполнения init запросов
        self.init_query_duration = Histogram(
            'etl_init_query_duration_seconds',
            'Time spent executing init queries',
            ['source_name', 'table_name', 'mapping_key'],
            buckets=[0.1, 0.5, 1.0, 2.5, 5.0, 10.0, 25.0, 50.0, 100.0, 250.0, 500.0],
            registry=self.registry
        )
        
        # Статус init потоков
        self.init_thread_status = Gauge(
            'etl_init_thread_status',
            'Status of init query threads (1=running, 0=stopped)',
            ['mapping_key', 'source_name', 'target_name'],
            registry=self.registry
        )
        
        # === QUEUE METRICS ===
        # Размер очереди сообщений
        self.message_queue_size = Gauge(
            'etl_message_queue_size',
            'Current size of message queue',
            registry=self.registry
        )
        
        # Максимальный размер очереди
        self.message_queue_max_size = Gauge(
            'etl_message_queue_max_size',
            'Maximum size of message queue',
            registry=self.registry
        )
        
        # Использование очереди в процентах
        self.message_queue_usage_percent = Gauge(
            'etl_message_queue_usage_percent',
            'Message queue usage percentage',
            registry=self.registry
        )
        
        # Переполнения очереди
        self.message_queue_overflows_total = Counter(
            'etl_message_queue_overflows_total',
            'Total number of message queue overflows',
            registry=self.registry
        )
        
        # === TARGET METRICS ===
        # Количество полученных записей в каждый из таргетов
        self.target_records_received_total = Counter(
            'etl_target_records_received_total',
            'Total number of records received by targets',
            ['target_name', 'event_type'],
            registry=self.registry
        )
        
        # Размер батчей и количество записей, записанных в каждую таблицу таргета
        self.target_batch_size = Histogram(
            'etl_target_batch_size',
            'Size of target batches',
            ['target_name', 'table_name'],
            buckets=[1, 5, 10, 25, 50, 100, 250, 500, 1000, 2500, 5000, 10000],
            registry=self.registry
        )
        
        self.target_records_written_total = Counter(
            'etl_target_records_written_total',
            'Total number of records written to target tables',
            ['target_name', 'table_name', 'operation_type'],
            registry=self.registry
        )
        
        # Время выполнения операций записи
        self.target_write_duration = Histogram(
            'etl_target_write_duration_seconds',
            'Time spent writing to target databases',
            ['target_name', 'table_name', 'operation_type'],
            buckets=[0.001, 0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1.0, 2.5, 5.0, 10.0],
            registry=self.registry
        )
        
        # Статус target потоков
        self.target_thread_status = Gauge(
            'etl_target_thread_status',
            'Status of target threads (1=running, 0=stopped)',
            ['target_name'],
            registry=self.registry
        )
        
        # Размер очереди target потоков
        self.target_queue_size = Gauge(
            'etl_target_queue_size',
            'Current size of target thread queue',
            ['target_name'],
            registry=self.registry
        )
        
        # === CONNECTION METRICS ===
        # Количество переподключений к БД
        self.database_reconnections_total = Counter(
            'etl_database_reconnections_total',
            'Total number of database reconnections',
            ['connection_name', 'connection_type'],
            registry=self.registry
        )
        
        # Статус активности соединения к источникам и приемникам
        self.database_connection_status = Gauge(
            'etl_database_connection_status',
            'Database connection status (1=connected, 0=disconnected)',
            ['connection_name', 'connection_type'],
            registry=self.registry
        )
        
        # Время последней активности соединения
        self.database_last_activity = Gauge(
            'etl_database_last_activity_timestamp',
            'Timestamp of last database activity',
            ['connection_name', 'connection_type'],
            registry=self.registry
        )
        
        # === REPLICATION METRICS ===
        # Статус активности соединения репликации
        self.replication_connection_status = Gauge(
            'etl_replication_connection_status',
            'Replication connection status (1=connected, 0=disconnected)',
            ['source_name'],
            registry=self.registry
        )
        
        # Количество записей полученных по репликации
        self.replication_records_received_total = Counter(
            'etl_replication_records_received_total',
            'Total number of records received via replication',
            ['source_name', 'event_type'],
            registry=self.registry
        )
        
        # Размер батчей и количество записей записанных по репликации
        self.replication_batch_size = Histogram(
            'etl_replication_batch_size',
            'Size of replication batches',
            ['source_name'],
            buckets=[1, 5, 10, 25, 50, 100, 250, 500, 1000, 2500, 5000, 10000],
            registry=self.registry
        )
        
        self.replication_records_processed_total = Counter(
            'etl_replication_records_processed_total',
            'Total number of replication records processed',
            ['source_name', 'event_type'],
            registry=self.registry
        )
        
        # Распределение событий репликации (update, insert, delete)
        self.replication_events_by_type_total = Counter(
            'etl_replication_events_by_type_total',
            'Total number of replication events by type',
            ['source_name', 'event_type'],
            registry=self.registry
        )
        
        # Время обработки событий репликации
        self.replication_event_duration = Histogram(
            'etl_replication_event_duration_seconds',
            'Time spent processing replication events',
            ['source_name', 'event_type'],
            buckets=[0.001, 0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1.0, 2.5, 5.0, 10.0],
            registry=self.registry
        )
        
        # === ERROR METRICS ===
        # Общее количество ошибок
        self.errors_total = Counter(
            'etl_errors_total',
            'Total number of errors',
            ['error_type', 'component', 'source_name', 'target_name'],
            registry=self.registry
        )
        
        # === PERFORMANCE METRICS ===
        # Пропускная способность (записей в секунду)
        self.throughput_records_per_second = Gauge(
            'etl_throughput_records_per_second',
            'Records processed per second',
            ['component', 'source_name', 'target_name'],
            registry=self.registry
        )
        
        # Задержка обработки (latency)
        self.processing_latency = Histogram(
            'etl_processing_latency_seconds',
            'Processing latency from source to target',
            ['source_name', 'target_name', 'table_name'],
            buckets=[0.001, 0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1.0, 2.5, 5.0, 10.0, 25.0, 50.0, 100.0],
            registry=self.registry
        )
        
        # === SYSTEM METRICS ===
        # Информация о системе
        self.system_info = Info(
            'etl_system_info',
            'ETL system information',
            registry=self.registry
        )
        
        # Время работы системы
        self.system_uptime = Gauge(
            'etl_system_uptime_seconds',
            'System uptime in seconds',
            registry=self.registry
        )
        
        # Количество активных потоков
        self.active_threads = Gauge(
            'etl_active_threads',
            'Number of active threads',
            ['thread_type'],
            registry=self.registry
        )
        
        self.logger.info("All Prometheus metrics initialized")
    
    def get_metrics(self) -> str:
        """Get metrics in Prometheus format"""
        return generate_latest(self.registry).decode('utf-8')
    
    def get_content_type(self) -> str:
        """Get content type for metrics endpoint"""
        return CONTENT_TYPE_LATEST
    
    # === INIT QUERY METRICS METHODS ===
    def record_init_record_sent(self, source_name: str, table_name: str, mapping_key: str) -> None:
        """Record init record sent"""
        self.init_records_sent_total.labels(
            source_name=source_name,
            table_name=table_name,
            mapping_key=mapping_key
        ).inc()
    
    def record_init_batch_size(self, source_name: str, table_name: str, batch_size: int) -> None:
        """Record init batch size"""
        self.init_batch_size.labels(
            source_name=source_name,
            table_name=table_name
        ).observe(batch_size)
    
    def record_init_query_duration(self, source_name: str, table_name: str, mapping_key: str, duration: float) -> None:
        """Record init query duration"""
        self.init_query_duration.labels(
            source_name=source_name,
            table_name=table_name,
            mapping_key=mapping_key
        ).observe(duration)
    
    def set_init_thread_status(self, mapping_key: str, source_name: str, target_name: str, is_running: bool) -> None:
        """Set init thread status"""
        self.init_thread_status.labels(
            mapping_key=mapping_key,
            source_name=source_name,
            target_name=target_name
        ).set(1 if is_running else 0)
    
    # === QUEUE METRICS METHODS ===
    def set_message_queue_size(self, size: int) -> None:
        """Set message queue size"""
        self.message_queue_size.set(size)
    
    def set_message_queue_max_size(self, max_size: int) -> None:
        """Set message queue max size"""
        self.message_queue_max_size.set(max_size)
    
    def set_message_queue_usage_percent(self, usage_percent: float) -> None:
        """Set message queue usage percentage"""
        self.message_queue_usage_percent.set(usage_percent)
    
    def record_message_queue_overflow(self) -> None:
        """Record message queue overflow"""
        self.message_queue_overflows_total.inc()
    
    # === TARGET METRICS METHODS ===
    def record_target_record_received(self, target_name: str, event_type: str) -> None:
        """Record target record received"""
        self.target_records_received_total.labels(
            target_name=target_name,
            event_type=event_type
        ).inc()
    
    def record_target_batch_size(self, target_name: str, table_name: str, batch_size: int) -> None:
        """Record target batch size"""
        self.target_batch_size.labels(
            target_name=target_name,
            table_name=table_name
        ).observe(batch_size)
    
    def record_target_record_written(self, target_name: str, table_name: str, operation_type: str) -> None:
        """Record target record written"""
        self.target_records_written_total.labels(
            target_name=target_name,
            table_name=table_name,
            operation_type=operation_type
        ).inc()
    
    def record_target_write_duration(self, target_name: str, table_name: str, operation_type: str, duration: float) -> None:
        """Record target write duration"""
        self.target_write_duration.labels(
            target_name=target_name,
            table_name=table_name,
            operation_type=operation_type
        ).observe(duration)
    
    def set_target_thread_status(self, target_name: str, is_running: bool) -> None:
        """Set target thread status"""
        self.target_thread_status.labels(target_name=target_name).set(1 if is_running else 0)
    
    def set_target_queue_size(self, target_name: str, size: int) -> None:
        """Set target queue size"""
        self.target_queue_size.labels(target_name=target_name).set(size)
    
    # === CONNECTION METRICS METHODS ===
    def record_database_reconnection(self, connection_name: str, connection_type: str) -> None:
        """Record database reconnection"""
        self.database_reconnections_total.labels(
            connection_name=connection_name,
            connection_type=connection_type
        ).inc()
    
    def set_database_connection_status(self, connection_name: str, connection_type: str, is_connected: bool) -> None:
        """Set database connection status"""
        self.database_connection_status.labels(
            connection_name=connection_name,
            connection_type=connection_type
        ).set(1 if is_connected else 0)
    
    def set_database_last_activity(self, connection_name: str, connection_type: str, timestamp: float) -> None:
        """Set database last activity timestamp"""
        self.database_last_activity.labels(
            connection_name=connection_name,
            connection_type=connection_type
        ).set(timestamp)
    
    # === REPLICATION METRICS METHODS ===
    def set_replication_connection_status(self, source_name: str, is_connected: bool) -> None:
        """Set replication connection status"""
        self.replication_connection_status.labels(source_name=source_name).set(1 if is_connected else 0)
    
    def record_replication_record_received(self, source_name: str, event_type: str) -> None:
        """Record replication record received"""
        self.replication_records_received_total.labels(
            source_name=source_name,
            event_type=event_type
        ).inc()
    
    def record_replication_batch_size(self, source_name: str, batch_size: int) -> None:
        """Record replication batch size"""
        self.replication_batch_size.labels(source_name=source_name).observe(batch_size)
    
    def record_replication_record_processed(self, source_name: str, event_type: str) -> None:
        """Record replication record processed"""
        self.replication_records_processed_total.labels(
            source_name=source_name,
            event_type=event_type
        ).inc()
    
    def record_replication_event_by_type(self, source_name: str, event_type: str) -> None:
        """Record replication event by type"""
        self.replication_events_by_type_total.labels(
            source_name=source_name,
            event_type=event_type
        ).inc()
    
    def record_replication_event_duration(self, source_name: str, event_type: str, duration: float) -> None:
        """Record replication event duration"""
        self.replication_event_duration.labels(
            source_name=source_name,
            event_type=event_type
        ).observe(duration)
    
    # === ERROR METRICS METHODS ===
    def record_error(self, error_type: str, component: str, source_name: str = "", target_name: str = "") -> None:
        """Record error"""
        self.errors_total.labels(
            error_type=error_type,
            component=component,
            source_name=source_name,
            target_name=target_name
        ).inc()
    
    # === PERFORMANCE METRICS METHODS ===
    def set_throughput(self, component: str, source_name: str, target_name: str, records_per_second: float) -> None:
        """Set throughput"""
        self.throughput_records_per_second.labels(
            component=component,
            source_name=source_name,
            target_name=target_name
        ).set(records_per_second)
    
    def record_processing_latency(self, source_name: str, target_name: str, table_name: str, latency: float) -> None:
        """Record processing latency"""
        self.processing_latency.labels(
            source_name=source_name,
            target_name=target_name,
            table_name=table_name
        ).observe(latency)
    
    # === SYSTEM METRICS METHODS ===
    def set_system_info(self, version: str, build_date: str, git_commit: str = "") -> None:
        """Set system information"""
        info_data = {
            'version': version,
            'build_date': build_date
        }
        if git_commit:
            info_data['git_commit'] = git_commit
        
        self.system_info.info(info_data)
    
    def set_system_uptime(self, uptime_seconds: float) -> None:
        """Set system uptime"""
        self.system_uptime.set(uptime_seconds)
    
    def set_active_threads(self, thread_type: str, count: int) -> None:
        """Set active threads count"""
        self.active_threads.labels(thread_type=thread_type).set(count)
    
    # === HEALTH CHECK METHODS ===
    def get_health_status(self) -> Dict[str, Any]:
        """Get comprehensive health status of the system"""
        health_status = {
            "status": "healthy",
            "timestamp": time.time(),
            "uptime_seconds": self.system_uptime._value._value if hasattr(self.system_uptime, '_value') else 0,
            "components": {}
        }
        
        # Check thread status
        health_status["components"]["threads"] = self._get_thread_health()
        
        # Check for any critical issues
        health_status["status"] = self._determine_overall_status(health_status["components"])
        
        return health_status
    
    def _get_thread_health(self) -> Dict[str, Any]:
        """Get thread health status"""
        thread_health = {
            "init_threads": {"status": "unknown", "count": 0, "details": [], "total": 0},
            "target_threads": {"status": "unknown", "count": 0, "details": [], "total": 0},
            "source_threads": {"status": "unknown", "count": 0, "details": [], "total": 0}
        }
        
        # Try to get thread status from ThreadManager first (more reliable)
        if self.thread_manager:
            try:
                stats = self.thread_manager.get_stats()
                
                # Get init thread status
                init_stats = stats.init_query_stats
                init_running = sum(1 for s in init_stats.values() if s.get('is_running', False))
                init_total = len(init_stats)
                
                thread_health["init_threads"]["count"] = init_running
                thread_health["init_threads"]["total"] = init_total
                thread_health["init_threads"]["status"] = "healthy" if init_running > 0 else "warning" if init_total > 0 else "unknown"
                
                for mapping_key, stat in init_stats.items():
                    # Determine status based on thread state
                    if stat.get('is_running', False):
                        status = 'running'
                    elif stat.get('is_completed', False):
                        status = 'completed'
                    elif stat.get('rows_processed', 0) > 0:
                        # Check completion reason to determine if thread can be resumed
                        completion_reason = stat.get('completion_reason')
                        if completion_reason in [None, 'queue_overflow', 'execution_error']:
                            status = 'incomplete'  # Can be resumed
                        else:
                            status = 'stopped'  # Stopped for other reasons
                    else:
                        status = 'unknown'
                    
                    thread_health["init_threads"]["details"].append({
                        "mapping": mapping_key,
                        "status": status,
                        "rows_processed": stat.get('rows_processed', 0),
                        "total_rows_estimated": stat.get('total_rows_estimated', -1),
                        "pages_processed": stat.get('pages_processed', 0),
                        "current_offset": stat.get('current_offset', 0),
                        "progress_percent": round((stat.get('rows_processed', 0) / max(stat.get('total_rows_estimated', 1), 1)) * 100, 2) if stat.get('total_rows_estimated', -1) > 0 else 0,
                        "errors_count": stat.get('errors_count', 0),
                        "queue_overflow_stops": stat.get('queue_overflow_stops', 0),
                        "completion_reason": stat.get('completion_reason'),
                        "completion_error": stat.get('completion_error'),
                        "last_activity_time": stat.get('last_activity_time')
                    })
                
                # Get target thread status
                target_stats = stats.target_stats
                target_running = sum(1 for s in target_stats.values() if s.get('is_running', False))
                target_total = len(target_stats)
                
                thread_health["target_threads"]["count"] = target_running
                thread_health["target_threads"]["total"] = target_total
                thread_health["target_threads"]["status"] = "healthy" if target_running > 0 else "warning" if target_total > 0 else "unknown"
                
                for target_name, stat in target_stats.items():
                    # Determine status based on thread state
                    if stat.get('is_running', False):
                        status = 'running'
                    elif stat.get('events_processed', 0) > 0 or stat.get('batch_records_processed', 0) > 0:
                        status = 'active'  # Has processed data but not currently running
                    else:
                        status = 'unknown'
                    
                    thread_health["target_threads"]["details"].append({
                        "target": target_name,
                        "status": status,
                        "events_processed": stat.get('events_processed', 0),
                        "records_saved": stat.get('batch_records_processed', 0) + stat.get('init_batch_records_processed', 0),
                        "batch_operations": stat.get('batch_operations', 0) + stat.get('init_batch_operations', 0),
                        "inserts_processed": stat.get('inserts_processed', 0),
                        "updates_processed": stat.get('updates_processed', 0),
                        "deletes_processed": stat.get('deletes_processed', 0),
                        "init_query_events": stat.get('init_query_events_processed', 0),
                        "errors_count": stat.get('errors_count', 0),
                        "queue_overflow_count": stat.get('queue_overflow_count', 0),
                        "queue_size": stat.get('queue_size', 0),
                        "queue_usage_percent": stat.get('queue_usage_percent', 0)
                    })
                
                # Get source thread status
                source_stats = stats.source_stats
                source_running = sum(1 for s in source_stats.values() if s.get('is_running', False))
                source_total = len(source_stats)
                
                thread_health["source_threads"]["count"] = source_running
                thread_health["source_threads"]["total"] = source_total
                thread_health["source_threads"]["status"] = "healthy" if source_running > 0 else "warning" if source_total > 0 else "unknown"
                
                for source_name, stat in source_stats.items():
                    # Determine status based on thread state
                    if stat.get('is_running', False):
                        status = 'running'
                    elif stat.get('events_processed', 0) > 0:
                        status = 'active'  # Has processed data but not currently running
                    else:
                        status = 'unknown'
                    
                    thread_health["source_threads"]["details"].append({
                        "source": source_name,
                        "status": status,
                        "events_processed": stat.get('events_processed', 0),
                        "errors_count": stat.get('errors_count', 0),
                        "last_event_time": stat.get('last_event_time')
                    })
                
                return thread_health
                
            except Exception as e:
                self.logger.warning("Failed to get thread status from ThreadManager", error=str(e))
        
        # Fallback to metrics if ThreadManager is not available
        try:
            # Get init thread status from metrics
            init_threads_running = 0
            init_threads_total = 0
            for sample in self.init_thread_status.collect()[0].samples:
                if sample.name.endswith('_value'):
                    init_threads_total += 1
                    if sample.value == 1:
                        init_threads_running += 1
                        thread_health["init_threads"]["details"].append({
                            "source": sample.labels.get("source_name", "unknown"),
                            "target": sample.labels.get("target_name", "unknown"),
                            "mapping": sample.labels.get("mapping_key", "unknown"),
                            "status": "running"
                        })
                    else:
                        thread_health["init_threads"]["details"].append({
                            "source": sample.labels.get("source_name", "unknown"),
                            "target": sample.labels.get("target_name", "unknown"),
                            "mapping": sample.labels.get("mapping_key", "unknown"),
                            "status": "stopped"
                        })
            
            thread_health["init_threads"]["count"] = init_threads_running
            thread_health["init_threads"]["total"] = init_threads_total
            thread_health["init_threads"]["status"] = "healthy" if init_threads_running > 0 else "warning"
        except Exception:
            thread_health["init_threads"]["status"] = "error"
        
        # Get target thread status from metrics
        try:
            target_threads_running = 0
            target_threads_total = 0
            for sample in self.target_thread_status.collect()[0].samples:
                if sample.name.endswith('_value'):
                    target_threads_total += 1
                    if sample.value == 1:
                        target_threads_running += 1
                        thread_health["target_threads"]["details"].append({
                            "target": sample.labels.get("target_name", "unknown"),
                            "status": "running"
                        })
                    else:
                        thread_health["target_threads"]["details"].append({
                            "target": sample.labels.get("target_name", "unknown"),
                            "status": "stopped"
                        })
            
            thread_health["target_threads"]["count"] = target_threads_running
            thread_health["target_threads"]["total"] = target_threads_total
            thread_health["target_threads"]["status"] = "healthy" if target_threads_running > 0 else "warning"
        except Exception:
            thread_health["target_threads"]["status"] = "error"
        
        # Get source thread status from metrics
        try:
            source_threads_running = 0
            source_threads_total = 0
            for sample in self.source_thread_status.collect()[0].samples:
                if sample.name.endswith('_value'):
                    source_threads_total += 1
                    if sample.value == 1:
                        source_threads_running += 1
                        thread_health["source_threads"]["details"].append({
                            "source": sample.labels.get("source_name", "unknown"),
                            "status": "running"
                        })
                    else:
                        thread_health["source_threads"]["details"].append({
                            "source": sample.labels.get("source_name", "unknown"),
                            "status": "stopped"
                        })
            
            thread_health["source_threads"]["count"] = source_threads_running
            thread_health["source_threads"]["total"] = source_threads_total
            thread_health["source_threads"]["status"] = "healthy" if source_threads_running > 0 else "warning"
        except Exception:
            thread_health["source_threads"]["status"] = "error"
        
        return thread_health
    
    def _determine_overall_status(self, components: Dict[str, Any]) -> str:
        """Determine overall system status based on component health"""
        statuses = []
        
        # Check thread status
        thread_health = components.get("threads", {})
        for thread_type in ["init_threads", "target_threads", "source_threads"]:
            status = thread_health.get(thread_type, {}).get("status", "unknown")
            statuses.append(status)
        
        # Determine overall status
        if "error" in statuses:
            return "unhealthy"
        elif "critical" in statuses:
            return "critical"
        elif "warning" in statuses:
            return "warning"
        else:
            return "healthy"
