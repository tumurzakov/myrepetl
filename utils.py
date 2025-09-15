"""
ETL Utilities

Utility functions for ETL operations, monitoring, and configuration management.
"""

import json
import yaml
import logging
import time
from typing import Dict, List, Any, Optional
from pathlib import Path
from datetime import datetime, timedelta

from dsl import ETLPipeline, ReplicationSource, TargetTable, TransformationRule


class ETLConfigManager:
    """
    Manages ETL configuration files
    """
    
    def __init__(self, config_dir: str = "configs"):
        self.config_dir = Path(config_dir)
        self.config_dir.mkdir(exist_ok=True)
        self.logger = logging.getLogger("ETLConfigManager")
    
    def save_pipeline(self, pipeline: ETLPipeline, filename: Optional[str] = None) -> str:
        """Save pipeline configuration to file"""
        if filename is None:
            filename = f"{pipeline.name}_{datetime.now().strftime('%Y%m%d_%H%M%S')}.json"
        
        filepath = self.config_dir / filename
        
        with open(filepath, 'w', encoding='utf-8') as f:
            f.write(pipeline.to_json(indent=2))
        
        self.logger.info(f"Pipeline configuration saved to {filepath}")
        return str(filepath)
    
    def load_pipeline(self, filename: str) -> ETLPipeline:
        """Load pipeline configuration from file"""
        filepath = self.config_dir / filename
        
        with open(filepath, 'r', encoding='utf-8') as f:
            return ETLPipeline.from_json(f.read())
    
    def list_pipelines(self) -> List[str]:
        """List all available pipeline configurations"""
        return [f.name for f in self.config_dir.glob("*.json")]
    
    def save_pipeline_yaml(self, pipeline: ETLPipeline, filename: Optional[str] = None) -> str:
        """Save pipeline configuration to YAML file"""
        if filename is None:
            filename = f"{pipeline.name}_{datetime.now().strftime('%Y%m%d_%H%M%S')}.yaml"
        
        filepath = self.config_dir / filename
        
        with open(filepath, 'w', encoding='utf-8') as f:
            yaml.dump(pipeline.to_dict(), f, default_flow_style=False, indent=2)
        
        self.logger.info(f"Pipeline configuration saved to {filepath}")
        return str(filepath)
    
    def load_pipeline_yaml(self, filename: str) -> ETLPipeline:
        """Load pipeline configuration from YAML file"""
        filepath = self.config_dir / filename
        
        with open(filepath, 'r', encoding='utf-8') as f:
            data = yaml.safe_load(f)
            return ETLPipeline.from_dict(data)


class ETLMonitor:
    """
    Monitors ETL process performance and health
    """
    
    def __init__(self, etl_engine):
        self.etl_engine = etl_engine
        self.logger = logging.getLogger("ETLMonitor")
        self.start_time = time.time()
        self.last_stats = None
        self.alert_thresholds = {
            "error_rate": 0.05,  # 5% error rate
            "queue_size": 1000,  # Queue size threshold
            "processing_delay": 60  # Processing delay in seconds
        }
    
    def get_health_status(self) -> Dict[str, Any]:
        """Get overall health status of ETL process"""
        status = self.etl_engine.get_status()
        stats = status["stats"]
        
        # Calculate metrics
        total_events = stats["events_processed"] + stats["events_failed"]
        error_rate = stats["events_failed"] / total_events if total_events > 0 else 0
        queue_size = status["queue_size"]
        processing_delay = time.time() - stats["last_event_time"] if stats["last_event_time"] > 0 else 0
        
        # Determine health status
        health_status = "healthy"
        alerts = []
        
        if error_rate > self.alert_thresholds["error_rate"]:
            health_status = "warning"
            alerts.append(f"High error rate: {error_rate:.2%}")
        
        if queue_size > self.alert_thresholds["queue_size"]:
            health_status = "warning"
            alerts.append(f"Large queue size: {queue_size}")
        
        if processing_delay > self.alert_thresholds["processing_delay"]:
            health_status = "warning"
            alerts.append(f"Processing delay: {processing_delay:.1f}s")
        
        return {
            "status": health_status,
            "alerts": alerts,
            "metrics": {
                "error_rate": error_rate,
                "queue_size": queue_size,
                "processing_delay": processing_delay,
                "uptime": time.time() - self.start_time,
                "events_per_second": self._calculate_events_per_second(stats)
            },
            "stats": stats
        }
    
    def _calculate_events_per_second(self, stats: Dict[str, Any]) -> float:
        """Calculate events processed per second"""
        uptime = stats.get("uptime", 0)
        if uptime > 0:
            return stats["events_processed"] / uptime
        return 0.0
    
    def log_health_status(self):
        """Log current health status"""
        health = self.get_health_status()
        
        if health["status"] == "healthy":
            self.logger.info(f"ETL Health: {health['status'].upper()}")
        else:
            self.logger.warning(f"ETL Health: {health['status'].upper()} - {', '.join(health['alerts'])}")
        
        metrics = health["metrics"]
        self.logger.info(f"Metrics - Error Rate: {metrics['error_rate']:.2%}, "
                        f"Queue Size: {metrics['queue_size']}, "
                        f"Events/sec: {metrics['events_per_second']:.2f}")
    
    def set_alert_thresholds(self, **thresholds):
        """Set alert thresholds"""
        self.alert_thresholds.update(thresholds)
        self.logger.info(f"Updated alert thresholds: {self.alert_thresholds}")


class ETLValidator:
    """
    Validates ETL configuration and setup
    """
    
    def __init__(self):
        self.logger = logging.getLogger("ETLValidator")
    
    def validate_pipeline(self, pipeline: ETLPipeline) -> List[str]:
        """Validate ETL pipeline configuration"""
        errors = []
        
        # Validate pipeline name
        if not pipeline.name or not pipeline.name.strip():
            errors.append("Pipeline name is required")
        
        # Validate sources
        if not pipeline.sources:
            errors.append("At least one replication source is required")
        
        for source in pipeline.sources:
            errors.extend(self._validate_source(source))
        
        # Validate target tables
        if not pipeline.target_tables:
            errors.append("At least one target table is required")
        
        for table in pipeline.target_tables:
            errors.extend(self._validate_target_table(table))
        
        # Validate transformation rules
        for rule in pipeline.transformation_rules:
            errors.extend(self._validate_transformation_rule(rule, pipeline))
        
        return errors
    
    def _validate_source(self, source: ReplicationSource) -> List[str]:
        """Validate replication source"""
        errors = []
        
        if not source.name:
            errors.append("Source name is required")
        
        if not source.host:
            errors.append(f"Source '{source.name}': Host is required")
        
        if source.port <= 0 or source.port > 65535:
            errors.append(f"Source '{source.name}': Invalid port number")
        
        if not source.user:
            errors.append(f"Source '{source.name}': User is required")
        
        return errors
    
    def _validate_target_table(self, table: TargetTable) -> List[str]:
        """Validate target table"""
        errors = []
        
        if not table.name:
            errors.append("Target table name is required")
        
        if not table.database:
            errors.append(f"Table '{table.name}': Database is required")
        
        if not table.fields:
            errors.append(f"Table '{table.name}': At least one field is required")
        
        # Check for duplicate field names
        field_names = [field.name for field in table.fields]
        if len(field_names) != len(set(field_names)):
            errors.append(f"Table '{table.name}': Duplicate field names found")
        
        # Check for primary key
        primary_keys = [field for field in table.fields if field.primary_key]
        if not primary_keys:
            errors.append(f"Table '{table.name}': Primary key is required")
        
        return errors
    
    def _validate_transformation_rule(self, rule: TransformationRule, pipeline: ETLPipeline) -> List[str]:
        """Validate transformation rule"""
        errors = []
        
        if not rule.name:
            errors.append("Transformation rule name is required")
        
        if not rule.source_table:
            errors.append(f"Rule '{rule.name}': Source table is required")
        
        if not rule.target_table:
            errors.append(f"Rule '{rule.name}': Target table is required")
        
        # Check if source table exists in any source
        source_tables = []
        for source in pipeline.sources:
            source_tables.extend(source.tables)
        
        if rule.source_table not in source_tables:
            errors.append(f"Rule '{rule.name}': Source table '{rule.source_table}' not found in any source")
        
        # Check if target table exists
        target_table_names = [table.name for table in pipeline.target_tables]
        if rule.target_table not in target_table_names:
            errors.append(f"Rule '{rule.name}': Target table '{rule.target_table}' not found")
        
        return errors


class ETLBackup:
    """
    Handles backup and restore of ETL configurations and data
    """
    
    def __init__(self, backup_dir: str = "backups"):
        self.backup_dir = Path(backup_dir)
        self.backup_dir.mkdir(exist_ok=True)
        self.logger = logging.getLogger("ETLBackup")
    
    def create_backup(self, pipeline: ETLPipeline, include_timestamp: bool = True) -> str:
        """Create backup of pipeline configuration"""
        timestamp = datetime.now().strftime('%Y%m%d_%H%M%S') if include_timestamp else ""
        filename = f"{pipeline.name}_backup_{timestamp}.json"
        filepath = self.backup_dir / filename
        
        backup_data = {
            "pipeline": pipeline.to_dict(),
            "backup_timestamp": datetime.now().isoformat(),
            "version": "1.0.0"
        }
        
        with open(filepath, 'w', encoding='utf-8') as f:
            json.dump(backup_data, f, indent=2, default=str)
        
        self.logger.info(f"Backup created: {filepath}")
        return str(filepath)
    
    def restore_backup(self, backup_file: str) -> ETLPipeline:
        """Restore pipeline from backup"""
        filepath = self.backup_dir / backup_file
        
        with open(filepath, 'r', encoding='utf-8') as f:
            backup_data = json.load(f)
        
        pipeline = ETLPipeline.from_dict(backup_data["pipeline"])
        self.logger.info(f"Pipeline restored from backup: {filepath}")
        return pipeline
    
    def list_backups(self) -> List[Dict[str, Any]]:
        """List all available backups"""
        backups = []
        
        for backup_file in self.backup_dir.glob("*_backup_*.json"):
            try:
                with open(backup_file, 'r', encoding='utf-8') as f:
                    backup_data = json.load(f)
                
                backups.append({
                    "filename": backup_file.name,
                    "pipeline_name": backup_data["pipeline"]["name"],
                    "backup_timestamp": backup_data["backup_timestamp"],
                    "version": backup_data.get("version", "unknown")
                })
            except Exception as e:
                self.logger.error(f"Error reading backup file {backup_file}: {e}")
        
        return sorted(backups, key=lambda x: x["backup_timestamp"], reverse=True)


def setup_logging(level: str = "INFO", log_file: Optional[str] = None) -> None:
    """Setup logging configuration for ETL"""
    log_level = getattr(logging, level.upper(), logging.INFO)
    
    # Create formatter
    formatter = logging.Formatter(
        '%(asctime)s - %(name)s - %(levelname)s - %(message)s'
    )
    
    # Setup root logger
    root_logger = logging.getLogger()
    root_logger.setLevel(log_level)
    
    # Clear existing handlers
    for handler in root_logger.handlers[:]:
        root_logger.removeHandler(handler)
    
    # Console handler
    console_handler = logging.StreamHandler()
    console_handler.setLevel(log_level)
    console_handler.setFormatter(formatter)
    root_logger.addHandler(console_handler)
    
    # File handler (if specified)
    if log_file:
        file_handler = logging.FileHandler(log_file)
        file_handler.setLevel(log_level)
        file_handler.setFormatter(formatter)
        root_logger.addHandler(file_handler)
    
    logging.info(f"Logging configured - Level: {level}, File: {log_file or 'console only'}")


def format_bytes(bytes_value: int) -> str:
    """Format bytes value to human readable string"""
    for unit in ['B', 'KB', 'MB', 'GB', 'TB']:
        if bytes_value < 1024.0:
            return f"{bytes_value:.1f} {unit}"
        bytes_value /= 1024.0
    return f"{bytes_value:.1f} PB"


def format_duration(seconds: float) -> str:
    """Format duration in seconds to human readable string"""
    if seconds < 60:
        return f"{seconds:.1f}s"
    elif seconds < 3600:
        minutes = seconds / 60
        return f"{minutes:.1f}m"
    else:
        hours = seconds / 3600
        return f"{hours:.1f}h"
