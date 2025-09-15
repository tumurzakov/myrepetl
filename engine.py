"""
ETL Engine

Main ETL engine that orchestrates replication, transformation, and loading processes.
"""

import logging
import threading
import time
from typing import Dict, List, Any, Optional, Callable
from dataclasses import dataclass
from queue import Queue, Empty
import pymysql
from contextlib import contextmanager

from dsl import (
    ETLPipeline,
    ReplicationSource,
    TargetTable,
    TransformationRule,
    OperationType,
    FieldMapping
)
from replication import MySQLReplicationClient, ReplicationEvent, ReplicationManager


@dataclass
class ETLStats:
    """ETL processing statistics"""
    events_processed: int = 0
    events_failed: int = 0
    rows_inserted: int = 0
    rows_updated: int = 0
    rows_deleted: int = 0
    start_time: float = 0
    last_event_time: float = 0

    def to_dict(self) -> Dict[str, Any]:
        return {
            "events_processed": self.events_processed,
            "events_failed": self.events_failed,
            "rows_inserted": self.rows_inserted,
            "rows_updated": self.rows_updated,
            "rows_deleted": self.rows_deleted,
            "start_time": self.start_time,
            "last_event_time": self.last_event_time,
            "uptime": time.time() - self.start_time if self.start_time > 0 else 0
        }


class TransformationEngine:
    """
    Handles data transformation according to transformation rules
    """

    def __init__(self):
        self.logger = logging.getLogger("TransformationEngine")
        self.rules: Dict[str, TransformationRule] = {}

    def add_rule(self, rule: TransformationRule):
        """Add transformation rule"""
        self.rules[rule.name] = rule
        self.logger.info(f"Added transformation rule: {rule.name}")

    def get_rule_for_table(self, source_table: str) -> Optional[TransformationRule]:
        """Get transformation rule for source table"""
        for rule in self.rules.values():
            if rule.source_table == source_table:
                return rule
        return None

    def transform_event(self, event: ReplicationEvent) -> Optional[Dict[str, Any]]:
        """Transform replication event according to rules"""
        rule = self.get_rule_for_table(event.table)
        if not rule:
            self.logger.warning(f"No transformation rule found for table: {event.table}")
            return None

        # Check if operation type is allowed
        if event.event_type not in rule.operations:
            self.logger.debug(f"Operation {event.event_type.value} not allowed for table {event.table}")
            return None

        transformed_data = {
            "event_type": event.event_type,
            "source_table": event.table,
            "target_table": rule.target_table,
            "transformed_rows": []
        }

        # Transform each row
        for row in event.rows:
            transformed_row = self._transform_row(row, rule)
            if transformed_row:
                transformed_data["transformed_rows"].append(transformed_row)

        return transformed_data

    def _transform_row(self, row: Dict[str, Any], rule: TransformationRule) -> Optional[Dict[str, Any]]:
        """Transform individual row"""
        transformed_row = {}

        # Apply field mappings
        for mapping in rule.field_mappings:
            source_value = row.get(mapping.source_field)

            # Apply transformation if provided
            if mapping.transformation and source_value is not None:
                try:
                    transformed_value = mapping.transformation(source_value)
                except Exception as e:
                    self.logger.error(f"Transformation error for field {mapping.source_field}: {e}")
                    transformed_value = source_value
            else:
                transformed_value = source_value

            transformed_row[mapping.target_field] = transformed_value

        # Apply filters
        if rule.filters:
            if not self._apply_filters(transformed_row, rule.filters):
                return None

        return transformed_row

    def _apply_filters(self, row: Dict[str, Any], filters: List[str]) -> bool:
        """Apply filter conditions to row"""
        for filter_condition in filters:
            try:
                # Simple filter evaluation (can be extended)
                if not self._evaluate_filter(row, filter_condition):
                    return False
            except Exception as e:
                self.logger.error(f"Filter evaluation error: {e}")
                return False
        return True

    def _evaluate_filter(self, row: Dict[str, Any], condition: str) -> bool:
        """Evaluate filter condition (simplified implementation)"""
        # This is a simplified filter evaluator
        # In production, you might want to use a more sophisticated expression evaluator
        try:
            # Replace field names with values
            for field, value in row.items():
                if isinstance(value, str):
                    condition = condition.replace(f"{{{field}}}", f"'{value}'")
                else:
                    condition = condition.replace(f"{{{field}}}", str(value))

            # Evaluate condition (basic implementation)
            return eval(condition)
        except:
            return True  # If filter evaluation fails, include the row


class TargetDatabase:
    """
    Manages connections to target databases
    """

    def __init__(self, host: str, port: int, user: str, password: str, database: str):
        self.host = host
        self.port = port
        self.user = user
        self.password = password
        self.database = database
        self.logger = logging.getLogger("TargetDatabase")

    @contextmanager
    def get_connection(self):
        """Get database connection context manager"""
        connection = None
        try:
            connection = pymysql.connect(
                host=self.host,
                port=self.port,
                user=self.user,
                password=self.password,
                database=self.database,
                charset='utf8mb4',
                autocommit=False
            )
            yield connection
        except Exception as e:
            if connection:
                connection.rollback()
            raise e
        finally:
            if connection:
                connection.close()

    def create_table(self, table: TargetTable):
        """Create target table"""
        with self.get_connection() as conn:
            cursor = conn.cursor()

            # Build CREATE TABLE statement
            fields_sql = []
            for field in table.fields:
                field_sql = f"`{field.name}` {self._get_field_type_sql(field)}"
                if not field.nullable:
                    field_sql += " NOT NULL"
                if field.default_value is not None:
                    field_sql += f" DEFAULT {self._format_default_value(field.default_value)}"
                if field.auto_increment:
                    field_sql += " AUTO_INCREMENT"
                fields_sql.append(field_sql)

            # Add primary key
            primary_keys = [f"`{field.name}`" for field in table.fields if field.primary_key]
            if primary_keys:
                fields_sql.append(f"PRIMARY KEY ({', '.join(primary_keys)})")

            # Add indexes
            for index in table.indexes:
                fields_sql.append(f"INDEX `{index}` (`{index}`)")

            create_sql = f"""
                CREATE TABLE IF NOT EXISTS `{table.database}`.`{table.name}` (
                    {', '.join(fields_sql)}
                ) ENGINE={table.engine} DEFAULT CHARSET={table.charset}
            """

            cursor.execute(create_sql)
            conn.commit()
            self.logger.info(f"Created table: {table.database}.{table.name}")

    def _get_field_type_sql(self, field) -> str:
        """Get SQL field type"""
        type_mapping = {
            "integer": "INT",
            "varchar": f"VARCHAR({field.length or 255})",
            "text": "TEXT",
            "datetime": "DATETIME",
            "decimal": f"DECIMAL({field.length or '10,2'})",
            "boolean": "BOOLEAN",
            "json": "JSON"
        }
        return type_mapping.get(field.field_type.value, "TEXT")

    def _format_default_value(self, value: Any) -> str:
        """Format default value for SQL"""
        if isinstance(value, str):
            return f"'{value}'"
        return str(value)


class ETLEngine:
    """
    Main ETL Engine that orchestrates the entire ETL process
    """

    def __init__(self, pipeline: ETLPipeline, target_db_config: Dict[str, Any]):
        self.pipeline = pipeline
        self.logger = logging.getLogger(f"ETLEngine.{pipeline.name}")

        # Initialize components
        self.replication_manager = ReplicationManager()
        self.transformation_engine = TransformationEngine()
        self.target_database = TargetDatabase(**target_db_config)

        # Statistics
        self.stats = ETLStats()

        # Processing queue
        self.processing_queue = Queue()
        self.is_running = False
        self._processing_thread: Optional[threading.Thread] = None

        # Setup pipeline
        self._setup_pipeline()

    def _setup_pipeline(self):
        """Setup ETL pipeline components"""
        # Add replication sources
        for source in self.pipeline.sources:
            client = self.replication_manager.add_source(source)
            client.add_event_handler(self._handle_replication_event)

        # Add transformation rules
        for rule in self.pipeline.transformation_rules:
            self.transformation_engine.add_rule(rule)

        # Create target tables
        for table in self.pipeline.target_tables:
            self.target_database.create_table(table)

    def _handle_replication_event(self, event: ReplicationEvent):
        """Handle replication event"""
        try:
            # Transform event
            transformed_data = self.transformation_engine.transform_event(event)
            if transformed_data:
                # Add to processing queue
                self.processing_queue.put(transformed_data)

            # Update statistics
            self.stats.events_processed += 1
            self.stats.last_event_time = time.time()

        except Exception as e:
            self.logger.error(f"Error handling replication event: {e}")
            self.stats.events_failed += 1

    def _process_transformed_data(self, data: Dict[str, Any]):
        """Process transformed data and load to target database"""
        try:
            with self.target_database.get_connection() as conn:
                cursor = conn.cursor()

                for row in data["transformed_rows"]:
                    if data["event_type"] == OperationType.INSERT:
                        self._insert_row(cursor, data["target_table"], row)
                        self.stats.rows_inserted += 1
                    elif data["event_type"] == OperationType.UPDATE:
                        self._update_row(cursor, data["target_table"], row)
                        self.stats.rows_updated += 1
                    elif data["event_type"] == OperationType.DELETE:
                        self._delete_row(cursor, data["target_table"], row)
                        self.stats.rows_deleted += 1

                conn.commit()

        except Exception as e:
            self.logger.error(f"Error processing transformed data: {e}")
            self.stats.events_failed += 1

    def _insert_row(self, cursor, table: str, row: Dict[str, Any]):
        """Insert row into target table"""
        fields = list(row.keys())
        values = list(row.values())
        placeholders = ', '.join(['%s'] * len(values))

        sql = f"INSERT INTO `{table}` (`{'`, `'.join(fields)}`) VALUES ({placeholders})"
        cursor.execute(sql, values)

    def _update_row(self, cursor, table: str, row: Dict[str, Any]):
        """Update row in target table"""
        if 'id' not in row:
            self.logger.warning(f"No id field for update in table {table}")
            return

        row_id = row.pop('id')
        fields = list(row.keys())
        values = list(row.values())

        set_clause = ', '.join([f"`{field}` = %s" for field in fields])
        sql = f"UPDATE `{table}` SET {set_clause} WHERE `id` = %s"
        cursor.execute(sql, values + [row_id])

    def _delete_row(self, cursor, table: str, row: Dict[str, Any]):
        """Delete row from target table"""
        if 'id' not in row:
            self.logger.warning(f"No id field for delete in table {table}")
            return

        sql = f"DELETE FROM `{table}` WHERE `id` = %s"
        cursor.execute(sql, [row['id']])

    def _processing_worker(self):
        """Process transformed data from queue"""
        while self.is_running:
            try:
                data = self.processing_queue.get(timeout=1.0)
                self._process_transformed_data(data)
                self.processing_queue.task_done()
            except Empty:
                continue
            except Exception as e:
                self.logger.error(f"Error in processing worker: {e}")

    def start(self):
        """Start ETL engine"""
        if self.is_running:
            self.logger.warning("ETL engine is already running")
            return

        self.logger.info(f"Starting ETL engine: {self.pipeline.name}")
        self.is_running = True
        self.stats.start_time = time.time()

        # Start processing worker
        self._processing_thread = threading.Thread(target=self._processing_worker, daemon=True)
        self._processing_thread.start()

        # Start replication manager
        self.replication_manager.start_all()

        self.logger.info("ETL engine started successfully")

    def stop(self):
        """Stop ETL engine"""
        if not self.is_running:
            return

        self.logger.info("Stopping ETL engine...")
        self.is_running = False

        # Stop replication manager
        self.replication_manager.stop_all()

        # Wait for processing to complete
        if self._processing_thread and self._processing_thread.is_alive():
            self._processing_thread.join(timeout=5.0)

        self.logger.info("ETL engine stopped")

    def get_status(self) -> Dict[str, Any]:
        """Get ETL engine status"""
        return {
            "pipeline": self.pipeline.to_dict(),
            "stats": self.stats.to_dict(),
            "replication": self.replication_manager.get_status(),
            "queue_size": self.processing_queue.qsize(),
            "is_running": self.is_running
        }
