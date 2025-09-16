"""
Database service for MySQL Replication ETL
"""

import pymysql
from typing import Dict, Any, Optional, Tuple
from contextlib import contextmanager

from ..exceptions import ConnectionError
from ..models.config import DatabaseConfig


class DatabaseService:
    """Service for database operations"""
    
    def __init__(self):
        self._connections: Dict[str, pymysql.Connection] = {}
    
    def connect(self, config: DatabaseConfig, connection_name: str = "default") -> pymysql.Connection:
        """Connect to database"""
        try:
            connection_params = config.to_connection_params()
            connection = pymysql.connect(**connection_params)
            self._connections[connection_name] = connection
            return connection
        except Exception as e:
            raise ConnectionError(f"Failed to connect to database: {e}")
    
    def get_connection(self, connection_name: str = "default") -> pymysql.Connection:
        """Get existing connection"""
        if connection_name not in self._connections:
            raise ConnectionError(f"Connection '{connection_name}' not found")
        return self._connections[connection_name]
    
    @contextmanager
    def get_cursor(self, connection_name: str = "default"):
        """Get database cursor with automatic cleanup"""
        connection = self.get_connection(connection_name)
        cursor = connection.cursor()
        try:
            yield cursor
        finally:
            cursor.close()
    
    def execute_query(self, sql: str, values: Tuple = None, connection_name: str = "default") -> Any:
        """Execute query and return result"""
        with self.get_cursor(connection_name) as cursor:
            cursor.execute(sql, values)
            return cursor.fetchall()
    
    def execute_update(self, sql: str, values: Tuple = None, connection_name: str = "default") -> int:
        """Execute update query and return affected rows"""
        connection = self.get_connection(connection_name)
        with self.get_cursor(connection_name) as cursor:
            cursor.execute(sql, values)
            connection.commit()
            return cursor.rowcount
    
    def execute_batch(self, sql: str, values_list: list, connection_name: str = "default") -> int:
        """Execute batch update"""
        connection = self.get_connection(connection_name)
        with self.get_cursor(connection_name) as cursor:
            cursor.executemany(sql, values_list)
            connection.commit()
            return cursor.rowcount
    
    def get_master_status(self, config: DatabaseConfig) -> Dict[str, Any]:
        """Get MySQL master status"""
        try:
            connection = self.connect(config, "master_status")
            with self.get_cursor("master_status") as cursor:
                cursor.execute("SHOW MASTER STATUS")
                result = cursor.fetchone()
                
                if result:
                    return {
                        'file': result[0],
                        'position': result[1],
                        'binlog_do_db': result[2],
                        'binlog_ignore_db': result[3],
                        'executed_gtid_set': result[4] if len(result) > 4 else None
                    }
                else:
                    raise ConnectionError("Could not get master status")
        except Exception as e:
            raise ConnectionError(f"Error getting master status: {e}")
        finally:
            self.close_connection("master_status")
    
    def test_connection(self, config: DatabaseConfig) -> bool:
        """Test database connection"""
        try:
            connection = self.connect(config, "test")
            with self.get_cursor("test") as cursor:
                cursor.execute("SELECT 1")
                result = cursor.fetchone()
                return result[0] == 1
        except Exception:
            return False
        finally:
            self.close_connection("test")
    
    def close_connection(self, connection_name: str = "default") -> None:
        """Close database connection"""
        if connection_name in self._connections:
            try:
                self._connections[connection_name].close()
            except Exception:
                pass
            finally:
                del self._connections[connection_name]
    
    def close_all_connections(self) -> None:
        """Close all database connections"""
        for connection_name in list(self._connections.keys()):
            self.close_connection(connection_name)
    
    def __del__(self):
        """Cleanup on destruction"""
        self.close_all_connections()
