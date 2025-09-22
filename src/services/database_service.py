"""
Database service for MySQL Replication ETL
"""

import pymysql
import pymysql.err
from typing import Dict, Any, Optional, Tuple
from contextlib import contextmanager

from ..exceptions import ConnectionError
from ..models.config import DatabaseConfig


class DatabaseService:
    """Service for database operations"""
    
    def __init__(self):
        self._connections: Dict[str, pymysql.Connection] = {}
        self._connection_configs: Dict[str, DatabaseConfig] = {}
        import structlog
        self.logger = structlog.get_logger()
    
    def connect(self, config: DatabaseConfig, connection_name: str = "default") -> pymysql.Connection:
        """Connect to database"""
        try:
            connection_params = config.to_connection_params()
            # Add connection timeout and other robustness parameters
            connection_params.update({
                'connect_timeout': 10,
                'read_timeout': 30,
                'write_timeout': 30,
                'autocommit': True,
                'charset': 'utf8mb4',
                'use_unicode': True,
                'sql_mode': 'TRADITIONAL',
                'init_command': "SET SESSION wait_timeout=28800, interactive_timeout=28800"
            })
            connection = pymysql.connect(**connection_params)
            self._connections[connection_name] = connection
            self._connection_configs[connection_name] = config
            return connection
        except Exception as e:
            raise ConnectionError(f"Failed to connect to database: {e}")
    
    def get_connection(self, connection_name: str = "default") -> pymysql.Connection:
        """Get existing connection"""
        if connection_name not in self._connections:
            raise ConnectionError(f"Connection '{connection_name}' not found")
        
        connection = self._connections[connection_name]
        if connection is None:
            # Clean up None connection
            del self._connections[connection_name]
            if connection_name in self._connection_configs:
                del self._connection_configs[connection_name]
            raise ConnectionError(f"Connection '{connection_name}' is None")
        
        # Check if connection is still valid
        if not self.is_connected(connection_name):
            self.logger.warning("Connection is no longer valid, removing from pool", 
                              connection_name=connection_name)
            del self._connections[connection_name]
            if connection_name in self._connection_configs:
                del self._connection_configs[connection_name]
            raise ConnectionError(f"Connection '{connection_name}' is no longer valid")
        
        return connection
    
    @contextmanager
    def get_cursor(self, connection_name: str = "default"):
        """Get database cursor with automatic cleanup"""
        connection = self.get_connection(connection_name)
        cursor = None
        try:
            cursor = connection.cursor()
            yield cursor
        except Exception as e:
            # If cursor creation or usage fails, ensure we clean up
            if cursor:
                try:
                    cursor.close()
                except Exception:
                    pass  # Ignore errors during cleanup
            raise
        finally:
            if cursor:
                try:
                    cursor.close()
                except Exception as e:
                    self.logger.debug("Error closing cursor (expected during cleanup)", 
                                    connection_name=connection_name, error=str(e))
    
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
        connection = None
        cursor = None
        
        try:
            # Create a direct connection without storing in connection pool
            connection_params = config.to_connection_params()
            # Add connection timeout and other robustness parameters
            connection_params.update({
                'connect_timeout': 10,
                'read_timeout': 30,
                'write_timeout': 30,
                'autocommit': True,
                'charset': 'utf8mb4',
                'use_unicode': True,
                'sql_mode': 'TRADITIONAL',
                'init_command': "SET SESSION wait_timeout=28800, interactive_timeout=28800"
            })
            connection = pymysql.connect(**connection_params)
            
            # Use the connection directly without validation checks
            cursor = connection.cursor()
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
            self.logger.error("Error getting master status", error=str(e))
            raise ConnectionError(f"Error getting master status: {e}")
        finally:
            # Ensure proper cleanup of direct connection
            if cursor:
                try:
                    cursor.close()
                except Exception as e:
                    self.logger.debug("Error closing cursor during master status cleanup", error=str(e))
            
            if connection:
                try:
                    connection.close()
                except Exception as e:
                    self.logger.debug("Error closing connection during master status cleanup", error=str(e))
    
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
    
    def get_connection_config(self, connection_name: str) -> DatabaseConfig:
        """Get connection configuration by name"""
        if connection_name not in self._connection_configs:
            raise ConnectionError(f"Connection config '{connection_name}' not found")
        return self._connection_configs[connection_name]
    
    def connection_exists(self, connection_name: str) -> bool:
        """Check if connection exists in the pool"""
        return connection_name in self._connections
    
    def get_connection_status(self, connection_name: str) -> Dict[str, Any]:
        """Get detailed connection status"""
        status = {
            'exists': connection_name in self._connections,
            'is_connected': False,
            'has_config': connection_name in self._connection_configs,
            'error': None
        }
        
        if status['exists']:
            try:
                status['is_connected'] = self.is_connected(connection_name)
            except Exception as e:
                status['error'] = str(e)
        
        return status
    
    def is_connected(self, connection_name: str) -> bool:
        """Check if connection is active"""
        if connection_name not in self._connections:
            return False
        
        connection = self._connections[connection_name]
        if connection is None:
            return False
            
        try:
            # Check if connection is still open
            if hasattr(connection, 'open') and not connection.open:
                return False
            # Check if connection has a valid socket
            if hasattr(connection, '_sock') and connection._sock is None:
                return False
            
            # Test the connection with a simple ping (with timeout)
            # Use a very short timeout to avoid hanging
            connection.ping(reconnect=False)
            return True
        except (pymysql.err.OperationalError, pymysql.err.InternalError, 
                pymysql.err.InterfaceError, pymysql.err.DatabaseError,
                ConnectionError, OSError, IOError, AttributeError, Exception) as e:
            # Log specific MySQL errors for debugging
            if isinstance(e, (pymysql.err.OperationalError, pymysql.err.InternalError)):
                self.logger.debug("MySQL connection error detected", 
                                connection_name=connection_name, 
                                error_type=type(e).__name__,
                                error_code=getattr(e, 'args', [None])[0] if e.args else None,
                                error_message=str(e))
            # Any exception means connection is not usable
            return False
    
    def reconnect_if_needed(self, connection_name: str) -> bool:
        """Reconnect if connection is lost"""
        if not self.is_connected(connection_name):
            if connection_name in self._connection_configs:
                config = self._connection_configs[connection_name]
                self.logger.info("Attempting to reconnect", connection_name=connection_name)
                try:
                    self.close_connection(connection_name)
                    self.connect(config, connection_name)
                    self.logger.info("Successfully reconnected", connection_name=connection_name)
                    return True
                except Exception as e:
                    self.logger.error("Failed to reconnect", connection_name=connection_name, error=str(e))
                    return False
            else:
                self.logger.error("No configuration found for reconnection", connection_name=connection_name)
                return False
        return True
    
    def close_connection(self, connection_name: str = "default") -> None:
        """Close database connection"""
        if connection_name not in self._connections:
            self.logger.debug("Connection not found for closing", connection_name=connection_name)
            return
            
        connection = self._connections[connection_name]
        
        try:
            # Check if connection is still valid before closing
            if hasattr(connection, 'open') and not connection.open:
                self.logger.debug("Connection already closed", connection_name=connection_name)
            else:
                # Check if connection has a valid socket before attempting to close
                if hasattr(connection, '_sock') and connection._sock is not None:
                    # Try to rollback any pending transactions first
                    if hasattr(connection, 'rollback'):
                        try:
                            # Check if connection is still valid before rollback
                            if hasattr(connection, 'ping'):
                                connection.ping(reconnect=False)
                            connection.rollback()
                        except (ConnectionError, OSError, IOError, AttributeError, pymysql.Error):
                            # These are expected when connection is already closed or invalid
                            pass
                        except Exception as e:
                            self.logger.debug("Error during rollback (expected during cleanup)", 
                                            connection_name=connection_name, error=str(e))
                    
                    # Close the connection
                    if hasattr(connection, 'close'):
                        connection.close()
                else:
                    self.logger.debug("Connection socket already closed", connection_name=connection_name)
                    
        except (ConnectionError, OSError, IOError, AttributeError, pymysql.Error) as e:
            # Connection errors during close are expected
            self.logger.debug("Connection error during close (expected)", 
                            connection_name=connection_name, error=str(e))
        except Exception as e:
            self.logger.warning("Unexpected error closing connection", 
                              connection_name=connection_name, error=str(e))
        finally:
            # Always remove from dictionaries, even if close failed
            try:
                if connection_name in self._connections:
                    del self._connections[connection_name]
                if connection_name in self._connection_configs:
                    del self._connection_configs[connection_name]
            except Exception as e:
                self.logger.debug("Error removing connection from dictionaries", 
                                connection_name=connection_name, error=str(e))
    
    def close_all_connections(self) -> None:
        """Close all database connections"""
        connection_names = list(self._connections.keys())
        for connection_name in connection_names:
            try:
                self.close_connection(connection_name)
            except Exception as e:
                # Логируем ошибку, но продолжаем закрывать остальные соединения
                import structlog
                logger = structlog.get_logger()
                logger.error("Error closing connection", connection_name=connection_name, error=str(e))
    
    def is_table_empty(self, table_name: str, connection_name: str = "default") -> bool:
        """Check if table is empty"""
        try:
            # Check if connection exists before trying to use it
            if connection_name not in self._connections:
                self.logger.warning("Connection not found, skipping table empty check", 
                                  connection_name=connection_name, table_name=table_name)
                return False
            
            with self.get_cursor(connection_name) as cursor:
                cursor.execute(f"SELECT COUNT(*) FROM `{table_name}`")
                result = cursor.fetchone()
                return result[0] == 0
        except Exception as e:
            # Log warning instead of raising exception to prevent cleanup failures
            import structlog
            logger = structlog.get_logger()
            logger.warning("Error checking if table is empty (continuing cleanup)", 
                         connection_name=connection_name, table_name=table_name, error=str(e))
            return False
    
    def execute_init_query(self, query: str, connection_name: str = "default") -> Tuple[list, list]:
        """Execute init query and return results with column names"""
        try:
            with self.get_cursor(connection_name) as cursor:
                cursor.execute(query)
                results = cursor.fetchall()
                columns = [desc[0] for desc in cursor.description] if cursor.description else []
                return results, columns
        except Exception as e:
            raise ConnectionError(f"Error executing init query: {e}")
    
    def execute_init_query_paginated(self, query: str, connection_name: str = "default", 
                                   page_size: int = 1000, offset: int = 0) -> Tuple[list, list, bool]:
        """Execute init query with pagination and return results with column names and has_more flag"""
        try:
            with self.get_cursor(connection_name) as cursor:
                # Add LIMIT and OFFSET to the query
                paginated_query = f"{query} LIMIT {page_size} OFFSET {offset}"
                cursor.execute(paginated_query)
                results = cursor.fetchall()
                columns = [desc[0] for desc in cursor.description] if cursor.description else []
                
                # Check if there are more results
                has_more = len(results) == page_size
                
                return results, columns, has_more
        except Exception as e:
            raise ConnectionError(f"Error executing paginated init query: {e}")
    
    def get_init_query_total_count(self, query: str, connection_name: str = "default") -> int:
        """Get total count of records that would be returned by init query"""
        try:
            # Convert SELECT query to COUNT query
            # Remove ORDER BY clause if present as it's not needed for count
            count_query = query.lower()
            if ' order by ' in count_query:
                count_query = count_query.split(' order by ')[0]
            
            # Replace SELECT ... FROM with SELECT COUNT(*) FROM
            if count_query.startswith('select '):
                # Find the FROM clause
                from_index = count_query.find(' from ')
                if from_index != -1:
                    count_query = f"SELECT COUNT(*) FROM {count_query[from_index + 6:]}"
                else:
                    # Fallback: wrap the entire query
                    count_query = f"SELECT COUNT(*) FROM ({query}) AS count_query"
            else:
                # Fallback: wrap the entire query
                count_query = f"SELECT COUNT(*) FROM ({query}) AS count_query"
            
            with self.get_cursor(connection_name) as cursor:
                cursor.execute(count_query)
                result = cursor.fetchone()
                return result[0] if result else 0
        except Exception as e:
            # If count query fails, return -1 to indicate unknown count
            import structlog
            logger = structlog.get_logger()
            logger.warning("Could not get count for init query, proceeding without count", 
                         query=query, error=str(e))
            return -1
    
    def get_table_columns(self, table_name: str, connection_name: str = "default") -> list:
        """Get table column names"""
        try:
            with self.get_cursor(connection_name) as cursor:
                cursor.execute(f"DESCRIBE `{table_name}`")
                result = cursor.fetchall()
                return [row[0] for row in result]
        except Exception as e:
            raise ConnectionError(f"Error getting columns for table '{table_name}': {e}")
    
    def __del__(self):
        """Cleanup on destruction"""
        self.close_all_connections()
