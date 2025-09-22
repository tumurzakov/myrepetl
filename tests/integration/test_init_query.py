"""
Integration tests for init_query functionality
"""

import pytest
import json
import tempfile
import os
from unittest.mock import Mock, patch

from src.models.config import ETLConfig
from src.etl_service import ETLService


class TestInitQuery:
    """Test init_query functionality"""
    
    def test_init_query_config_loading(self):
        """Test that init_query is properly loaded from configuration"""
        config_dict = {
            "sources": {
                "source1": {
                    "host": "localhost",
                    "port": 3306,
                    "user": "test",
                    "password": "test",
                    "database": "test_db"
                }
            },
            "targets": {
                "target1": {
                    "host": "localhost",
                    "port": 3306,
                    "user": "test",
                    "password": "test",
                    "database": "test_db"
                }
            },
            "replication": {
                "server_id": 100
            },
            "mapping": {
                "source1.users": {
                    "init_query": "SELECT * FROM users WHERE status = 'active'",
                    "target_table": "users",
                    "target": "target1",
                    "primary_key": "id",
                    "column_mapping": {
                        "id": {"column": "id", "primary_key": True},
                        "name": {"column": "name"}
                    }
                },
                "source1.orders": {
                    "target_table": "target1.orders",
                    "primary_key": "id",
                    "column_mapping": {
                        "id": {"column": "id", "primary_key": True},
                        "amount": {"column": "amount"}
                    }
                }
            }
        }
        
        config = ETLConfig.from_dict(config_dict)
        
        # Check init_query for source1.users
        users_mapping = config.mapping['source1.users']
        assert users_mapping.init_query == "SELECT * FROM users WHERE status = 'active'"
        
        # Check init_query for source1.orders (should be None)
        orders_mapping = config.mapping['source1.orders']
        assert orders_mapping.init_query is None
    
    @patch('src.services.database_service.DatabaseService')
    def test_execute_init_queries_empty_table(self, mock_db_service):
        """Test init_query execution for empty table"""
        # Mock database service
        mock_db_service.is_table_empty.return_value = True
        mock_db_service.execute_init_query.return_value = (
            [(1, 'John', 'active'), (2, 'Jane', 'active')],
            ['id', 'name', 'status']
        )
        mock_db_service.execute_update.return_value = 1
        
        # Create test config
        config_dict = {
            "sources": {
                "source1": {
                    "host": "localhost",
                    "port": 3306,
                    "user": "test",
                    "password": "test",
                    "database": "test_db"
                }
            },
            "targets": {
                "target1": {
                    "host": "localhost",
                    "port": 3306,
                    "user": "test",
                    "password": "test",
                    "database": "test_db"
                }
            },
            "replication": {
                "server_id": 100
            },
            "mapping": {
                "source1.users": {
                    "init_query": "SELECT * FROM users WHERE status = 'active'",
                    "target_table": "users",
                    "target": "target1",
                    "primary_key": "id",
                    "column_mapping": {
                        "id": {"column": "id", "primary_key": True},
                        "name": {"column": "name"}
                    }
                }
            }
        }
        
        config = ETLConfig.from_dict(config_dict)
        
        # Create ETL service
        etl_service = ETLService()
        etl_service.config = config
        
        # Mock thread manager
        mock_thread_manager = Mock()
        mock_thread_manager.database_service = mock_db_service
        mock_thread_manager.transform_service = Mock()
        mock_thread_manager.filter_service = Mock()
        mock_thread_manager.filter_service.apply_filter.return_value = True
        mock_thread_manager.transform_service.apply_column_transforms.return_value = {
            'id': 1, 'name': 'John'
        }
        etl_service.thread_manager = mock_thread_manager
        
        # Execute init queries
        etl_service.execute_init_queries()
        
        # Verify calls
        mock_db_service.is_table_empty.assert_called_once_with('users', 'target1')
        mock_db_service.execute_init_query.assert_called_once_with(
            "SELECT * FROM users WHERE status = 'active'", 'init_source_source1'
        )
        assert mock_db_service.execute_update.call_count == 2  # Two rows processed
    
    @patch('src.services.database_service.DatabaseService')
    def test_execute_init_queries_non_empty_table(self, mock_db_service):
        """Test init_query is skipped for non-empty table"""
        # Mock database service
        mock_db_service.is_table_empty.return_value = False
        
        # Create test config
        config_dict = {
            "sources": {
                "source1": {
                    "host": "localhost",
                    "port": 3306,
                    "user": "test",
                    "password": "test",
                    "database": "test_db"
                }
            },
            "targets": {
                "target1": {
                    "host": "localhost",
                    "port": 3306,
                    "user": "test",
                    "password": "test",
                    "database": "test_db"
                }
            },
            "replication": {
                "server_id": 100
            },
            "mapping": {
                "source1.users": {
                    "init_query": "SELECT * FROM users WHERE status = 'active'",
                    "target_table": "users",
                    "target": "target1",
                    "primary_key": "id",
                    "column_mapping": {
                        "id": {"column": "id", "primary_key": True},
                        "name": {"column": "name"}
                    }
                }
            }
        }
        
        config = ETLConfig.from_dict(config_dict)
        
        # Create ETL service
        etl_service = ETLService()
        etl_service.config = config
        
        # Mock thread manager
        mock_thread_manager = Mock()
        mock_thread_manager.database_service = mock_db_service
        mock_thread_manager.transform_service = Mock()
        mock_thread_manager.filter_service = Mock()
        etl_service.thread_manager = mock_thread_manager
        
        # Execute init queries
        etl_service.execute_init_queries()
        
        # Verify calls
        mock_db_service.is_table_empty.assert_called_once_with('users', 'target1')
        mock_db_service.execute_init_query.assert_not_called()
        mock_db_service.execute_update.assert_not_called()
    
    @patch('src.services.database_service.DatabaseService')
    def test_execute_init_queries_with_filter(self, mock_db_service):
        """Test init_query execution with filter applied"""
        # Mock database service
        mock_db_service.is_table_empty.return_value = True
        mock_db_service.execute_init_query.return_value = (
            [(1, 'John', 'active'), (2, 'Jane', 'inactive')],
            ['id', 'name', 'status']
        )
        mock_db_service.execute_update.return_value = 1
        
        # Create test config with filter
        config_dict = {
            "sources": {
                "source1": {
                    "host": "localhost",
                    "port": 3306,
                    "user": "test",
                    "password": "test",
                    "database": "test_db"
                }
            },
            "targets": {
                "target1": {
                    "host": "localhost",
                    "port": 3306,
                    "user": "test",
                    "password": "test",
                    "database": "test_db"
                }
            },
            "replication": {
                "server_id": 100
            },
            "mapping": {
                "source1.users": {
                    "init_query": "SELECT * FROM users",
                    "target_table": "users",
                    "target": "target1",
                    "primary_key": "id",
                    "column_mapping": {
                        "id": {"column": "id", "primary_key": True},
                        "name": {"column": "name"},
                        "status": {"column": "status"}
                    },
                    "filter": {
                        "status": {"eq": "active"}
                    }
                }
            }
        }
        
        config = ETLConfig.from_dict(config_dict)
        
        # Create ETL service
        etl_service = ETLService()
        etl_service.config = config
        
        # Mock thread manager
        mock_thread_manager = Mock()
        mock_thread_manager.database_service = mock_db_service
        mock_thread_manager.transform_service = Mock()
        mock_thread_manager.filter_service = Mock()
        etl_service.thread_manager = mock_thread_manager
        
        # Mock filter service to filter out inactive users
        def mock_apply_filter(row, filter_config):
            return row.get('status') == 'active'
        
        mock_thread_manager.filter_service.apply_filter.side_effect = mock_apply_filter
        mock_thread_manager.transform_service.apply_column_transforms.return_value = {
            'id': 1, 'name': 'John', 'status': 'active'
        }
        
        # Execute init queries
        etl_service.execute_init_queries()
        
        # Verify calls
        mock_db_service.is_table_empty.assert_called_once_with('users', 'target1')
        mock_db_service.execute_init_query.assert_called_once_with(
            "SELECT * FROM users", 'init_source_source1'
        )
        # Only one row should be processed (active user)
        assert mock_db_service.execute_update.call_count == 1
