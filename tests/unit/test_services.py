"""
Unit tests for services
"""

import pytest
from unittest.mock import Mock, patch, MagicMock
import json
import tempfile
import os

from src.services.config_service import ConfigService
from src.services.database_service import DatabaseService
from src.services.transform_service import TransformService
from src.services.replication_service import ReplicationService
from src.models.config import DatabaseConfig, ETLConfig
from src.exceptions import ConfigurationError, ConnectionError, TransformError, ReplicationError


class TestConfigService:
    """Test ConfigService"""
    
    def test_load_json_config(self):
        """Test loading JSON configuration"""
        config_data = {
            "sources": {
                "source1": {
                    "host": "source_host",
                    "port": 3306,
                    "user": "source_user",
                    "password": "source_password",
                    "database": "source_db"
                }
            },
            "targets": {
                "target1": {
                    "host": "target_host",
                    "port": 3306,
                    "user": "target_user",
                    "password": "target_password",
                    "database": "target_db"
                }
            },
            "replication": {
                "server_id": 100
            },
            "mapping": {
                "source1.users": {
                    "target_table": "target1.users",
                    "primary_key": "id",
                    "column_mapping": {
                        "id": {"column": "id", "primary_key": True},
                        "name": {"column": "name"}
                    }
                }
            }
        }
        
        with tempfile.NamedTemporaryFile(mode='w', suffix='.json', delete=False) as f:
            json.dump(config_data, f)
            config_path = f.name
        
        try:
            service = ConfigService()
            config = service.load_config(config_path)
            
            assert isinstance(config, ETLConfig)
            assert config.sources["source1"].host == "source_host"
            assert config.targets["target1"].host == "target_host"
            assert config.replication.server_id == 100
            assert "source1.users" in config.mapping
        finally:
            os.unlink(config_path)
    
    def test_load_yaml_config(self):
        """Test loading YAML configuration"""
        config_data = {
            "sources": {
                "source1": {
                    "host": "source_host",
                    "port": 3306,
                    "user": "source_user",
                    "password": "source_password",
                    "database": "source_db"
                }
            },
            "targets": {
                "target1": {
                    "host": "target_host",
                    "port": 3306,
                    "user": "target_user",
                    "password": "target_password",
                    "database": "target_db"
                }
            },
            "replication": {
                "server_id": 100
            },
            "mapping": {
                "source1.users": {
                    "target_table": "target1.users",
                    "primary_key": "id",
                    "column_mapping": {
                        "id": {"column": "id", "primary_key": True},
                        "name": {"column": "name"}
                    }
                }
            }
        }
        
        with tempfile.NamedTemporaryFile(mode='w', suffix='.yaml', delete=False) as f:
            import yaml
            yaml.dump(config_data, f)
            config_path = f.name
        
        try:
            service = ConfigService()
            config = service.load_config(config_path)
            
            assert isinstance(config, ETLConfig)
            assert config.sources["source1"].host == "source_host"
        finally:
            os.unlink(config_path)
    
    def test_config_file_not_found(self):
        """Test configuration file not found"""
        service = ConfigService()
        
        with pytest.raises(ConfigurationError, match="Configuration file not found"):
            service.load_config("nonexistent.json")
    
    def test_invalid_json_config(self):
        """Test invalid JSON configuration"""
        with tempfile.NamedTemporaryFile(mode='w', suffix='.json', delete=False) as f:
            f.write("invalid json content")
            config_path = f.name
        
        try:
            service = ConfigService()
            with pytest.raises(ConfigurationError, match="Invalid JSON configuration"):
                service.load_config(config_path)
        finally:
            os.unlink(config_path)
    
    def test_unsupported_format(self):
        """Test unsupported configuration format"""
        with tempfile.NamedTemporaryFile(mode='w', suffix='.txt', delete=False) as f:
            f.write("some content")
            config_path = f.name
        
        try:
            service = ConfigService()
            with pytest.raises(ConfigurationError, match="Unsupported configuration format"):
                service.load_config(config_path)
        finally:
            os.unlink(config_path)
    
    def test_validate_config(self):
        """Test configuration validation"""
        service = ConfigService()
        
        # Valid config
        valid_config = ETLConfig(
            sources={"source1": DatabaseConfig(host="localhost", user="user", password="pass", database="db")},
            targets={"target1": DatabaseConfig(host="localhost", user="user", password="pass", database="db")},
            replication=Mock(),
            mapping={
                "source1.table": Mock(
                    target_table="target1.table",
                    primary_key="id",
                    column_mapping={"id": Mock(column="id")}
                )
            }
        )
        
        assert service.validate_config(valid_config) is True
        
        # Invalid config - test with mock to avoid validation errors
        invalid_config = Mock()
        invalid_config.sources = {"source1": Mock()}
        invalid_config.sources["source1"].to_connection_params.return_value = {}  # Missing required keys
        invalid_config.targets = {"target1": Mock()}
        invalid_config.targets["target1"].to_connection_params.return_value = {}  # Missing required keys
        invalid_config.mapping = {}
        
        assert service.validate_config(invalid_config) is False


class TestDatabaseService:
    """Test DatabaseService"""
    
    def test_connect_success(self):
        """Test successful database connection"""
        with patch('pymysql.connect') as mock_connect:
            mock_connection = Mock()
            mock_connect.return_value = mock_connection
            
            service = DatabaseService()
            config = DatabaseConfig(
                host="localhost",
                user="user",
                password="pass",
                database="db"
            )
            
            connection = service.connect(config, "test")
            
            assert connection == mock_connection
            assert "test" in service._connections
            mock_connect.assert_called_once()
    
    def test_connect_failure(self):
        """Test database connection failure"""
        with patch('pymysql.connect') as mock_connect:
            mock_connect.side_effect = Exception("Connection failed")
            
            service = DatabaseService()
            config = DatabaseConfig(
                host="localhost",
                user="user",
                password="pass",
                database="db"
            )
            
            with pytest.raises(ConnectionError, match="Failed to connect to database"):
                service.connect(config, "test")
    
    def test_get_connection(self):
        """Test getting existing connection"""
        service = DatabaseService()
        mock_connection = Mock()
        service._connections["test"] = mock_connection
        
        connection = service.get_connection("test")
        assert connection == mock_connection
    
    def test_get_nonexistent_connection(self):
        """Test getting nonexistent connection"""
        service = DatabaseService()
        
        with pytest.raises(ConnectionError, match="Connection 'test' not found"):
            service.get_connection("test")
    
    def test_execute_query(self):
        """Test executing query"""
        with patch('pymysql.connect') as mock_connect:
            mock_connection = Mock()
            mock_cursor = Mock()
            mock_cursor.fetchall.return_value = [("result1",), ("result2",)]
            mock_connect.return_value = mock_connection
            
            service = DatabaseService()
            config = DatabaseConfig(
                host="localhost",
                user="user",
                password="pass",
                database="db"
            )
            
            service.connect(config, "test")
            
            # Mock the get_cursor context manager
            with patch.object(service, 'get_cursor') as mock_get_cursor:
                mock_get_cursor.return_value.__enter__.return_value = mock_cursor
                mock_get_cursor.return_value.__exit__.return_value = None
                
                result = service.execute_query("SELECT * FROM table", ("param1",), "test")
                
                assert result == [("result1",), ("result2",)]
                mock_cursor.execute.assert_called_once_with("SELECT * FROM table", ("param1",))
                mock_cursor.fetchall.assert_called_once()
    
    def test_execute_update(self):
        """Test executing update"""
        with patch('pymysql.connect') as mock_connect:
            mock_connection = Mock()
            mock_cursor = Mock()
            mock_cursor.rowcount = 5
            mock_connect.return_value = mock_connection
            
            service = DatabaseService()
            config = DatabaseConfig(
                host="localhost",
                user="user",
                password="pass",
                database="db"
            )
            
            service.connect(config, "test")
            
            # Mock the get_cursor context manager
            with patch.object(service, 'get_cursor') as mock_get_cursor:
                mock_get_cursor.return_value.__enter__.return_value = mock_cursor
                mock_get_cursor.return_value.__exit__.return_value = None
                
                result = service.execute_update("UPDATE table SET col = %s", ("value",), "test")
                
                assert result == 5
                mock_cursor.execute.assert_called_once_with("UPDATE table SET col = %s", ("value",))
                mock_connection.commit.assert_called_once()
    
    def test_get_master_status(self):
        """Test getting master status"""
        with patch('pymysql.connect') as mock_connect:
            mock_connection = Mock()
            mock_cursor = Mock()
            mock_cursor.fetchone.return_value = ("mysql-bin.000001", 1234, "db1", "db2", "gtid_set")
            mock_connect.return_value = mock_connection
            
            service = DatabaseService()
            config = DatabaseConfig(
                host="localhost",
                user="user",
                password="pass",
                database="db"
            )
            
            # Mock the get_cursor context manager
            with patch.object(service, 'get_cursor') as mock_get_cursor:
                mock_get_cursor.return_value.__enter__.return_value = mock_cursor
                mock_get_cursor.return_value.__exit__.return_value = None
                
                result = service.get_master_status(config)
                
                expected = {
                    'file': 'mysql-bin.000001',
                    'position': 1234,
                    'binlog_do_db': 'db1',
                    'binlog_ignore_db': 'db2',
                    'executed_gtid_set': 'gtid_set'
                }
                assert result == expected
    
    def test_test_connection_success(self):
        """Test successful connection test"""
        with patch('pymysql.connect') as mock_connect:
            mock_connection = Mock()
            mock_cursor = Mock()
            mock_cursor.fetchone.return_value = (1,)
            mock_connect.return_value = mock_connection
            
            service = DatabaseService()
            config = DatabaseConfig(
                host="localhost",
                user="user",
                password="pass",
                database="db"
            )
            
            # Mock the get_cursor context manager
            with patch.object(service, 'get_cursor') as mock_get_cursor:
                mock_get_cursor.return_value.__enter__.return_value = mock_cursor
                mock_get_cursor.return_value.__exit__.return_value = None
                
                result = service.test_connection(config)
                assert result is True
    
    def test_test_connection_failure(self):
        """Test failed connection test"""
        with patch('pymysql.connect') as mock_connect:
            mock_connect.side_effect = Exception("Connection failed")
            
            service = DatabaseService()
            config = DatabaseConfig(
                host="localhost",
                user="user",
                password="pass",
                database="db"
            )
            
            result = service.test_connection(config)
            assert result is False
    
    def test_close_connection(self):
        """Test closing connection"""
        service = DatabaseService()
        mock_connection = Mock()
        service._connections["test"] = mock_connection
        
        service.close_connection("test")
        
        assert "test" not in service._connections
        mock_connection.close.assert_called_once()
    
    def test_close_all_connections(self):
        """Test closing all connections"""
        service = DatabaseService()
        mock_connection1 = Mock()
        mock_connection2 = Mock()
        service._connections["test1"] = mock_connection1
        service._connections["test2"] = mock_connection2
        
        service.close_all_connections()
        
        assert len(service._connections) == 0
        mock_connection1.close.assert_called_once()
        mock_connection2.close.assert_called_once()


class TestTransformService:
    """Test TransformService"""
    
    def test_load_transform_module_success(self):
        """Test successful transform module loading"""
        with patch('importlib.import_module') as mock_import:
            mock_module = Mock()
            mock_import.return_value = mock_module
            
            service = TransformService()
            service.load_transform_module("test_module")
            
            assert service._transform_module == mock_module
            mock_import.assert_called_once_with("test_module")
    
    def test_load_transform_module_failure(self):
        """Test failed transform module loading"""
        with patch('importlib.import_module') as mock_import:
            mock_import.side_effect = ImportError("Module not found")
            
            service = TransformService()
            
            with pytest.raises(TransformError, match="Failed to import transform module"):
                service.load_transform_module("nonexistent_module")
    
    def test_get_transform_function_success(self):
        """Test successful transform function retrieval"""
        with patch('importlib.import_module') as mock_import:
            mock_module = Mock()
            mock_function = Mock()
            mock_module.uppercase = mock_function
            mock_import.return_value = mock_module
            
            service = TransformService()
            service.load_transform_module("transform")
            
            result = service.get_transform_function("transform.uppercase")
            
            assert result == mock_function
    
    def test_get_transform_function_invalid_path(self):
        """Test invalid transform function path"""
        with patch('importlib.import_module') as mock_import:
            mock_module = Mock()
            mock_import.return_value = mock_module
            
            service = TransformService()
            service.load_transform_module("transform")
            
            with pytest.raises(TransformError, match="Invalid transform path format"):
                service.get_transform_function("invalid.path")
    
    def test_get_transform_function_not_found(self):
        """Test transform function not found"""
        with patch('importlib.import_module') as mock_import:
            mock_module = Mock()
            del mock_module.uppercase  # Function doesn't exist
            mock_import.return_value = mock_module
            
            service = TransformService()
            service.load_transform_module("transform")
            
            with pytest.raises(TransformError, match="Transform function not found"):
                service.get_transform_function("transform.uppercase")
    
    def test_apply_column_transforms_static_value(self):
        """Test applying column transforms with static value"""
        service = TransformService()
        
        row_data = {"id": 1, "name": "test"}
        column_mapping = {
            "id": Mock(column="id", value=None, transform=None),
            "name": Mock(column="name", value="STATIC", transform=None)
        }
        
        result = service.apply_column_transforms(row_data, column_mapping)
        
        assert result == {"id": 1, "name": "STATIC"}
    
    def test_apply_column_transforms_with_transform(self):
        """Test applying column transforms with transformation"""
        with patch('importlib.import_module') as mock_import:
            mock_module = Mock()
            mock_function = Mock(return_value="TRANSFORMED")
            mock_module.uppercase = mock_function
            mock_import.return_value = mock_module
            
            service = TransformService()
            service.load_transform_module("transform")
            
            row_data = {"id": 1, "name": "test"}
            column_mapping = {
                "id": Mock(column="id", value=None, transform=None),
                "name": Mock(column="name", value=None, transform="transform.uppercase")
            }
            
            result = service.apply_column_transforms(row_data, column_mapping)
            
            assert result == {"id": 1, "name": "TRANSFORMED"}
            mock_function.assert_called_once_with("test")
    
    def test_apply_column_transforms_simple_copy(self):
        """Test applying column transforms with simple copy"""
        service = TransformService()
        
        row_data = {"id": 1, "name": "test"}
        column_mapping = {
            "id": Mock(column="id", value=None, transform=None),
            "name": Mock(column="name", value=None, transform=None)
        }
        
        result = service.apply_column_transforms(row_data, column_mapping)
        
        assert result == {"id": 1, "name": "test"}
    
    def test_register_transform(self):
        """Test registering custom transform"""
        service = TransformService()
        
        def custom_transform(x):
            return f"custom_{x}"
        
        service.register_transform("custom", custom_transform, "Custom transform")
        
        assert "custom" in service._transform_functions
        transform_func = service._transform_functions["custom"]
        assert transform_func.name == "custom"
        assert transform_func.function == custom_transform
        assert transform_func.description == "Custom transform"
    
    def test_get_registered_transforms(self):
        """Test getting registered transforms"""
        service = TransformService()
        
        def custom_transform(x):
            return f"custom_{x}"
        
        service.register_transform("custom", custom_transform)
        
        transforms = service.get_registered_transforms()
        assert "custom" in transforms
        assert "uppercase" in transforms  # Default transform
        assert "lowercase" in transforms  # Default transform


class TestReplicationService:
    """Test ReplicationService"""
    
    def test_connect_to_replication_success(self):
        """Test successful replication connection"""
        with patch('src.services.database_service.DatabaseService.get_master_status') as mock_status, \
             patch('src.services.replication_service.BinLogStreamReader') as mock_reader:
            
            mock_status.return_value = {
                'file': 'mysql-bin.000001',
                'position': 1234
            }
            
            mock_stream = Mock()
            mock_reader.return_value = mock_stream
            
            service = ReplicationService(Mock())
            source_config = DatabaseConfig(
                host="localhost",
                user="user",
                password="pass",
                database="db"
            )
            replication_config = Mock()
            
            result = service.connect_to_replication("source1", source_config, replication_config)
            
            # The result should be a BinLogStreamReader instance, not the mock
            assert result is not None
            mock_reader.assert_called_once()
    
    def test_connect_to_replication_failure(self):
        """Test failed replication connection"""
        mock_db_service = Mock()
        mock_db_service.get_master_status.side_effect = Exception("Master status failed")
        
        service = ReplicationService(mock_db_service)
        source_config = DatabaseConfig(
            host="localhost",
            user="user",
            password="pass",
            database="db"
        )
        replication_config = Mock()
        
        with pytest.raises(ReplicationError, match="Failed to connect to replication for source 'source1': Master status failed"):
            service.connect_to_replication("source1", source_config, replication_config)
    
    def test_get_events_not_connected(self):
        """Test getting events when not connected"""
        service = ReplicationService(Mock())
        
        with pytest.raises(ReplicationError, match="Replication stream for source 'source1' not connected"):
            list(service.get_events("source1"))
    
    def test_get_events_success(self):
        """Test successful event retrieval"""
        mock_stream = Mock()
        mock_event = Mock()
        mock_event.schema = "test_schema"
        mock_event.table = "test_table"
        mock_stream.__iter__ = Mock(return_value=iter([mock_event]))
        
        service = ReplicationService(Mock())
        service._streams = {"source1": mock_stream}
        
        events = list(service.get_events("source1"))
        assert len(events) == 1
        assert events[0].schema == "test_schema"
        assert events[0].table == "test_table"
    
    def test_close(self):
        """Test closing replication service"""
        mock_stream = Mock()
        service = ReplicationService(Mock())
        service._streams = {"source1": mock_stream}
        
        service.close()
        
        assert service._streams == {}
        mock_stream.close.assert_called_once()
