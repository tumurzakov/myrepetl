"""
Unit tests for data models
"""

import pytest
from src.models.config import DatabaseConfig, ReplicationConfig, ColumnMapping, TableMapping, ETLConfig
from src.models.events import BinlogEvent, InsertEvent, UpdateEvent, DeleteEvent, EventType
from src.models.transforms import TransformFunction, TransformResult, TransformStatus
from src.exceptions import ConfigurationError


class TestDatabaseConfig:
    """Test DatabaseConfig model"""
    
    def test_valid_config(self):
        """Test valid database configuration"""
        config = DatabaseConfig(
            host="localhost",
            port=3306,
            user="test_user",
            password="test_password",
            database="test_db"
        )
        
        assert config.host == "localhost"
        assert config.port == 3306
        assert config.user == "test_user"
        assert config.password == "test_password"
        assert config.database == "test_db"
        assert config.charset == "utf8mb4"
        assert config.autocommit is False
    
    def test_missing_host(self):
        """Test missing host validation"""
        with pytest.raises(ConfigurationError, match="Host is required"):
            DatabaseConfig(host="", user="test", password="test", database="test")
    
    def test_missing_user(self):
        """Test missing user validation"""
        with pytest.raises(ConfigurationError, match="User is required"):
            DatabaseConfig(host="localhost", user="", password="test", database="test")
    
    def test_missing_password(self):
        """Test missing password validation"""
        with pytest.raises(ConfigurationError, match="Password is required"):
            DatabaseConfig(host="localhost", user="test", password="", database="test")
    
    def test_missing_database(self):
        """Test missing database validation"""
        with pytest.raises(ConfigurationError, match="Database is required"):
            DatabaseConfig(host="localhost", user="test", password="test", database="")
    
    def test_invalid_port(self):
        """Test invalid port validation"""
        with pytest.raises(ConfigurationError, match="Port must be between 1 and 65535"):
            DatabaseConfig(host="localhost", port=0, user="test", password="test", database="test")
        
        with pytest.raises(ConfigurationError, match="Port must be between 1 and 65535"):
            DatabaseConfig(host="localhost", port=70000, user="test", password="test", database="test")
    
    def test_to_connection_params(self):
        """Test conversion to connection parameters"""
        config = DatabaseConfig(
            host="localhost",
            port=3306,
            user="test_user",
            password="test_password",
            database="test_db"
        )
        
        params = config.to_connection_params()
        expected = {
            'host': 'localhost',
            'port': 3306,
            'user': 'test_user',
            'password': 'test_password',
            'database': 'test_db',
            'charset': 'utf8mb4',
            'autocommit': False
        }
        
        assert params == expected


class TestReplicationConfig:
    """Test ReplicationConfig model"""
    
    def test_default_config(self):
        """Test default replication configuration"""
        config = ReplicationConfig()
        
        assert config.server_id == 100
        assert config.log_file is None
        assert config.log_pos == 4
        assert config.resume_stream is True
        assert config.blocking is True
        assert config.only_events is None
    
    def test_custom_config(self):
        """Test custom replication configuration"""
        config = ReplicationConfig(
            server_id=200,
            log_file="mysql-bin.000001",
            log_pos=1000,
            resume_stream=False,
            blocking=False
        )
        
        assert config.server_id == 200
        assert config.log_file == "mysql-bin.000001"
        assert config.log_pos == 1000
        assert config.resume_stream is False
        assert config.blocking is False
    
    def test_invalid_server_id(self):
        """Test invalid server ID validation"""
        with pytest.raises(ConfigurationError, match="Server ID must be positive"):
            ReplicationConfig(server_id=0)
        
        with pytest.raises(ConfigurationError, match="Server ID must be positive"):
            ReplicationConfig(server_id=-1)


class TestColumnMapping:
    """Test ColumnMapping model"""
    
    def test_simple_mapping(self):
        """Test simple column mapping"""
        mapping = ColumnMapping(column="target_col")
        
        assert mapping.column == "target_col"
        assert mapping.primary_key is False
        assert mapping.transform is None
        assert mapping.value is None
    
    def test_mapping_with_transform(self):
        """Test column mapping with transform"""
        mapping = ColumnMapping(
            column="target_col",
            transform="transform.uppercase"
        )
        
        assert mapping.column == "target_col"
        assert mapping.transform == "transform.uppercase"
        assert mapping.value is None
    
    def test_mapping_with_value(self):
        """Test column mapping with static value"""
        mapping = ColumnMapping(
            column="target_col",
            value="static_value"
        )
        
        assert mapping.column == "target_col"
        assert mapping.value == "static_value"
        assert mapping.transform is None
    
    def test_mapping_with_primary_key(self):
        """Test column mapping with primary key"""
        mapping = ColumnMapping(
            column="id",
            primary_key=True
        )
        
        assert mapping.column == "id"
        assert mapping.primary_key is True
    
    def test_conflicting_transform_and_value(self):
        """Test conflicting transform and value"""
        with pytest.raises(ConfigurationError, match="Cannot specify both transform and static value"):
            ColumnMapping(
                column="target_col",
                transform="transform.uppercase",
                value="static_value"
            )
    
    def test_missing_column(self):
        """Test missing column name"""
        with pytest.raises(ConfigurationError, match="Column name is required"):
            ColumnMapping(column="")


class TestTableMapping:
    """Test TableMapping model"""
    
    def test_valid_mapping(self):
        """Test valid table mapping"""
        column_mapping = {
            "id": ColumnMapping(column="id", primary_key=True),
            "name": ColumnMapping(column="name"),
            "email": ColumnMapping(column="email")
        }
        
        mapping = TableMapping(
            target_table="target_table",
            primary_key="id",
            column_mapping=column_mapping
        )
        
        assert mapping.target_table == "target_table"
        assert mapping.primary_key == "id"
        assert len(mapping.column_mapping) == 3
    
    def test_missing_target_table(self):
        """Test missing target table"""
        with pytest.raises(ConfigurationError, match="Target table is required"):
            TableMapping(
                target_table="",
                primary_key="id",
                column_mapping={}
            )
    
    def test_missing_primary_key(self):
        """Test missing primary key"""
        with pytest.raises(ConfigurationError, match="Primary key is required"):
            TableMapping(
                target_table="target_table",
                primary_key="",
                column_mapping={}
            )
    
    def test_empty_column_mapping(self):
        """Test empty column mapping"""
        with pytest.raises(ConfigurationError, match="Column mapping is required"):
            TableMapping(
                target_table="target_table",
                primary_key="id",
                column_mapping={}
            )


class TestETLConfig:
    """Test ETLConfig model"""
    
    def test_from_dict(self):
        """Test creating ETLConfig from dictionary"""
        config_dict = {
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
                        "name": {"column": "name", "transform": "transform.uppercase"},
                        "email": {"column": "email"}
                    }
                }
            }
        }
        
        config = ETLConfig.from_dict(config_dict)
        
        assert config.sources["source1"].host == "source_host"
        assert config.targets["target1"].host == "target_host"
        assert config.replication.server_id == 100
        assert "source1.users" in config.mapping
    
    def test_from_dict_missing_required(self):
        """Test missing required fields in dictionary"""
        config_dict = {
            "sources": {
                "source1": {
                    "host": "source_host",
                    "user": "source_user",
                    "password": "source_password",
                    "database": "source_db"
                }
            }
            # Missing targets
        }
        
        with pytest.raises(ConfigurationError, match="At least one target is required"):
            ETLConfig.from_dict(config_dict)
    
    def test_from_dict_invalid_format(self):
        """Test invalid format in dictionary"""
        config_dict = {
            "sources": "invalid_format",  # Should be dict
            "targets": {
                "target1": {
                    "host": "target_host",
                    "user": "target_user",
                    "password": "target_password",
                    "database": "target_db"
                }
            },
            "mapping": {}
        }
        
        with pytest.raises(ConfigurationError, match="Sources must be a dictionary"):
            ETLConfig.from_dict(config_dict)


class TestBinlogEvent:
    """Test BinlogEvent model"""
    
    def test_valid_event(self):
        """Test valid binlog event"""
        event = BinlogEvent(
            schema="test_schema",
            table="test_table",
            event_type=EventType.INSERT
        )
        
        assert event.schema == "test_schema"
        assert event.table == "test_table"
        assert event.event_type == EventType.INSERT
        assert event.timestamp is None
    
    def test_missing_schema(self):
        """Test missing schema validation"""
        with pytest.raises(ValueError, match="Schema is required"):
            BinlogEvent(schema="", table="test_table", event_type=EventType.INSERT)
    
    def test_missing_table(self):
        """Test missing table validation"""
        with pytest.raises(ValueError, match="Table is required"):
            BinlogEvent(schema="test_schema", table="", event_type=EventType.INSERT)


class TestInsertEvent:
    """Test InsertEvent model"""
    
    def test_valid_insert_event(self):
        """Test valid insert event"""
        event = InsertEvent(
            schema="test_schema",
            table="test_table",
            values={"id": 1, "name": "test"}
        )
        
        assert event.schema == "test_schema"
        assert event.table == "test_table"
        assert event.event_type == EventType.INSERT
        assert event.values == {"id": 1, "name": "test"}
    
    def test_empty_values(self):
        """Test empty values validation"""
        with pytest.raises(ValueError, match="Values are required for INSERT event"):
            InsertEvent(
                schema="test_schema",
                table="test_table",
                values={}
            )


class TestUpdateEvent:
    """Test UpdateEvent model"""
    
    def test_valid_update_event(self):
        """Test valid update event"""
        event = UpdateEvent(
            schema="test_schema",
            table="test_table",
            before_values={"id": 1, "name": "old"},
            after_values={"id": 1, "name": "new"}
        )
        
        assert event.schema == "test_schema"
        assert event.table == "test_table"
        assert event.event_type == EventType.UPDATE
        assert event.before_values == {"id": 1, "name": "old"}
        assert event.after_values == {"id": 1, "name": "new"}
    
    def test_empty_before_values(self):
        """Test empty before values validation"""
        with pytest.raises(ValueError, match="Before values are required for UPDATE event"):
            UpdateEvent(
                schema="test_schema",
                table="test_table",
                before_values={},
                after_values={"id": 1, "name": "new"}
            )
    
    def test_empty_after_values(self):
        """Test empty after values validation"""
        with pytest.raises(ValueError, match="After values are required for UPDATE event"):
            UpdateEvent(
                schema="test_schema",
                table="test_table",
                before_values={"id": 1, "name": "old"},
                after_values={}
            )


class TestDeleteEvent:
    """Test DeleteEvent model"""
    
    def test_valid_delete_event(self):
        """Test valid delete event"""
        event = DeleteEvent(
            schema="test_schema",
            table="test_table",
            values={"id": 1, "name": "test"}
        )
        
        assert event.schema == "test_schema"
        assert event.table == "test_table"
        assert event.event_type == EventType.DELETE
        assert event.values == {"id": 1, "name": "test"}
    
    def test_empty_values(self):
        """Test empty values validation"""
        with pytest.raises(ValueError, match="Values are required for DELETE event"):
            DeleteEvent(
                schema="test_schema",
                table="test_table",
                values={}
            )


class TestTransformFunction:
    """Test TransformFunction model"""
    
    def test_valid_transform_function(self):
        """Test valid transform function"""
        def test_func(x):
            return x.upper() if isinstance(x, str) else x
        
        transform = TransformFunction(
            name="test_transform",
            function=test_func,
            description="Test transform"
        )
        
        assert transform.name == "test_transform"
        assert transform.function == test_func
        assert transform.description == "Test transform"
    
    def test_missing_name(self):
        """Test missing name validation"""
        with pytest.raises(ValueError, match="Transform name is required"):
            TransformFunction(name="", function=lambda x: x)
    
    def test_invalid_function(self):
        """Test invalid function validation"""
        with pytest.raises(ValueError, match="Transform must be callable"):
            TransformFunction(name="test", function="not_callable")
    
    def test_execute_success(self):
        """Test successful transform execution"""
        def test_func(x):
            return x.upper() if isinstance(x, str) else x
        
        transform = TransformFunction(name="test", function=test_func)
        result = transform.execute("hello")
        
        assert result.status == TransformStatus.SUCCESS
        assert result.original_value == "hello"
        assert result.transformed_value == "HELLO"
        assert result.error is None
    
    def test_execute_with_none(self):
        """Test transform execution with None value"""
        def test_func(x):
            return x.upper() if isinstance(x, str) else x
        
        transform = TransformFunction(name="test", function=test_func)
        result = transform.execute(None)
        
        assert result.status == TransformStatus.SKIPPED
        assert result.original_value is None
        assert result.transformed_value is None
        assert result.error is None
    
    def test_execute_with_error(self):
        """Test transform execution with error"""
        def test_func(x):
            raise ValueError("Test error")
        
        transform = TransformFunction(name="test", function=test_func)
        result = transform.execute("test")
        
        assert result.status == TransformStatus.ERROR
        assert result.original_value == "test"
        assert result.transformed_value == "test"
        assert "Test error" in result.error


class TestTransformResult:
    """Test TransformResult model"""
    
    def test_success_result(self):
        """Test success result"""
        result = TransformResult(
            status=TransformStatus.SUCCESS,
            original_value="hello",
            transformed_value="HELLO"
        )
        
        assert result.is_success is True
        assert result.is_error is False
        assert result.is_skipped is False
        assert result.original_value == "hello"
        assert result.transformed_value == "HELLO"
        assert result.error is None
    
    def test_error_result(self):
        """Test error result"""
        result = TransformResult(
            status=TransformStatus.ERROR,
            original_value="hello",
            transformed_value="hello",
            error="Test error"
        )
        
        assert result.is_success is False
        assert result.is_error is True
        assert result.is_skipped is False
        assert result.error == "Test error"
    
    def test_skipped_result(self):
        """Test skipped result"""
        result = TransformResult(
            status=TransformStatus.SKIPPED,
            original_value=None,
            transformed_value=None
        )
        
        assert result.is_success is False
        assert result.is_error is False
        assert result.is_skipped is True
