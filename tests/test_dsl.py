"""
Tests for ETL DSL module
"""

import pytest
import json
from etl.dsl import (
    ReplicationSource,
    TargetTable,
    TransformationRule,
    FieldDefinition,
    FieldMapping,
    FieldType,
    OperationType,
    ETLPipeline,
    uppercase_transform,
    lowercase_transform
)


class TestReplicationSource:
    """Test ReplicationSource class"""
    
    def test_create_source(self):
        """Test creating a replication source"""
        source = ReplicationSource(
            name="test_source",
            host="localhost",
            port=3306,
            user="test_user",
            password="test_password",
            database="test_db",
            tables=["table1", "table2"]
        )
        
        assert source.name == "test_source"
        assert source.host == "localhost"
        assert source.port == 3306
        assert source.user == "test_user"
        assert source.password == "test_password"
        assert source.database == "test_db"
        assert source.tables == ["table1", "table2"]
    
    def test_source_validation(self):
        """Test source validation"""
        # Test missing name
        with pytest.raises(ValueError, match="Source name is required"):
            ReplicationSource(name="", host="localhost")
        
        # Test missing host
        with pytest.raises(ValueError, match="Host is required"):
            ReplicationSource(name="test", host="")
        
        # Test invalid port
        with pytest.raises(ValueError, match="Port must be between 1 and 65535"):
            ReplicationSource(name="test", host="localhost", port=0)
    
    def test_source_serialization(self):
        """Test source serialization to/from dict"""
        source = ReplicationSource(
            name="test_source",
            host="localhost",
            port=3306,
            user="test_user",
            password="test_password",
            database="test_db",
            tables=["table1", "table2"]
        )
        
        # Test to_dict
        source_dict = source.to_dict()
        assert source_dict["name"] == "test_source"
        assert source_dict["host"] == "localhost"
        assert source_dict["port"] == 3306
        
        # Test from_dict
        restored_source = ReplicationSource.from_dict(source_dict)
        assert restored_source.name == source.name
        assert restored_source.host == source.host
        assert restored_source.port == source.port


class TestTargetTable:
    """Test TargetTable class"""
    
    def test_create_table(self):
        """Test creating a target table"""
        table = TargetTable(
            name="test_table",
            database="test_db"
        )
        
        assert table.name == "test_table"
        assert table.database == "test_db"
        # Should have default id field
        assert len(table.fields) == 1
        assert table.fields[0].name == "id"
        assert table.fields[0].primary_key is True
    
    def test_add_field(self):
        """Test adding fields to table"""
        table = TargetTable(
            name="test_table",
            database="test_db"
        ).add_field("name", FieldType.VARCHAR, length=255) \
         .add_field("email", FieldType.VARCHAR, length=255)
        
        assert len(table.fields) == 3  # id + name + email
        assert table.fields[1].name == "name"
        assert table.fields[1].field_type == FieldType.VARCHAR
        assert table.fields[1].length == 255
        assert table.fields[2].name == "email"
    
    def test_table_validation(self):
        """Test table validation"""
        # Test missing name
        with pytest.raises(ValueError, match="Table name is required"):
            TargetTable(name="", database="test_db")
        
        # Test missing database
        with pytest.raises(ValueError, match="Database name is required"):
            TargetTable(name="test", database="")
    
    def test_table_serialization(self):
        """Test table serialization to/from dict"""
        table = TargetTable(
            name="test_table",
            database="test_db"
        ).add_field("name", FieldType.VARCHAR, length=255)
        
        # Test to_dict
        table_dict = table.to_dict()
        assert table_dict["name"] == "test_table"
        assert table_dict["database"] == "test_db"
        assert len(table_dict["fields"]) == 2  # id + name
        
        # Test from_dict
        restored_table = TargetTable.from_dict(table_dict)
        assert restored_table.name == table.name
        assert restored_table.database == table.database
        assert len(restored_table.fields) == len(table.fields)


class TestTransformationRule:
    """Test TransformationRule class"""
    
    def test_create_rule(self):
        """Test creating a transformation rule"""
        rule = TransformationRule(
            name="test_rule",
            source_table="source_table",
            target_table="target_table"
        )
        
        assert rule.name == "test_rule"
        assert rule.source_table == "source_table"
        assert rule.target_table == "target_table"
        assert OperationType.INSERT in rule.operations
        assert OperationType.UPDATE in rule.operations
        assert OperationType.DELETE in rule.operations
    
    def test_add_mapping(self):
        """Test adding field mappings"""
        rule = TransformationRule(
            name="test_rule",
            source_table="source_table",
            target_table="target_table"
        ).add_mapping("source_field", "target_field", uppercase_transform)
        
        assert len(rule.field_mappings) == 1
        mapping = rule.field_mappings[0]
        assert mapping.source_field == "source_field"
        assert mapping.target_field == "target_field"
        assert mapping.transformation == uppercase_transform
    
    def test_add_filter(self):
        """Test adding filters"""
        rule = TransformationRule(
            name="test_rule",
            source_table="source_table",
            target_table="target_table"
        ).add_filter("{status} != 'deleted'")
        
        assert len(rule.filters) == 1
        assert rule.filters[0] == "{status} != 'deleted'"
    
    def test_rule_validation(self):
        """Test rule validation"""
        # Test missing name
        with pytest.raises(ValueError, match="Rule name is required"):
            TransformationRule(name="", source_table="source", target_table="target")
        
        # Test missing source table
        with pytest.raises(ValueError, match="Source table is required"):
            TransformationRule(name="test", source_table="", target_table="target")
        
        # Test missing target table
        with pytest.raises(ValueError, match="Target table is required"):
            TransformationRule(name="test", source_table="source", target_table="")


class TestETLPipeline:
    """Test ETLPipeline class"""
    
    def test_create_pipeline(self):
        """Test creating an ETL pipeline"""
        pipeline = ETLPipeline(name="test_pipeline")
        
        assert pipeline.name == "test_pipeline"
        assert len(pipeline.sources) == 0
        assert len(pipeline.target_tables) == 0
        assert len(pipeline.transformation_rules) == 0
    
    def test_add_components(self):
        """Test adding components to pipeline"""
        source = ReplicationSource(name="test", host="localhost")
        table = TargetTable(name="test", database="test_db")
        rule = TransformationRule(name="test", source_table="source", target_table="target")
        
        pipeline = ETLPipeline(name="test_pipeline") \
            .add_source(source) \
            .add_target_table(table) \
            .add_transformation_rule(rule)
        
        assert len(pipeline.sources) == 1
        assert len(pipeline.target_tables) == 1
        assert len(pipeline.transformation_rules) == 1
    
    def test_pipeline_serialization(self):
        """Test pipeline serialization to/from JSON"""
        source = ReplicationSource(name="test", host="localhost")
        table = TargetTable(name="test", database="test_db")
        rule = TransformationRule(name="test", source_table="source", target_table="target")
        
        pipeline = ETLPipeline(name="test_pipeline") \
            .add_source(source) \
            .add_target_table(table) \
            .add_transformation_rule(rule)
        
        # Test to_json
        json_str = pipeline.to_json()
        assert "test_pipeline" in json_str
        assert "test" in json_str
        
        # Test from_json
        restored_pipeline = ETLPipeline.from_json(json_str)
        assert restored_pipeline.name == pipeline.name
        assert len(restored_pipeline.sources) == len(pipeline.sources)
        assert len(restored_pipeline.target_tables) == len(pipeline.target_tables)
        assert len(restored_pipeline.transformation_rules) == len(pipeline.transformation_rules)


class TestTransformations:
    """Test transformation functions"""
    
    def test_uppercase_transform(self):
        """Test uppercase transformation"""
        assert uppercase_transform("hello") == "HELLO"
        assert uppercase_transform("Hello World") == "HELLO WORLD"
        assert uppercase_transform(None) is None
        assert uppercase_transform(123) == "123"
    
    def test_lowercase_transform(self):
        """Test lowercase transformation"""
        assert lowercase_transform("HELLO") == "hello"
        assert lowercase_transform("Hello World") == "hello world"
        assert lowercase_transform(None) is None
        assert lowercase_transform(123) == "123"
