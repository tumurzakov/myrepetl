"""
Integration tests for filter functionality in ETL pipeline
"""

import pytest
import json
import tempfile
import os
from unittest.mock import Mock, patch

from src.models.config import ETLConfig, TableMapping, ColumnMapping
from src.models.events import InsertEvent, UpdateEvent, DeleteEvent, EventType
from src.services.filter_service import FilterService
from src.exceptions import FilterError


class TestFilterIntegration:
    """Integration tests for filter functionality"""
    
    def setup_method(self):
        """Set up test fixtures"""
        self.filter_service = FilterService()
    
    def test_table_mapping_with_filter(self):
        """Test TableMapping with filter configuration"""
        column_mapping = {
            "id": ColumnMapping(column="id", primary_key=True),
            "name": ColumnMapping(column="name"),
            "status": ColumnMapping(column="status")
        }
        
        filter_config = {
            "status": {"eq": "active"},
            "id": {"gt": 2}
        }
        
        table_mapping = TableMapping(
            target_table="target1.users",
            primary_key="id",
            column_mapping=column_mapping,
            filter=filter_config
        )
        
        assert table_mapping.filter == filter_config
        assert table_mapping.target_table == "target1.users"
        assert table_mapping.primary_key == "id"
    
    def test_etl_config_with_filters(self):
        """Test ETLConfig loading with filter configuration"""
        config_dict = {
            "sources": {
                "source1": {
                    "host": "localhost",
                    "port": 3306,
                    "user": "root",
                    "password": "password",
                    "database": "source_db"
                }
            },
            "targets": {
                "target1": {
                    "host": "localhost",
                    "port": 3306,
                    "user": "root",
                    "password": "password",
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
                        "name": {"column": "name"},
                        "status": {"column": "status"}
                    },
                    "filter": {
                        "status": {"eq": "active"},
                        "id": {"gt": 2}
                    }
                }
            }
        }
        
        config = ETLConfig.from_dict(config_dict)
        
        # Check that filter was loaded correctly
        table_mapping = config.mapping["source1.users"]
        assert table_mapping.filter is not None
        assert table_mapping.filter["status"]["eq"] == "active"
        assert table_mapping.filter["id"]["gt"] == 2
    
    def test_filter_with_insert_event(self):
        """Test filter application with INSERT event"""
        # Create test data
        event_data = {
            "id": 5,
            "name": "John Doe",
            "status": "active"
        }
        
        filter_config = {
            "status": {"eq": "active"},
            "id": {"gt": 2}
        }
        
        # Test that data passes filter
        result = self.filter_service.apply_filter(event_data, filter_config)
        assert result is True
        
        # Test that data fails filter
        event_data["status"] = "inactive"
        result = self.filter_service.apply_filter(event_data, filter_config)
        assert result is False
        
        # Test with lower ID
        event_data["status"] = "active"
        event_data["id"] = 1
        result = self.filter_service.apply_filter(event_data, filter_config)
        assert result is False
    
    def test_filter_with_update_event(self):
        """Test filter application with UPDATE event scenarios"""
        filter_config = {
            "status": {"eq": "active"}
        }
        
        # Scenario 1: Both before and after pass filter
        before_data = {"id": 1, "status": "active"}
        after_data = {"id": 1, "status": "active"}
        
        before_passes = self.filter_service.apply_filter(before_data, filter_config)
        after_passes = self.filter_service.apply_filter(after_data, filter_config)
        
        assert before_passes is True
        assert after_passes is True
        
        # Scenario 2: Before passes, after doesn't (should delete)
        before_data = {"id": 1, "status": "active"}
        after_data = {"id": 1, "status": "inactive"}
        
        before_passes = self.filter_service.apply_filter(before_data, filter_config)
        after_passes = self.filter_service.apply_filter(after_data, filter_config)
        
        assert before_passes is True
        assert after_passes is False
        
        # Scenario 3: Neither passes (should skip)
        before_data = {"id": 1, "status": "inactive"}
        after_data = {"id": 1, "status": "inactive"}
        
        before_passes = self.filter_service.apply_filter(before_data, filter_config)
        after_passes = self.filter_service.apply_filter(after_data, filter_config)
        
        assert before_passes is False
        assert after_passes is False
    
    def test_filter_with_delete_event(self):
        """Test filter application with DELETE event"""
        filter_config = {
            "status": {"eq": "active"}
        }
        
        # Data that passes filter
        event_data = {"id": 1, "status": "active"}
        result = self.filter_service.apply_filter(event_data, filter_config)
        assert result is True
        
        # Data that doesn't pass filter
        event_data = {"id": 1, "status": "inactive"}
        result = self.filter_service.apply_filter(event_data, filter_config)
        assert result is False
    
    def test_complex_filter_scenarios(self):
        """Test complex filter scenarios"""
        # Complex filter: active users with high scores OR premium users
        filter_config = {
            "and": [
                {"status": {"eq": "active"}},
                {
                    "or": [
                        {"score": {"gte": 90}},
                        {"category": {"eq": "premium"}}
                    ]
                }
            ]
        }
        
        # Test case 1: Active user with high score
        data1 = {"id": 1, "status": "active", "score": 95, "category": "basic"}
        result1 = self.filter_service.apply_filter(data1, filter_config)
        assert result1 is True
        
        # Test case 2: Active premium user with low score
        data2 = {"id": 2, "status": "active", "score": 70, "category": "premium"}
        result2 = self.filter_service.apply_filter(data2, filter_config)
        assert result2 is True
        
        # Test case 3: Active user with low score and basic category
        data3 = {"id": 3, "status": "active", "score": 70, "category": "basic"}
        result3 = self.filter_service.apply_filter(data3, filter_config)
        assert result3 is False
        
        # Test case 4: Inactive user (should fail regardless of other conditions)
        data4 = {"id": 4, "status": "inactive", "score": 95, "category": "premium"}
        result4 = self.filter_service.apply_filter(data4, filter_config)
        assert result4 is False
    
    def test_filter_with_missing_fields(self):
        """Test filter behavior with missing fields"""
        filter_config = {
            "status": {"eq": "active"},
            "id": {"gt": 2}
        }
        
        # Missing status field
        data1 = {"id": 5}
        result1 = self.filter_service.apply_filter(data1, filter_config)
        assert result1 is False
        
        # Missing id field
        data2 = {"status": "active"}
        result2 = self.filter_service.apply_filter(data2, filter_config)
        assert result2 is False
        
        # Both fields present
        data3 = {"id": 5, "status": "active"}
        result3 = self.filter_service.apply_filter(data3, filter_config)
        assert result3 is True
    
    def test_filter_validation_in_config(self):
        """Test filter validation when loading configuration"""
        # Valid filter configuration
        valid_config = {
            "sources": {"source1": {"host": "localhost", "port": 3306, "user": "root", "password": "pass", "database": "db"}},
            "targets": {"target1": {"host": "localhost", "port": 3306, "user": "root", "password": "pass", "database": "db"}},
            "replication": {"server_id": 100},
            "mapping": {
                "source1.users": {
                    "target_table": "target1.users",
                    "primary_key": "id",
                    "column_mapping": {"id": {"column": "id", "primary_key": True}},
                    "filter": {"status": {"eq": "active"}}
                }
            }
        }
        
        # Should not raise exception
        config = ETLConfig.from_dict(valid_config)
        assert config is not None
        
        # Validate the filter
        table_mapping = config.mapping["source1.users"]
        is_valid = self.filter_service.validate_filter_config(table_mapping.filter)
        assert is_valid is True
    
    def test_filter_performance_with_large_data(self):
        """Test filter performance with larger datasets"""
        filter_config = {
            "status": {"eq": "active"},
            "id": {"gt": 1000}
        }
        
        # Test with larger data structure
        large_data = {
            "id": 1500,
            "status": "active",
            "name": "John Doe",
            "email": "john@example.com",
            "created_at": "2023-01-01",
            "updated_at": "2023-01-02",
            "metadata": {"key1": "value1", "key2": "value2"},
            "tags": ["tag1", "tag2", "tag3"]
        }
        
        # Should pass filter
        result = self.filter_service.apply_filter(large_data, filter_config)
        assert result is True
        
        # Change status to inactive
        large_data["status"] = "inactive"
        result = self.filter_service.apply_filter(large_data, filter_config)
        assert result is False
