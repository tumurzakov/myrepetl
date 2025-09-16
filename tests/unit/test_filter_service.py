"""
Unit tests for FilterService
"""

import pytest

from src.services.filter_service import FilterService
from src.exceptions import FilterError


class TestFilterService:
    """Test cases for FilterService"""
    
    def setup_method(self):
        """Set up test fixtures"""
        self.filter_service = FilterService()
    
    def test_apply_filter_no_filter(self):
        """Test applying no filter returns True"""
        data = {"id": 1, "name": "test", "status": "active"}
        result = self.filter_service.apply_filter(data, None)
        assert result is True
    
    def test_apply_filter_empty_filter(self):
        """Test applying empty filter returns True"""
        data = {"id": 1, "name": "test", "status": "active"}
        result = self.filter_service.apply_filter(data, {})
        assert result is True
    
    def test_apply_filter_simple_equality(self):
        """Test simple equality filter"""
        data = {"id": 1, "name": "test", "status": "active"}
        filter_config = {"status": {"eq": "active"}}
        result = self.filter_service.apply_filter(data, filter_config)
        assert result is True
        
        filter_config = {"status": {"eq": "inactive"}}
        result = self.filter_service.apply_filter(data, filter_config)
        assert result is False
    
    def test_apply_filter_multiple_conditions(self):
        """Test multiple conditions (default AND)"""
        data = {"id": 5, "name": "test", "status": "active"}
        filter_config = {
            "status": {"eq": "active"},
            "id": {"gt": 2}
        }
        result = self.filter_service.apply_filter(data, filter_config)
        assert result is True
        
        filter_config = {
            "status": {"eq": "active"},
            "id": {"gt": 10}
        }
        result = self.filter_service.apply_filter(data, filter_config)
        assert result is False
    
    def test_apply_filter_comparison_operations(self):
        """Test comparison operations"""
        data = {"id": 5, "score": 85.5}
        
        # Greater than
        filter_config = {"id": {"gt": 3}}
        assert self.filter_service.apply_filter(data, filter_config) is True
        
        filter_config = {"id": {"gt": 10}}
        assert self.filter_service.apply_filter(data, filter_config) is False
        
        # Greater than or equal
        filter_config = {"id": {"gte": 5}}
        assert self.filter_service.apply_filter(data, filter_config) is True
        
        filter_config = {"id": {"gte": 6}}
        assert self.filter_service.apply_filter(data, filter_config) is False
        
        # Less than
        filter_config = {"id": {"lt": 10}}
        assert self.filter_service.apply_filter(data, filter_config) is True
        
        filter_config = {"id": {"lt": 3}}
        assert self.filter_service.apply_filter(data, filter_config) is False
        
        # Less than or equal
        filter_config = {"id": {"lte": 5}}
        assert self.filter_service.apply_filter(data, filter_config) is True
        
        filter_config = {"id": {"lte": 3}}
        assert self.filter_service.apply_filter(data, filter_config) is False
    
    def test_apply_filter_not_operation(self):
        """Test NOT operation"""
        data = {"status": "active"}
        
        filter_config = {"not": {"status": {"eq": "inactive"}}}
        result = self.filter_service.apply_filter(data, filter_config)
        assert result is True
        
        filter_config = {"not": {"status": {"eq": "active"}}}
        result = self.filter_service.apply_filter(data, filter_config)
        assert result is False
    
    def test_apply_filter_and_operation(self):
        """Test AND operation"""
        data = {"id": 5, "status": "active", "score": 85}
        
        filter_config = {
            "and": [
                {"id": {"gt": 3}},
                {"status": {"eq": "active"}},
                {"score": {"gte": 80}}
            ]
        }
        result = self.filter_service.apply_filter(data, filter_config)
        assert result is True
        
        filter_config = {
            "and": [
                {"id": {"gt": 3}},
                {"status": {"eq": "inactive"}},
                {"score": {"gte": 80}}
            ]
        }
        result = self.filter_service.apply_filter(data, filter_config)
        assert result is False
    
    def test_apply_filter_or_operation(self):
        """Test OR operation"""
        data = {"id": 5, "status": "active"}
        
        filter_config = {
            "or": [
                {"id": {"lt": 3}},
                {"status": {"eq": "active"}}
            ]
        }
        result = self.filter_service.apply_filter(data, filter_config)
        assert result is True
        
        filter_config = {
            "or": [
                {"id": {"lt": 3}},
                {"status": {"eq": "inactive"}}
            ]
        }
        result = self.filter_service.apply_filter(data, filter_config)
        assert result is False
    
    def test_apply_filter_complex_nested(self):
        """Test complex nested filters"""
        data = {"id": 5, "status": "active", "category": "premium", "score": 85}
        
        filter_config = {
            "and": [
                {"status": {"eq": "active"}},
                {
                    "or": [
                        {"category": {"eq": "premium"}},
                        {"score": {"gte": 90}}
                    ]
                }
            ]
        }
        result = self.filter_service.apply_filter(data, filter_config)
        assert result is True
        
        # Change category to non-premium
        data["category"] = "basic"
        result = self.filter_service.apply_filter(data, filter_config)
        assert result is False
        
        # But if score is high enough, it should pass
        data["score"] = 95
        result = self.filter_service.apply_filter(data, filter_config)
        assert result is True
    
    def test_apply_filter_missing_fields(self):
        """Test filter with missing fields"""
        data = {"id": 5}
        
        # Missing field should fail equality
        filter_config = {"status": {"eq": "active"}}
        result = self.filter_service.apply_filter(data, filter_config)
        assert result is False
        
        # Missing field should fail comparison
        filter_config = {"score": {"gt": 80}}
        result = self.filter_service.apply_filter(data, filter_config)
        assert result is False
    
    def test_apply_filter_null_values(self):
        """Test filter with null values"""
        data = {"id": 5, "status": None}
        
        filter_config = {"status": {"eq": "active"}}
        result = self.filter_service.apply_filter(data, filter_config)
        assert result is False
        
        filter_config = {"status": {"eq": None}}
        result = self.filter_service.apply_filter(data, filter_config)
        assert result is True
    
    def test_validate_filter_config_valid(self):
        """Test validation of valid filter configurations"""
        valid_configs = [
            {"status": {"eq": "active"}},
            {"id": {"gt": 5}},
            {"and": [{"status": {"eq": "active"}}, {"id": {"gt": 5}}]},
            {"or": [{"status": {"eq": "active"}}, {"id": {"gt": 5}}]},
            {"not": {"status": {"eq": "inactive"}}},
            {"status": "active"},  # Direct field condition
            {}  # Empty filter
        ]
        
        for config in valid_configs:
            result = self.filter_service.validate_filter_config(config)
            assert result is True
    
    def test_validate_filter_config_invalid(self):
        """Test validation of invalid filter configurations"""
        invalid_configs = [
            {"and": "not_a_list"},  # and/or should be lists
            {"or": "not_a_list"},
            {"not": "not_a_dict"},  # not should be dict
            {"and": [{"status": "active"}, "not_a_dict"]}  # conditions should be dicts
        ]
        
        for config in invalid_configs:
            with pytest.raises(FilterError):
                self.filter_service.validate_filter_config(config)
    
    def test_apply_filter_error_handling(self):
        """Test error handling in filter application"""
        data = {"id": 5}
        
        # Invalid filter configuration should raise FilterError
        invalid_filter = {"and": "not_a_list"}
        with pytest.raises(FilterError):
            self.filter_service.apply_filter(data, invalid_filter)
    
    def test_supported_operations(self):
        """Test that all supported operations are available"""
        expected_operations = {"eq", "gt", "gte", "lt", "lte", "not", "and", "or"}
        assert self.filter_service.supported_operations == expected_operations
