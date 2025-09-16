"""
Unit tests for utilities
"""

import pytest
import time
from unittest.mock import Mock, patch

from src.utils.retry import retry, RetryConfig, retry_on_connection_error, retry_on_transform_error
from src.utils.sql_builder import SQLBuilder
from src.exceptions import ETLException


class TestRetryConfig:
    """Test RetryConfig"""
    
    def test_default_config(self):
        """Test default retry configuration"""
        config = RetryConfig()
        
        assert config.max_attempts == 3
        assert config.base_delay == 1.0
        assert config.max_delay == 60.0
        assert config.exponential_base == 2.0
        assert config.jitter is True
        assert ETLException in config.retryable_exceptions
    
    def test_custom_config(self):
        """Test custom retry configuration"""
        config = RetryConfig(
            max_attempts=5,
            base_delay=2.0,
            max_delay=120.0,
            exponential_base=3.0,
            jitter=False,
            retryable_exceptions=(ValueError,)
        )
        
        assert config.max_attempts == 5
        assert config.base_delay == 2.0
        assert config.max_delay == 120.0
        assert config.exponential_base == 3.0
        assert config.jitter is False
        assert ValueError in config.retryable_exceptions


class TestRetryDecorator:
    """Test retry decorator"""
    
    def test_success_on_first_attempt(self):
        """Test function succeeds on first attempt"""
        config = RetryConfig(max_attempts=3)
        
        @retry(config)
        def test_func():
            return "success"
        
        result = test_func()
        assert result == "success"
    
    def test_success_on_retry(self):
        """Test function succeeds after retries"""
        config = RetryConfig(max_attempts=3, base_delay=0.01)
        call_count = 0
        
        @retry(config)
        def test_func():
            nonlocal call_count
            call_count += 1
            if call_count < 3:
                raise ETLException("Temporary error")
            return "success"
        
        result = test_func()
        assert result == "success"
        assert call_count == 3
    
    def test_failure_after_max_attempts(self):
        """Test function fails after max attempts"""
        config = RetryConfig(max_attempts=2, base_delay=0.01)
        
        @retry(config)
        def test_func():
            raise ETLException("Persistent error")
        
        with pytest.raises(ETLException, match="Persistent error"):
            test_func()
    
    def test_non_retryable_exception(self):
        """Test non-retryable exception is raised immediately"""
        config = RetryConfig(max_attempts=3)
        
        @retry(config)
        def test_func():
            raise ValueError("Non-retryable error")
        
        with pytest.raises(ValueError, match="Non-retryable error"):
            test_func()
    
    def test_delay_calculation(self):
        """Test delay calculation with exponential backoff"""
        config = RetryConfig(
            max_attempts=3,
            base_delay=1.0,
            exponential_base=2.0,
            jitter=False
        )
        
        call_times = []
        
        @retry(config)
        def test_func():
            call_times.append(time.time())
            if len(call_times) < 3:
                raise ETLException("Temporary error")
            return "success"
        
        start_time = time.time()
        test_func()
        
        # Check that delays were applied (allowing some tolerance)
        assert len(call_times) == 3
        assert call_times[1] - call_times[0] >= 0.9  # ~1 second delay
        assert call_times[2] - call_times[1] >= 1.9  # ~2 second delay
    
    def test_jitter(self):
        """Test jitter is applied to delays"""
        config = RetryConfig(
            max_attempts=3,
            base_delay=0.01,  # Very small delay for testing
            jitter=True
        )
        
        @retry(config)
        def test_func():
            return "success"  # Always succeed
        
        result = test_func()
        assert result == "success"


class TestRetryConvenienceDecorators:
    """Test convenience retry decorators"""
    
    def test_retry_on_connection_error(self):
        """Test retry on connection error decorator"""
        call_count = 0
        
        @retry_on_connection_error(max_attempts=2)
        def test_func():
            nonlocal call_count
            call_count += 1
            if call_count < 2:
                raise ConnectionError("Connection failed")
            return "success"
        
        result = test_func()
        assert result == "success"
        assert call_count == 2
    
    def test_retry_on_transform_error(self):
        """Test retry on transform error decorator"""
        call_count = 0
        
        @retry_on_transform_error(max_attempts=2)
        def test_func():
            nonlocal call_count
            call_count += 1
            if call_count < 2:
                raise ETLException("Transform failed")
            return "success"
        
        result = test_func()
        assert result == "success"
        assert call_count == 2


class TestSQLBuilder:
    """Test SQLBuilder utility"""
    
    def test_build_upsert_sql(self):
        """Test building UPSERT SQL"""
        data = {"id": 1, "name": "test", "email": "test@example.com"}
        primary_key = "id"
        
        sql, values = SQLBuilder.build_upsert_sql("users", data, primary_key)
        
        expected_sql = """INSERT INTO users (id, name, email)
VALUES (%s, %s, %s)
ON DUPLICATE KEY UPDATE
name = VALUES(name), email = VALUES(email)"""
        
        assert sql.strip() == expected_sql.strip()
        assert values == [1, "test", "test@example.com"]
    
    def test_build_upsert_sql_only_primary_key(self):
        """Test building UPSERT SQL with only primary key"""
        data = {"id": 1}
        primary_key = "id"
        
        sql, values = SQLBuilder.build_upsert_sql("users", data, primary_key)
        
        expected_sql = """INSERT INTO users (id)
VALUES (%s)
ON DUPLICATE KEY UPDATE
id = VALUES(id)"""
        
        assert sql.strip() == expected_sql.strip()
        assert values == [1]
    
    def test_build_upsert_sql_empty_data(self):
        """Test building UPSERT SQL with empty data"""
        with pytest.raises(ValueError, match="Data cannot be empty"):
            SQLBuilder.build_upsert_sql("users", {}, "id")
    
    def test_build_delete_sql(self):
        """Test building DELETE SQL"""
        data = {"id": 1, "name": "test"}
        primary_key = "id"
        
        sql, values = SQLBuilder.build_delete_sql("users", data, primary_key)
        
        expected_sql = "DELETE FROM users WHERE id = %s"
        assert sql == expected_sql
        assert values == [1]
    
    def test_build_delete_sql_missing_primary_key(self):
        """Test building DELETE SQL with missing primary key"""
        data = {"name": "test"}
        primary_key = "id"
        
        with pytest.raises(ValueError, match="Primary key 'id' not found in data"):
            SQLBuilder.build_delete_sql("users", data, primary_key)
    
    def test_build_insert_sql(self):
        """Test building INSERT SQL"""
        data = {"id": 1, "name": "test", "email": "test@example.com"}
        
        sql, values = SQLBuilder.build_insert_sql("users", data)
        
        expected_sql = """INSERT INTO users (id, name, email)
VALUES (%s, %s, %s)"""
        
        assert sql.strip() == expected_sql.strip()
        assert values == [1, "test", "test@example.com"]
    
    def test_build_insert_sql_empty_data(self):
        """Test building INSERT SQL with empty data"""
        with pytest.raises(ValueError, match="Data cannot be empty"):
            SQLBuilder.build_insert_sql("users", {})
    
    def test_build_update_sql(self):
        """Test building UPDATE SQL"""
        data = {"id": 1, "name": "test", "email": "test@example.com"}
        primary_key = "id"
        
        sql, values = SQLBuilder.build_update_sql("users", data, primary_key)
        
        expected_sql = """UPDATE users 
SET name = %s, email = %s
WHERE id = %s"""
        
        assert sql.strip() == expected_sql.strip()
        assert values == ["test", "test@example.com", 1]
    
    def test_build_update_sql_missing_primary_key(self):
        """Test building UPDATE SQL with missing primary key"""
        data = {"name": "test"}
        primary_key = "id"
        
        with pytest.raises(ValueError, match="Primary key 'id' not found in data"):
            SQLBuilder.build_update_sql("users", data, primary_key)
    
    def test_build_update_sql_only_primary_key(self):
        """Test building UPDATE SQL with only primary key"""
        data = {"id": 1}
        primary_key = "id"
        
        with pytest.raises(ValueError, match="No columns to update"):
            SQLBuilder.build_update_sql("users", data, primary_key)
    
    def test_build_batch_upsert_sql(self):
        """Test building batch UPSERT SQL"""
        data_list = [
            {"id": 1, "name": "test1"},
            {"id": 2, "name": "test2"}
        ]
        primary_key = "id"
        
        sql, values_list = SQLBuilder.build_batch_upsert_sql("users", data_list, primary_key)
        
        expected_sql = """INSERT INTO users (id, name)
VALUES (%s, %s)
ON DUPLICATE KEY UPDATE
name = VALUES(name)"""
        
        assert sql.strip() == expected_sql.strip()
        assert values_list == [[1, "test1"], [2, "test2"]]
    
    def test_build_batch_upsert_sql_empty_list(self):
        """Test building batch UPSERT SQL with empty list"""
        with pytest.raises(ValueError, match="Data list cannot be empty"):
            SQLBuilder.build_batch_upsert_sql("users", [], "id")
