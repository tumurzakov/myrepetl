"""
Filter service for MySQL Replication ETL
"""

from typing import Dict, Any, Union
from ..exceptions import FilterError


class FilterService:
    """Service for applying filters to data"""
    
    def __init__(self):
        self.supported_operations = {
            "eq", "gt", "gte", "lt", "lte", "not", "and", "or"
        }
    
    def apply_filter(self, data: Dict[str, Any], filter_config: Dict[str, Any]) -> bool:
        """
        Apply filter to data and return True if data passes the filter
        
        Args:
            data: Dictionary containing the data to filter
            filter_config: Filter configuration dictionary
            
        Returns:
            bool: True if data passes the filter, False otherwise
            
        Raises:
            FilterError: If filter configuration is invalid
        """
        if not filter_config:
            return True
            
        try:
            return self._evaluate_filter(data, filter_config)
        except Exception as e:
            raise FilterError(f"Error applying filter: {e}")
    
    def _evaluate_filter(self, data: Dict[str, Any], filter_config: Dict[str, Any]) -> bool:
        """Recursively evaluate filter conditions"""
        
        # Check if this is a single operation
        if len(filter_config) == 1:
            key, value = next(iter(filter_config.items()))
            
            if key in self.supported_operations:
                return self._evaluate_operation(data, key, value)
            else:
                # Field with operation: {"field": {"op": "value"}}
                if isinstance(value, dict) and len(value) == 1:
                    op_key, op_value = next(iter(value.items()))
                    if op_key in self.supported_operations:
                        return self._evaluate_operation(data, op_key, {key: op_value})
                # Direct field comparison (default to eq)
                return self._evaluate_operation(data, "eq", {key: value})
        
        # Multiple conditions - check if any are operations
        has_operations = any(key in self.supported_operations for key in filter_config.keys())
        
        if has_operations:
            # Mixed: some operations, some direct fields
            results = []
            for key, value in filter_config.items():
                if key in self.supported_operations:
                    result = self._evaluate_operation(data, key, value)
                else:
                    # Field with operation: {"field": {"op": "value"}}
                    if isinstance(value, dict) and len(value) == 1:
                        op_key, op_value = next(iter(value.items()))
                        if op_key in self.supported_operations:
                            result = self._evaluate_operation(data, op_key, {key: op_value})
                        else:
                            result = self._evaluate_operation(data, "eq", {key: value})
                    else:
                        result = self._evaluate_operation(data, "eq", {key: value})
                    results.append(result)
            # Default to AND
            return all(results)
        else:
            # All direct field comparisons
            results = []
            for key, value in filter_config.items():
                # Field with operation: {"field": {"op": "value"}}
                if isinstance(value, dict) and len(value) == 1:
                    op_key, op_value = next(iter(value.items()))
                    if op_key in self.supported_operations:
                        result = self._evaluate_operation(data, op_key, {key: op_value})
                    else:
                        result = self._evaluate_operation(data, "eq", {key: value})
                else:
                    result = self._evaluate_operation(data, "eq", {key: value})
                results.append(result)
            # Default to AND
            return all(results)
    
    def _evaluate_operation(self, data: Dict[str, Any], operation: str, value: Any) -> bool:
        """Evaluate a specific operation"""
        
        if operation == "eq":
            return self._evaluate_equality(data, value)
        elif operation == "gt":
            return self._evaluate_greater_than(data, value)
        elif operation == "gte":
            return self._evaluate_greater_than_or_equal(data, value)
        elif operation == "lt":
            return self._evaluate_less_than(data, value)
        elif operation == "lte":
            return self._evaluate_less_than_or_equal(data, value)
        elif operation == "not":
            return not self._evaluate_filter(data, value)
        elif operation == "and":
            return self._evaluate_and(data, value)
        elif operation == "or":
            return self._evaluate_or(data, value)
        else:
            raise FilterError(f"Unsupported operation: {operation}")
    
    def _evaluate_equality(self, data: Dict[str, Any], conditions: Dict[str, Any]) -> bool:
        """Evaluate equality conditions"""
        for field, expected_value in conditions.items():
            actual_value = data.get(field)
            if actual_value != expected_value:
                return False
        return True
    
    def _evaluate_greater_than(self, data: Dict[str, Any], conditions: Dict[str, Any]) -> bool:
        """Evaluate greater than conditions"""
        for field, expected_value in conditions.items():
            actual_value = data.get(field)
            if actual_value is None or not (actual_value > expected_value):
                return False
        return True
    
    def _evaluate_greater_than_or_equal(self, data: Dict[str, Any], conditions: Dict[str, Any]) -> bool:
        """Evaluate greater than or equal conditions"""
        for field, expected_value in conditions.items():
            actual_value = data.get(field)
            if actual_value is None or not (actual_value >= expected_value):
                return False
        return True
    
    def _evaluate_less_than(self, data: Dict[str, Any], conditions: Dict[str, Any]) -> bool:
        """Evaluate less than conditions"""
        for field, expected_value in conditions.items():
            actual_value = data.get(field)
            if actual_value is None or not (actual_value < expected_value):
                return False
        return True
    
    def _evaluate_less_than_or_equal(self, data: Dict[str, Any], conditions: Dict[str, Any]) -> bool:
        """Evaluate less than or equal conditions"""
        for field, expected_value in conditions.items():
            actual_value = data.get(field)
            if actual_value is None or not (actual_value <= expected_value):
                return False
        return True
    
    def _evaluate_and(self, data: Dict[str, Any], conditions: Dict[str, Any]) -> bool:
        """Evaluate AND conditions"""
        for condition in conditions:
            if not self._evaluate_filter(data, condition):
                return False
        return True
    
    def _evaluate_or(self, data: Dict[str, Any], conditions: Dict[str, Any]) -> bool:
        """Evaluate OR conditions"""
        for condition in conditions:
            if self._evaluate_filter(data, condition):
                return True
        return False
    
    def validate_filter_config(self, filter_config: Dict[str, Any]) -> bool:
        """Validate filter configuration"""
        if not filter_config:
            return True
            
        try:
            self._validate_filter_recursive(filter_config)
            return True
        except Exception as e:
            raise FilterError(f"Invalid filter configuration: {e}")
    
    def _validate_filter_recursive(self, filter_config: Dict[str, Any]) -> None:
        """Recursively validate filter configuration"""
        for key, value in filter_config.items():
            if key in self.supported_operations:
                if key in ["and", "or"]:
                    if not isinstance(value, list):
                        raise FilterError(f"'{key}' operation requires a list of conditions")
                    for condition in value:
                        if not isinstance(condition, dict):
                            raise FilterError(f"Each condition in '{key}' must be a dictionary")
                        self._validate_filter_recursive(condition)
                elif key == "not":
                    if not isinstance(value, dict):
                        raise FilterError("'not' operation requires a dictionary condition")
                    self._validate_filter_recursive(value)
                else:
                    # Comparison operations (eq, gt, gte, lt, lte)
                    if not isinstance(value, dict):
                        raise FilterError(f"'{key}' operation requires a dictionary of field conditions")
            else:
                # Direct field condition - this is valid, no need to validate further
                pass
