"""
Transform models for MySQL Replication ETL
"""

from dataclasses import dataclass
from typing import Any, Callable, Optional
from enum import Enum


class TransformStatus(Enum):
    """Transform execution status"""
    SUCCESS = "success"
    ERROR = "error"
    SKIPPED = "skipped"


@dataclass
class TransformResult:
    """Result of a transform operation"""
    status: TransformStatus
    original_value: Any
    transformed_value: Any
    error: Optional[str] = None
    
    @property
    def is_success(self) -> bool:
        """Check if transform was successful"""
        return self.status == TransformStatus.SUCCESS
    
    @property
    def is_error(self) -> bool:
        """Check if transform failed"""
        return self.status == TransformStatus.ERROR
    
    @property
    def is_skipped(self) -> bool:
        """Check if transform was skipped"""
        return self.status == TransformStatus.SKIPPED


@dataclass
class TransformFunction:
    """Transform function definition"""
    name: str
    function: Callable[[Any], Any]
    description: Optional[str] = None
    
    def __post_init__(self):
        """Validate transform function after initialization"""
        if not self.name:
            raise ValueError("Transform name is required")
        if not callable(self.function):
            raise ValueError("Transform must be callable")
    
    def execute(self, value: Any) -> TransformResult:
        """Execute the transform function"""
        try:
            if value is None:
                return TransformResult(
                    status=TransformStatus.SKIPPED,
                    original_value=value,
                    transformed_value=value
                )
            
            transformed_value = self.function(value)
            return TransformResult(
                status=TransformStatus.SUCCESS,
                original_value=value,
                transformed_value=transformed_value
            )
        except Exception as e:
            return TransformResult(
                status=TransformStatus.ERROR,
                original_value=value,
                transformed_value=value,
                error=str(e)
            )
