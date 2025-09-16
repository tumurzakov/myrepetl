"""
Retry utilities for MySQL Replication ETL
"""

import time
import random
from typing import Callable, Any, Optional, Type, Tuple
from functools import wraps
from dataclasses import dataclass

from ..exceptions import ETLException


@dataclass
class RetryConfig:
    """Configuration for retry mechanism"""
    max_attempts: int = 3
    base_delay: float = 1.0
    max_delay: float = 60.0
    exponential_base: float = 2.0
    jitter: bool = True
    retryable_exceptions: Tuple[Type[Exception], ...] = (ETLException,)


def retry(config: RetryConfig = None):
    """
    Decorator for retrying function calls with exponential backoff
    
    Args:
        config: Retry configuration. If None, uses default config.
    """
    if config is None:
        config = RetryConfig()
    
    def decorator(func: Callable) -> Callable:
        @wraps(func)
        def wrapper(*args, **kwargs) -> Any:
            last_exception = None
            
            for attempt in range(config.max_attempts):
                try:
                    return func(*args, **kwargs)
                except config.retryable_exceptions as e:
                    last_exception = e
                    
                    if attempt == config.max_attempts - 1:
                        # Last attempt, re-raise the exception
                        raise e
                    
                    # Calculate delay with exponential backoff
                    delay = min(
                        config.base_delay * (config.exponential_base ** attempt),
                        config.max_delay
                    )
                    
                    # Add jitter to prevent thundering herd
                    if config.jitter:
                        delay *= (0.5 + random.random() * 0.5)
                    
                    time.sleep(delay)
                except Exception as e:
                    # Non-retryable exception, re-raise immediately
                    raise e
            
            # This should never be reached, but just in case
            if last_exception:
                raise last_exception
                
        return wrapper
    return decorator


def retry_on_connection_error(max_attempts: int = 3):
    """
    Convenience decorator for retrying on connection errors
    """
    config = RetryConfig(
        max_attempts=max_attempts,
        retryable_exceptions=(ConnectionError, ETLException)
    )
    return retry(config)


def retry_on_transform_error(max_attempts: int = 2):
    """
    Convenience decorator for retrying on transform errors
    """
    config = RetryConfig(
        max_attempts=max_attempts,
        retryable_exceptions=(ETLException,)
    )
    return retry(config)
