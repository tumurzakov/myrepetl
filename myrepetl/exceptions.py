"""
Custom exceptions for MySQL Replication ETL
"""


class ETLException(Exception):
    """Base exception for ETL operations"""
    pass


class ConfigurationError(ETLException):
    """Configuration related errors"""
    pass


class ConnectionError(ETLException):
    """Database connection errors"""
    pass


class TransformError(ETLException):
    """Data transformation errors"""
    pass


class ReplicationError(ETLException):
    """Replication stream errors"""
    pass


class ValidationError(ETLException):
    """Data validation errors"""
    pass
