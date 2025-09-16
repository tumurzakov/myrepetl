"""
MyRepETL - MySQL Replication ETL Tool

Инструмент для репликации данных из MySQL с поддержкой трансформаций.
"""

__version__ = "1.0.0"
__author__ = "Tumurzakov"
__email__ = "tumurzakov@example.com"

from .etl_service import ETLService
from .exceptions import ETLException

__all__ = [
    "ETLService",
    "ETLException",
    "__version__",
    "__author__",
    "__email__",
]