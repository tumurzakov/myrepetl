"""
ETL Tool for MySQL Replication Protocol

This package provides a comprehensive ETL solution using MySQL replication protocol.
It includes DSL for describing replication sources, target tables, and transformation rules.
"""

__version__ = "1.0.0"
__author__ = "ETL Team"

from dsl import (
    ReplicationSource,
    TargetTable,
    TransformationRule,
    ETLPipeline
)

from engine import ETLEngine
from replication import MySQLReplicationClient

__all__ = [
    'ReplicationSource',
    'TargetTable', 
    'TransformationRule',
    'ETLPipeline',
    'ETLEngine',
    'MySQLReplicationClient'
]
