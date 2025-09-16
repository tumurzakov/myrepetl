"""
Basic ETL Example

This example demonstrates how to set up a basic ETL pipeline using the ETL tool.
"""

import sys
import os
import time
import logging

# Add parent directory to path to import ETL modules
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from etl.dsl import (
    ETLPipeline,
    ReplicationSource,
    TargetTable,
    TransformationRule,
    FieldDefinition,
    FieldMapping,
    FieldType,
    OperationType,
    uppercase_transform,
    lowercase_transform
)
from etl.engine import ETLEngine


def setup_logging():
    """Setup logging configuration"""
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
    )


def create_basic_pipeline():
    """Create a basic ETL pipeline"""
    
    # 1. Define replication source
    source = ReplicationSource(
        name="main_database",
        host="localhost",
        port=3306,
        user="replication_user",
        password="replication_password",
        database="source_db",
        tables=["users", "orders", "products"],
        server_id=100
    )
    
    # 2. Define target tables
    users_table = TargetTable(
        name="users",
        database="target_db"
    ).add_field("name", FieldType.VARCHAR, length=255) \
     .add_field("email", FieldType.VARCHAR, length=255) \
     .add_field("status", FieldType.VARCHAR, length=50)
    
    orders_table = TargetTable(
        name="orders",
        database="target_db"
    ).add_field("user_id", FieldType.INTEGER) \
     .add_field("product_id", FieldType.INTEGER) \
     .add_field("amount", FieldType.DECIMAL, length="10,2") \
     .add_field("order_date", FieldType.DATETIME) \
     .add_field("status", FieldType.VARCHAR, length=50)
    
    products_table = TargetTable(
        name="products",
        database="target_db"
    ).add_field("name", FieldType.VARCHAR, length=255) \
     .add_field("description", FieldType.TEXT) \
     .add_field("price", FieldType.DECIMAL, length="10,2") \
     .add_field("category", FieldType.VARCHAR, length=100)
    
    # 3. Define transformation rules
    users_rule = TransformationRule(
        name="users_transform",
        source_table="users",
        target_table="users"
    ).add_mapping("id", "id") \
     .add_mapping("name", "name", uppercase_transform) \
     .add_mapping("email", "email", lowercase_transform) \
     .add_mapping("status", "status")
    
    orders_rule = TransformationRule(
        name="orders_transform",
        source_table="orders",
        target_table="orders"
    ).add_mapping("id", "id") \
     .add_mapping("user_id", "user_id") \
     .add_mapping("product_id", "product_id") \
     .add_mapping("amount", "amount") \
     .add_mapping("order_date", "order_date") \
     .add_mapping("status", "status", uppercase_transform)
    
    products_rule = TransformationRule(
        name="products_transform",
        source_table="products",
        target_table="products"
    ).add_mapping("id", "id") \
     .add_mapping("name", "name", uppercase_transform) \
     .add_mapping("description", "description") \
     .add_mapping("price", "price") \
     .add_mapping("category", "category", uppercase_transform)
    
    # 4. Create ETL pipeline
    pipeline = ETLPipeline(
        name="basic_etl_pipeline"
    ).add_source(source) \
     .add_target_table(users_table) \
     .add_target_table(orders_table) \
     .add_target_table(products_table) \
     .add_transformation_rule(users_rule) \
     .add_transformation_rule(orders_rule) \
     .add_transformation_rule(products_rule)
    
    return pipeline


def main():
    """Main function"""
    setup_logging()
    logger = logging.getLogger("BasicExample")
    
    try:
        # Create pipeline
        pipeline = create_basic_pipeline()
        logger.info("Created ETL pipeline")
        
        # Target database configuration
        target_db_config = {
            "host": "localhost",
            "port": 3306,
            "user": "target_user",
            "password": "target_password",
            "database": "target_db"
        }
        
        # Create and start ETL engine
        etl_engine = ETLEngine(pipeline, target_db_config)
        logger.info("Created ETL engine")
        
        # Start ETL process
        etl_engine.start()
        logger.info("ETL engine started")
        
        # Run for some time and show statistics
        try:
            while True:
                time.sleep(10)
                status = etl_engine.get_status()
                stats = status["stats"]
                logger.info(f"ETL Stats - Processed: {stats['events_processed']}, "
                           f"Failed: {stats['events_failed']}, "
                           f"Inserted: {stats['rows_inserted']}, "
                           f"Updated: {stats['rows_updated']}, "
                           f"Deleted: {stats['rows_deleted']}")
                
        except KeyboardInterrupt:
            logger.info("Received interrupt signal")
        
    except Exception as e:
        logger.error(f"Error in ETL process: {e}")
        raise
    
    finally:
        # Stop ETL engine
        if 'etl_engine' in locals():
            etl_engine.stop()
            logger.info("ETL engine stopped")


if __name__ == "__main__":
    main()
