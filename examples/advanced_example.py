"""
Advanced ETL Example

This example demonstrates advanced features like filtering, custom transformations,
and multiple sources.
"""

import sys
import os
import time
import logging
import json
from datetime import datetime

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
    OperationType
)
from etl.engine import ETLEngine


def setup_logging():
    """Setup logging configuration"""
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
    )


def custom_status_transform(value):
    """Custom transformation for status field"""
    status_mapping = {
        'active': 'ACTIVE',
        'inactive': 'INACTIVE',
        'pending': 'PENDING',
        'suspended': 'SUSPENDED'
    }
    return status_mapping.get(str(value).lower(), 'UNKNOWN')


def custom_amount_transform(value):
    """Custom transformation for amount field"""
    if value is None:
        return 0.0
    try:
        # Convert to float and round to 2 decimal places
        return round(float(value), 2)
    except (ValueError, TypeError):
        return 0.0


def custom_json_transform(value):
    """Custom transformation for JSON fields"""
    if value is None:
        return None
    if isinstance(value, str):
        try:
            return json.loads(value)
        except json.JSONDecodeError:
            return {"raw": value}
    return value


def create_advanced_pipeline():
    """Create an advanced ETL pipeline with multiple sources and complex transformations"""
    
    # 1. Define multiple replication sources
    main_source = ReplicationSource(
        name="main_database",
        host="db1.example.com",
        port=3306,
        user="replication_user",
        password="replication_password",
        database="main_db",
        tables=["users", "orders", "products", "categories"],
        server_id=100
    )
    
    analytics_source = ReplicationSource(
        name="analytics_database",
        host="db2.example.com",
        port=3306,
        user="replication_user",
        password="replication_password",
        database="analytics_db",
        tables=["user_events", "page_views"],
        server_id=101
    )
    
    # 2. Define target tables with complex schemas
    users_table = TargetTable(
        name="users",
        database="warehouse_db"
    ).add_field("name", FieldType.VARCHAR, length=255) \
     .add_field("email", FieldType.VARCHAR, length=255) \
     .add_field("status", FieldType.VARCHAR, length=50) \
     .add_field("created_at", FieldType.DATETIME) \
     .add_field("updated_at", FieldType.DATETIME) \
     .add_field("metadata", FieldType.JSON)
    
    orders_table = TargetTable(
        name="orders",
        database="warehouse_db"
    ).add_field("user_id", FieldType.INTEGER) \
     .add_field("product_id", FieldType.INTEGER) \
     .add_field("amount", FieldType.DECIMAL, length="10,2") \
     .add_field("order_date", FieldType.DATETIME) \
     .add_field("status", FieldType.VARCHAR, length=50) \
     .add_field("shipping_info", FieldType.JSON)
    
    products_table = TargetTable(
        name="products",
        database="warehouse_db"
    ).add_field("name", FieldType.VARCHAR, length=255) \
     .add_field("description", FieldType.TEXT) \
     .add_field("price", FieldType.DECIMAL, length="10,2") \
     .add_field("category_id", FieldType.INTEGER) \
     .add_field("in_stock", FieldType.BOOLEAN) \
     .add_field("attributes", FieldType.JSON)
    
    categories_table = TargetTable(
        name="categories",
        database="warehouse_db"
    ).add_field("name", FieldType.VARCHAR, length=255) \
     .add_field("description", FieldType.TEXT) \
     .add_field("parent_id", FieldType.INTEGER) \
     .add_field("is_active", FieldType.BOOLEAN)
    
    user_events_table = TargetTable(
        name="user_events",
        database="warehouse_db"
    ).add_field("user_id", FieldType.INTEGER) \
     .add_field("event_type", FieldType.VARCHAR, length=100) \
     .add_field("event_data", FieldType.JSON) \
     .add_field("timestamp", FieldType.DATETIME) \
     .add_field("session_id", FieldType.VARCHAR, length=255)
    
    # 3. Define complex transformation rules with filters
    users_rule = TransformationRule(
        name="users_transform",
        source_table="users",
        target_table="users"
    ).add_mapping("id", "id") \
     .add_mapping("name", "name") \
     .add_mapping("email", "email") \
     .add_mapping("status", "status", custom_status_transform) \
     .add_mapping("created_at", "created_at") \
     .add_mapping("updated_at", "updated_at") \
     .add_mapping("metadata", "metadata", custom_json_transform) \
     .add_filter("{status} != 'deleted'")  # Filter out deleted users
    
    orders_rule = TransformationRule(
        name="orders_transform",
        source_table="orders",
        target_table="orders"
    ).add_mapping("id", "id") \
     .add_mapping("user_id", "user_id") \
     .add_mapping("product_id", "product_id") \
     .add_mapping("amount", "amount", custom_amount_transform) \
     .add_mapping("order_date", "order_date") \
     .add_mapping("status", "status", custom_status_transform) \
     .add_mapping("shipping_info", "shipping_info", custom_json_transform) \
     .add_filter("{amount} > 0")  # Only process orders with positive amount
    
    products_rule = TransformationRule(
        name="products_transform",
        source_table="products",
        target_table="products"
    ).add_mapping("id", "id") \
     .add_mapping("name", "name") \
     .add_mapping("description", "description") \
     .add_mapping("price", "price", custom_amount_transform) \
     .add_mapping("category_id", "category_id") \
     .add_mapping("in_stock", "in_stock") \
     .add_mapping("attributes", "attributes", custom_json_transform)
    
    categories_rule = TransformationRule(
        name="categories_transform",
        source_table="categories",
        target_table="categories"
    ).add_mapping("id", "id") \
     .add_mapping("name", "name") \
     .add_mapping("description", "description") \
     .add_mapping("parent_id", "parent_id") \
     .add_mapping("is_active", "is_active")
    
    user_events_rule = TransformationRule(
        name="user_events_transform",
        source_table="user_events",
        target_table="user_events"
    ).add_mapping("id", "id") \
     .add_mapping("user_id", "user_id") \
     .add_mapping("event_type", "event_type") \
     .add_mapping("event_data", "event_data", custom_json_transform) \
     .add_mapping("timestamp", "timestamp") \
     .add_mapping("session_id", "session_id") \
     .add_filter("{user_id} IS NOT NULL")  # Only process events with valid user_id
    
    # 4. Create ETL pipeline
    pipeline = ETLPipeline(
        name="advanced_etl_pipeline"
    ).add_source(main_source) \
     .add_source(analytics_source) \
     .add_target_table(users_table) \
     .add_target_table(orders_table) \
     .add_target_table(products_table) \
     .add_target_table(categories_table) \
     .add_target_table(user_events_table) \
     .add_transformation_rule(users_rule) \
     .add_transformation_rule(orders_rule) \
     .add_transformation_rule(products_rule) \
     .add_transformation_rule(categories_rule) \
     .add_transformation_rule(user_events_rule)
    
    return pipeline


def save_pipeline_config(pipeline, filename="pipeline_config.json"):
    """Save pipeline configuration to JSON file"""
    with open(filename, 'w') as f:
        f.write(pipeline.to_json(indent=2))
    print(f"Pipeline configuration saved to {filename}")


def load_pipeline_config(filename="pipeline_config.json"):
    """Load pipeline configuration from JSON file"""
    with open(filename, 'r') as f:
        return ETLPipeline.from_json(f.read())


def main():
    """Main function"""
    setup_logging()
    logger = logging.getLogger("AdvancedExample")
    
    try:
        # Create pipeline
        pipeline = create_advanced_pipeline()
        logger.info("Created advanced ETL pipeline")
        
        # Save pipeline configuration
        save_pipeline_config(pipeline)
        
        # Target database configuration
        target_db_config = {
            "host": "warehouse.example.com",
            "port": 3306,
            "user": "warehouse_user",
            "password": "warehouse_password",
            "database": "warehouse_db"
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
                time.sleep(30)
                status = etl_engine.get_status()
                stats = status["stats"]
                replication_status = status["replication"]
                
                logger.info(f"ETL Stats - Processed: {stats['events_processed']}, "
                           f"Failed: {stats['events_failed']}, "
                           f"Inserted: {stats['rows_inserted']}, "
                           f"Updated: {stats['rows_updated']}, "
                           f"Deleted: {stats['rows_deleted']}, "
                           f"Uptime: {stats['uptime']:.1f}s")
                
                # Show replication status
                for source_name, client_status in replication_status["clients"].items():
                    logger.info(f"Replication {source_name}: "
                               f"Running: {client_status['is_running']}, "
                               f"Queue: {client_status['queue_size']}")
                
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
