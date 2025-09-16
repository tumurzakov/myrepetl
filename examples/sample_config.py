"""
Sample ETL Configuration

This file demonstrates how to create a complete ETL configuration
that can be saved and loaded.
"""

import sys
import os

# Add parent directory to path to import ETL modules
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from etl.dsl import (
    ETLPipeline,
    ReplicationSource,
    TargetTable,
    TransformationRule,
    FieldDefinition,
    FieldType,
    OperationType,
    uppercase_transform,
    lowercase_transform
)
from etl.utils import ETLConfigManager


def create_sample_pipeline():
    """Create a sample ETL pipeline configuration"""
    
    # 1. Define replication source
    source = ReplicationSource(
        name="production_db",
        host="prod-mysql.example.com",
        port=3306,
        user="replication_user",
        password="secure_password",
        database="production",
        tables=["users", "orders", "products", "categories"],
        server_id=200
    )
    
    # 2. Define target tables
    users_table = TargetTable(
        name="users",
        database="warehouse"
    ).add_field("name", FieldType.VARCHAR, length=255) \
     .add_field("email", FieldType.VARCHAR, length=255) \
     .add_field("status", FieldType.VARCHAR, length=50) \
     .add_field("created_at", FieldType.DATETIME) \
     .add_field("updated_at", FieldType.DATETIME)
    
    orders_table = TargetTable(
        name="orders",
        database="warehouse"
    ).add_field("user_id", FieldType.INTEGER) \
     .add_field("product_id", FieldType.INTEGER) \
     .add_field("amount", FieldType.DECIMAL, length="10,2") \
     .add_field("order_date", FieldType.DATETIME) \
     .add_field("status", FieldType.VARCHAR, length=50)
    
    products_table = TargetTable(
        name="products",
        database="warehouse"
    ).add_field("name", FieldType.VARCHAR, length=255) \
     .add_field("description", FieldType.TEXT) \
     .add_field("price", FieldType.DECIMAL, length="10,2") \
     .add_field("category_id", FieldType.INTEGER) \
     .add_field("in_stock", FieldType.BOOLEAN)
    
    categories_table = TargetTable(
        name="categories",
        database="warehouse"
    ).add_field("name", FieldType.VARCHAR, length=255) \
     .add_field("description", FieldType.TEXT) \
     .add_field("parent_id", FieldType.INTEGER)
    
    # 3. Define transformation rules
    users_rule = TransformationRule(
        name="users_transform",
        source_table="users",
        target_table="users"
    ).add_mapping("id", "id") \
     .add_mapping("name", "name", uppercase_transform) \
     .add_mapping("email", "email", lowercase_transform) \
     .add_mapping("status", "status") \
     .add_mapping("created_at", "created_at") \
     .add_mapping("updated_at", "updated_at") \
     .add_filter("{status} != 'deleted'")
    
    orders_rule = TransformationRule(
        name="orders_transform",
        source_table="orders",
        target_table="orders"
    ).add_mapping("id", "id") \
     .add_mapping("user_id", "user_id") \
     .add_mapping("product_id", "product_id") \
     .add_mapping("amount", "amount") \
     .add_mapping("order_date", "order_date") \
     .add_mapping("status", "status", uppercase_transform) \
     .add_filter("{amount} > 0")
    
    products_rule = TransformationRule(
        name="products_transform",
        source_table="products",
        target_table="products"
    ).add_mapping("id", "id") \
     .add_mapping("name", "name", uppercase_transform) \
     .add_mapping("description", "description") \
     .add_mapping("price", "price") \
     .add_mapping("category_id", "category_id") \
     .add_mapping("in_stock", "in_stock")
    
    categories_rule = TransformationRule(
        name="categories_transform",
        source_table="categories",
        target_table="categories"
    ).add_mapping("id", "id") \
     .add_mapping("name", "name", uppercase_transform) \
     .add_mapping("description", "description") \
     .add_mapping("parent_id", "parent_id")
    
    # 4. Create ETL pipeline
    pipeline = ETLPipeline(
        name="sample_warehouse_pipeline"
    ).add_source(source) \
     .add_target_table(users_table) \
     .add_target_table(orders_table) \
     .add_target_table(products_table) \
     .add_target_table(categories_table) \
     .add_transformation_rule(users_rule) \
     .add_transformation_rule(orders_rule) \
     .add_transformation_rule(products_rule) \
     .add_transformation_rule(categories_rule)
    
    return pipeline


def main():
    """Main function to create and save sample configuration"""
    
    # Create sample pipeline
    pipeline = create_sample_pipeline()
    
    # Save configuration
    config_manager = ETLConfigManager("configs")
    config_file = config_manager.save_pipeline(pipeline, "sample_pipeline.json")
    
    print(f"Sample pipeline configuration saved to: {config_file}")
    
    # Also save as YAML
    yaml_file = config_manager.save_pipeline_yaml(pipeline, "sample_pipeline.yaml")
    print(f"Sample pipeline configuration (YAML) saved to: {yaml_file}")
    
    # Validate configuration
    from etl.utils import ETLValidator
    validator = ETLValidator()
    errors = validator.validate_pipeline(pipeline)
    
    if errors:
        print("Validation errors:")
        for error in errors:
            print(f"  - {error}")
    else:
        print("Configuration is valid!")
    
    # Show pipeline summary
    print(f"\nPipeline Summary:")
    print(f"  Name: {pipeline.name}")
    print(f"  Sources: {len(pipeline.sources)}")
    print(f"  Target Tables: {len(pipeline.target_tables)}")
    print(f"  Transformation Rules: {len(pipeline.transformation_rules)}")
    
    for source in pipeline.sources:
        print(f"    Source: {source.name} ({source.host}:{source.port})")
        print(f"      Tables: {', '.join(source.tables)}")
    
    for table in pipeline.target_tables:
        print(f"    Table: {table.database}.{table.name}")
        print(f"      Fields: {len(table.fields)}")


if __name__ == "__main__":
    main()
