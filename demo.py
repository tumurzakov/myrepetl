#!/usr/bin/env python3
"""
ETL Tool Demo

Демонстрационный скрипт для показа возможностей ETL Tool.
"""

import sys
import os
import time
import logging
from pathlib import Path

# Add parent directory to path to import ETL modules
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from dsl import (
    ETLPipeline,
    ReplicationSource,
    TargetTable,
    TransformationRule,
    FieldType,
    OperationType,
    uppercase_transform,
    lowercase_transform
)
from utils import ETLConfigManager, ETLValidator, setup_logging


def create_demo_pipeline():
    """Создать демонстрационный ETL пайплайн"""
    
    print("🔧 Создание демонстрационного ETL пайплайна...")
    
    # 1. Источник репликации
    source = ReplicationSource(
        name="demo_source",
        host="localhost",
        port=3306,
        user="replication_user",
        password="replication_password",
        database="demo_source_db",
        tables=["users", "orders", "products"],
        server_id=100
    )
    
    # 2. Таблицы-приемники
    users_table = TargetTable(
        name="users",
        database="demo_target_db"
    ).add_field("name", FieldType.VARCHAR, length=255) \
     .add_field("email", FieldType.VARCHAR, length=255) \
     .add_field("status", FieldType.VARCHAR, length=50) \
     .add_field("created_at", FieldType.DATETIME)
    
    orders_table = TargetTable(
        name="orders",
        database="demo_target_db"
    ).add_field("user_id", FieldType.INTEGER) \
     .add_field("product_id", FieldType.INTEGER) \
     .add_field("amount", FieldType.DECIMAL, length="10,2") \
     .add_field("order_date", FieldType.DATETIME) \
     .add_field("status", FieldType.VARCHAR, length=50)
    
    products_table = TargetTable(
        name="products",
        database="demo_target_db"
    ).add_field("name", FieldType.VARCHAR, length=255) \
     .add_field("description", FieldType.TEXT) \
     .add_field("price", FieldType.DECIMAL, length="10,2") \
     .add_field("category", FieldType.VARCHAR, length=100)
    
    # 3. Правила трансформации
    users_rule = TransformationRule(
        name="users_transform",
        source_table="users",
        target_table="users"
    ).add_mapping("id", "id") \
     .add_mapping("name", "name", uppercase_transform) \
     .add_mapping("email", "email", lowercase_transform) \
     .add_mapping("status", "status") \
     .add_mapping("created_at", "created_at") \
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
     .add_mapping("category", "category", uppercase_transform)
    
    # 4. ETL пайплайн
    pipeline = ETLPipeline(
        name="demo_etl_pipeline"
    ).add_source(source) \
     .add_target_table(users_table) \
     .add_target_table(orders_table) \
     .add_target_table(products_table) \
     .add_transformation_rule(users_rule) \
     .add_transformation_rule(orders_rule) \
     .add_transformation_rule(products_rule)
    
    print("✅ ETL пайплайн создан успешно!")
    return pipeline


def validate_pipeline(pipeline):
    """Валидация ETL пайплайна"""
    
    print("🔍 Валидация ETL пайплайна...")
    
    validator = ETLValidator()
    errors = validator.validate_pipeline(pipeline)
    
    if errors:
        print("❌ Ошибки валидации:")
        for error in errors:
            print(f"   - {error}")
        return False
    else:
        print("✅ Валидация прошла успешно!")
        return True


def save_pipeline(pipeline):
    """Сохранение ETL пайплайна"""
    
    print("💾 Сохранение ETL пайплайна...")
    
    # Создание директории для конфигураций
    config_dir = Path("configs")
    config_dir.mkdir(exist_ok=True)
    
    config_manager = ETLConfigManager(str(config_dir))
    
    # Сохранение в JSON
    json_file = config_manager.save_pipeline(pipeline, "demo_pipeline.json")
    print(f"   📄 JSON: {json_file}")
    
    # Сохранение в YAML
    yaml_file = config_manager.save_pipeline_yaml(pipeline, "demo_pipeline.yaml")
    print(f"   📄 YAML: {yaml_file}")
    
    print("✅ Конфигурация сохранена!")


def show_pipeline_info(pipeline):
    """Показать информацию о пайплайне"""
    
    print("\n📊 Информация о ETL пайплайне:")
    print(f"   Название: {pipeline.name}")
    print(f"   Источники: {len(pipeline.sources)}")
    print(f"   Таблицы-приемники: {len(pipeline.target_tables)}")
    print(f"   Правила трансформации: {len(pipeline.transformation_rules)}")
    
    print("\n🔗 Источники репликации:")
    for source in pipeline.sources:
        print(f"   - {source.name}: {source.host}:{source.port}")
        print(f"     База данных: {source.database}")
        print(f"     Таблицы: {', '.join(source.tables)}")
    
    print("\n📋 Таблицы-приемники:")
    for table in pipeline.target_tables:
        print(f"   - {table.database}.{table.name}")
        print(f"     Поля: {len(table.fields)}")
        for field in table.fields:
            print(f"       • {field.name} ({field.field_type.value})")
    
    print("\n🔄 Правила трансформации:")
    for rule in pipeline.transformation_rules:
        print(f"   - {rule.name}")
        print(f"     {rule.source_table} → {rule.target_table}")
        print(f"     Маппинги: {len(rule.field_mappings)}")
        print(f"     Фильтры: {len(rule.filters)}")


def show_usage_examples():
    """Показать примеры использования"""
    
    print("\n🚀 Примеры использования:")
    
    print("\n1. Запуск через CLI:")
    print("   python -m etl.cli run configs/demo_pipeline.json \\")
    print("       --target-host localhost \\")
    print("       --target-user target_user \\")
    print("       --target-password target_password \\")
    print("       --target-database demo_target_db \\")
    print("       --monitor")
    
    print("\n2. Запуск через Python:")
    print("   from etl.engine import ETLEngine")
    print("   etl_engine = ETLEngine(pipeline, target_db_config)")
    print("   etl_engine.start()")
    
    print("\n3. Docker Compose:")
    print("   docker-compose up -d")
    
    print("\n4. Валидация конфигурации:")
    print("   python -m etl.cli validate configs/demo_pipeline.json")


def main():
    """Основная функция демо"""
    
    print("🎯 ETL Tool Demo")
    print("=" * 50)
    
    # Настройка логирования
    setup_logging("INFO")
    
    try:
        # Создание пайплайна
        pipeline = create_demo_pipeline()
        
        # Валидация
        if not validate_pipeline(pipeline):
            print("❌ Демо завершено с ошибками валидации")
            return
        
        # Сохранение
        save_pipeline(pipeline)
        
        # Информация о пайплайне
        show_pipeline_info(pipeline)
        
        # Примеры использования
        show_usage_examples()
        
        print("\n🎉 Демо завершено успешно!")
        print("\n📚 Для получения дополнительной информации:")
        print("   - Изучите README.md")
        print("   - Посмотрите примеры в папке examples/")
        print("   - Запустите: make help")
        
    except Exception as e:
        print(f"❌ Ошибка в демо: {e}")
        logging.exception("Demo error")
        sys.exit(1)


if __name__ == "__main__":
    main()
