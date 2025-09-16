# ETL Tool для MySQL Replication Protocol

Общий инструмент ETL (Extract, Transform, Load) для работы с MySQL replication протоколом на Python.

## Особенности

- **DSL на Python**: Описание источников репликации, таблиц-приемников и правил трансформации через Python DSL
- **MySQL Replication**: Поддержка MySQL binlog replication для real-time обработки данных
- **Гибкие трансформации**: Настраиваемые правила трансформации данных с поддержкой фильтров
- **Множественные источники**: Поддержка нескольких источников репликации одновременно
- **Автоматическое создание таблиц**: Автоматическое создание таблиц-приемников на основе схемы
- **Мониторинг**: Встроенная статистика и мониторинг процесса ETL

## Структура проекта

```
etl/
├── __init__.py              # Основной модуль
├── dsl.py                   # DSL для описания ETL конфигурации
├── replication.py           # MySQL replication клиент
├── engine.py                # Основной ETL движок
├── requirements.txt         # Зависимости
├── examples/                # Примеры использования
│   ├── basic_example.py     # Базовый пример
│   └── advanced_example.py  # Продвинутый пример
└── README.md               # Документация
```

## Установка

1. Установите зависимости:
```bash
pip install -r requirements.txt
```

2. Убедитесь, что MySQL настроен для репликации:
```sql
-- На источнике данных
GRANT REPLICATION SLAVE ON *.* TO 'replication_user'@'%' IDENTIFIED BY 'password';
FLUSH PRIVILEGES;

-- Включите binlog с полными метаданными
SET GLOBAL binlog_format = 'ROW';
SET GLOBAL log_bin = ON;
SET GLOBAL binlog_row_metadata = 'FULL';
SET GLOBAL binlog_row_image = 'FULL';
```

Или в my.cnf:
```ini
[mysqld]
server-id = 1
log-bin = mysql-bin
binlog-format = ROW
binlog-row-metadata = FULL
binlog-row-image = FULL
gtid-mode = ON
enforce-gtid-consistency = ON
```

## Быстрый старт

### 1. Описание источника репликации

```python
from etl.dsl import ReplicationSource

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
```

### 2. Описание таблиц-приемников

```python
from etl.dsl import TargetTable, FieldType

users_table = TargetTable(
    name="users",
    database="target_db"
).add_field("name", FieldType.VARCHAR, length=255) \
 .add_field("email", FieldType.VARCHAR, length=255) \
 .add_field("status", FieldType.VARCHAR, length=50)
```

### 3. Описание правил трансформации

```python
from etl.dsl import TransformationRule, FieldMapping, OperationType
from etl.dsl import uppercase_transform, lowercase_transform

users_rule = TransformationRule(
    name="users_transform",
    source_table="users",
    target_table="users"
).add_mapping("id", "id") \
 .add_mapping("name", "name", uppercase_transform) \
 .add_mapping("email", "email", lowercase_transform) \
 .add_mapping("status", "status")
```

### 4. Создание и запуск ETL пайплайна

```python
from etl.dsl import ETLPipeline
from etl.engine import ETLEngine

# Создание пайплайна
pipeline = ETLPipeline(
    name="my_etl_pipeline"
).add_source(source) \
 .add_target_table(users_table) \
 .add_transformation_rule(users_rule)

# Конфигурация целевой базы данных
target_db_config = {
    "host": "localhost",
    "port": 3306,
    "user": "target_user",
    "password": "target_password",
    "database": "target_db"
}

# Создание и запуск ETL движка
etl_engine = ETLEngine(pipeline, target_db_config)
etl_engine.start()
```

## DSL Reference

### ReplicationSource

Описание источника MySQL репликации:

```python
ReplicationSource(
    name: str,                    # Уникальное имя источника
    host: str,                    # Хост MySQL сервера
    port: int = 3306,            # Порт MySQL сервера
    user: str = "replication",    # Пользователь для репликации
    password: str = "",           # Пароль
    database: str = "",           # База данных (опционально)
    tables: List[str] = [],       # Список таблиц для репликации
    server_id: int = 1,          # Server ID для репликации
    auto_position: bool = True,   # Автоматическое позиционирование
    charset: str = "utf8mb4"      # Кодировка
)
```

### TargetTable

Описание таблицы-приемника:

```python
TargetTable(
    name: str,                    # Имя таблицы
    database: str,                # База данных
    fields: List[FieldDefinition] = [],  # Поля таблицы
    indexes: List[str] = [],      # Индексы
    engine: str = "InnoDB",       # Движок таблицы
    charset: str = "utf8mb4"      # Кодировка
)
```

#### FieldDefinition

```python
FieldDefinition(
    name: str,                    # Имя поля
    field_type: FieldType,        # Тип поля
    length: Optional[int] = None, # Длина поля
    nullable: bool = True,        # Может быть NULL
    default_value: Any = None,    # Значение по умолчанию
    primary_key: bool = False,    # Первичный ключ
    auto_increment: bool = False  # Автоинкремент
)
```

#### FieldType

Доступные типы полей:
- `FieldType.INTEGER` - Целое число
- `FieldType.VARCHAR` - Строка переменной длины
- `FieldType.TEXT` - Текст
- `FieldType.DATETIME` - Дата и время
- `FieldType.DECIMAL` - Десятичное число
- `FieldType.BOOLEAN` - Логическое значение
- `FieldType.JSON` - JSON данные

### TransformationRule

Правило трансформации данных:

```python
TransformationRule(
    name: str,                    # Уникальное имя правила
    source_table: str,            # Исходная таблица
    target_table: str,            # Целевая таблица
    field_mappings: List[FieldMapping] = [],  # Маппинг полей
    filters: List[str] = [],      # Фильтры
    operations: List[OperationType] = [INSERT, UPDATE, DELETE]  # Операции
)
```

#### FieldMapping

```python
FieldMapping(
    source_field: str,            # Поле источника
    target_field: str,            # Поле приемника
    transformation: Optional[Callable] = None  # Функция трансформации
)
```

#### OperationType

Типы операций:
- `OperationType.INSERT` - Вставка
- `OperationType.UPDATE` - Обновление
- `OperationType.DELETE` - Удаление

## Встроенные трансформации

```python
from etl.dsl import (
    uppercase_transform,    # Преобразование в верхний регистр
    lowercase_transform,    # Преобразование в нижний регистр
    trim_transform,         # Удаление пробелов
    datetime_transform,     # Трансформация даты/времени
    json_transform          # Трансформация JSON
)
```

## Кастомные трансформации

```python
def custom_transform(value):
    """Кастомная функция трансформации"""
    if value is None:
        return None
    # Ваша логика трансформации
    return transformed_value

# Использование в маппинге
.add_mapping("field", "target_field", custom_transform)
```

## Фильтры

Фильтры позволяют исключать определенные записи из обработки:

```python
rule.add_filter("{status} != 'deleted'")  # Исключить удаленные записи
rule.add_filter("{amount} > 0")           # Только положительные суммы
rule.add_filter("{user_id} IS NOT NULL")  # Только записи с user_id
```

## Мониторинг и статистика

```python
# Получение статуса ETL движка
status = etl_engine.get_status()

# Статистика обработки
stats = status["stats"]
print(f"Обработано событий: {stats['events_processed']}")
print(f"Ошибок: {stats['events_failed']}")
print(f"Вставлено записей: {stats['rows_inserted']}")
print(f"Обновлено записей: {stats['rows_updated']}")
print(f"Удалено записей: {stats['rows_deleted']}")

# Статус репликации
replication_status = status["replication"]
for source_name, client_status in replication_status["clients"].items():
    print(f"Источник {source_name}: {client_status['is_running']}")
```

## Сохранение и загрузка конфигурации

```python
# Сохранение конфигурации в JSON
pipeline_json = pipeline.to_json(indent=2)
with open("pipeline_config.json", "w") as f:
    f.write(pipeline_json)

# Загрузка конфигурации из JSON
with open("pipeline_config.json", "r") as f:
    pipeline = ETLPipeline.from_json(f.read())
```

## Примеры

### Базовый пример

См. `examples/basic_example.py` для простого примера настройки ETL пайплайна.

### Продвинутый пример

См. `examples/advanced_example.py` для примера с:
- Множественными источниками
- Сложными трансформациями
- Фильтрами
- JSON полями
- Кастомными функциями трансформации

## Требования

- Python 3.8+
- MySQL 5.7+ с включенным binlog
- Права на репликацию в MySQL

## Зависимости

- `pymysql` - MySQL клиент
- `pymysql-replication` - MySQL replication протокол
- `pandas` - Обработка данных
- `pyyaml` - Конфигурационные файлы

## Лицензия

MIT License

## Поддержка

Для вопросов и предложений создавайте issues в репозитории проекта.
