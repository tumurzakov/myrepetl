# Логирование в MyRepETL

## Обзор

В MyRepETL реализовано детальное структурированное логирование для отслеживания всех операций ETL-процесса. Логирование использует библиотеку `structlog` для создания структурированных JSON-логов с контекстной информацией.

## Уровни логирования

- **DEBUG**: Детальная информация для отладки (трансформации, конвертация событий)
- **INFO**: Основные операции (обработка событий, выполнение SQL)
- **WARNING**: Предупреждения (события отфильтрованы, функции трансформации не найдены)
- **ERROR**: Ошибки (ошибки подключения, ошибки выполнения SQL)

## Структура логов

### Базовые поля

Все логи содержат следующие базовые поля:
- `timestamp`: Время события в ISO формате
- `level`: Уровень логирования
- `logger`: Имя логгера
- `event`: Сообщение события

### Контекстные поля

В зависимости от типа операции добавляются дополнительные поля:

#### События источника (Source Events)
- `source_name`: Имя источника данных
- `event_type`: Тип события (WriteRowsEvent, UpdateRowsEvent, DeleteRowsEvent)
- `schema`: Схема базы данных
- `table`: Имя таблицы
- `log_pos`: Позиция в binlog
- `server_id`: ID сервера MySQL
- `rows_count`: Количество строк в событии

#### События трансформации (Transform Events)
- `source_table`: Исходная таблица
- `columns_count`: Количество колонок для трансформации
- `transformations_applied`: Количество примененных трансформаций
- `static_values_applied`: Количество статических значений
- `values_copied`: Количество скопированных значений
- `source_column`: Исходная колонка
- `target_column`: Целевая колонка
- `transform`: Название функции трансформации
- `original_value`: Исходное значение
- `transformed_value`: Трансформированное значение

#### Операции на таргете (Target Operations)
- `target_name`: Имя целевой базы данных
- `operation_id`: Уникальный ID операции (8 символов)
- `event_id`: ID события из binlog
- `sql`: SQL запрос
- `values_count`: Количество параметров
- `affected_rows`: Количество затронутых строк
- `original_data`: Исходные данные
- `transformed_data`: Трансформированные данные
- `filter`: Настройки фильтрации

## Примеры логов

### Обработка INSERT события

```json
{
  "timestamp": "2024-01-15T10:30:45.123456Z",
  "level": "info",
  "logger": "target_thread_service",
  "event": "Processing INSERT event",
  "operation_id": "a1b2c3d4",
  "event_id": "e5f6g7h8",
  "table": "users",
  "schema": "myapp",
  "source_name": "mysql_source",
  "target_name": "postgres_target",
  "timestamp": 1705312245,
  "log_pos": 12345,
  "server_id": 1,
  "original_data": {"id": 1, "name": "John", "email": "john@example.com"}
}
```

### Трансформация данных

```json
{
  "timestamp": "2024-01-15T10:30:45.124000Z",
  "level": "info",
  "logger": "transform_service",
  "event": "Starting column transformations",
  "source_table": "myapp.users",
  "columns_count": 3,
  "row_keys": ["id", "name", "email"]
}
```

### Выполнение SQL

```json
{
  "timestamp": "2024-01-15T10:30:45.125000Z",
  "level": "info",
  "logger": "target_thread_service",
  "event": "Executing UPSERT for INSERT",
  "operation_id": "a1b2c3d4",
  "sql": "INSERT INTO users (id, name, email) VALUES (%s, %s, %s) ON CONFLICT (id) DO UPDATE SET name = EXCLUDED.name, email = EXCLUDED.email",
  "values_count": 3,
  "target_name": "postgres_target"
}
```

### Успешное завершение операции

```json
{
  "timestamp": "2024-01-15T10:30:45.126000Z",
  "level": "info",
  "logger": "target_thread_service",
  "event": "INSERT processed successfully",
  "operation_id": "a1b2c3d4",
  "original": {"id": 1, "name": "John", "email": "john@example.com"},
  "transformed": {"id": 1, "name": "JOHN", "email": "john@example.com"},
  "sql": "INSERT INTO users (id, name, email) VALUES (%s, %s, %s) ON CONFLICT (id) DO UPDATE SET name = EXCLUDED.name, email = EXCLUDED.email",
  "affected_rows": 1,
  "target_name": "postgres_target"
}
```

## Отслеживание операций

### Operation ID
Каждая операция на таргете получает уникальный `operation_id` (8 символов), который позволяет отслеживать весь жизненный цикл операции от начала до завершения.

### Event ID
Каждое событие из binlog получает уникальный `event_id` (8 символов), который связывает событие с его обработкой.

### Корреляция логов
Используя `operation_id` и `event_id`, можно легко найти все логи, связанные с конкретной операцией:

```bash
# Найти все логи для операции a1b2c3d4
grep "a1b2c3d4" /var/log/myrepetl/*.log

# Найти все логи для события e5f6g7h8
grep "e5f6g7h8" /var/log/myrepetl/*.log
```

## Настройка логирования

### Уровень логирования
Установите уровень логирования через переменную окружения:
```bash
export LOG_LEVEL=DEBUG  # DEBUG, INFO, WARNING, ERROR
```

### Формат вывода
```bash
export LOG_FORMAT=json  # json или console
```

### Пример конфигурации
```python
from src.utils.logger import setup_logging

# Настройка логирования
setup_logging(level="INFO", format_type="json")
```

## Мониторинг и алертинг

### Ключевые метрики для мониторинга
- `events_processed`: Количество обработанных событий
- `errors_count`: Количество ошибок
- `affected_rows`: Количество затронутых строк в БД
- `queue_size`: Размер очереди событий

### Алерты
Настройте алерты на:
- Ошибки уровня ERROR
- Высокое количество ошибок в статистике
- Остановку потоков обработки
- Ошибки подключения к базам данных

## Производительность

### Оптимизация логов
- DEBUG логи могут быть отключены в продакшене
- Используйте ротацию логов для управления размером файлов
- Рассмотрите возможность отправки логов в централизованную систему (ELK, Splunk)

### Пример конфигурации logrotate
```
/var/log/myrepetl/*.log {
    daily
    missingok
    rotate 30
    compress
    delaycompress
    notifempty
    create 644 myrepetl myrepetl
    postrotate
        systemctl reload myrepetl
    endscript
}
```

## Отладка

### Типичные проблемы
1. **События не обрабатываются**: Проверьте логи на ошибки подключения к источнику
2. **Трансформации не применяются**: Проверьте DEBUG логи трансформаций
3. **SQL ошибки**: Проверьте логи выполнения SQL с полными запросами
4. **Фильтрация событий**: Проверьте логи фильтрации

### Полезные команды
```bash
# Показать все ошибки
grep '"level":"error"' /var/log/myrepetl/*.log

# Показать операции конкретной таблицы
grep '"table":"users"' /var/log/myrepetl/*.log

# Показать все операции за последний час
grep "$(date -d '1 hour ago' '+%Y-%m-%dT%H')" /var/log/myrepetl/*.log
```
