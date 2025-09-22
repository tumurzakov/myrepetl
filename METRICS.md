# Prometheus Metrics Documentation

## Обзор

Система ETL теперь включает в себя комплексные Prometheus метрики для мониторинга всех внутренних процессов. Метрики предоставляются через HTTP endpoint `/metrics` на порту 8080 (по умолчанию).

## Endpoints

- **`/metrics`** - Prometheus метрики в формате OpenMetrics
- **`/health`** - Health check endpoint

## Метрики

### 1. Init Query Metrics

#### `etl_init_records_sent_total`
- **Тип**: Counter
- **Описание**: Общее количество init записей отправлено из каждой таблицы source
- **Метки**: `source_name`, `table_name`, `mapping_key`

#### `etl_init_batch_size`
- **Тип**: Histogram
- **Описание**: Размер батчей init запросов
- **Метки**: `source_name`, `table_name`
- **Buckets**: [1, 5, 10, 25, 50, 100, 250, 500, 1000, 2500, 5000, 10000]

#### `etl_init_query_duration_seconds`
- **Тип**: Histogram
- **Описание**: Время выполнения init запросов
- **Метки**: `source_name`, `table_name`, `mapping_key`
- **Buckets**: [0.1, 0.5, 1.0, 2.5, 5.0, 10.0, 25.0, 50.0, 100.0, 250.0, 500.0]

#### `etl_init_thread_status`
- **Тип**: Gauge
- **Описание**: Статус init query потоков (1=running, 0=stopped)
- **Метки**: `mapping_key`, `source_name`, `target_name`

### 2. Queue Metrics

#### `etl_message_queue_size`
- **Тип**: Gauge
- **Описание**: Текущий размер очереди сообщений

#### `etl_message_queue_max_size`
- **Тип**: Gauge
- **Описание**: Максимальный размер очереди сообщений

#### `etl_message_queue_usage_percent`
- **Тип**: Gauge
- **Описание**: Использование очереди в процентах

#### `etl_message_queue_overflows_total`
- **Тип**: Counter
- **Описание**: Общее количество переполнений очереди сообщений

### 3. Target Metrics

#### `etl_target_records_received_total`
- **Тип**: Counter
- **Описание**: Общее количество записей полученных таргетами
- **Метки**: `target_name`, `event_type`

#### `etl_target_batch_size`
- **Тип**: Histogram
- **Описание**: Размер батчей таргетов
- **Метки**: `target_name`, `table_name`
- **Buckets**: [1, 5, 10, 25, 50, 100, 250, 500, 1000, 2500, 5000, 10000]

#### `etl_target_records_written_total`
- **Тип**: Counter
- **Описание**: Общее количество записей записанных в таблицы таргетов
- **Метки**: `target_name`, `table_name`, `operation_type`

#### `etl_target_write_duration_seconds`
- **Тип**: Histogram
- **Описание**: Время выполнения операций записи
- **Метки**: `target_name`, `table_name`, `operation_type`
- **Buckets**: [0.001, 0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1.0, 2.5, 5.0, 10.0]

#### `etl_target_thread_status`
- **Тип**: Gauge
- **Описание**: Статус target потоков (1=running, 0=stopped)
- **Метки**: `target_name`

#### `etl_target_queue_size`
- **Тип**: Gauge
- **Описание**: Текущий размер очереди target потоков
- **Метки**: `target_name`

### 4. Connection Metrics

#### `etl_database_reconnections_total`
- **Тип**: Counter
- **Описание**: Общее количество переподключений к БД
- **Метки**: `connection_name`, `connection_type`

#### `etl_database_connection_status`
- **Тип**: Gauge
- **Описание**: Статус соединений с БД (1=connected, 0=disconnected)
- **Метки**: `connection_name`, `connection_type`

#### `etl_database_last_activity_timestamp`
- **Тип**: Gauge
- **Описание**: Время последней активности соединения
- **Метки**: `connection_name`, `connection_type`

### 5. Replication Metrics

#### `etl_replication_connection_status`
- **Тип**: Gauge
- **Описание**: Статус соединений репликации (1=connected, 0=disconnected)
- **Метки**: `source_name`

#### `etl_replication_records_received_total`
- **Тип**: Counter
- **Описание**: Общее количество записей полученных по репликации
- **Метки**: `source_name`, `event_type`

#### `etl_replication_batch_size`
- **Тип**: Histogram
- **Описание**: Размер батчей репликации
- **Метки**: `source_name`
- **Buckets**: [1, 5, 10, 25, 50, 100, 250, 500, 1000, 2500, 5000, 10000]

#### `etl_replication_records_processed_total`
- **Тип**: Counter
- **Описание**: Общее количество записей репликации обработанных
- **Метки**: `source_name`, `event_type`

#### `etl_replication_events_by_type_total`
- **Тип**: Counter
- **Описание**: Общее количество событий репликации по типам
- **Метки**: `source_name`, `event_type`

#### `etl_replication_event_duration_seconds`
- **Тип**: Histogram
- **Описание**: Время обработки событий репликации
- **Метки**: `source_name`, `event_type`
- **Buckets**: [0.001, 0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1.0, 2.5, 5.0, 10.0]

### 6. Error Metrics

#### `etl_errors_total`
- **Тип**: Counter
- **Описание**: Общее количество ошибок
- **Метки**: `error_type`, `component`, `source_name`, `target_name`

### 7. Performance Metrics

#### `etl_throughput_records_per_second`
- **Тип**: Gauge
- **Описание**: Пропускная способность (записей в секунду)
- **Метки**: `component`, `source_name`, `target_name`

#### `etl_processing_latency_seconds`
- **Тип**: Histogram
- **Описание**: Задержка обработки от источника до таргета
- **Метки**: `source_name`, `target_name`, `table_name`
- **Buckets**: [0.001, 0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1.0, 2.5, 5.0, 10.0, 25.0, 50.0, 100.0]

### 8. System Metrics

#### `etl_system_info`
- **Тип**: Info
- **Описание**: Информация о системе ETL

#### `etl_system_uptime_seconds`
- **Тип**: Gauge
- **Описание**: Время работы системы в секундах

#### `etl_active_threads`
- **Тип**: Gauge
- **Описание**: Количество активных потоков
- **Метки**: `thread_type`

## Конфигурация

### Порт метрик

Порт для метрик можно настроить в конфигурации ETL:

```json
{
  "metrics_port": 8080
}
```

По умолчанию используется порт 8080.

## Примеры запросов Prometheus

### Мониторинг очереди
```promql
# Использование очереди в процентах
etl_message_queue_usage_percent

# Переполнения очереди
rate(etl_message_queue_overflows_total[5m])
```

### Мониторинг производительности
```promql
# Пропускная способность
etl_throughput_records_per_second

# Задержка обработки
histogram_quantile(0.95, rate(etl_processing_latency_seconds_bucket[5m]))
```

### Мониторинг ошибок
```promql
# Количество ошибок по типам
rate(etl_errors_total[5m])

# Ошибки по компонентам
sum(rate(etl_errors_total[5m])) by (component)
```

### Мониторинг соединений
```promql
# Статус соединений
etl_database_connection_status

# Переподключения
rate(etl_database_reconnections_total[5m])
```

### Мониторинг потоков
```promql
# Активные потоки
etl_active_threads

# Статус init потоков
etl_init_thread_status

# Статус target потоков
etl_target_thread_status
```

## Grafana Dashboard

Рекомендуется создать Grafana dashboard с панелями для:

1. **Обзор системы**
   - Время работы
   - Количество активных потоков
   - Статус соединений

2. **Производительность**
   - Пропускная способность
   - Задержка обработки
   - Размеры батчей

3. **Очереди**
   - Использование очереди
   - Переполнения
   - Размеры очередей

4. **Ошибки**
   - Количество ошибок
   - Типы ошибок
   - Компоненты с ошибками

5. **Репликация**
   - События по типам
   - Статус соединений репликации
   - Обработанные записи

## Алерты

Рекомендуемые алерты:

```yaml
# Высокое использование очереди
- alert: HighQueueUsage
  expr: etl_message_queue_usage_percent > 80
  for: 2m
  labels:
    severity: warning
  annotations:
    summary: "High message queue usage"

# Переполнения очереди
- alert: QueueOverflows
  expr: rate(etl_errors_total{error_type="queue_overflow"}[5m]) > 0
  for: 1m
  labels:
    severity: critical
  annotations:
    summary: "Message queue overflows detected"

# Потеря соединения
- alert: DatabaseConnectionLost
  expr: etl_database_connection_status == 0
  for: 1m
  labels:
    severity: critical
  annotations:
    summary: "Database connection lost"

# Высокая задержка
- alert: HighProcessingLatency
  expr: histogram_quantile(0.95, rate(etl_processing_latency_seconds_bucket[5m])) > 10
  for: 5m
  labels:
    severity: warning
  annotations:
    summary: "High processing latency detected"
```

## Health Check Endpoint

Система предоставляет comprehensive health check endpoint по адресу `/health`, который возвращает детальную информацию о состоянии системы, включая статус потоков, соединения с базами данных, статус репликации и состояние очереди.

### Использование

```bash
curl http://localhost:8080/health
```

### Формат ответа

Health endpoint возвращает JSON ответ со следующей структурой:

```json
{
  "status": "healthy|warning|critical|unhealthy",
  "timestamp": 1758571336.1312053,
  "uptime_seconds": 3600.5,
  "components": {
    "threads": {
      "init_threads": {
        "status": "healthy|warning|error",
        "count": 2,
        "total": 3,
        "details": [
          {
            "source": "source1",
            "target": "target1", 
            "mapping": "source1.users",
            "status": "running|stopped"
          }
        ]
      },
      "target_threads": {
        "status": "healthy|warning|error",
        "count": 1,
        "total": 1,
        "details": [
          {
            "target": "target1",
            "status": "running|stopped"
          }
        ]
      },
      "source_threads": {
        "status": "healthy|warning|error",
        "count": 1,
        "total": 1,
        "details": [
          {
            "source": "source1",
            "status": "running|stopped"
          }
        ]
      }
    },
    "database_connections": {
      "sources": {
        "status": "healthy|warning|error",
        "total": 2,
        "connected": 2,
        "connections": [
          {
            "name": "source1",
            "status": "connected|disconnected"
          }
        ]
      },
      "targets": {
        "status": "healthy|warning|error",
        "total": 1,
        "connected": 1,
        "connections": [
          {
            "name": "target1",
            "status": "connected|disconnected"
          }
        ]
      }
    },
    "replication_connections": {
      "status": "healthy|warning|error",
      "total": 1,
      "connected": 1,
      "connections": [
        {
          "source": "source1",
          "status": "connected|disconnected"
        }
      ]
    },
    "message_queue": {
      "status": "healthy|warning|critical|error",
      "size": 150,
      "max_size": 50000,
      "usage_percent": 0.3
    }
  }
}
```

### HTTP коды ответов

- **200 OK**: Система здорова или имеет предупреждения
- **503 Service Unavailable**: Система критична или нездорова

### Значения статусов

#### Общий статус
- **healthy**: Все компоненты функционируют нормально
- **warning**: Некоторые компоненты имеют проблемы, но система работает
- **critical**: Обнаружены критические проблемы (например, очередь >90% заполнена)
- **unhealthy**: Система имеет ошибки и может не функционировать

#### Статус компонентов
- **healthy**: Компонент функционирует нормально
- **warning**: Компонент имеет незначительные проблемы, но работает
- **error**: Компонент имеет ошибки или не функционирует
- **critical**: Компонент имеет критические проблемы (использование очереди >90%)

### Интеграция с мониторингом

Health endpoint может использоваться с системами мониторинга:

- **Prometheus**: Используйте метрику `up` с health endpoint
- **Grafana**: Создавайте health дашборды используя JSON ответ
- **AlertManager**: Настройте алерты на основе кодов ответов и содержимого
- **Kubernetes**: Используйте как liveness/readiness probe
