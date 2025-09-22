# Init Batch Processing

## Обзор

Init Batch Processing - это функциональность для эффективной обработки init событий (событий инициализации) в пакетном режиме. Это позволяет значительно улучшить производительность при загрузке больших объемов данных во время инициализации.

## Как это работает

### Отдельная batch обработка для Init событий

В отличие от обычных binlog событий, init события имеют отдельную систему batch обработки:

1. **Отдельный накопитель**: `_init_batch_accumulator` - накапливает init события по таблицам
2. **Отдельные настройки**: Использует те же `batch_size` и `batch_flush_interval`, что и обычные события
3. **Отдельная обработка**: `_process_init_batch_events()` - обрабатывает batch init событий

### Процесс обработки

1. **Накопление**: Init события добавляются в `_init_batch_accumulator` по ключу `source_name.target_table`
2. **Флаш по размеру**: Когда batch достигает `batch_size`, он автоматически обрабатывается
3. **Флаш по времени**: Каждые `batch_flush_interval` секунд все накопленные batch'и обрабатываются
4. **Batch UPSERT**: Используется `SQLBuilder.build_batch_upsert_sql()` для эффективной вставки/обновления

## Конфигурация

### Настройки batch обработки

```json
{
  "targets": {
    "target1": {
      "host": "localhost",
      "port": 3306,
      "user": "target_user",
      "password": "password",
      "database": "target_db",
      "batch_size": 50,           // Размер batch для init событий
      "batch_flush_interval": 3.0 // Интервал флаша в секундах
    }
  }
}
```

### Пример конфигурации

См. `examples/init_batch_config_example.json` для полного примера конфигурации.

## Логирование

### Init Batch логи

Теперь в логах init секции вы увидите:

```
INFO: Init event added to batch table_key=source1.users current_batch_size=1 batch_size_limit=50
INFO: Init batch size limit reached, processing batch table_key=source1.users table_info=source1.users batch_size=50
INFO: Processing init batch events operation_id=abc12345 table_key=source1.users target_table=users total_events=50 batch_size=50
INFO: Executing init batch UPSERT operation_id=abc12345 target_table=users batch_size=50 primary_keys=[1,2,3...] total_primary_keys=50
INFO: Init batch events processed successfully operation_id=abc12345 table_key=source1.users table_info=source1.users target_table=users total_events=50 batch_size=50 affected_rows=50
```

### Статистика

В статистике target thread теперь доступны:

- `init_batch_operations` - количество batch операций для init событий
- `init_batch_records_processed` - количество обработанных записей в init batch'ах

## Преимущества

1. **Производительность**: Batch операции значительно быстрее отдельных UPSERT'ов
2. **Консистентность**: Init события теперь обрабатываются так же эффективно, как и binlog события
3. **Мониторинг**: Подробное логирование и статистика для отслеживания производительности
4. **Гибкость**: Те же настройки `batch_size` и `batch_flush_interval` для init событий

## Тестирование

Созданы comprehensive тесты для init batch функциональности:

- `test_init_batch_processing_initialization` - инициализация
- `test_get_init_table_key` - генерация ключей таблиц
- `test_add_to_init_batch` - добавление в batch
- `test_should_flush_init_batches_*` - условия флаша
- `test_flush_all_init_batches` - флаш всех batch'ей
- `test_init_batch_statistics` - статистика

Запуск тестов:
```bash
pytest tests/unit/test_target_thread_service.py -k "init_batch" -v
```

## Совместимость

- ✅ Обратная совместимость: существующие конфигурации работают без изменений
- ✅ Те же настройки: `batch_size` и `batch_flush_interval` используются для init событий
- ✅ Логирование: подробные логи для мониторинга и отладки
- ✅ Статистика: полная статистика для анализа производительности
