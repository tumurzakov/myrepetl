# Как я построил многопоточный ETL для MySQL репликации и почему это оказалось сложнее, чем казалось

*История о том, как попытка решить простую задачу синхронизации данных превратилась в полноценный ETL-фреймворк с многопоточной архитектурой, message bus и кучей подводных камней.*

## Проблема: когда простые решения не работают

Всё началось с банальной задачи: нужно было синхронизировать данные между несколькими MySQL базами в реальном времени. Казалось бы, что тут сложного? MySQL же умеет репликацию из коробки!

Но реальность оказалась суровее:

- **Множественные источники**: данные приходят из разных баз, каждая со своей схемой
- **Разные целевые системы**: данные нужно отправлять в data warehouse, аналитическую БД и backup-систему
- **Трансформации на лету**: имена таблиц не совпадают, нужны вычисляемые поля, фильтрация
- **Надёжность**: если один источник упал, остальные должны продолжать работать

Стандартная MySQL репликация здесь не подходила — она работает только 1:1. А внешние решения типа Debezium или Maxwell показались избыточными для наших нужд.

В итоге решил написать свой ETL-инструмент. Назвал его **MyRepETL** — MySQL Replication ETL Tool.

## Архитектура: от простого к сложному

### Первая версия: наивный подход

Начал с простого решения — один поток читает binlog, обрабатывает события и пишет в целевую БД:

```python
def simple_etl():
    stream = BinLogStreamReader(connection_settings=source_config)
    for binlog_event in stream:
        # Обработать событие
        process_event(binlog_event)
        # Записать в целевую БД
        write_to_target(transformed_data)
```

Работало, но только для одного источника. Когда добавил второй источник, столкнулся с классической проблемой: **блокировка на `fetchone()`**.

```python
# Проблемный код
for source in sources:
    stream = BinLogStreamReader(connection_settings=source.config)
    for event in stream:  # ← Блокируется здесь!
        process_event(event)
```

Если в первом источнике нет новых событий, второй источник никогда не получит управление. Round-robin не помог — события приходят неравномерно.

### Вторая версия: многопоточность

Понял, что нужна многопоточная архитектура. Каждый источник должен работать в своём потоке:

```python
class SourceThread(threading.Thread):
    def __init__(self, source_name, source_config):
        self.source_name = source_name
        self.source_config = source_config
        self.message_bus = message_bus
        
    def run(self):
        stream = BinLogStreamReader(connection_settings=self.source_config)
        for event in stream:
            # Отправить событие в общую очередь
            self.message_bus.publish(event)
```

Но тут возникла новая проблема: **как координировать потоки?** Нужна была система обмена сообщениями между потоками.

### Финальная архитектура: Message Bus + Thread Manager

В итоге получилась такая архитектура:

```
┌─────────────────────────────────────────────────────────────┐
│                    ETLService                               │
│  ┌─────────────┐  ┌─────────────┐  ┌─────────────────────┐ │
│  │ConfigService│  │ThreadManager│  │  Signal Handlers   │ │
│  └─────────────┘  └─────────────┘  └─────────────────────┘ │
└─────────────────────────────────────────────────────────────┘
                              │
                              ▼
┌─────────────────────────────────────────────────────────────┐
│                  ThreadManager                             │
│  ┌─────────────┐  ┌─────────────┐  ┌─────────────────────┐ │
│  │ MessageBus  │  │SourceThreads│  │   TargetThreads     │ │
│  │             │  │             │  │                     │ │
│  │ ┌─────────┐ │  │ ┌─────────┐ │  │ ┌─────────────────┐ │ │
│  │ │Message  │ │  │ │BinLog   │ │  │ │  Event Queue    │ │ │
│  │ │Queue    │ │  │ │Stream   │ │  │ │                 │ │ │
│  │ └─────────┘ │  │ └─────────┘ │  │ │ ┌─────────────┐ │ │ │
│  └─────────────┘  │             │  │ │ │Event        │ │ │ │
│                   │ ┌─────────┐ │  │ │ │Processor    │ │ │ │
│                   │ │BinLog   │ │  │ │ └─────────────┘ │ │ │
│                   │ │Stream   │ │  │ └─────────────────┘ │ │
│                   │ └─────────┘ │  └─────────────────────┘ │
│                   └─────────────┘                          │
└─────────────────────────────────────────────────────────────┘
```

**Ключевые компоненты:**

1. **MessageBus** — thread-safe очередь сообщений между потоками
2. **SourceThreadService** — управляет потоками источников
3. **TargetThreadService** — управляет потоками приёмников
4. **ThreadManager** — координирует все потоки и их жизненный цикл

## Реализация: подводные камни и решения

### Message Bus: не просто очередь

Message Bus оказался не такой простой штукой, как казалось. Нужно было решить несколько проблем:

**1. Thread-safe операции**
```python
class MessageBus:
    def __init__(self, max_queue_size=10000):
        self._message_queue = queue.Queue(maxsize=max_queue_size)
        self._subscribers = {}
        self._subscriber_lock = threading.RLock()
        
    def publish(self, message):
        try:
            self._message_queue.put_nowait(message)
        except queue.Full:
            # Что делать, если очередь переполнена?
            self._stats['messages_dropped'] += 1
```

**2. Graceful shutdown**
```python
def request_shutdown(self):
    # Отправить shutdown сообщение, чтобы разбудить ждущие потоки
    self.publish_shutdown("message_bus")
    with self._shutdown_lock:
        self._shutdown_requested = True
```

**3. Обработка ошибок в подписчиках**
```python
def _process_message(self, message):
    subscribers = self._subscribers.get(message.message_type, [])
    for callback in subscribers:
        try:
            callback(message)
        except Exception as e:
            # Ошибка в одном подписчике не должна ломать других
            self.logger.error("Error in message subscriber", error=str(e))
```

### Управление потоками: когда простота обманчива

ThreadManager выглядит просто, но на деле это самый сложный компонент:

```python
class ThreadManager:
    def start(self, config):
        # Запустить target threads первыми (они должны быть готовы принимать события)
        self._start_target_threads(config)
        
        # Затем source threads
        self._start_source_threads(config)
        
        # И мониторинг
        self._start_monitoring()
```

**Проблема порядка запуска**: если запустить source threads раньше target threads, события будут потеряны.

**Автоматическое восстановление**: если поток источника упал, его нужно перезапустить:

```python
def _check_source_thread_health(self):
    for source_name, stats in source_stats.items():
        if not stats.get('is_running', False):
            self.logger.warning("Source thread not running, restarting", 
                              source_name=source_name)
            # Остановить упавший поток
            self.source_thread_service.stop_source(source_name)
            # Подождать немного
            time.sleep(2.0)
            # Перезапустить
            self.source_thread_service.start_source(source_name, ...)
```

### Обработка binlog событий: детали имеют значение

Чтение binlog событий — это отдельная история. Казалось бы, что тут сложного? Но на практике:

**1. Управление соединениями**
```python
def _connect_to_replication(self):
    # Получить master status с retry
    for attempt in range(max_retries):
        try:
            master_status = self.database_service.get_master_status(self.source_config)
            break
        except Exception as e:
            if "read of closed file" in str(e).lower():
                # Соединение закрылось, попробовать ещё раз
                time.sleep(delay)
```

**2. Обработка разных типов событий**
```python
def _convert_binlog_event(self, binlog_event):
    if isinstance(binlog_event, row_event.WriteRowsEvent):
        return InsertEvent(
            schema=binlog_event.schema,
            table=binlog_event.table,
            values=binlog_event.rows[0]["values"],
            source_name=self.source_name
        )
    elif isinstance(binlog_event, row_event.UpdateRowsEvent):
        row = binlog_event.rows[0]
        return UpdateEvent(
            before_values=row.get("before_values", {}),
            after_values=row.get("after_values", {}),
            # ...
        )
```

**3. Graceful shutdown**
```python
def _process_events(self):
    for binlog_event in stream:
        # Проверить флаг остановки
        if self._is_shutdown_requested():
            break
            
        # Обработать событие
        event = self._convert_binlog_event(binlog_event)
        if event:
            self.message_bus.publish_binlog_event(self.source_name, event)
```

### Трансформации: гибкость vs производительность

Система трансформаций должна быть гибкой, но не тормозить:

```python
def apply_column_transforms(self, row_data, column_mapping, source_table):
    transformed_data = {}
    
    for source_col, target_config in column_mapping.items():
        if target_config.value is not None:
            # Статическое значение
            transformed_data[target_col] = target_config.value
        elif target_config.transform:
            # Применить трансформацию
            transform_func = self.get_transform_function(target_config.transform)
            value = row_data.get(source_col)
            transformed_value = transform_func(value, row_data, source_table)
            transformed_data[target_col] = transformed_value
        else:
            # Простое копирование
            transformed_data[target_col] = row_data.get(source_col)
    
    return transformed_data
```

**Динамическая загрузка модулей трансформаций**:
```python
def load_transform_module(self, module_name="transform", config_dir=None):
    # Попробовать импортировать как модуль
    try:
        self._transform_module = importlib.import_module(module_name)
    except ImportError:
        # Попробовать загрузить из файла
        transform_path = os.path.join(config_dir, f"{module_name}.py")
        spec = importlib.util.spec_from_file_location("transform", transform_path)
        self._transform_module = importlib.util.module_from_spec(spec)
        spec.loader.exec_module(self._transform_module)
```

### Фильтрация: когда SQL недостаточно

Иногда нужна сложная логика фильтрации, которую сложно выразить в SQL:

```python
def apply_filter(self, data, filter_config):
    # Простые условия
    if "status" in filter_config:
        if data.get("status") != filter_config["status"]:
            return False
    
    # Сложные условия
    if "and" in filter_config:
        for condition in filter_config["and"]:
            if not self.apply_filter(data, condition):
                return False
    
    if "or" in filter_config:
        for condition in filter_config["or"]:
            if self.apply_filter(data, condition):
                return True
        return False
    
    return True
```

## Конфигурация: JSON как DSL

Вместо программирования на Python решил сделать конфигурацию через JSON. Получился своеобразный DSL:

```json
{
  "sources": {
    "ecommerce_db": {
      "host": "ecommerce-mysql",
      "user": "repl_user",
      "password": "repl_password",
      "database": "ecommerce"
    }
  },
  "targets": {
    "data_warehouse": {
      "host": "warehouse-mysql",
      "user": "dw_user",
      "password": "dw_password",
      "database": "data_warehouse"
    }
  },
  "mapping": {
    "ecommerce_db.users": {
      "target_table": "data_warehouse.users",
      "primary_key": "id",
      "column_mapping": {
        "id": {"column": "id", "primary_key": true},
        "name": {"column": "name", "transform": "transform.uppercase"},
        "email": {"column": "email", "transform": "transform.lowercase"},
        "source_system": {"column": "source_system", "value": "ecommerce"}
      },
      "filter": {
        "status": {"eq": "active"},
        "age": {"gte": 18}
      }
    }
  }
}
```

**Преимущества**:
- Не нужно перезапускать приложение для изменения конфигурации
- Легко версионировать и деплоить
- Можно генерировать конфигурацию программно

**Недостатки**:
- Сложные трансформации всё равно требуют Python кода
- Валидация конфигурации — отдельная задача

## Мониторинг и логирование: видимость процесса

ETL процесс должен быть прозрачным. Использую структурированное логирование:

```python
import structlog

logger = structlog.get_logger()

# Каждое событие получает уникальный ID
operation_id = str(uuid.uuid4())[:8]

logger.info("Processing INSERT event", 
           operation_id=operation_id,
           table=event.table,
           source_name=event.source_name,
           target_name=self.target_name,
           original_data=event.values)
```

**Статистика в реальном времени**:
```python
def get_stats(self):
    return ServiceStats(
        status=self.get_status(),
        uptime=time.time() - self._start_time,
        sources_count=len(self._source_threads),
        targets_count=len(self._target_threads),
        total_events_processed=total_events,
        total_errors=total_errors,
        message_bus_stats=self.message_bus.get_stats()
    )
```

## Тестирование: как тестировать многопоточный код

Тестирование многопоточного кода — это отдельная боль. Основные подходы:

**1. Unit тесты с моками**
```python
def test_message_bus_publish():
    message_bus = MessageBus()
    message = Message(MessageType.BINLOG_EVENT, "test_source")
    
    result = message_bus.publish(message)
    assert result is True
    assert message_bus.get_queue_size() == 1
```

**2. Интеграционные тесты с реальными БД**
```python
def test_full_pipeline():
    # Запустить ETL с тестовой конфигурацией
    etl_service = ETLService()
    etl_service.initialize("test_config.json")
    etl_service.run_replication()
    
    # Проверить, что данные синхронизировались
    assert check_data_sync()
```

**3. Нагрузочные тесты**
```python
def test_high_load():
    # Сгенерировать много событий
    generate_test_events(10000)
    
    # Проверить, что система справляется
    stats = etl_service.get_stats()
    assert stats.total_events_processed == 10000
    assert stats.total_errors == 0
```

## Docker и деплой: контейнеризация ETL

ETL процесс должен быть легко деплоимым:

```dockerfile
FROM python:3.9-slim

WORKDIR /app
COPY requirements.txt .
RUN pip install -r requirements.txt

COPY src/ ./src/
COPY configs/ ./configs/

CMD ["python", "-m", "src.cli", "run", "configs/demo_pipeline.json"]
```

**Docker Compose для разработки**:
```yaml
version: '3.8'
services:
  mysql-source:
    image: mysql:8.0
    environment:
      MYSQL_ROOT_PASSWORD: rootpassword
      MYSQL_DATABASE: source_db
    ports:
      - "3306:3306"
  
  mysql-target:
    image: mysql:8.0
    environment:
      MYSQL_ROOT_PASSWORD: target_password
      MYSQL_DATABASE: target_db
    ports:
      - "3307:3306"
  
  etl-service:
    build: .
    volumes:
      - ./configs:/app/configs
    depends_on:
      - mysql-source
      - mysql-target
```

## Производительность: где узкие места

После профилирования выяснилось, что основные узкие места:

**1. Сетевые операции** — 60% времени
- Чтение binlog событий
- Запись в целевую БД

**2. Трансформации** — 20% времени
- Вызовы пользовательских функций
- Копирование данных

**3. Синхронизация потоков** — 10% времени
- Блокировки в message bus
- Ожидание в очередях

**Оптимизации**:
- Batch операции для записи в БД
- Кэширование трансформаций
- Увеличение размера очередей

## Что получилось и что бы сделал иначе

### Что получилось хорошо:

1. **Модульная архитектура** — легко добавлять новые источники и приёмники
2. **Надёжность** — автоматическое восстановление после сбоев
3. **Гибкость** — сложные трансформации и фильтрация
4. **Мониторинг** — детальная статистика и логирование

### Что бы сделал иначе:

1. **Начать с message queue** — Redis или RabbitMQ вместо собственной реализации
2. **Использовать async/await** — для лучшей производительности I/O операций
3. **Добавить метрики** — Prometheus + Grafana для мониторинга
4. **Сделать API** — для управления конфигурацией без перезапуска

### Планы на будущее:

1. **Поддержка других БД** — PostgreSQL, MongoDB
2. **Web UI** — для управления конфигурацией
3. **Кластеризация** — горизонтальное масштабирование
4. **Schema evolution** — автоматическая миграция схем

## Заключение

Создание ETL-системы оказалось гораздо сложнее, чем казалось изначально. Простая задача синхронизации данных превратилась в полноценный фреймворк с многопоточной архитектурой, системой сообщений и кучей подводных камней.

Но результат того стоил — получился гибкий и надёжный инструмент, который решает реальные задачи и может быть легко расширен под новые требования.

**Ключевые уроки**:
- Многопоточность — это не только про производительность, но и про архитектуру
- Message bus — отличный паттерн для координации потоков
- Структурированное логирование критически важно для отладки
- Тестирование многопоточного кода требует особого подхода

Код проекта доступен на GitHub: [github.com/tumurzakov/myrepetl](https://github.com/tumurzakov/myrepetl)

---

*P.S. Если у вас есть похожие задачи или вопросы по архитектуре — пишите в комментарии, обсудим!*
