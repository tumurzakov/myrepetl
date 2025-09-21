# MyRepETL

[![PyPI version](https://badge.fury.io/py/myrepetl.svg)](https://badge.fury.io/py/myrepetl)
[![Python 3.8+](https://img.shields.io/badge/python-3.8+-blue.svg)](https://www.python.org/downloads/)
[![License: MIT](https://img.shields.io/badge/License-MIT-yellow.svg)](https://opensource.org/licenses/MIT)

MySQL Replication ETL Tool - инструмент для репликации данных из MySQL с поддержкой трансформаций.

## Установка

### Быстрая установка

```bash
pip install git+https://github.com/tumurzakov/myrepetl.git
```

### Проверка установки

```bash
myrepetl --help
```

Подробные инструкции по установке см. в [INSTALL.md](INSTALL.md).

## Последние обновления

### Исправления тестов (21.09.2025)

- **Исправлена проблема с зависанием тестов** - устранена бесконечная петля в `MessageBus.process_messages()`
- **Исправлены проблемы с мокированием** в тестах ETL сервиса и ThreadManager
- **Улучшена стабильность тестов** - все 219 тестов теперь проходят успешно
- **Оптимизированы таймауты** в тестах для предотвращения зависания

Все тесты теперь проходят без зависания и корректно проверяют функциональность системы.

## Логирование

MyRepETL предоставляет детальное структурированное логирование всех операций ETL-процесса. Логи содержат контекстную информацию для отслеживания событий от источника до целевой базы данных.

### Основные возможности логирования:
- **Структурированные JSON логи** с использованием `structlog`
- **Уникальные ID операций** для корреляции логов
- **Детальное логирование трансформаций** данных
- **Отслеживание SQL операций** с полными запросами
- **Мониторинг производительности** и ошибок

Подробная документация по логированию доступна в [LOGGING.md](LOGGING.md).

## Пользовательские трансформации

Система поддерживает пользовательские функции трансформации данных. Создайте файл `transform.py` в той же директории, где находится ваш конфигурационный файл:

```python
from datetime import datetime

def to_date(ts: int) -> str:
    """Convert timestamp to date string"""
    return datetime.fromtimestamp(ts).strftime("%Y-%m-%d %H:%M:%S")

def uppercase(value):
    """Convert string to uppercase"""
    if value is None:
        return None
    elif isinstance(value, str):
        return value.upper()
    else:
        return value
```

Используйте в конфигурации:
```json
{
  "column_mapping": {
    "modified": {"column": "modify_time", "transform": "transform.to_date"},
    "name": {"column": "name", "transform": "transform.uppercase"}
  }
}
```

Подробная документация по трансформациям доступна в [TRANSFORMS.md](TRANSFORMS.md).

## Возможности

- **Репликация в реальном времени**: Чтение binlog событий MySQL
- **Множественные источники**: Поддержка нескольких источников данных одновременно
- **Множественные приемники**: Возможность репликации в несколько целевых баз данных
- **Гибкие трансформации**: Поддержка пользовательских функций трансформации с автоматической загрузкой модулей
- **Конфигурируемые mapping'и**: Настройка соответствия таблиц и колонок
- **Инициализация данных**: Автоматическая загрузка исторических данных через init_query
- **Фильтрация данных**: Гибкая система фильтрации с поддержкой сложных условий
- **Мониторинг**: Логирование и отслеживание процесса репликации
- **Docker поддержка**: Готовые контейнеры для развертывания
- **Модульная архитектура**: Четкое разделение ответственности между компонентами
- **Полное покрытие тестами**: Unit и интеграционные тесты с покрытием >90%
- **Обработка ошибок**: Retry механизм и детальное логирование

## Архитектура

Проект построен на модульной архитектуре с четким разделением ответственности:

```
src/
├── models/           # Модели данных
│   ├── config.py     # Конфигурационные модели
│   ├── events.py     # Модели событий binlog
│   └── transforms.py # Модели трансформаций
├── services/         # Бизнес-логика
│   ├── config_service.py      # Управление конфигурацией
│   ├── database_service.py    # Работа с БД
│   ├── transform_service.py   # Трансформации данных
│   └── replication_service.py # Репликация MySQL
├── utils/            # Утилиты
│   ├── retry.py      # Retry механизм
│   ├── sql_builder.py # Построение SQL запросов
│   └── logger.py     # Настройка логирования
├── exceptions.py     # Пользовательские исключения
└── etl_service.py   # Основной ETL сервис
```

## Быстрый старт

### 1. Установка через pip

```bash
# Установка из GitHub
pip install git+https://github.com/tumurzakov/myrepetl.git

# Или для разработки
pip install -e git+https://github.com/tumurzakov/myrepetl.git#egg=myrepetl
```

### 2. Альтернативная установка (для разработки)

```bash
# Клонирование репозитория
git clone https://github.com/tumurzakov/myrepetl.git
cd myrepetl

# Установка в режиме разработки
pip install -e .

# Или установка зависимостей вручную
pip install -r requirements.txt
```

### 3. Использование после установки

После установки пакета команда `myrepetl` будет доступна в системе:

```bash
# Запуск репликации
myrepetl run config.json

# Тестирование подключения
myrepetl test config.json

# Справка
myrepetl --help
```

### 4. Запуск с Docker Compose

```bash
# Сборка и запуск всех сервисов
make build
make up

# Проверка статуса
make status

# Тестирование подключения
make test-connection

# Запуск репликации
make run-replication
```

### 5. Локальная разработка

```bash
# Тестирование подключения
make test-local

# Запуск репликации
make run-local
```

## Тестирование

### Запуск тестов

```bash
# Все тесты
make test

# Только unit тесты
make test-unit

# Только интеграционные тесты
make test-integration

# Тесты с покрытием
make test-coverage

# Быстрые тесты (исключить медленные)
make test-fast
```

### Покрытие кода

Проект имеет покрытие тестами >90%. Отчеты генерируются в формате HTML в папке `htmlcov/`.

## Конфигурация

### Структура конфигурации

```json
{
  "sources": {
    "source1": {
      "host": "mysql-source1",
      "port": 3306,
      "user": "root",
      "password": "rootpassword",
      "database": "source_db1"
    },
    "source2": {
      "host": "mysql-source2",
      "port": 3306,
      "user": "root",
      "password": "rootpassword",
      "database": "source_db2"
    }
  },
  "targets": {
    "target1": {
      "host": "mysql-target1",
      "port": 3306,
      "user": "target_user",
      "password": "target_password",
      "database": "target_db1"
    },
    "target2": {
      "host": "mysql-target2",
      "port": 3306,
      "user": "target_user",
      "password": "target_password",
      "database": "target_db2"
    }
  },
  "replication": {
    "server_id": 100,
    "log_file": null,
    "log_pos": 4,
    "resume_stream": true,
    "blocking": true
  },
  "monitoring": {
    "enabled": true,
    "interval": 30,
    "log_level": "INFO"
  },
  "mapping": {
    "source1.users": {
      "target_table": "target1.users",
      "primary_key": "id",
      "column_mapping": {
        "id": {"column": "id", "primary_key": true},
        "name": {"column": "name", "transform": "transform.uppercase"},
        "email": {"column": "email"},
        "source_id": {"column": "source_id", "value": "1"}
      }
    },
    "source2.orders": {
      "target_table": "target2.orders",
      "primary_key": "id",
      "column_mapping": {
        "id": {"column": "id", "primary_key": true},
        "user_id": {"column": "user_id"},
        "amount": {"column": "amount"},
        "source_id": {"column": "source_id", "value": "2"}
      }
    }
  }
}
```

### Параметры конфигурации

#### Sources (Источники)
Секция `sources` содержит словарь с конфигурациями источников данных. Каждый источник имеет уникальное имя и следующие параметры:

- `host`: Хост MySQL сервера
- `port`: Порт (по умолчанию 3306)
- `user`: Имя пользователя
- `password`: Пароль
- `database`: Имя базы данных

#### Targets (Приемники)
Секция `targets` содержит словарь с конфигурациями целевых баз данных. Каждый приемник имеет уникальное имя и следующие параметры:

- `host`: Хост целевого MySQL сервера
- `port`: Порт (по умолчанию 3306)
- `user`: Имя пользователя
- `password`: Пароль
- `database`: Имя целевой базы данных

#### Replication (Репликация)
- `server_id`: ID сервера для репликации
- `log_file`: Файл binlog для начала чтения
- `log_pos`: Позиция в binlog файле
- `resume_stream`: Продолжить с последней позиции
- `blocking`: Блокирующий режим чтения

#### Mapping (Соответствие)
Секция `mapping` определяет соответствие между таблицами источников и приемников. Ключ имеет формат `{source_name}.{table_name}`, а значение содержит:

- `target_table`: Имя целевой таблицы в формате `{target_name}.{table_name}`
- `primary_key`: Первичный ключ для upsert операций
- `column_mapping`: Соответствие колонок:
  - `column`: Имя целевой колонки
  - `primary_key`: Флаг первичного ключа
  - `transform`: Путь к функции трансформации
  - `value`: Статическое значение
- `init_query`: (опционально) SQL запрос для инициализации пустой целевой таблицы
- `source_table`: (опционально) Явное указание исходной таблицы в формате `{source_name}.{table_name}`. Если указано, система будет использовать это поле для выбора записей вместо ключа mapping
- `filter`: (опционально) Условия фильтрации данных

**Примеры маппинга:**
- `"source1.users"` → `"target1.users"` - таблица users из source1 в target1
- `"source2.orders"` → `"target2.orders"` - таблица orders из source2 в target2
- `"source1.users"` → `"target2.users"` - таблица users из source1 в target2

#### Init Query (Инициализация)
Параметр `init_query` позволяет выполнить SQL запрос для инициализации пустой целевой таблицы. Это полезно для:

- Первичной загрузки исторических данных
- Синхронизации существующих данных при первом запуске
- Загрузки данных по определенным критериям

**Как работает init_query:**
1. При запуске ETL проверяется, пуста ли целевая таблица
2. Если таблица пуста и указан `init_query`, выполняется запрос на источнике
3. Результаты запроса обрабатываются через mapping и фильтры
4. Данные вставляются/обновляются в целевой таблице

**Пример использования:**
```json
{
  "mapping": {
    "source1.users": {
      "init_query": "SELECT * FROM users WHERE status = 'active' AND id > 2",
      "source_table": "source1.users",
      "target_table": "target1.users",
      "primary_key": "id",
      "column_mapping": {
        "id": {"column": "id", "primary_key": true},
        "name": {"column": "name", "transform": "transform.uppercase"},
        "email": {"column": "email"},
        "status": {"column": "status"}
      },
      "filter": {
        "status": {"eq": "active"},
        "id": {"gt": 2}
      }
    }
  }
}
```

#### Source Table (Исходная таблица)
Параметр `source_table` позволяет явно указать исходную таблицу для обработки. Это особенно полезно когда:

- Ключ mapping не соответствует реальной структуре таблиц
- Нужно обрабатывать таблицы с одинаковыми именами из разных источников
- Требуется более гибкое управление выбором записей

**Как работает source_table:**
1. Если указан `source_table`, система использует его для выбора записей
2. Ключ mapping игнорируется при выборе записей (но остается для обратной совместимости)
3. `source_table` должен быть в формате `{source_name}.{table_name}`
4. Система автоматически определяет источник и таблицу из этого поля

**Пример с source_table:**
```json
{
  "mapping": {
    "custom_key": {
      "source_table": "source1.users",
      "target_table": "target1.users",
      "primary_key": "id",
      "column_mapping": {
        "id": {"column": "id", "primary_key": true},
        "name": {"column": "name"}
      }
    }
  }
}
```

#### Filter (Фильтрация)
Секция `filter` позволяет фильтровать данные на основе условий. Поддерживаются следующие операции:

- `eq` - равенство
- `gt` - больше чем
- `gte` - больше или равно
- `lt` - меньше чем
- `lte` - меньше или равно
- `not` - отрицание
- `and` - логическое И
- `or` - логическое ИЛИ

**Примеры фильтров:**

```json
{
  "mapping": {
    "source1.users": {
      "target_table": "target1.users",
      "primary_key": "id",
      "column_mapping": {
        "id": {"column": "id", "primary_key": true},
        "name": {"column": "name"},
        "status": {"column": "status"},
        "age": {"column": "age"}
      },
      "filter": {
        "status": {"eq": "active"},
        "age": {"gte": 18}
      }
    }
  }
}
```

**Сложные фильтры:**

```json
{
  "filter": {
    "and": [
      {"status": {"eq": "active"}},
      {
        "or": [
          {"category": {"eq": "premium"}},
          {"score": {"gte": 90}}
        ]
      }
    ]
  }
}
```

**Фильтр с отрицанием:**

```json
{
  "filter": {
    "not": {
      "status": {"eq": "deleted"}
    }
  }
}
```

## Трансформации

### Встроенные трансформации

```python
def uppercase(value):
    """Преобразование в верхний регистр"""
    if value is None:
        return None
    elif isinstance(value, str):
        return value.upper()
    else:
        return value

def lowercase(value):
    """Преобразование в нижний регистр"""
    if value is None:
        return None
    elif isinstance(value, str):
        return value.lower()
    else:
        return value

def trim(value):
    """Удаление пробелов"""
    if value is None:
        return None
    elif isinstance(value, str):
        return value.strip()
    else:
        return value
```

### Создание пользовательских трансформаций

1. Создайте файл `transform.py` в корне проекта
2. Добавьте функции трансформации:

```python
def custom_transform(value):
    """Пользовательская трансформация"""
    if value is None:
        return None
    # Ваша логика трансформации
    return transformed_value
```

3. Используйте в конфигурации:

```json
{
  "column_mapping": {
    "field_name": {
      "column": "target_field",
      "transform": "transform.custom_transform"
    }
  }
}
```

## Использование

### Примеры конфигураций

#### Простая конфигурация с одним источником и одним приемником

```json
{
  "sources": {
    "main_source": {
      "host": "mysql-source",
      "port": 3306,
      "user": "root",
      "password": "rootpassword",
      "database": "source_db"
    }
  },
  "targets": {
    "main_target": {
      "host": "mysql-target",
      "port": 3306,
      "user": "target_user",
      "password": "target_password",
      "database": "target_db"
    }
  },
  "replication": {
    "server_id": 100
  },
  "mapping": {
    "main_source.users": {
      "target_table": "main_target.users",
      "primary_key": "id",
      "column_mapping": {
        "id": {"column": "id", "primary_key": true},
        "name": {"column": "name"},
        "email": {"column": "email"}
      }
    }
  }
}
```

#### Конфигурация с фильтрацией данных

```json
{
  "sources": {
    "main_source": {
      "host": "mysql-source",
      "port": 3306,
      "user": "root",
      "password": "rootpassword",
      "database": "source_db"
    }
  },
  "targets": {
    "main_target": {
      "host": "mysql-target",
      "port": 3306,
      "user": "target_user",
      "password": "target_password",
      "database": "target_db"
    }
  },
  "replication": {
    "server_id": 100
  },
  "mapping": {
    "main_source.users": {
      "target_table": "main_target.users",
      "primary_key": "id",
      "column_mapping": {
        "id": {"column": "id", "primary_key": true},
        "name": {"column": "name"},
        "email": {"column": "email"},
        "status": {"column": "status"},
        "age": {"column": "age"}
      },
      "filter": {
        "status": {"eq": "active"},
        "age": {"gte": 18}
      }
    },
    "main_source.orders": {
      "target_table": "main_target.orders",
      "primary_key": "id",
      "column_mapping": {
        "id": {"column": "id", "primary_key": true},
        "user_id": {"column": "user_id"},
        "amount": {"column": "amount"},
        "status": {"column": "status"}
      },
      "filter": {
        "and": [
          {"status": {"eq": "completed"}},
          {"amount": {"gt": 0}}
        ]
      }
    }
  }
}
```

#### Конфигурация с множественными источниками и приемниками

```json
{
  "sources": {
    "ecommerce_db": {
      "host": "ecommerce-mysql",
      "port": 3306,
      "user": "repl_user",
      "password": "repl_password",
      "database": "ecommerce"
    },
    "analytics_db": {
      "host": "analytics-mysql",
      "port": 3306,
      "user": "repl_user",
      "password": "repl_password",
      "database": "analytics"
    }
  },
  "targets": {
    "data_warehouse": {
      "host": "warehouse-mysql",
      "port": 3306,
      "user": "dw_user",
      "password": "dw_password",
      "database": "data_warehouse"
    },
    "reporting_db": {
      "host": "reporting-mysql",
      "port": 3306,
      "user": "report_user",
      "password": "report_password",
      "database": "reporting"
    }
  },
  "replication": {
    "server_id": 100
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
      }
    },
    "ecommerce_db.orders": {
      "target_table": "data_warehouse.orders",
      "primary_key": "id",
      "column_mapping": {
        "id": {"column": "id", "primary_key": true},
        "user_id": {"column": "user_id"},
        "amount": {"column": "amount"},
        "source_system": {"column": "source_system", "value": "ecommerce"}
      }
    },
    "analytics_db.events": {
      "target_table": "reporting.events",
      "primary_key": "id",
      "column_mapping": {
        "id": {"column": "id", "primary_key": true},
        "event_type": {"column": "event_type"},
        "timestamp": {"column": "timestamp"},
        "source_system": {"column": "source_system", "value": "analytics"}
      }
    }
  }
}
```

### CLI команды

```bash
# Запуск репликации
python cli.py run configs/demo_pipeline.json

# Тестирование подключения
python cli.py test configs/demo_pipeline.json

# С дополнительными параметрами
python cli.py run configs/demo_pipeline.json --log-level DEBUG --log-format console
```

### Параметры командной строки

- `--log-level`: Уровень логирования (DEBUG, INFO, WARNING, ERROR)
- `--log-format`: Формат логирования (json, console)
- `--monitor`: Включить мониторинг
- `--monitor-interval`: Интервал мониторинга в секундах

## Мониторинг

### Логи

Логи выводятся в формате JSON для удобного парсинга:

```json
{
  "timestamp": "2024-01-15T10:30:00Z",
  "level": "info",
  "message": "INSERT событие",
  "table": "users",
  "schema": "source_db",
  "rows_count": 1
}
```

### Docker Compose мониторинг

```bash
# Запуск с мониторингом
make dev

# Просмотр логов
make dev-logs
```

## Разработка

### Структура проекта

```
myrepetl/
├── src/                    # Исходный код
│   ├── models/            # Модели данных
│   ├── services/          # Бизнес-логика
│   ├── utils/             # Утилиты
│   ├── cli.py             # CLI интерфейс
│   ├── etl_service.py     # Основной ETL сервис
│   ├── exceptions.py      # Исключения
│   └── transform.py       # Функции трансформации
├── tests/                 # Тесты
│   ├── unit/              # Unit тесты
│   └── integration/       # Интеграционные тесты
├── configs/               # Конфигурационные файлы
├── kube/                  # Kubernetes манифесты
├── docker-compose.yml     # Docker Compose конфигурация
├── Dockerfile            # Docker образ
├── requirements.txt      # Python зависимости
├── pytest.ini           # Конфигурация тестов
└── README.md            # Документация
```

### Качество кода

```bash
# Линтинг
make lint

# Форматирование кода
make format

# Проверка типов
mypy src/
```

### Добавление новых трансформаций

1. Добавьте функцию в `transform.py`
2. Добавьте тесты в `tests/unit/test_services.py`
3. Обновите документацию
4. Запустите тесты: `make test`

## Развертывание

### Docker

```bash
# Сборка образа
docker build -t myrepetl .

# Запуск контейнера
docker run -v $(pwd)/configs:/app/configs myrepetl run configs/demo_pipeline.json
```

### Kubernetes

```bash
# Применение манифестов
kubectl apply -f kube/

# Проверка статуса
kubectl get pods
```

## Устранение неполадок

### Проблемы с подключением

1. Проверьте настройки сети между контейнерами
2. Убедитесь, что MySQL серверы доступны
3. Проверьте права пользователей на репликацию

### Проблемы с трансформациями

1. Убедитесь, что функции трансформации корректны
2. Проверьте логи на ошибки трансформации
3. Протестируйте функции отдельно

### Проблемы с производительностью

1. Настройте `server_id` для репликации
2. Проверьте настройки MySQL binlog
3. Мониторьте использование ресурсов

### Проблемы с подключением к базе данных

#### Ошибка "read of closed file"

Эта ошибка возникает при попытке чтения из закрытого MySQL соединения. В версии 1.0.0+ эта проблема решена:

- **Улучшенное управление соединениями**: Использование контекстных менеджеров для автоматического закрытия курсоров
- **Повторные попытки**: Автоматические retry при ошибках соединения с экспоненциальной задержкой
- **Таймауты соединения**: Настройка таймаутов для предотвращения зависания
- **Валидация соединений**: Проверка состояния соединения перед использованием

**Решение проблемы:**
1. Обновитесь до последней версии: `pip install --upgrade myrepetl`
2. Проверьте настройки сети между контейнерами
3. Убедитесь, что MySQL серверы доступны и не перезагружаются

#### Другие проблемы с подключением

1. Проверьте настройки сети между контейнерами
2. Убедитесь, что MySQL серверы доступны
3. Проверьте права пользователей на репликацию
4. Проверьте логи на наличие ошибок соединения

### Проблемы с завершением приложения

Приложение корректно обрабатывает сигналы SIGINT (Ctrl+C) и SIGTERM для graceful shutdown:

- **Обработка сигналов**: CLI перехватывает сигналы и инициирует корректное завершение
- **Флаги остановки**: Все сервисы проверяют флаги остановки перед обработкой событий
- **Cleanup ресурсов**: Принудительное закрытие всех соединений и потоков
- **Логирование**: Детальное логирование процесса завершения

Если приложение не завершается с первого раза:
1. Убедитесь, что используется последняя версия
2. Проверьте логи на наличие ошибок cleanup
3. При необходимости используйте `kill -9` для принудительного завершения

## Лицензия

MIT License

## Поддержка

Для вопросов и предложений создавайте issues в репозитории.