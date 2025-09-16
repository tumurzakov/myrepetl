# Структура пакета MyRepETL

## Файлы конфигурации

- `setup.py` - Основной файл конфигурации для setuptools
- `pyproject.toml` - Современная конфигурация проекта
- `MANIFEST.in` - Список дополнительных файлов для включения в пакет
- `requirements.txt` - Зависимости Python
- `LICENSE` - MIT лицензия

## Структура пакета

```
myrepetl/
├── myrepetl/              # Основной пакет
│   ├── __init__.py        # Инициализация пакета
│   ├── cli.py             # CLI интерфейс
│   ├── etl_service.py     # Основной ETL сервис
│   ├── exceptions.py      # Пользовательские исключения
│   ├── transform.py       # Функции трансформации
│   ├── models/            # Модели данных
│   │   ├── __init__.py
│   │   ├── config.py      # Конфигурационные модели
│   │   ├── events.py      # Модели событий binlog
│   │   └── transforms.py  # Модели трансформаций
│   ├── services/          # Бизнес-логика
│   │   ├── __init__.py
│   │   ├── config_service.py      # Управление конфигурацией
│   │   ├── database_service.py    # Работа с БД
│   │   ├── transform_service.py   # Трансформации данных
│   │   └── replication_service.py # Репликация MySQL
│   └── utils/             # Утилиты
│       ├── __init__.py
│       ├── logger.py      # Настройка логирования
│       ├── retry.py       # Retry механизм
│       └── sql_builder.py # Построение SQL запросов
├── configs/               # Конфигурационные файлы
│   └── demo_pipeline.json
├── examples/              # Примеры использования
│   ├── advanced_example.py
│   ├── basic_example.py
│   ├── multi_source_example.py
│   └── sample_config.py
├── kube/                  # Kubernetes манифесты
│   ├── source_init.sql
│   └── target_init.sql
└── tests/                 # Тесты
    ├── integration/
    └── unit/
```

## Entry Points

Пакет предоставляет следующие entry points:

- `myrepetl` - CLI команда для запуска репликации

## Установка

### Из GitHub

```bash
pip install git+https://github.com/tumurzakov/myrepetl.git
```

### Локальная разработка

```bash
git clone https://github.com/tumurzakov/myrepetl.git
cd myrepetl
pip install -e .
```

## Использование

После установки команда `myrepetl` будет доступна в системе:

```bash
# Справка
myrepetl --help

# Запуск репликации
myrepetl run config.json

# Тестирование подключения
myrepetl test config.json
```

## Сборка пакета

```bash
# Установка build tools
pip install build

# Сборка wheel
python -m build --wheel

# Сборка source distribution
python -m build --sdist
```

## Тестирование

```bash
# Запуск тестов
make test

# Тесты с покрытием
make test-coverage
```
