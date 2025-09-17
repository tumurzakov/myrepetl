# Установка MyRepETL

## Установка из GitHub

### Через pip (рекомендуется)

```bash
# Установка последней версии
pip install git+https://github.com/tumurzakov/myrepetl.git

# Установка в режиме разработки
pip install -e git+https://github.com/tumurzakov/myrepetl.git#egg=myrepetl
```

### Локальная установка для разработки

```bash
# Клонирование репозитория
git clone https://github.com/tumurzakov/myrepetl.git
cd myrepetl

# Установка в режиме разработки
pip install -e .

# Или установка зависимостей вручную
pip install -r requirements.txt
```

## Проверка установки

После установки проверьте, что команда `myrepetl` доступна:

```bash
# Проверка версии
myrepetl --help

# Должно показать справку по командам
```

## Использование

```bash
# Запуск репликации
myrepetl run config.json

# Тестирование подключения
myrepetl test config.json
```

## Требования

- Python 3.8+
- MySQL сервер с включенным binlog
- Права на репликацию в MySQL

## Зависимости

Пакет автоматически установит следующие зависимости:
- pymysql - для подключения к MySQL
- mysql-replication - для чтения binlog событий
- pyyaml - для парсинга конфигурации
- structlog - для структурированного логирования

**Оптимизация зависимостей**: Проект использует только необходимые пакеты для ускорения сборки и уменьшения размера образа.
