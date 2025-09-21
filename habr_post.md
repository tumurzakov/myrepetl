# Вечерний вайбкодер: MyRepETL — ETL для MySQL через репликацию

Привет, коллеги! 👋 

Снова с вами рубрика "вечерний вайбкодер", и сегодня я принёс вам **MyRepETL** ([cсылка на github](https://github.com/tumurzakov/myrepetl))— инструмент для ETL через MySQL репликацию. 

## Зачем это нужно?

Задача: у вас куча MySQL баз в микросервисах, нужно всё это затащить в **Metabase** для красивых отчетов. 

Проблема в том, что:
- В каждой базе своя схема и структура
- Данные нужно объединить и нормализовать
- Metabase любит когда всё в одном месте
- Ручной экспорт/импорт — это боль

**MyRepETL** решает это: берёт данные из всех ваших баз, трансформирует их на лету и складывает в единую аналитическую базу для Metabase.

## Что умеет MyRepETL

### 🚀 Основные фишки

**Многопоточность из коробки**
- Каждый источник работает в своём потоке
- Не блокирует друг друга
- Автоматически восстанавливается при сбоях

**Гибкие трансформации**
- Переименование таблиц и колонок
- Вычисляемые поля
- Фильтрация данных
- Кастомные Python-функции

**JSON-конфигурация**
- Всё настраивается через конфиг

## Как использовать

### Простая синхронизация

Самый базовый случай — просто скопировать данные из одной базы в другую:

```json
{
  "sources": {
    "prod_db": {
      "host": "prod-mysql",
      "user": "repl_user", 
      "password": "repl_pass",
      "database": "production"
    }
  },
  "targets": {
    "backup_db": {
      "host": "backup-mysql",
      "user": "backup_user",
      "password": "backup_pass", 
      "database": "backup"
    }
  },
  "mapping": {
    "prod_db.users": {
      "source": "prod_db",
      "target": "backup_db",
      "source_table": "users",
      "target_table": "users"
    }
  }
}
```

### С трансформациями

А теперь добавим магию — переименуем таблицу, добавим вычисляемые поля:

```json
{
  "mapping": {
    "prod_db.customers": {
      "source": "prod_db",
      "target": "analytics_db",
      "source_table": "customers",
      "target_table": "users",
      "column_mapping": {
        "id": {"column": "user_id"},
        "name": {"column": "full_name"},
        "email": {"column": "email"},
        "birth_date": {"column": "age", "transform": "transform.calculate_age"},
        "phone": {"column": "formatted_phone", "transform": "transform.format_phone"},
        "created_at": {"column": "registration_date"},
        "source": {"column": "source_system", "value": "production"}
      }
    }
  }
}
```

Создайте файл `transform.py` с вашими функциями:

```python
# transform.py
def calculate_age(birth_date, row_data, table):
    from datetime import datetime
    if not birth_date:
        return None
    birth = datetime.strptime(birth_date, '%Y-%m-%d')
    return (datetime.now() - birth).days // 365

def format_phone(phone, row_data, table):
    if not phone:
        return None
    # 79991234567 -> +7 (999) 123-45-67
    return f"+7 ({phone[1:4]}) {phone[4:7]}-{phone[7:9]}-{phone[9:11]}"
```

### Запуск

```bash
# Установка с GitHub
pip install git+https://github.com/tumurzakov/myrepetl.git

# Или клонировать и установить локально
git clone https://github.com/tumurzakov/myrepetl.git
cd myrepetl
pip install -e .

# Запуск с конфигом
myrepetl run config.json

# Или через Docker
docker run -v ./config.json:/app/config.json myrepetl:latest
```

*На этом всё, удачного кодинга! 👨‍💻*
