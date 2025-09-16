# Пользовательские трансформации

## Обзор

Система поддерживает пользовательские функции трансформации данных. Вы можете создавать свои собственные функции для преобразования данных во время репликации.

## Создание трансформаций

### Способ 1: Файл transform.py рядом с конфигурацией

Создайте файл `transform.py` в той же директории, где находится ваш конфигурационный файл (например, рядом с `pipeline.json`):

```python
from datetime import datetime

def to_date(ts: int) -> str:
    """Convert timestamp to date string"""
    return datetime.fromtimestamp(ts).strftime("%Y-%m-%d %H:%M:%S")

def format_phone(phone: str) -> str:
    """Format phone number"""
    if not phone:
        return ""
    # Remove all non-digits
    digits = ''.join(filter(str.isdigit, phone))
    if len(digits) == 10:
        return f"({digits[:3]}) {digits[3:6]}-{digits[6:]}"
    return phone

def hash_email(email: str) -> str:
    """Hash email for privacy"""
    import hashlib
    if not email:
        return ""
    return hashlib.md5(email.encode()).hexdigest()
```

### Способ 2: Использование абсолютного пути

Если ваш файл трансформаций находится в другом месте, вы можете указать абсолютный путь к файлу в конфигурации.

### Способ 3: Файл transform.py в корне проекта

Альтернативно, вы можете создать файл `transform.py` в корневой директории проекта, и система попытается загрузить его как Python модуль.

## Как система ищет файл трансформаций

Система ищет файл `transform.py` в следующем порядке:

1. **В директории конфигурационного файла** - если вы запускаете `myrepetl run /path/to/pipeline.json`, система будет искать `transform.py` в `/path/to/`
2. **Как Python модуль** - если файл не найден в директории конфигурации, система попытается загрузить его как стандартный Python модуль
3. **По абсолютному пути** - если указан полный путь к файлу

## Использование в конфигурации

В файле конфигурации JSON используйте формат `"transform.function_name"`:

```json
{
  "mapping": {
    "source1.users": {
      "target_table": "target1.users",
      "column_mapping": {
        "id": {"column": "id", "primary_key": true},
        "name": {"column": "name", "transform": "transform.uppercase"},
        "email": {"column": "email", "transform": "transform.hash_email"},
        "phone": {"column": "phone", "transform": "transform.format_phone"},
        "created_at": {"column": "created_date", "transform": "transform.to_date"}
      }
    }
  }
}
```

## Встроенные трансформации

Система включает следующие встроенные трансформации:

- `transform.uppercase` - Преобразует строку в верхний регистр
- `transform.lowercase` - Преобразует строку в нижний регистр  
- `transform.trim` - Удаляет пробелы в начале и конце строки
- `transform.length` - Возвращает длину строки

## Требования к функциям трансформации

1. **Сигнатура функции**: Функция должна принимать один аргумент (значение для преобразования)
2. **Возвращаемое значение**: Функция должна возвращать преобразованное значение
3. **Обработка ошибок**: Функция должна корректно обрабатывать `None` и невалидные значения
4. **Именование**: Имя функции должно быть валидным идентификатором Python

## Примеры

### Преобразование timestamp в дату
```python
def to_date(ts: int) -> str:
    if ts is None:
        return None
    return datetime.fromtimestamp(ts).strftime("%Y-%m-%d %H:%M:%S")
```

### Форматирование JSON
```python
import json

def format_json(data: str) -> dict:
    if not data:
        return {}
    try:
        return json.loads(data)
    except json.JSONDecodeError:
        return {}
```

### Маскирование данных
```python
def mask_ssn(ssn: str) -> str:
    if not ssn or len(ssn) < 4:
        return ssn
    return "***-**-" + ssn[-4:]
```

## Отладка

Если трансформация не работает:

1. Проверьте, что файл `transform.py` находится в правильной директории
2. Убедитесь, что функция имеет правильную сигнатуру
3. Проверьте логи на наличие ошибок импорта
4. Убедитесь, что имя функции в конфигурации точно соответствует имени в коде

## Лучшие практики

1. **Валидация входных данных**: Всегда проверяйте входные данные на `None` и невалидные значения
2. **Обработка ошибок**: Используйте try-catch блоки для обработки исключений
3. **Документация**: Добавляйте docstring к функциям трансформации
4. **Тестирование**: Тестируйте функции трансформации отдельно перед использованием в ETL
5. **Производительность**: Избегайте тяжелых вычислений в функциях трансформации
