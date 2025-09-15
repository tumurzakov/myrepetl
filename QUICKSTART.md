# ETL Tool - Быстрый старт

## 🚀 Запуск за 5 минут

### 1. Установка зависимостей

```bash
cd etl/
pip install -r requirements.txt
```

### 2. Создание демо конфигурации

```bash
python demo.py
```

### 3. Валидация конфигурации

```bash
python -m etl.cli validate configs/demo_pipeline.json
```

### 4. Запуск с Docker Compose (рекомендуется)

```bash
# Запуск полного стека
docker-compose up -d

# Просмотр логов
docker-compose logs -f etl-tool

# Остановка
docker-compose down
```

### 5. Доступ к сервисам

- **phpMyAdmin**: http://localhost:8080
- **Grafana**: http://localhost:3000 (admin/admin)
- **Prometheus**: http://localhost:9090

## 📋 Что включено в демо

- ✅ MySQL источник с тестовыми данными
- ✅ MySQL приемник
- ✅ ETL процесс с трансформациями
- ✅ Мониторинг и логирование
- ✅ Веб-интерфейсы для управления

## 🔧 Настройка для продакшена

1. Измените пароли в `docker-compose.yml`
2. Настройте SSL сертификаты
3. Обновите конфигурацию ETL под ваши таблицы
4. Настройте мониторинг и алерты

## 📚 Дополнительные ресурсы

- [Полная документация](README.md)
- [Примеры использования](examples/)
- [CLI команды](cli.py)
- [API документация](dsl.py)
