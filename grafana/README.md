# MyRepETL Monitoring Stack

Полный стек мониторинга для MyRepETL системы, включающий Prometheus, Grafana и AlertManager с готовыми дашбордами и алертами.

## Быстрый старт

### Автоматическая установка

```bash
# Запустите скрипт автоматической установки
./setup-monitoring.sh
```

Этот скрипт автоматически:
- Создаст необходимые директории
- Запустит Docker Compose стек
- Настроит все сервисы
- Предоставит инструкции по использованию

### Ручная установка

#### 1. Запуск мониторинг стека

```bash
# Запустите все сервисы
docker-compose up -d

# Проверьте статус
docker-compose ps
```

#### 2. Доступ к сервисам

- **Grafana**: http://localhost:3000 (admin/admin)
- **Prometheus**: http://localhost:9090
- **AlertManager**: http://localhost:9093

#### 3. Импорт дашбордов в Grafana

1. Откройте Grafana: http://localhost:3000
2. Войдите с admin/admin
3. Перейдите в **Dashboards** → **Import**
4. Импортируйте дашборды:
   - `myrepetl-dashboard.json` - основной дашборд мониторинга
   - `myrepetl-health-dashboard.json` - дашборд health check

#### 4. Настройка MyRepETL

Убедитесь, что MyRepETL запущен с метриками:

```bash
# Запустите MyRepETL с конфигурацией, включающей метрики
python -m src.cli --config examples/metrics_config_example.json
```

Проверьте доступность метрик: http://localhost:8000/metrics

## Файлы конфигурации

### Дашборды
- `myrepetl-dashboard.json` - Основной дашборд с полным мониторингом ETL пайплайна
- `myrepetl-health-dashboard.json` - Дашборд для мониторинга health check статуса

### Prometheus
- `prometheus.yml` - Конфигурация Prometheus для сбора метрик MyRepETL
- `myrepetl-alerts.yml` - Правила алертов для Prometheus

### AlertManager
- `alertmanager.yml` - Конфигурация AlertManager для обработки алертов

### Docker
- `docker-compose.yml` - Docker Compose конфигурация для всего стека мониторинга
- `setup-monitoring.sh` - Скрипт автоматической установки

## Панели дашборда

### Статус потоков
- **Init Query Threads Status**: Статус потоков инициализации запросов
- **Target Threads Status**: Статус потоков обработки целевых данных
- **Source Threads Status**: Статус потоков источников данных

### Соединения
- **Database Connections Status**: Статус соединений с базами данных
- **Replication Connections Status**: Статус соединений репликации

### Очередь сообщений
- **Message Queue Size**: Размер очереди сообщений
- **Message Queue Usage Percentage**: Процент использования очереди

### Производительность
- **Init Records Sent Rate**: Скорость отправки init записей
- **Target Records Received Rate**: Скорость получения записей target потоками
- **Target Records Written Rate**: Скорость записи в целевые таблицы
- **Replication Records Received Rate**: Скорость получения записей репликации
- **Replication Events by Type Rate**: Распределение событий репликации по типам

### Ошибки и переподключения
- **Database Reconnections Rate**: Скорость переподключений к БД
- **Errors Rate**: Скорость ошибок по типам и компонентам

### Производительность обработки
- **Batch Size Percentiles**: Процентили размеров батчей
- **Processing Latency Percentiles**: Процентили задержки обработки

### Общая статистика
- **System Uptime**: Время работы системы
- **Total Active Threads**: Общее количество активных потоков
- **Connected Databases**: Количество подключенных БД
- **Active Replications**: Количество активных репликаций

## Алерты

### Встроенные алерты

Система включает готовые алерты в файле `myrepetl-alerts.yml`:

#### Критические алерты
- **Критическое использование очереди** (>90%)
- **Потеря соединения с БД**
- **Потеря соединения репликации**
- **Остановка всех init потоков**
- **Остановка всех target потоков**
- **Health check недоступен**

#### Предупреждения
- **Высокое использование очереди** (>70%)
- **Высокая частота ошибок** (>10/сек)
- **Высокая частота переподключений** (>5/сек)
- **Высокая задержка обработки** (>10 сек)
- **Отсутствие обработки записей** (10 мин)

#### Информационные
- **Недавний перезапуск системы** (<5 мин)

### Настройка уведомлений

Отредактируйте `alertmanager.yml` для настройки уведомлений:

```yaml
# Email уведомления
email_configs:
  - to: 'admin@company.com'
    subject: 'MyRepETL Alert'
    body: 'Alert: {{ .Annotations.summary }}'

# Slack уведомления
slack_configs:
  - api_url: 'YOUR_SLACK_WEBHOOK_URL'
    channel: '#alerts'
    title: 'MyRepETL Alert'
    text: '{{ .Annotations.description }}'
```

### Просмотр алертов

- **Prometheus**: http://localhost:9090/alerts
- **AlertManager**: http://localhost:9093

## Кастомизация

### Добавление новых панелей

1. Нажмите **Add Panel** в дашборде
2. Выберите тип визуализации
3. Настройте PromQL запрос
4. Сохраните панель

### Изменение временных интервалов

1. Нажмите на время в правом верхнем углу
2. Выберите нужный интервал
3. Или введите custom интервал

### Настройка refresh интервала

1. Нажмите на иконку обновления в правом верхнем углу
2. Выберите интервал обновления (по умолчанию 30s)

## Troubleshooting

### Метрики не отображаются

1. Проверьте, что MyRepETL запущен с метриками
2. Убедитесь, что Prometheus собирает метрики с MyRepETL
3. Проверьте правильность PromQL запросов
4. Убедитесь, что временной интервал корректный

### Нет данных в панелях

1. Проверьте, что метрики существуют: `http://localhost:8000/metrics`
2. Убедитесь, что Prometheus datasource настроен правильно
3. Проверьте временной диапазон дашборда
4. Убедитесь, что MyRepETL активно обрабатывает данные

### Медленная загрузка дашборда

1. Увеличьте интервал обновления
2. Уменьшите временной диапазон
3. Оптимизируйте PromQL запросы
4. Проверьте производительность Prometheus

## Дополнительные ресурсы

- [Prometheus Query Language (PromQL)](https://prometheus.io/docs/prometheus/latest/querying/basics/)
- [Grafana Documentation](https://grafana.com/docs/)
- [MyRepETL Metrics Documentation](../METRICS.md)
