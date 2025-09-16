# MyRepETL Makefile

.PHONY: help build up down logs test clean install

# Default target
help: ## Показать справку по командам
	@echo "Доступные команды:"
	@grep -E '^[a-zA-Z_-]+:.*?## .*$$' $(MAKEFILE_LIST) | sort | awk 'BEGIN {FS = ":.*?## "}; {printf "\033[36m%-20s\033[0m %s\n", $$1, $$2}'

build: ## Собрать Docker образы
	docker-compose build

up: ## Запустить все сервисы
	docker-compose up -d

down: ## Остановить все сервисы
	docker-compose down

logs: ## Показать логи ETL инструмента
	docker-compose logs -f etl-tool

logs-all: ## Показать логи всех сервисов
	docker-compose logs -f

test-connection: ## Тестировать подключение к MySQL
	docker-compose exec etl-tool python cli.py test configs/demo_pipeline.json

run-replication: ## Запустить репликацию
	docker-compose exec etl-tool python cli.py run configs/demo_pipeline.json --log-level DEBUG

install: ## Установить зависимости Python
	pip install -r requirements.txt

test-local: ## Тестировать подключение локально
	python cli.py test configs/demo_pipeline.json

run-local: ## Запустить репликацию локально
	python cli.py run configs/demo_pipeline.json --log-level DEBUG

test: ## Запустить все тесты
	python -m pytest tests/ -v

test-unit: ## Запустить только unit тесты
	python -m pytest tests/unit/ -v -m unit

test-integration: ## Запустить только интеграционные тесты
	python -m pytest tests/integration/ -v -m integration

test-coverage: ## Запустить тесты с отчетом покрытия
	python -m pytest tests/ -v --cov=src --cov-report=html --cov-report=term-missing

test-fast: ## Запустить быстрые тесты (исключить медленные)
	python -m pytest tests/ -v -m "not slow"

lint: ## Запустить линтеры
	flake8 src/ tests/
	black --check src/ tests/
	mypy src/

format: ## Форматировать код
	black src/ tests/

clean: ## Очистить Docker ресурсы
	docker-compose down -v
	docker system prune -f

reset: clean build up ## Полный сброс (очистка + пересборка + запуск)

# Мониторинг
status: ## Показать статус сервисов
	docker-compose ps

# База данных
db-source: ## Подключиться к исходной БД
	docker-compose exec mysql-source mysql -u root -prootpassword

db-target: ## Подключиться к целевой БД
	docker-compose exec mysql-target mysql -u root -prootpassword

# Разработка
dev: ## Запуск в режиме разработки
	docker-compose -f docker-compose.yml -f docker-compose.monitoring.yaml up -d

dev-logs: ## Логи в режиме разработки
	docker-compose -f docker-compose.yml -f docker-compose.monitoring.yaml logs -f

