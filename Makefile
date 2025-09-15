# ETL Tool Makefile

.PHONY: help install test lint format clean run-example run-advanced-example create-sample-config

help: ## Show this help message
	@echo "ETL Tool - Available commands:"
	@grep -E '^[a-zA-Z_-]+:.*?## .*$$' $(MAKEFILE_LIST) | sort | awk 'BEGIN {FS = ":.*?## "}; {printf "  \033[36m%-20s\033[0m %s\n", $$1, $$2}'

install: ## Install dependencies
	pip install -r requirements.txt

install-dev: ## Install development dependencies
	pip install -r requirements.txt
	pip install pytest pytest-cov black flake8 mypy

test: ## Run tests
	python -m pytest tests/ -v

test-coverage: ## Run tests with coverage
	python -m pytest tests/ -v --cov=etl --cov-report=html --cov-report=term

lint: ## Run linting
	flake8 etl/ tests/ examples/
	mypy etl/

format: ## Format code
	black etl/ tests/ examples/

clean: ## Clean up temporary files
	find . -type f -name "*.pyc" -delete
	find . -type d -name "__pycache__" -delete
	find . -type d -name "*.egg-info" -exec rm -rf {} +
	rm -rf build/ dist/ .coverage htmlcov/ .pytest_cache/

create-sample-config: ## Create sample configuration
	python examples/sample_config.py

run-example: ## Run basic example
	python examples/basic_example.py

run-advanced-example: ## Run advanced example
	python examples/advanced_example.py

validate-config: ## Validate sample configuration
	python -m etl.cli validate configs/sample_pipeline.json

list-configs: ## List available configurations
	python -m etl.cli list

backup-config: ## Create backup of sample configuration
	python -m etl.cli backup configs/sample_pipeline.json

docker-build: ## Build Docker image
	docker build -t etl-tool .

docker-run: ## Run ETL tool in Docker
	docker run -it --rm etl-tool

docs: ## Generate documentation
	@echo "Documentation is available in README.md"

setup-mysql: ## Setup MySQL for replication (requires sudo)
	@echo "Setting up MySQL for replication..."
	@echo "Please run the following SQL commands on your MySQL server:"
	@echo "GRANT REPLICATION SLAVE ON *.* TO 'replication_user'@'%' IDENTIFIED BY 'password';"
	@echo "FLUSH PRIVILEGES;"
	@echo "SET GLOBAL binlog_format = 'ROW';"
	@echo "SET GLOBAL log_bin = ON;"

check-deps: ## Check if all dependencies are installed
	python -c "import pymysql, pymysqlreplication, pandas, yaml; print('All dependencies are installed')"

version: ## Show version information
	python -c "import etl; print(f'ETL Tool version: {etl.__version__}')"

# Development targets
dev-setup: install-dev create-sample-config ## Setup development environment
	@echo "Development environment setup complete!"

ci: lint test ## Run CI pipeline (lint + test)

all: clean install-dev test lint format ## Run all checks and tests
