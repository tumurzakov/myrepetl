#!/usr/bin/env python3
"""
CLI Tool for MySQL Replication ETL

This tool connects to MySQL replication sources, reads binlog events,
and processes them according to configuration.
"""

import argparse
import logging
import sys
from typing import Optional

from src.etl_service import ETLService
from src.utils.logger import setup_logging, get_logger
from src.exceptions import ETLException


class MySQLReplicationCLI:
    """CLI для работы с MySQL репликацией"""
    
    def __init__(self):
        self.logger = get_logger()
        self.etl_service = ETLService()
    
    def run_replication(self, config_path: str, **kwargs) -> None:
        """Запуск репликации"""
        try:
            # Инициализируем ETL сервис
            self.etl_service.initialize(config_path)
            
            # Запускаем репликацию
            self.etl_service.run_replication()
            
        except ETLException as e:
            self.logger.error("ETL error", error=str(e))
            raise
        except Exception as e:
            self.logger.error("Unexpected error", error=str(e))
            raise
    
    def test_connection(self, config_path: str) -> None:
        """Тестирование подключения"""
        try:
            # Инициализируем ETL сервис
            self.etl_service.initialize(config_path)
            
            # Тестируем подключения
            if self.etl_service.test_connections():
                self.logger.info("All connections tested successfully")
            else:
                self.logger.error("Connection test failed")
                raise ETLException("Connection test failed")
                
        except ETLException as e:
            self.logger.error("ETL error", error=str(e))
            raise
        except Exception as e:
            self.logger.error("Unexpected error", error=str(e))
            raise


def main():
    """Основная функция CLI"""
    parser = argparse.ArgumentParser(description='MySQL Replication ETL CLI')
    subparsers = parser.add_subparsers(dest='command', help='Доступные команды')
    
    # Команда run
    run_parser = subparsers.add_parser('run', help='Запуск репликации')
    run_parser.add_argument('config', help='Путь к конфигурационному файлу')
    run_parser.add_argument('--monitor', action='store_true', help='Включить мониторинг')
    run_parser.add_argument('--monitor-interval', type=int, default=30, help='Интервал мониторинга в секундах')
    run_parser.add_argument('--log-level', default='INFO', help='Уровень логирования')
    run_parser.add_argument('--log-format', default='json', choices=['json', 'console'], help='Формат логирования')
    
    # Команда test
    test_parser = subparsers.add_parser('test', help='Тестирование подключения')
    test_parser.add_argument('config', help='Путь к конфигурационному файлу')
    test_parser.add_argument('--log-level', default='INFO', help='Уровень логирования')
    test_parser.add_argument('--log-format', default='json', choices=['json', 'console'], help='Формат логирования')
    
    args = parser.parse_args()
    
    if not args.command:
        parser.print_help()
        return
    
    # Настраиваем логирование
    log_level = getattr(args, 'log_level', 'INFO')
    log_format = getattr(args, 'log_format', 'json')
    setup_logging(level=log_level, format_type=log_format)
    
    # Создаем CLI экземпляр
    cli = MySQLReplicationCLI()
    
    try:
        if args.command == 'run':
            cli.run_replication(args.config, **vars(args))
        elif args.command == 'test':
            cli.test_connection(args.config)
        else:
            parser.print_help()
    except ETLException as e:
        logging.error(f"ETL error: {e}")
        sys.exit(1)
    except Exception as e:
        logging.error(f"Unexpected error: {e}")
        sys.exit(1)


if __name__ == '__main__':
    main()

