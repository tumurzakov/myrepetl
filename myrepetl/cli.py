#!/usr/bin/env python3
"""
CLI Tool for MySQL Replication ETL

This tool connects to MySQL replication sources, reads binlog events,
and processes them according to configuration.
"""

import argparse
import logging
import signal
import sys
from typing import Optional

from .etl_service import ETLService
from .utils.logger import setup_logging, get_logger
from .exceptions import ETLException


class MySQLReplicationCLI:
    """CLI для работы с MySQL репликацией"""
    
    def __init__(self):
        self.logger = get_logger()
        self.etl_service = ETLService()
        self._shutdown_requested = False
        self._setup_signal_handlers()
    
    def _setup_signal_handlers(self) -> None:
        """Настройка обработчиков сигналов для корректного завершения"""
        def signal_handler(signum, frame):
            signal_name = signal.Signals(signum).name
            self.logger.info(f"Received signal {signal_name}, initiating graceful shutdown...")
            self._shutdown_requested = True
            # Передаем сигнал остановки в ETL сервис
            if hasattr(self.etl_service, 'request_shutdown'):
                self.etl_service.request_shutdown()
        
        # Обработка SIGINT (Ctrl+C) и SIGTERM
        signal.signal(signal.SIGINT, signal_handler)
        signal.signal(signal.SIGTERM, signal_handler)
    
    def run_replication(self, config_path: str, **kwargs) -> None:
        """Запуск репликации"""
        try:
            # Инициализируем ETL сервис
            self.etl_service.initialize(config_path)
            
            # Запускаем репликацию
            self.etl_service.run_replication()
            
        except KeyboardInterrupt:
            self.logger.info("Replication interrupted by user")
            raise
        except ETLException as e:
            self.logger.error("ETL error", error=str(e))
            raise
        except Exception as e:
            self.logger.error("Unexpected error", error=str(e))
            raise
        finally:
            # Принудительная очистка ресурсов
            self._force_cleanup()
    
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
    
    def _force_cleanup(self) -> None:
        """Принудительная очистка всех ресурсов"""
        try:
            if hasattr(self.etl_service, 'cleanup'):
                self.etl_service.cleanup()
            self.logger.info("Force cleanup completed")
        except Exception as e:
            self.logger.error("Error during force cleanup", error=str(e))


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
    except KeyboardInterrupt:
        logging.info("Application interrupted by user")
        sys.exit(0)
    except ETLException as e:
        logging.error(f"ETL error: {e}")
        sys.exit(1)
    except Exception as e:
        logging.error(f"Unexpected error: {e}")
        sys.exit(1)


if __name__ == '__main__':
    main()

