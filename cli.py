"""
ETL CLI

Command-line interface for managing ETL pipelines.
"""

import argparse
import json
import sys
import time
import signal
import logging
from pathlib import Path
from typing import Dict, Any

from dsl import ETLPipeline
from engine import ETLEngine
from utils import (
    ETLConfigManager,
    ETLMonitor,
    ETLValidator,
    ETLBackup,
    setup_logging
)


class ETLCLI:
    """
    Command-line interface for ETL management
    """

    def __init__(self):
        self.parser = self._create_parser()
        self.config_manager = ETLConfigManager()
        self.validator = ETLValidator()
        self.backup_manager = ETLBackup()
        self.etl_engine = None
        self.monitor = None

    def _create_parser(self) -> argparse.ArgumentParser:
        """Create command-line argument parser"""
        parser = argparse.ArgumentParser(
            description="ETL Tool for MySQL Replication Protocol",
            formatter_class=argparse.RawDescriptionHelpFormatter
        )

        parser.add_argument(
            "--log-level",
            choices=["DEBUG", "INFO", "WARNING", "ERROR"],
            default="INFO",
            help="Set logging level"
        )

        parser.add_argument(
            "--log-file",
            help="Log file path"
        )

        subparsers = parser.add_subparsers(dest="command", help="Available commands")

        # Run command
        run_parser = subparsers.add_parser("run", help="Run ETL pipeline")
        run_parser.add_argument("config", help="Pipeline configuration file")
        run_parser.add_argument("--target-host", required=True, help="Target database host")
        run_parser.add_argument("--target-port", type=int, default=3306, help="Target database port")
        run_parser.add_argument("--target-user", required=True, help="Target database user")
        run_parser.add_argument("--target-password", required=True, help="Target database password")
        run_parser.add_argument("--target-database", required=True, help="Target database name")
        run_parser.add_argument("--monitor", action="store_true", help="Enable monitoring")
        run_parser.add_argument("--monitor-interval", type=int, default=30, help="Monitoring interval in seconds")

        # Validate command
        validate_parser = subparsers.add_parser("validate", help="Validate pipeline configuration")
        validate_parser.add_argument("config", help="Pipeline configuration file")

        # List command
        list_parser = subparsers.add_parser("list", help="List available configurations")
        list_parser.add_argument("--type", choices=["pipelines", "backups"], default="pipelines", help="Type to list")

        # Backup command
        backup_parser = subparsers.add_parser("backup", help="Create backup of pipeline")
        backup_parser.add_argument("config", help="Pipeline configuration file")
        backup_parser.add_argument("--output", help="Output backup file path")

        # Restore command
        restore_parser = subparsers.add_parser("restore", help="Restore pipeline from backup")
        restore_parser.add_argument("backup", help="Backup file path")
        restore_parser.add_argument("--output", help="Output configuration file path")

        # Status command
        status_parser = subparsers.add_parser("status", help="Show ETL status (if running)")

        return parser

    def run(self, args: argparse.Namespace):
        """Run ETL pipeline"""
        try:
            # Load pipeline configuration
            pipeline = self.config_manager.load_pipeline(args.config)

            # Validate configuration
            errors = self.validator.validate_pipeline(pipeline)
            if errors:
                print("Configuration validation errors:")
                for error in errors:
                    print(f"  - {error}")
                sys.exit(1)

            # Target database configuration
            target_db_config = {
                "host": args.target_host,
                "port": args.target_port,
                "user": args.target_user,
                "password": args.target_password,
                "database": args.target_database
            }

            # Create and start ETL engine
            self.etl_engine = ETLEngine(pipeline, target_db_config)

            if args.monitor:
                self.monitor = ETLMonitor(self.etl_engine)

            # Setup signal handlers
            signal.signal(signal.SIGINT, self._signal_handler)
            signal.signal(signal.SIGTERM, self._signal_handler)

            # Start ETL engine
            self.etl_engine.start()
            print(f"ETL pipeline '{pipeline.name}' started successfully")

            # Main loop
            try:
                while True:
                    time.sleep(args.monitor_interval)

                    if args.monitor and self.monitor:
                        self.monitor.log_health_status()

                    # Show basic stats
                    status = self.etl_engine.get_status()
                    stats = status["stats"]
                    print(f"Stats - Processed: {stats['events_processed']}, "
                          f"Failed: {stats['events_failed']}, "
                          f"Queue: {status['queue_size']}")

            except KeyboardInterrupt:
                print("\nReceived interrupt signal")

        except Exception as e:
            print(f"Error running ETL pipeline: {e}")
            sys.exit(1)

        finally:
            if self.etl_engine:
                self.etl_engine.stop()
                print("ETL pipeline stopped")

    def validate(self, args: argparse.Namespace):
        """Validate pipeline configuration"""
        try:
            pipeline = self.config_manager.load_pipeline(args.config)
            errors = self.validator.validate_pipeline(pipeline)

            if errors:
                print("Configuration validation errors:")
                for error in errors:
                    print(f"  - {error}")
                sys.exit(1)
            else:
                print("Configuration is valid")

        except Exception as e:
            print(f"Error validating configuration: {e}")
            sys.exit(1)

    def list_configs(self, args: argparse.Namespace):
        """List available configurations"""
        try:
            if args.type == "pipelines":
                configs = self.config_manager.list_pipelines()
                print("Available pipeline configurations:")
                for config in configs:
                    print(f"  - {config}")
            elif args.type == "backups":
                backups = self.backup_manager.list_backups()
                print("Available backups:")
                for backup in backups:
                    print(f"  - {backup['filename']} ({backup['pipeline_name']}) - {backup['backup_timestamp']}")

        except Exception as e:
            print(f"Error listing configurations: {e}")
            sys.exit(1)

    def backup(self, args: argparse.Namespace):
        """Create backup of pipeline"""
        try:
            pipeline = self.config_manager.load_pipeline(args.config)
            backup_file = self.backup_manager.create_backup(pipeline)
            print(f"Backup created: {backup_file}")

        except Exception as e:
            print(f"Error creating backup: {e}")
            sys.exit(1)

    def restore(self, args: argparse.Namespace):
        """Restore pipeline from backup"""
        try:
            pipeline = self.backup_manager.restore_backup(args.backup)

            if args.output:
                output_file = args.output
            else:
                output_file = f"{pipeline.name}_restored.json"

            self.config_manager.save_pipeline(pipeline, output_file)
            print(f"Pipeline restored to: {output_file}")

        except Exception as e:
            print(f"Error restoring backup: {e}")
            sys.exit(1)

    def status(self, args: argparse.Namespace):
        """Show ETL status"""
        if not self.etl_engine:
            print("ETL engine is not running")
            return

        try:
            status = self.etl_engine.get_status()

            print("ETL Engine Status:")
            print(f"  Pipeline: {status['pipeline']['name']}")
            print(f"  Running: {status['is_running']}")
            print(f"  Queue Size: {status['queue_size']}")

            stats = status["stats"]
            print(f"  Events Processed: {stats['events_processed']}")
            print(f"  Events Failed: {stats['events_failed']}")
            print(f"  Rows Inserted: {stats['rows_inserted']}")
            print(f"  Rows Updated: {stats['rows_updated']}")
            print(f"  Rows Deleted: {stats['rows_deleted']}")
            print(f"  Uptime: {stats['uptime']:.1f}s")

            replication = status["replication"]
            print(f"  Replication Sources: {len(replication['clients'])}")
            for source_name, client_status in replication["clients"].items():
                print(f"    - {source_name}: {'Running' if client_status['is_running'] else 'Stopped'}")

        except Exception as e:
            print(f"Error getting status: {e}")
            sys.exit(1)

    def _signal_handler(self, signum, frame):
        """Handle interrupt signals"""
        print(f"\nReceived signal {signum}, stopping ETL engine...")
        if self.etl_engine:
            self.etl_engine.stop()
        sys.exit(0)

    def main(self):
        """Main CLI entry point"""
        args = self.parser.parse_args()

        # Setup logging
        setup_logging(args.log_level, args.log_file)

        # Execute command
        if args.command == "run":
            self.run(args)
        elif args.command == "validate":
            self.validate(args)
        elif args.command == "list":
            self.list_configs(args)
        elif args.command == "backup":
            self.backup(args)
        elif args.command == "restore":
            self.restore(args)
        elif args.command == "status":
            self.status(args)
        else:
            self.parser.print_help()


def main():
    """Main entry point"""
    cli = ETLCLI()
    cli.main()


if __name__ == "__main__":
    main()
