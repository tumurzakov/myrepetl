# Examples

This directory contains example configurations and usage patterns for the MyRepETL system.

## Configuration Examples

### `metrics_config_example.json`
Example configuration with Prometheus metrics enabled. This configuration includes:
- Basic source and target database connections
- Table mappings for users and orders
- Metrics endpoint configuration (port 8080)
- Batch processing settings

### `health_check_example.py`
Example script for monitoring system health using the health endpoint. Features:
- Single health check with detailed status display
- Continuous monitoring mode
- JSON output option
- Color-coded status indicators

### `basic_example.py`
Basic usage example showing how to initialize and run the ETL service.

### `advanced_example.py`
Advanced usage example with custom transforms and error handling.

### `multi_source_example.py`
Example showing how to configure multiple source databases.

### `batch_config_example.json`
Example configuration focused on batch processing settings.

### `init_batch_config_example.json`
Example configuration for initial data loading with batch processing.

## Usage

1. Copy one of the example configurations to your project directory
2. Modify the database connection details to match your environment
3. Adjust table mappings according to your schema
4. Run the ETL service with your configuration:

```bash
python -m src.cli --config your_config.json
```

### Health Check Monitoring

Use the health check example script to monitor your system:

```bash
# Single health check
python examples/health_check_example.py

# Continuous monitoring
python examples/health_check_example.py --monitor --interval 10

# JSON output
python examples/health_check_example.py --json

# Custom health endpoint URL
python examples/health_check_example.py --url http://localhost:8081/health
```

## Metrics

When using the metrics configuration, you can access Prometheus metrics at:
- Metrics endpoint: `http://localhost:8080/metrics`
- Health check: `http://localhost:8080/health`

The metrics provide insights into:
- Thread status and activity
- Queue sizes and usage
- Record processing counts
- Batch sizes and performance
- Database connection status
- Error rates and types
- Replication event distribution
