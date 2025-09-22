# Connection Management Improvements

## Overview

This document describes the improvements made to MySQL connection handling in MyRepETL to address connection timeout and error issues.

## Problems Addressed

### 1. MySQL Connection Errors
The following MySQL-specific errors were causing failures:
- `(2006, "MySQL server has gone away")` - Connection timeout
- `Packet sequence number wrong` - Connection state corruption
- `Bad file descriptor` - Socket connection issues
- `Connection 'target1' is no longer valid` - Invalid connection state

### 2. Missing Retry Logic
- Init query events were not using retry mechanisms
- Connection failures caused immediate errors without recovery attempts

## Solutions Implemented

### 1. Enhanced Error Handling

#### MySQL-Specific Exception Handling
```python
import pymysql.err

# Handle specific MySQL errors
except (pymysql.err.OperationalError, pymysql.err.InternalError, 
        pymysql.err.InterfaceError) as e:
    # MySQL-specific connection errors - these are retryable
    error_code = getattr(e, 'args', [None])[0] if e.args else None
    self.logger.warning("MySQL connection error, retrying", 
                      target_name=self.target_name,
                      error_type=type(e).__name__,
                      error_code=error_code,
                      error_message=str(e))
```

#### Improved Connection Validation
```python
def is_connected(self, connection_name: str) -> bool:
    """Check if connection is active with MySQL-specific error handling"""
    try:
        # Test the connection with a simple ping
        connection.ping(reconnect=False)
        return True
    except (pymysql.err.OperationalError, pymysql.err.InternalError, 
            pymysql.err.InterfaceError, pymysql.err.DatabaseError,
            ConnectionError, OSError, IOError, AttributeError, Exception) as e:
        # Log specific MySQL errors for debugging
        if isinstance(e, (pymysql.err.OperationalError, pymysql.err.InternalError)):
            self.logger.debug("MySQL connection error detected", 
                            connection_name=connection_name, 
                            error_type=type(e).__name__,
                            error_code=getattr(e, 'args', [None])[0] if e.args else None,
                            error_message=str(e))
        return False
```

### 2. Retry Logic for Init Query Events

#### Before (Problematic)
```python
# Execute the UPSERT
self.database_service.execute_update(sql, values, self.target_name)
```

#### After (Fixed)
```python
# Execute the UPSERT with retry logic
result = self._execute_with_retry(
    self.database_service.execute_update, sql, values, self.target_name
)
```

### 3. Enhanced Retry Mechanism

#### MySQL-Aware Retry Logic
```python
def _execute_with_retry(self, operation_func, *args, **kwargs):
    """Execute database operation with MySQL-aware retry logic"""
    max_retries = 3
    retry_delay = 1.0
    
    for attempt in range(max_retries):
        try:
            # Ensure connection is available before each attempt
            if not self._ensure_target_connection():
                # Retry logic for connection issues
                continue
            
            # Execute the operation
            return operation_func(*args, **kwargs)
            
        except (pymysql.err.OperationalError, pymysql.err.InternalError, 
                pymysql.err.InterfaceError) as e:
            # MySQL-specific connection errors - these are retryable
            if attempt < max_retries - 1:
                # Force connection cleanup and recreation on MySQL errors
                try:
                    self.database_service.close_connection(self.target_name)
                except Exception:
                    pass  # Ignore cleanup errors
                time.sleep(retry_delay * (attempt + 1))
                continue
            else:
                raise e
```

### 4. Improved Connection Parameters

#### Enhanced Connection Configuration
```python
connection_params.update({
    'connect_timeout': 10,
    'read_timeout': 30,
    'write_timeout': 30,
    'autocommit': True,
    'charset': 'utf8mb4',
    'use_unicode': True,
    'sql_mode': 'TRADITIONAL',
    'init_command': "SET SESSION wait_timeout=28800, interactive_timeout=28800"
})
```

#### Key Parameters Explained
- `wait_timeout=28800` - 8 hours before MySQL closes idle connections
- `interactive_timeout=28800` - 8 hours for interactive connections
- `charset='utf8mb4'` - Full UTF-8 support
- `sql_mode='TRADITIONAL'` - Strict SQL mode for data integrity

## Error Types Handled

### 1. OperationalError (2006)
- **Cause**: MySQL server has gone away
- **Solution**: Connection recreation with retry
- **Recovery**: Automatic reconnection

### 2. InternalError (Packet sequence)
- **Cause**: Connection state corruption
- **Solution**: Force connection cleanup and recreation
- **Recovery**: New connection establishment

### 3. InterfaceError (Bad file descriptor)
- **Cause**: Socket connection issues
- **Solution**: Connection validation and recreation
- **Recovery**: Socket reinitialization

## Monitoring and Logging

### Enhanced Error Logging
```python
self.logger.warning("MySQL connection error, retrying", 
                  target_name=self.target_name,
                  attempt=attempt + 1,
                  max_retries=max_retries,
                  error_type=type(e).__name__,
                  error_code=error_code,
                  error_message=str(e))
```

### Connection Status Monitoring
```python
def get_connection_status(self, connection_name: str) -> Dict[str, Any]:
    """Get detailed connection status for monitoring"""
    status = {
        'exists': connection_name in self._connections,
        'is_connected': False,
        'has_config': connection_name in self._connection_configs,
        'error': None
    }
    
    if status['exists']:
        try:
            status['is_connected'] = self.is_connected(connection_name)
        except Exception as e:
            status['error'] = str(e)
    
    return status
```

## Best Practices

### 1. Connection Pool Management
- Always validate connections before use
- Implement proper cleanup on errors
- Use connection timeouts to prevent hanging

### 2. Error Handling
- Distinguish between retryable and non-retryable errors
- Log detailed error information for debugging
- Implement exponential backoff for retries

### 3. Monitoring
- Track connection status and errors
- Monitor retry attempts and success rates
- Alert on persistent connection issues

## Testing

### Connection Error Simulation
To test the improvements, you can simulate connection errors:

1. **Network interruption**: Disconnect network during operation
2. **MySQL restart**: Restart MySQL server during replication
3. **Connection timeout**: Set very short timeouts in MySQL config

### Expected Behavior
- Operations should retry automatically
- Connections should be recreated on errors
- No data loss should occur
- Detailed error logging should be available

## Configuration

### MySQL Server Configuration
```sql
-- Recommended MySQL settings
SET GLOBAL wait_timeout = 28800;
SET GLOBAL interactive_timeout = 28800;
SET GLOBAL max_connections = 200;
```

### Application Configuration
```json
{
  "database": {
    "connect_timeout": 10,
    "read_timeout": 30,
    "write_timeout": 30,
    "charset": "utf8mb4"
  }
}
```

## Troubleshooting

### Common Issues

1. **Persistent Connection Errors**
   - Check MySQL server status
   - Verify network connectivity
   - Review MySQL error logs

2. **High Retry Rates**
   - Monitor connection pool usage
   - Check for connection leaks
   - Review timeout settings

3. **Performance Impact**
   - Monitor retry delays
   - Check connection creation overhead
   - Review error logging verbosity

### Debug Commands
```bash
# Check connection status
myrepetl status --target target1

# Monitor connection errors
tail -f /var/log/myrepetl/error.log | grep "MySQL connection error"

# Test connection
myrepetl test-connection --target target1
```