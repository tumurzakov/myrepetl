FROM python:3.11-slim

# Set working directory
WORKDIR /app

# Install system dependencies
RUN apt-get update && apt-get install -y \
    default-libmysqlclient-dev \
    pkg-config \
    && rm -rf /var/lib/apt/lists/*

# Copy requirements first for better caching
COPY requirements.txt .

# Install Python dependencies
RUN pip install --no-cache-dir -r requirements.txt

# Copy ETL tool source code
COPY . .

# Create directories for configs and logs
RUN mkdir -p /app/configs /app/logs /app/backups

# Set environment variables
ENV PYTHONPATH=/app
ENV ETL_CONFIG_DIR=/app/configs
ENV ETL_LOG_DIR=/app/logs
ENV ETL_BACKUP_DIR=/app/backups

# Create non-root user
RUN useradd -m -u 1000 etluser && \
    chown -R etluser:etluser /app
USER etluser

# Expose port for monitoring (if needed)
EXPOSE 8080

# Default command
CMD ["python", "cli.py", "--help"]

# Health check
HEALTHCHECK --interval=30s --timeout=10s --start-period=5s --retries=3 \
    CMD python -c "import cli; print('ETL tool is healthy')" || exit 1
