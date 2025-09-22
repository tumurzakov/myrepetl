#!/bin/bash

# MyRepETL Monitoring Setup Script
# This script sets up Prometheus and Grafana for MyRepETL monitoring

set -e

echo "🚀 Setting up MyRepETL monitoring stack..."

# Check if Docker is installed
if ! command -v docker &> /dev/null; then
    echo "❌ Docker is not installed. Please install Docker first."
    exit 1
fi

# Check if Docker Compose is installed
if ! command -v docker-compose &> /dev/null; then
    echo "❌ Docker Compose is not installed. Please install Docker Compose first."
    exit 1
fi

# Create necessary directories
echo "📁 Creating directories..."
mkdir -p grafana_data
mkdir -p prometheus_data
mkdir -p alertmanager_data

# Set proper permissions
echo "🔐 Setting permissions..."
chmod 755 grafana_data
chmod 755 prometheus_data
chmod 755 alertmanager_data

# Start the monitoring stack
echo "🐳 Starting monitoring stack..."
docker-compose up -d

# Wait for services to be ready
echo "⏳ Waiting for services to start..."
sleep 10

# Check if services are running
echo "🔍 Checking service status..."
if docker-compose ps | grep -q "Up"; then
    echo "✅ Monitoring stack is running!"
    echo ""
    echo "📊 Access URLs:"
    echo "  - Grafana: http://localhost:3000 (admin/admin)"
    echo "  - Prometheus: http://localhost:9090"
    echo "  - AlertManager: http://localhost:9093"
    echo ""
    echo "📋 Next steps:"
    echo "  1. Open Grafana at http://localhost:3000"
    echo "  2. Login with admin/admin"
    echo "  3. Import the MyRepETL dashboards:"
    echo "     - Go to Dashboards → Import"
    echo "     - Upload myrepetl-dashboard.json"
    echo "     - Upload myrepetl-health-dashboard.json"
    echo "  4. Make sure MyRepETL is running and exposing metrics on port 8080"
    echo ""
    echo "🔧 To stop the monitoring stack:"
    echo "  docker-compose down"
    echo ""
    echo "🔧 To view logs:"
    echo "  docker-compose logs -f"
else
    echo "❌ Failed to start monitoring stack. Check logs:"
    docker-compose logs
    exit 1
fi
