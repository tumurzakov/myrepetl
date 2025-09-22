"""
HTTP endpoint for Prometheus metrics
Provides /metrics endpoint for Prometheus scraping
"""

import json
import threading
import time
from typing import Optional
from http.server import HTTPServer, BaseHTTPRequestHandler
import structlog

from .metrics_service import MetricsService


class MetricsHandler(BaseHTTPRequestHandler):
    """HTTP handler for metrics endpoint"""
    
    def __init__(self, metrics_service: MetricsService, *args, **kwargs):
        self.metrics_service = metrics_service
        super().__init__(*args, **kwargs)
    
    def do_GET(self):
        """Handle GET requests"""
        if self.path == '/metrics':
            self._handle_metrics()
        elif self.path == '/health':
            self._handle_health()
        else:
            self._handle_not_found()
    
    def _handle_metrics(self):
        """Handle /metrics endpoint"""
        try:
            # Get metrics from service
            metrics_data = self.metrics_service.get_metrics()
            content_type = self.metrics_service.get_content_type()
            
            # Send response
            self.send_response(200)
            self.send_header('Content-Type', content_type)
            self.send_header('Content-Length', str(len(metrics_data.encode('utf-8'))))
            self.end_headers()
            self.wfile.write(metrics_data.encode('utf-8'))
            
        except Exception as e:
            self.logger.error("Error serving metrics", error=str(e))
            self._handle_error()
    
    def _handle_health(self):
        """Handle /health endpoint"""
        try:
            # Get comprehensive health status
            health_status = self.metrics_service.get_health_status()
            
            # Determine HTTP status code based on health
            if health_status["status"] == "healthy":
                http_status = 200
            elif health_status["status"] == "warning":
                http_status = 200  # Still OK but with warnings
            elif health_status["status"] == "critical":
                http_status = 503  # Service Unavailable
            else:  # unhealthy or error
                http_status = 503
            
            # Format health data as JSON
            health_data = json.dumps(health_status, indent=2, default=str)
            
            self.send_response(http_status)
            self.send_header('Content-Type', 'application/json')
            self.send_header('Content-Length', str(len(health_data.encode('utf-8'))))
            self.end_headers()
            self.wfile.write(health_data.encode('utf-8'))
            
        except Exception as e:
            self.logger.error("Error serving health check", error=str(e))
            self._handle_error()
    
    def _handle_not_found(self):
        """Handle 404 errors"""
        self.send_response(404)
        self.send_header('Content-Type', 'text/plain')
        self.end_headers()
        self.wfile.write(b'Not Found')
    
    def _handle_error(self):
        """Handle 500 errors"""
        self.send_response(500)
        self.send_header('Content-Type', 'text/plain')
        self.end_headers()
        self.wfile.write(b'Internal Server Error')
    
    def log_message(self, format, *args):
        """Override to use structlog"""
        logger = structlog.get_logger()
        logger.info("HTTP request", message=format % args)


class MetricsEndpoint:
    """HTTP endpoint for Prometheus metrics"""
    
    def __init__(self, metrics_service: MetricsService, host: str = '0.0.0.0', port: int = 8080):
        self.metrics_service = metrics_service
        self.host = host
        self.port = port
        self.logger = structlog.get_logger()
        
        # Server management
        self._server: Optional[HTTPServer] = None
        self._server_thread: Optional[threading.Thread] = None
        self._shutdown_requested = False
        self._shutdown_lock = threading.Lock()
        
        # System info
        self._start_time = time.time()
        
        self.logger.info("Metrics endpoint initialized", host=host, port=port)
    
    def start(self) -> None:
        """Start the metrics HTTP server"""
        with self._shutdown_lock:
            if self._shutdown_requested:
                self.logger.warning("Cannot start metrics endpoint, shutdown already requested")
                return
        
        if self._server_thread and self._server_thread.is_alive():
            self.logger.warning("Metrics endpoint already running")
            return
        
        try:
            # Create handler factory
            def handler_factory(*args, **kwargs):
                return MetricsHandler(self.metrics_service, *args, **kwargs)
            
            # Create HTTP server
            self._server = HTTPServer((self.host, self.port), handler_factory)
            
            # Start server in separate thread
            self._server_thread = threading.Thread(
                target=self._run_server, 
                name="metrics_endpoint",
                daemon=True
            )
            self._server_thread.start()
            
            # Set system info
            self.metrics_service.set_system_info(
                version="1.0.0",
                build_date=time.strftime("%Y-%m-%d %H:%M:%S", time.localtime(self._start_time))
            )
            
            self.logger.info("Metrics endpoint started", 
                           host=self.host, 
                           port=self.port,
                           metrics_url=f"http://{self.host}:{self.port}/metrics",
                           health_url=f"http://{self.host}:{self.port}/health")
            
        except Exception as e:
            self.logger.error("Failed to start metrics endpoint", error=str(e))
            raise
    
    def stop(self) -> None:
        """Stop the metrics HTTP server"""
        with self._shutdown_lock:
            self._shutdown_requested = True
        
        if self._server:
            try:
                self._server.shutdown()
                self.logger.info("Metrics endpoint server shutdown requested")
            except Exception as e:
                self.logger.warning("Error shutting down metrics endpoint server", error=str(e))
        
        # Wait for server thread to finish
        if self._server_thread and self._server_thread.is_alive():
            self._server_thread.join(timeout=5.0)
            if self._server_thread.is_alive():
                self.logger.warning("Metrics endpoint thread did not stop gracefully")
        
        self.logger.info("Metrics endpoint stopped")
    
    def _run_server(self) -> None:
        """Run the HTTP server"""
        try:
            self.logger.info("Starting metrics HTTP server", 
                           host=self.host, 
                           port=self.port)
            
            # Update uptime metric periodically
            last_uptime_update = time.time()
            
            while not self._shutdown_requested:
                try:
                    # Handle one request with timeout
                    self._server.timeout = 1.0
                    self._server.handle_request()
                    
                    # Update uptime metric every 10 seconds
                    current_time = time.time()
                    if current_time - last_uptime_update >= 10.0:
                        uptime = current_time - self._start_time
                        self.metrics_service.set_system_uptime(uptime)
                        last_uptime_update = current_time
                    
                except Exception as e:
                    if not self._shutdown_requested:
                        self.logger.warning("Error handling HTTP request", error=str(e))
                    continue
            
            self.logger.info("Metrics HTTP server stopped")
            
        except Exception as e:
            self.logger.error("Error in metrics HTTP server", error=str(e))
    
    def is_running(self) -> bool:
        """Check if the endpoint is running"""
        return (self._server_thread is not None and 
                self._server_thread.is_alive() and 
                not self._shutdown_requested)
    
    def get_url(self) -> str:
        """Get the metrics URL"""
        return f"http://{self.host}:{self.port}/metrics"
    
    def get_health_url(self) -> str:
        """Get the health check URL"""
        return f"http://{self.host}:{self.port}/health"
