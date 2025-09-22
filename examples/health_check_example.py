#!/usr/bin/env python3
"""
Example script demonstrating health check endpoint usage
"""

import json
import requests
import time
from typing import Dict, Any

def check_system_health(health_url: str) -> Dict[str, Any]:
    """Check system health and return parsed response"""
    try:
        response = requests.get(health_url, timeout=5)
        
        if response.status_code == 200:
            return {
                "status_code": response.status_code,
                "healthy": True,
                "data": response.json()
            }
        else:
            return {
                "status_code": response.status_code,
                "healthy": False,
                "error": f"HTTP {response.status_code}: {response.text}"
            }
    except requests.exceptions.RequestException as e:
        return {
            "status_code": None,
            "healthy": False,
            "error": str(e)
        }

def print_health_summary(health_data: Dict[str, Any]):
    """Print a summary of health status"""
    if not health_data["healthy"]:
        print(f"❌ System is unhealthy: {health_data.get('error', 'Unknown error')}")
        return
    
    data = health_data["data"]
    status = data["status"]
    uptime = data["uptime_seconds"]
    components = data["components"]
    
    # Status emoji
    status_emoji = {
        "healthy": "✅",
        "warning": "⚠️",
        "critical": "🚨",
        "unhealthy": "❌"
    }.get(status, "❓")
    
    print(f"{status_emoji} System Status: {status.upper()}")
    print(f"⏱️  Uptime: {uptime:.1f} seconds")
    print()
    
    # Thread status
    threads = components["threads"]
    print("🧵 Threads:")
    for thread_type, info in threads.items():
        count = info["count"]
        total = info["total"]
        status = info["status"]
        emoji = "✅" if status == "healthy" else "⚠️" if status == "warning" else "❌"
        print(f"  {emoji} {thread_type}: {count}/{total} running ({status})")
    print()
    
    # Database connections
    db_conn = components["database_connections"]
    print("🗄️  Database Connections:")
    
    sources = db_conn["sources"]
    sources_emoji = "✅" if sources["status"] == "healthy" else "⚠️" if sources["status"] == "warning" else "❌"
    print(f"  {sources_emoji} Sources: {sources['connected']}/{sources['total']} connected ({sources['status']})")
    
    targets = db_conn["targets"]
    targets_emoji = "✅" if targets["status"] == "healthy" else "⚠️" if targets["status"] == "warning" else "❌"
    print(f"  {targets_emoji} Targets: {targets['connected']}/{targets['total']} connected ({targets['status']})")
    print()
    
    # Replication
    replication = components["replication_connections"]
    repl_emoji = "✅" if replication["status"] == "healthy" else "⚠️" if replication["status"] == "warning" else "❌"
    print(f"🔄 Replication: {repl_emoji} {replication['connected']}/{replication['total']} connected ({replication['status']})")
    print()
    
    # Message queue
    queue = components["message_queue"]
    queue_emoji = "✅" if queue["status"] == "healthy" else "⚠️" if queue["status"] == "warning" else "🚨" if queue["status"] == "critical" else "❌"
    usage_percent = queue["usage_percent"]
    print(f"📬 Message Queue: {queue_emoji} {queue['size']}/{queue['max_size']} ({usage_percent:.1f}% used) - {queue['status']}")

def monitor_health(health_url: str, interval: int = 30):
    """Continuously monitor system health"""
    print(f"🔍 Monitoring system health at {health_url}")
    print(f"⏰ Check interval: {interval} seconds")
    print("Press Ctrl+C to stop")
    print("-" * 50)
    
    try:
        while True:
            health_data = check_system_health(health_url)
            timestamp = time.strftime("%Y-%m-%d %H:%M:%S")
            print(f"\n[{timestamp}]")
            print_health_summary(health_data)
            print("-" * 50)
            time.sleep(interval)
    except KeyboardInterrupt:
        print("\n👋 Monitoring stopped")

def main():
    """Main function"""
    import argparse
    
    parser = argparse.ArgumentParser(description="MyRepETL Health Check Tool")
    parser.add_argument("--url", default="http://localhost:8080/health", 
                       help="Health check URL (default: http://localhost:8080/health)")
    parser.add_argument("--monitor", action="store_true", 
                       help="Continuously monitor health status")
    parser.add_argument("--interval", type=int, default=30, 
                       help="Monitoring interval in seconds (default: 30)")
    parser.add_argument("--json", action="store_true", 
                       help="Output raw JSON response")
    
    args = parser.parse_args()
    
    if args.monitor:
        monitor_health(args.url, args.interval)
    else:
        health_data = check_system_health(args.url)
        
        if args.json:
            print(json.dumps(health_data, indent=2))
        else:
            print_health_summary(health_data)

if __name__ == "__main__":
    main()
