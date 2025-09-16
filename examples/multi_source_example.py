"""
Multi-Source ETL Example

This example demonstrates how to set up an ETL pipeline with multiple sources and targets.
"""

import sys
import os
import time
import logging

# Add parent directory to path to import ETL modules
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from src.etl_service import ETLService


def setup_logging():
    """Setup logging configuration"""
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
    )


def main():
    """Main function"""
    setup_logging()
    logger = logging.getLogger("MultiSourceExample")
    
    try:
        # Initialize ETL service
        etl_service = ETLService()
        
        # Load configuration from demo_pipeline.json
        config_path = os.path.join(os.path.dirname(__file__), '..', 'configs', 'demo_pipeline.json')
        etl_service.initialize(config_path)
        logger.info("ETL service initialized")
        
        # Test connections
        if etl_service.test_connections():
            logger.info("All connections tested successfully")
        else:
            logger.error("Connection test failed")
            return
        
        # Start replication process
        logger.info("Starting replication process...")
        etl_service.run_replication()
        
    except KeyboardInterrupt:
        logger.info("Received interrupt signal")
    except Exception as e:
        logger.error(f"Error in ETL process: {e}")
        raise
    finally:
        # Cleanup is handled by ETLService
        logger.info("ETL process completed")


if __name__ == "__main__":
    main()