#!/usr/bin/env python3
"""
Test script to debug database connection issues
"""

import pymysql
import sys

def test_target_connection():
    """Test target database connection"""
    try:
        print("Testing target database connection...")
        print(f"Host: mysql-target")
        print(f"Port: 3306")
        print(f"User: target_user")
        print(f"Password: target_password")
        print(f"Database: target_db")
        
        # Try with all parameters explicitly
        connection = pymysql.connect(
            host='mysql-target',
            port=3306,
            user='target_user',
            password='target_password',
            database='target_db',
            charset='utf8mb4',
            autocommit=False
        )
        
        with connection.cursor() as cursor:
            cursor.execute("SELECT 1 as test")
            result = cursor.fetchone()
            print(f"Connection successful! Result: {result}")
            
        connection.close()
        return True
        
    except Exception as e:
        print(f"Connection failed: {e}")
        return False

def test_source_connection():
    """Test source database connection"""
    try:
        print("\nTesting source database connection...")
        print(f"Host: mysql-source")
        print(f"Port: 3306")
        print(f"User: replication_user")
        print(f"Password: replication_password")
        print(f"Database: source_db")
        
        connection = pymysql.connect(
            host='mysql-source',
            port=3306,
            user='replication_user',
            passwd='replication_password',  # Use passwd instead of password
            db='source_db',  # Use db instead of database
            charset='utf8mb4',
            autocommit=False
        )
        
        with connection.cursor() as cursor:
            cursor.execute("SELECT 1 as test")
            result = cursor.fetchone()
            print(f"Connection successful! Result: {result}")
            
        connection.close()
        return True
        
    except Exception as e:
        print(f"Connection failed: {e}")
        return False

if __name__ == "__main__":
    print("=== Database Connection Test ===")
    
    target_ok = test_target_connection()
    source_ok = test_source_connection()
    
    if target_ok and source_ok:
        print("\n✅ All connections successful!")
        sys.exit(0)
    else:
        print("\n❌ Some connections failed!")
        sys.exit(1)
