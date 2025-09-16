"""
SQL builder utilities for MySQL Replication ETL
"""

from typing import Dict, Any, List, Tuple


class SQLBuilder:
    """Utility class for building SQL statements"""
    
    @staticmethod
    def build_upsert_sql(table_name: str, data: Dict[str, Any], primary_key: str) -> Tuple[str, List[Any]]:
        """
        Build UPSERT SQL statement (INSERT ... ON DUPLICATE KEY UPDATE)
        
        Args:
            table_name: Target table name
            data: Data dictionary with column names and values
            primary_key: Primary key column name
            
        Returns:
            Tuple of (SQL statement, values list)
        """
        if not data:
            raise ValueError("Data cannot be empty")
        
        columns = list(data.keys())
        placeholders = ['%s'] * len(columns)
        
        # Build INSERT statement
        insert_sql = f"""INSERT INTO {table_name} ({', '.join(columns)})
VALUES ({', '.join(placeholders)})
ON DUPLICATE KEY UPDATE
"""
        
        # Build UPDATE clause for all columns except primary key
        update_parts = []
        for col in columns:
            if col != primary_key:
                update_parts.append(f"{col} = VALUES({col})")
        
        if update_parts:
            insert_sql += ', '.join(update_parts)
        else:
            # If only primary key, just update the primary key itself
            insert_sql += f"{primary_key} = VALUES({primary_key})"
        
        values = list(data.values())
        return insert_sql.strip(), values
    
    @staticmethod
    def build_delete_sql(table_name: str, data: Dict[str, Any], primary_key: str) -> Tuple[str, List[Any]]:
        """
        Build DELETE SQL statement
        
        Args:
            table_name: Target table name
            data: Data dictionary containing primary key value
            primary_key: Primary key column name
            
        Returns:
            Tuple of (SQL statement, values list)
        """
        if primary_key not in data:
            raise ValueError(f"Primary key '{primary_key}' not found in data")
        
        delete_sql = f"DELETE FROM {table_name} WHERE {primary_key} = %s"
        values = [data[primary_key]]
        return delete_sql, values
    
    @staticmethod
    def build_insert_sql(table_name: str, data: Dict[str, Any]) -> Tuple[str, List[Any]]:
        """
        Build INSERT SQL statement
        
        Args:
            table_name: Target table name
            data: Data dictionary with column names and values
            
        Returns:
            Tuple of (SQL statement, values list)
        """
        if not data:
            raise ValueError("Data cannot be empty")
        
        columns = list(data.keys())
        placeholders = ['%s'] * len(columns)
        
        insert_sql = f"""INSERT INTO {table_name} ({', '.join(columns)})
VALUES ({', '.join(placeholders)})"""
        
        values = list(data.values())
        return insert_sql.strip(), values
    
    @staticmethod
    def build_update_sql(table_name: str, data: Dict[str, Any], primary_key: str) -> Tuple[str, List[Any]]:
        """
        Build UPDATE SQL statement
        
        Args:
            table_name: Target table name
            data: Data dictionary with column names and values
            primary_key: Primary key column name
            
        Returns:
            Tuple of (SQL statement, values list)
        """
        if not data:
            raise ValueError("Data cannot be empty")
        
        if primary_key not in data:
            raise ValueError(f"Primary key '{primary_key}' not found in data")
        
        # Build SET clause for all columns except primary key
        set_parts = []
        values = []
        
        for col, value in data.items():
            if col != primary_key:
                set_parts.append(f"{col} = %s")
                values.append(value)
        
        if not set_parts:
            raise ValueError("No columns to update (only primary key provided)")
        
        update_sql = f"""UPDATE {table_name} 
SET {', '.join(set_parts)}
WHERE {primary_key} = %s"""
        
        values.append(data[primary_key])
        return update_sql.strip(), values
    
    @staticmethod
    def build_batch_upsert_sql(table_name: str, data_list: List[Dict[str, Any]], primary_key: str) -> Tuple[str, List[List[Any]]]:
        """
        Build batch UPSERT SQL statement for multiple rows
        
        Args:
            table_name: Target table name
            data_list: List of data dictionaries
            primary_key: Primary key column name
            
        Returns:
            Tuple of (SQL statement, values list for executemany)
        """
        if not data_list:
            raise ValueError("Data list cannot be empty")
        
        # Use first row to determine columns
        first_row = data_list[0]
        columns = list(first_row.keys())
        placeholders = ['%s'] * len(columns)
        
        # Build INSERT statement
        insert_sql = f"""INSERT INTO {table_name} ({', '.join(columns)})
VALUES ({', '.join(placeholders)})
ON DUPLICATE KEY UPDATE
"""
        
        # Build UPDATE clause for all columns except primary key
        update_parts = []
        for col in columns:
            if col != primary_key:
                update_parts.append(f"{col} = VALUES({col})")
        
        if update_parts:
            insert_sql += ', '.join(update_parts)
        else:
            insert_sql += f"{primary_key} = VALUES({primary_key})"
        
        # Prepare values for executemany
        values_list = [list(row.values()) for row in data_list]
        
        return insert_sql.strip(), values_list
