"""
Transform service for MySQL Replication ETL
"""

import importlib
import importlib.util
from typing import Dict, Any, Optional, Callable
from functools import lru_cache

from ..exceptions import TransformError
from ..models.transforms import TransformFunction, TransformResult
from ..models.config import ColumnMapping


class TransformService:
    """Service for data transformations"""
    
    def __init__(self):
        self._transform_module = None
        self._transform_functions: Dict[str, TransformFunction] = {}
        self._register_default_transforms()
    
    def load_transform_module(self, module_name: str = "transform", config_dir: str = None) -> None:
        """Load transform module dynamically"""
        import structlog
        logger = structlog.get_logger()
        
        logger.debug("Starting transform module loading", 
                    module_name=module_name, 
                    config_dir=config_dir)
        
        try:
            # Try to import as module first
            logger.debug("Attempting to import as Python module", module_name=module_name)
            self._transform_module = importlib.import_module(module_name)
            logger.debug("Successfully imported as Python module", module_name=module_name)
        except ImportError as e:
            logger.debug("Failed to import as Python module, trying file path", 
                        module_name=module_name, 
                        error=str(e))
            # If module import fails, try to load as file path
            try:
                import sys
                import os
                
                # Check if it's a file path
                if os.path.exists(module_name) and module_name.endswith('.py'):
                    logger.debug("Loading module from file path", file_path=module_name)
                    # Load module from file path
                    spec = importlib.util.spec_from_file_location("transform", module_name)
                    if spec and spec.loader:
                        self._transform_module = importlib.util.module_from_spec(spec)
                        spec.loader.exec_module(self._transform_module)
                        logger.debug("Successfully loaded module from file path", file_path=module_name)
                    else:
                        raise TransformError(f"Could not load module from file: {module_name}")
                else:
                    # Try to find transform.py in config directory
                    if config_dir:
                        logger.debug("Searching for transform file in config directory", 
                                   config_dir=config_dir, 
                                   module_name=module_name)
                        
                        # List files in config directory for debugging
                        try:
                            files_in_dir = os.listdir(config_dir)
                            logger.debug("Files in config directory", 
                                       config_dir=config_dir, 
                                       files=files_in_dir)
                        except Exception as list_error:
                            logger.debug("Could not list files in config directory", 
                                       config_dir=config_dir, 
                                       error=str(list_error))
                        
                        transform_path = os.path.join(config_dir, f"{module_name}.py")
                        logger.debug("Looking for transform file", 
                                   transform_path=transform_path, 
                                   exists=os.path.exists(transform_path))
                        
                        if os.path.exists(transform_path):
                            logger.debug("Found transform file, loading", transform_path=transform_path)
                            spec = importlib.util.spec_from_file_location("transform", transform_path)
                            if spec and spec.loader:
                                self._transform_module = importlib.util.module_from_spec(spec)
                                spec.loader.exec_module(self._transform_module)
                                logger.debug("Successfully loaded transform module from config directory", 
                                           transform_path=transform_path)
                            else:
                                raise TransformError(f"Could not load module from file: {transform_path}")
                        else:
                            raise TransformError(f"Failed to import transform module '{module_name}': No module named '{module_name}' and no {module_name}.py found in {config_dir}")
                    else:
                        logger.debug("No config directory provided")
                        raise TransformError(f"Failed to import transform module '{module_name}': No module named '{module_name}'")
            except Exception as e:
                logger.error("Error loading transform module", 
                           module_name=module_name, 
                           config_dir=config_dir, 
                           error=str(e))
                raise TransformError(f"Failed to import transform module '{module_name}': {e}")
        except Exception as e:
            logger.error("Unexpected error loading transform module", 
                       module_name=module_name, 
                       config_dir=config_dir, 
                       error=str(e))
            raise TransformError(f"Unexpected error loading transform module: {e}")
    
    def get_transform_function(self, transform_path: str) -> Optional[Callable]:
        """Get transform function by path"""
        if not self._transform_module:
            raise TransformError("Transform module not loaded")
        
        try:
            # Parse path like "transform.uppercase"
            parts = transform_path.split('.')
            if len(parts) != 2 or parts[0] != 'transform':
                raise TransformError(f"Invalid transform path format: {transform_path}")
            
            function_name = parts[1]
            if hasattr(self._transform_module, function_name):
                return getattr(self._transform_module, function_name)
            else:
                raise TransformError(f"Transform function not found: {function_name}")
        except Exception as e:
            raise TransformError(f"Error getting transform function: {e}")
    
    def apply_column_transforms(self, row_data: Dict[str, Any], column_mapping: Dict[str, ColumnMapping], source_table: str = None) -> Dict[str, Any]:
        """Apply transformations to row data according to column mapping"""
        import structlog
        logger = structlog.get_logger()
        
        logger.info("Starting column transformations", 
                   source_table=source_table,
                   columns_count=len(column_mapping),
                   row_keys=list(row_data.keys()) if row_data else [])
        
        transformed_data = {}
        transformation_count = 0
        static_value_count = 0
        copy_count = 0
        
        for source_col, target_config in column_mapping.items():
            try:
                target_col = target_config.column
                
                if target_config.value is not None:
                    # Static value
                    transformed_data[target_col] = target_config.value
                    static_value_count += 1
                    logger.debug("Applied static value", 
                               source_column=source_col,
                               target_column=target_col,
                               value=target_config.value)
                elif target_config.transform:
                    # Apply transformation
                    transform_func = self.get_transform_function(target_config.transform)
                    if transform_func:
                        value = row_data.get(source_col)
                        logger.debug("Applying transformation", 
                                   source_column=source_col,
                                   target_column=target_col,
                                   transform=target_config.transform,
                                   original_value=value)
                        
                        transformed_value = transform_func(value, row_data, source_table)
                        transformed_data[target_col] = transformed_value
                        transformation_count += 1
                        
                        logger.debug("Transformation completed", 
                                   source_column=source_col,
                                   target_column=target_col,
                                   transform=target_config.transform,
                                   original_value=value,
                                   transformed_value=transformed_value)
                    else:
                        # Fallback to original value
                        transformed_data[target_col] = row_data.get(source_col)
                        copy_count += 1
                        logger.warning("Transform function not found, using original value", 
                                     source_column=source_col,
                                     target_column=target_col,
                                     transform=target_config.transform)
                else:
                    # Simple copy
                    transformed_data[target_col] = row_data.get(source_col)
                    copy_count += 1
                    logger.debug("Copied value", 
                               source_column=source_col,
                               target_column=target_col,
                               value=row_data.get(source_col))
                    
            except Exception as e:
                # Log error but continue with original value
                transformed_data[target_col] = row_data.get(source_col)
                logger.error("Error transforming column", 
                           source_column=source_col,
                           target_column=target_col,
                           error=str(e),
                           original_value=row_data.get(source_col))
                raise TransformError(f"Error transforming column '{source_col}': {e}")
        
        logger.info("Column transformations completed", 
                   source_table=source_table,
                   transformations_applied=transformation_count,
                   static_values_applied=static_value_count,
                   values_copied=copy_count,
                   result_keys=list(transformed_data.keys()))
        
        return transformed_data
    
    def register_transform(self, name: str, function: Callable, description: str = None) -> None:
        """Register a custom transform function"""
        transform_func = TransformFunction(
            name=name,
            function=function,
            description=description
        )
        self._transform_functions[name] = transform_func
    
    def get_registered_transforms(self) -> Dict[str, TransformFunction]:
        """Get all registered transform functions"""
        return self._transform_functions.copy()
    
    def _register_default_transforms(self) -> None:
        """Register default transform functions"""
        # Uppercase transform
        self.register_transform(
            "uppercase",
            lambda x: x.upper() if isinstance(x, str) else x,
            "Convert string to uppercase"
        )
        
        # Lowercase transform
        self.register_transform(
            "lowercase", 
            lambda x: x.lower() if isinstance(x, str) else x,
            "Convert string to lowercase"
        )
        
        # Trim transform
        self.register_transform(
            "trim",
            lambda x: x.strip() if isinstance(x, str) else x,
            "Remove leading and trailing whitespace"
        )
        
        # String length transform
        self.register_transform(
            "length",
            lambda x: len(str(x)) if x is not None else 0,
            "Get string length"
        )
    
    @lru_cache(maxsize=128)
    def _get_cached_transform(self, transform_path: str) -> Optional[Callable]:
        """Get cached transform function"""
        return self.get_transform_function(transform_path)
