"""
Logger Configuration - Centralized logging setup for data discovery
"""

import logging
import logging.handlers
from typing import Dict, Any
from pathlib import Path
def setup_logger(config: Dict[str, Any]) -> logging.Logger:
    """
    Setup centralized logging for data discovery
    
    Args:
        config: Logging configuration dictionary
        
    Returns:
        Configured logger instance
    """
    log_level = config.get('level', 'INFO').upper()
    log_file = config.get('file', 'data_discovery.log')
    max_size_mb = config.get('max_size_mb', 100)
    backup_count = config.get('backup_count', 5)
    
    logger = logging.getLogger('DataDiscovery')
    logger.setLevel(getattr(logging, log_level, logging.INFO))
    
    logger.handlers.clear()
    
    formatter = logging.Formatter(
        '%(asctime)s - %(name)s - %(levelname)s - %(message)s',
        datefmt='%Y-%m-%d %H:%M:%S'
    )
    
    console_handler = logging.StreamHandler()
    console_handler.setLevel(getattr(logging, log_level, logging.INFO))
    console_handler.setFormatter(formatter)
    logger.addHandler(console_handler)
    
    if log_file:
        try:
            log_path = Path(log_file)
            log_path.parent.mkdir(parents=True, exist_ok=True)
            
            file_handler = logging.handlers.RotatingFileHandler(
                log_file,
                maxBytes=max_size_mb * 1024 * 1024,  # Convert MB to bytes
                backupCount=backup_count
            )
            file_handler.setLevel(getattr(logging, log_level, logging.INFO))
            file_handler.setFormatter(formatter)
            logger.addHandler(file_handler)
            
        except Exception as e:
            logger.error(f"Failed to setup file logging: {e}")
    
    logger.info("Data Discovery logging initialized")
    return logger
def get_logger(name: str = None) -> logging.Logger:
    """
    Get a logger instance
    
    Args:
        name: Logger name (optional)
        
    Returns:
        Logger instance
    """
    if name:
        return logging.getLogger(f'DataDiscovery.{name}')
    else:
        return logging.getLogger('DataDiscovery')
