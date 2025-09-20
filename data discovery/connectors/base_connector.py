"""
Base connector class for all data source connectors
"""

from abc import ABC, abstractmethod
from typing import List, Dict, Any
import logging


class BaseConnector(ABC):
    """
    Abstract base class for all data source connectors
    """
    
    def __init__(self, config: Dict[str, Any]):
        self.config = config
        self.logger = logging.getLogger(self.__class__.__name__)
    
    @abstractmethod
    def discover_assets(self) -> List[Dict[str, Any]]:
        """
        Discover data assets from the source
        
        Returns:
            List of asset dictionaries with standardized format:
            {
                'name': str,
                'type': str,  # 'table', 'file', 'bucket', 'database', etc.
                'source': str,  # 'aws_s3', 'local_file', 'mysql', etc.
                'location': str,  # Full path or URI
                'size': int,  # Size in bytes if applicable
                'created_date': datetime,
                'modified_date': datetime,
                'schema': Dict,  # Schema information if available
                'tags': List[str],
                'metadata': Dict  # Additional metadata
            }
        """
        pass
    
    @abstractmethod
    def test_connection(self) -> bool:
        """
        Test connection to the data source
        
        Returns:
            True if connection successful, False otherwise
        """
        pass
    
    def validate_config(self) -> bool:
        """
        Validate connector configuration
        
        Returns:
            True if configuration is valid, False otherwise
        """
        return True
    
    def get_connection_info(self) -> Dict[str, Any]:
        """
        Get connection information (without sensitive data)
        
        Returns:
            Dictionary with connection details
        """
        return {
            'connector_type': self.__class__.__name__,
            'config_keys': list(self.config.keys())
        }
