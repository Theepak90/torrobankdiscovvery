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
    
    # Metadata for dynamic discovery
    connector_type: str = "base"
    connector_name: str = "Base Connector"
    description: str = "Base connector class"
    category: str = "other"
    supported_services: List[str] = []
    required_config_fields: List[str] = []
    optional_config_fields: List[str] = []
    
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
    
    def validate_credentials(self, required_fields: List[str]) -> bool:
        """
        Validate that required credential fields are present
        
        Args:
            required_fields: List of required configuration fields
            
        Returns:
            True if all required fields are present, False otherwise
        """
        missing_fields = []
        for field in required_fields:
            if not self.config.get(field):
                missing_fields.append(field)
        
        if missing_fields:
            self.logger.error(f"Missing required credential fields: {missing_fields}")
            return False
        
        return True
    
    def mask_sensitive_data(self, data: Dict[str, Any], sensitive_fields: List[str] = None) -> Dict[str, Any]:
        """
        Mask sensitive data in configuration for logging
        
        Args:
            data: Dictionary to mask
            sensitive_fields: List of fields to mask (default: common sensitive fields)
            
        Returns:
            Dictionary with sensitive data masked
        """
        if sensitive_fields is None:
            sensitive_fields = ['password', 'secret', 'key', 'token', 'credentials', 'auth']
        
        masked_data = data.copy()
        for key, value in masked_data.items():
            if any(sensitive in key.lower() for sensitive in sensitive_fields):
                if isinstance(value, str) and len(value) > 8:
                    masked_data[key] = value[:4] + '*' * (len(value) - 8) + value[-4:]
                else:
                    masked_data[key] = '***'
            elif isinstance(value, dict):
                masked_data[key] = self.mask_sensitive_data(value, sensitive_fields)
        
        return masked_data