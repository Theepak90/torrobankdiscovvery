"""
Dynamic Connector Registry - Plugin-based connector management
Allows dynamic loading and registration of connectors without hardcoding
"""

import importlib
import inspect
from typing import Dict, Type, Any, List
from pathlib import Path
import logging

from connectors.base_connector import BaseConnector
class ConnectorRegistry:
    """
    Dynamic registry for managing connectors
    Automatically discovers and loads connector classes
    """
    
    def __init__(self):
        self.connectors: Dict[str, Type[BaseConnector]] = {}
        self.connector_configs: Dict[str, Dict[str, Any]] = {}
        self.logger = logging.getLogger(__name__)
        self.discover_connectors()
        
    def discover_connectors(self, connectors_dir: str = "connectors") -> None:
        """
        Dynamically discover all connector classes in the connectors directory
        """
        connectors_path = Path(connectors_dir)
        
        if not connectors_path.exists():
            self.logger.error(f"Connectors directory {connectors_dir} not found")
            return
            
        for file_path in connectors_path.glob("*.py"):
            if file_path.name.startswith("__") or file_path.name == "base_connector.py":
                continue
                
            module_name = file_path.stem
            try:
                module = importlib.import_module(f"connectors.{module_name}")
                
                for name, obj in inspect.getmembers(module, inspect.isclass):
                    if (issubclass(obj, BaseConnector) and 
                        obj != BaseConnector and 
                        hasattr(obj, 'connector_type')):
                        
                        connector_type = getattr(obj, 'connector_type', module_name)
                        self.connectors[connector_type] = obj
                        self.logger.info(f"Discovered connector: {connector_type}")
                        
            except Exception as e:
                self.logger.error(f"Failed to load connector {module_name}: {e}")
    
    def register_connector(self, connector_type: str, connector_class: Type[BaseConnector]) -> None:
        """Register a connector class"""
        self.connectors[connector_type] = connector_class
        self.logger.info(f"Registered connector: {connector_type}")
    
    def get_connector_class(self, connector_type: str) -> Type[BaseConnector]:
        """Get connector class by type"""
        return self.connectors.get(connector_type)
    
    def create_connector(self, connector_type: str, config: Dict[str, Any]) -> BaseConnector:
        """Create a connector instance with given configuration"""
        connector_class = self.get_connector_class(connector_type)
        if not connector_class:
            raise ValueError(f"Connector type '{connector_type}' not found")
        
        return connector_class(config)
    
    def get_available_connectors(self) -> List[str]:
        """Get list of available connector types"""
        return list(self.connectors.keys())
    
    def get_connector_info(self, connector_type: str) -> Dict[str, Any]:
        """Get connector information including metadata"""
        connector_class = self.get_connector_class(connector_type)
        if not connector_class:
            return {}
        
        return {
            "type": connector_type,
            "name": getattr(connector_class, 'connector_name', connector_type),
            "description": getattr(connector_class, 'description', ''),
            "services": getattr(connector_class, 'supported_services', []),
            "required_config": getattr(connector_class, 'required_config_fields', []),
            "optional_config": getattr(connector_class, 'optional_config_fields', [])
        }
    
    def validate_connector_config(self, connector_type: str, config: Dict[str, Any]) -> Dict[str, Any]:
        """Validate connector configuration"""
        connector_class = self.get_connector_class(connector_type)
        if not connector_class:
            return {"valid": False, "error": f"Connector type '{connector_type}' not found"}
        
        try:
            temp_instance = connector_class(config)
            validation_result = temp_instance.validate_config()
            return {
                "valid": validation_result,
                "error": None if validation_result else "Configuration validation failed"
            }
        except Exception as e:
            return {
                "valid": False,
                "error": f"Configuration validation error: {str(e)}"
            }
    
    def get_connector_categories(self) -> Dict[str, List[str]]:
        """Get connectors organized by category"""
        categories = {
            "cloud_providers": [],
            "databases": [],
            "data_warehouses": [],
            "network_storage": [],
            "streaming": [],
            "data_lakes": [],
            "network": [],
            "file_systems": []
        }
        
        for connector_type, connector_class in self.connectors.items():
            category = getattr(connector_class, 'category', 'other')
            if category in categories:
                categories[category].append(connector_type)
            else:
                categories.setdefault('other', []).append(connector_type)
        
        return categories
connector_registry = ConnectorRegistry()
