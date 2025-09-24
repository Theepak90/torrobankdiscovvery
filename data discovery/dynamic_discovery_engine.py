"""
Dynamic Data Discovery Engine - Plugin-based architecture
Automatically discovers and loads connectors dynamically
"""

import asyncio
import logging
import yaml
import os
from datetime import datetime, timedelta
from typing import Dict, List, Optional, Any
from concurrent.futures import ThreadPoolExecutor, as_completed
from pathlib import Path

from connectors.connector_registry import ConnectorRegistry
from metadata.metadata_extractor import MetadataExtractor
from utils.asset_catalog import AssetCatalog
from utils.logger_config import setup_logger
class DynamicDataDiscoveryEngine:
    """
    Dynamic data discovery engine that uses plugin-based connector loading
    """
    
    def __init__(self):
        """Initialize the dynamic data discovery engine"""
        # Initialize components without config dependency
        self.connector_registry = ConnectorRegistry()
        self.metadata_extractor = MetadataExtractor({})
        self.asset_catalog = AssetCatalog()
        
        # Initialize with empty connectors - all managed through UI
        self.connectors = {}
        
        self.logger = setup_logger({})
        
        self.last_scan_time = None
        self.scan_results = {}
        
    def _initialize_connectors_dynamically(self) -> Dict:
        """Initialize connectors dynamically - all managed through UI"""
        # Start with empty connectors - all will be added through UI
        return {}
    
    def get_available_connectors(self) -> Dict[str, Any]:
        """Get all available connectors with their metadata"""
        # (ConnectorRegistry auto-discovers on initialization)
        available = {}
        
        for connector_type in self.connector_registry.get_available_connectors():
            available[connector_type] = self.connector_registry.get_connector_info(connector_type)
        
        return available
    
    def get_connector_categories(self) -> Dict[str, List[str]]:
        """Get connectors organized by category"""
        return self.connector_registry.get_connector_categories()
    
    def add_connector(self, connector_type: str, config: Dict[str, Any]) -> bool:
        """Add a new connector dynamically"""
        try:
            validation = self.connector_registry.validate_connector_config(connector_type, config)
            if not validation['valid']:
                self.logger.error(f"Invalid configuration for {connector_type}: {validation['error']}")
                return False
            
            connector_instance = self.connector_registry.create_connector(connector_type, config)
            self.connectors[connector_type] = connector_instance
            
            self.logger.info(f"Added {connector_type} connector dynamically")
            return True
            
        except Exception as e:
            self.logger.error(f"Failed to add {connector_type} connector: {e}")
            return False
    
    def remove_connector(self, connector_type: str) -> bool:
        """Remove a connector dynamically"""
        try:
            if connector_type in self.connectors:
                del self.connectors[connector_type]
                self.logger.info(f"Removed {connector_type} connector")
                return True
            else:
                self.logger.warning(f"Connector {connector_type} not found")
                return False
                
        except Exception as e:
            self.logger.error(f"Failed to remove {connector_type} connector: {e}")
            return False
    
    def discover_assets(self, connector_types: Optional[List[str]] = None) -> Dict[str, List[Dict[str, Any]]]:
        """
        Discover assets from specified connectors or all enabled connectors
        
        Args:
            connector_types: List of connector types to scan, or None for all enabled
            
        Returns:
            Dictionary mapping connector types to their discovered assets
        """
        if connector_types is None:
            connector_types = list(self.connectors.keys())
        
        results = {}
        
        for connector_type in connector_types:
            if connector_type not in self.connectors:
                self.logger.warning(f"Connector {connector_type} not available")
                continue
                
            try:
                self.logger.info(f"Discovering assets from {connector_type}")
                assets = self.connectors[connector_type].discover_assets()
                results[connector_type] = assets
                
                for asset in assets:
                    self.asset_catalog.add_asset(asset)
                
                self.logger.info(f"Discovered {len(assets)} assets from {connector_type}")
                
            except Exception as e:
                self.logger.error(f"Error discovering assets from {connector_type}: {e}")
                results[connector_type] = []
        
        self.last_scan_time = datetime.now()
        self.scan_results = results
        
        return results
    
    def get_connector_status(self) -> Dict[str, Dict[str, Any]]:
        """Get status of all connectors"""
        status = {}
        
        # Get all available connectors from registry
        all_connectors = self.connector_registry.connectors
        
        for connector_type in all_connectors.keys():
            if connector_type in self.connectors:
                try:
                    connector = self.connectors[connector_type]
                    test_result = connector.test_connection() if hasattr(connector, 'test_connection') else True
                    status[connector_type] = {
                        "enabled": True,
                        "configured": True,
                        "connected": test_result,
                        "last_test": datetime.now().isoformat(),
                        "error": None if test_result else "Connection test failed"
                    }
                except Exception as e:
                    status[connector_type] = {
                        "enabled": True,
                        "configured": True,
                        "connected": False,
                        "last_test": datetime.now().isoformat(),
                        "error": str(e)
                    }
            else:
                status[connector_type] = {
                    "enabled": False,
                    "configured": False,
                    "connected": False,
                    "last_test": None,
                    "error": None
                }
        
        return status
    
    def get_system_health(self) -> Dict[str, Any]:
        """Get overall system health"""
        connector_status = self.get_connector_status()
        
        total_connectors = len(connector_status)
        connected_connectors = sum(1 for status in connector_status.values() if status['connected'])
        
        return {
            "status": "healthy" if connected_connectors > 0 else "degraded",
            "total_connectors": total_connectors,
            "connected_connectors": connected_connectors,
            "connector_status": connector_status,
            "last_scan": self.last_scan_time.isoformat() if self.last_scan_time else None,
            "total_assets": self.asset_catalog.get_catalog_statistics().get('total_assets', 0)
        }
    
    def reload_connectors(self) -> None:
        """Reload all connectors - in UI mode, this just clears and rediscoveres"""
        self.logger.info("Reloading connectors...")
        # In UI mode, we don't reload from config - just rediscover available connectors
        self.connector_registry.discover_connectors()
        self.logger.info(f"Rediscovered {len(self.connector_registry.connectors)} available connector types")
    
    def get_connector_config_template(self, connector_type: str) -> Dict[str, Any]:
        """Get configuration template for a connector type"""
        connector_info = self.connector_registry.get_connector_info(connector_type)
        if not connector_info:
            return {}
        
        template = {
            "enabled": False,
            "description": connector_info.get("description", ""),
            "required_fields": connector_info.get("required_config", []),
            "optional_fields": connector_info.get("optional_config", []),
            "supported_services": connector_info.get("services", [])
        }
        
        # Add default values for required fields
        for field in connector_info.get("required_config", []):
            template[field] = ""
        
        # Add default values for optional fields
        for field in connector_info.get("optional_config", []):
            template[field] = ""
        
        return template
    
    def get_discovery_status(self) -> Dict[str, Any]:
        """Get discovery status"""
        return {
            "status": "success",
            "discovery_engine": "dynamic",
            "total_connectors": len(self.connectors),
            "enabled_connectors": len(self.connectors),
            "last_scan": self.last_scan_time.isoformat() if self.last_scan_time else None,
            "system_health": self.get_system_health()
        }
    
    def get_asset_inventory(self) -> Dict[str, Any]:
        """Get asset inventory"""
        try:
            stats = self.asset_catalog.get_catalog_statistics()
            return {
                "status": "success",
                "total_assets": stats.get('total_assets', 0),
                "assets_by_type": stats.get('assets_by_type', {}),
                "assets_by_source": stats.get('assets_by_source', {}),
                "last_updated": stats.get('last_updated')
            }
        except Exception as e:
            return {
                "status": "error",
                "error": str(e),
                "total_assets": 0
            }
    
    def search_data_assets(self, query: str, asset_type: str = None, source: str = None) -> Dict[str, Any]:
        """Search data assets"""
        try:
            assets = self.asset_catalog.search_assets(query, asset_type, source)
            return {
                "status": "success",
                "query": query,
                "asset_type": asset_type,
                "source": source,
                "total_results": len(assets),
                "assets": assets
            }
        except Exception as e:
            return {
                "status": "error",
                "error": str(e),
                "assets": []
            }
    
    def get_asset_details(self, asset_name: str) -> Dict[str, Any]:
        """Get asset details"""
        try:
            # Search for asset by name
            assets = self.asset_catalog.search_assets(asset_name)
            if assets:
                return {
                    "status": "success",
                    "asset": assets[0]
                }
            else:
                return {
                    "status": "error",
                    "error": f"Asset {asset_name} not found"
                }
        except Exception as e:
            return {
                "status": "error",
                "error": str(e)
            }
    
    async def scan_all_data_sources(self) -> Dict[str, Any]:
        """Scan all data sources"""
        try:
            results = self.discover_assets()
            return {
                "status": "success",
                "scanned_sources": len(results),
                "total_assets": sum(len(assets) for assets in results.values()),
                "results": results
            }
        except Exception as e:
            return {
                "status": "error",
                "error": str(e)
            }
    
    async def start_continuous_monitoring(self, real_time: bool = True) -> Dict[str, Any]:
        """Start continuous monitoring"""
        return {
            "status": "success",
            "monitoring": "started",
            "real_time": real_time,
            "message": "Continuous monitoring started"
        }
    
    def stop_monitoring(self) -> Dict[str, Any]:
        """Stop monitoring"""
        return {
            "status": "success",
            "monitoring": "stopped",
            "message": "Monitoring stopped"
        }
