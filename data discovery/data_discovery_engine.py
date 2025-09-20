"""
Data Discovery Engine - Main orchestrator for asset discovery across multiple sources
"""

import asyncio
import logging
import yaml
import os
from datetime import datetime, timedelta
from typing import Dict, List, Optional, Any
from concurrent.futures import ThreadPoolExecutor, as_completed
from pathlib import Path

from connectors.file_system_connector import FileSystemConnector
from connectors.aws_connector import AWSConnector
from connectors.azure_connector import AzureConnector
from connectors.gcp_connector import GCPConnector
from connectors.oracle_cloud_connector import OracleCloudConnector
from connectors.ibm_cloud_connector import IBMCloudConnector
from connectors.alibaba_cloud_connector import AlibabaCloudConnector
from connectors.database_connector import DatabaseConnector
from connectors.network_connector import NetworkConnector
from connectors.data_warehouse_connector import DataWarehouseConnector
from connectors.saas_connector import SaaSConnector
from connectors.streaming_connector import StreamingConnector
from connectors.data_lake_connector import DataLakeConnector
from connectors.bigquery_connector import BigQueryConnector
from metadata.metadata_extractor import MetadataExtractor
from utils.asset_catalog import AssetCatalog
from utils.logger_config import setup_logger


class DataDiscoveryEngine:
    """
    Main engine for discovering data assets across multiple environments
    """
    
    def __init__(self, config_path: str = "config.yaml"):
        """Initialize the data discovery engine"""
        self.config = self._load_config(config_path)
        self.logger = setup_logger(self.config.get('logging', {}))
        
        # Initialize components
        self.metadata_extractor = MetadataExtractor(self.config.get('metadata', {}))
        self.asset_catalog = AssetCatalog()
        
        # Initialize connectors
        self.connectors = self._initialize_connectors()
        
        self.last_scan_time = None
        self.scan_results = {}
        
    def _load_config(self, config_path: str) -> Dict:
        """Load configuration from YAML file"""
        try:
            with open(config_path, 'r') as file:
                return yaml.safe_load(file)
        except FileNotFoundError:
            self.logger.error(f"Configuration file {config_path} not found")
            return {}
        except yaml.YAMLError as e:
            self.logger.error(f"Error parsing configuration file: {e}")
            return {}
    
    def _initialize_connectors(self) -> Dict:
        """Initialize all data source connectors"""
        connectors = {}
        
        try:
            # File System Connector
            if self.config.get('discovery', {}).get('file_system', {}).get('enabled', False):
                connectors['file_system'] = FileSystemConnector(
                    self.config.get('discovery', {}).get('file_system', {})
                )
                self.logger.info("File System connector initialized")
            
            # AWS Connector
            if self.config.get('discovery', {}).get('aws', {}).get('enabled', False):
                connectors['aws'] = AWSConnector(
                    self.config.get('discovery', {}).get('aws', {})
                )
                self.logger.info("AWS connector initialized")
            
            # Azure Connector
            if self.config.get('discovery', {}).get('azure', {}).get('enabled', False):
                connectors['azure'] = AzureConnector(
                    self.config.get('discovery', {}).get('azure', {})
                )
                self.logger.info("Azure connector initialized")
            
            # GCP Connector
            if self.config.get('discovery', {}).get('gcp', {}).get('enabled', False):
                connectors['gcp'] = GCPConnector(
                    self.config.get('discovery', {}).get('gcp', {})
                )
                self.logger.info("GCP connector initialized")
            
            # Database Connector
            if self.config.get('discovery', {}).get('databases', {}).get('enabled', False):
                connectors['databases'] = DatabaseConnector(
                    self.config.get('discovery', {}).get('databases', {})
                )
                self.logger.info("Database connector initialized")
            
            # Network Connector (NAS, SFTP, SMB, FTP, NFS)
            if self.config.get('discovery', {}).get('network', {}).get('enabled', False):
                connectors['network'] = NetworkConnector(
                    self.config.get('discovery', {}).get('network', {})
                )
                self.logger.info("Network connector initialized")
            
            # Oracle Cloud Connector
            if self.config.get('discovery', {}).get('oracle_cloud', {}).get('enabled', False):
                connectors['oracle_cloud'] = OracleCloudConnector(
                    self.config.get('discovery', {}).get('oracle_cloud', {})
                )
                self.logger.info("Oracle Cloud connector initialized")
            
            # IBM Cloud Connector
            if self.config.get('discovery', {}).get('ibm_cloud', {}).get('enabled', False):
                connectors['ibm_cloud'] = IBMCloudConnector(
                    self.config.get('discovery', {}).get('ibm_cloud', {})
                )
                self.logger.info("IBM Cloud connector initialized")
            
            # Alibaba Cloud Connector
            if self.config.get('discovery', {}).get('alibaba_cloud', {}).get('enabled', False):
                connectors['alibaba_cloud'] = AlibabaCloudConnector(
                    self.config.get('discovery', {}).get('alibaba_cloud', {})
                )
                self.logger.info("Alibaba Cloud connector initialized")
            
            # Data Warehouse Connector
            if self.config.get('discovery', {}).get('data_warehouses', {}).get('enabled', False):
                connectors['data_warehouses'] = DataWarehouseConnector(
                    self.config.get('discovery', {}).get('data_warehouses', {})
                )
                self.logger.info("Data Warehouse connector initialized")
            
            # SaaS Connector
            if self.config.get('discovery', {}).get('saas_platforms', {}).get('enabled', False):
                connectors['saas_platforms'] = SaaSConnector(
                    self.config.get('discovery', {}).get('saas_platforms', {})
                )
                self.logger.info("SaaS connector initialized")
            
            # Streaming Connector
            if self.config.get('discovery', {}).get('streaming_platforms', {}).get('enabled', False):
                connectors['streaming_platforms'] = StreamingConnector(
                    self.config.get('discovery', {}).get('streaming_platforms', {})
                )
                self.logger.info("Streaming connector initialized")
            
            # Data Lake Connector
            if self.config.get('discovery', {}).get('data_lakes', {}).get('enabled', False):
                connectors['data_lakes'] = DataLakeConnector(
                    self.config.get('discovery', {}).get('data_lakes', {})
                )
                self.logger.info("Data Lake connector initialized")
            
            # BigQuery Connector
            if self.config.get('discovery', {}).get('bigquery', {}).get('enabled', False):
                connectors['bigquery'] = BigQueryConnector(
                    self.config.get('discovery', {}).get('bigquery', {})
                )
                self.logger.info("BigQuery connector initialized")
                
        except Exception as e:
            self.logger.error(f"Error initializing connectors: {e}")
        
        return connectors
    
    async def discover_all_assets(self) -> Dict[str, List[Dict]]:
        """
        Main function to discover all data assets across all sources
        """
        self.logger.info("Starting comprehensive data asset discovery")
        start_time = datetime.now()
        
        all_assets = {}
        max_workers = self.config.get('discovery', {}).get('max_concurrent_scans', 10)
        
        with ThreadPoolExecutor(max_workers=max_workers) as executor:
            # Submit discovery tasks for each connector
            future_to_connector = {}
            
            for connector_name, connector in self.connectors.items():
                future = executor.submit(self._discover_connector_assets, connector_name, connector)
                future_to_connector[future] = connector_name
            
            # Collect results as they complete
            for future in as_completed(future_to_connector):
                connector_name = future_to_connector[future]
                try:
                    assets = future.result()
                    all_assets[connector_name] = assets
                    self.logger.info(f"Discovered {len(assets)} assets from {connector_name}")
                except Exception as e:
                    self.logger.error(f"Error discovering assets from {connector_name}: {e}")
                    all_assets[connector_name] = []
        
        # Update scan results and catalog
        self.scan_results = all_assets
        self.last_scan_time = datetime.now()
        
        # Update asset catalog
        await self._update_asset_catalog(all_assets)
        
        scan_duration = datetime.now() - start_time
        total_assets = sum(len(assets) for assets in all_assets.values())
        
        self.logger.info(f"Discovery completed in {scan_duration}. Total assets found: {total_assets}")
        
        return all_assets
    
    def _discover_connector_assets(self, connector_name: str, connector) -> List[Dict]:
        """Discover assets from a specific connector"""
        try:
            self.logger.info(f"Starting asset discovery for {connector_name}")
            assets = connector.discover_assets()
            
            # Extract metadata for each asset
            enriched_assets = []
            for asset in assets:
                try:
                    metadata = self.metadata_extractor.extract_metadata(asset)
                    asset.update(metadata)
                    enriched_assets.append(asset)
                except Exception as e:
                    self.logger.warning(f"Failed to extract metadata for asset {asset.get('name', 'unknown')}: {e}")
                    enriched_assets.append(asset)
            
            return enriched_assets
            
        except Exception as e:
            self.logger.error(f"Error in {connector_name} discovery: {e}")
            return []
    
    async def _update_asset_catalog(self, all_assets: Dict[str, List[Dict]]):
        """Update the asset catalog with discovered assets"""
        try:
            for source, assets in all_assets.items():
                for asset in assets:
                    await self.asset_catalog.add_or_update_asset(asset)
            
            self.logger.info("Asset catalog updated successfully")
        except Exception as e:
            self.logger.error(f"Error updating asset catalog: {e}")
    
    def get_assets_by_type(self, asset_type: str) -> List[Dict]:
        """Get all assets of a specific type"""
        matching_assets = []
        for source_assets in self.scan_results.values():
            matching_assets.extend([
                asset for asset in source_assets 
                if asset.get('type') == asset_type
            ])
        return matching_assets
    
    def get_assets_by_source(self, source: str) -> List[Dict]:
        """Get all assets from a specific source"""
        return self.scan_results.get(source, [])
    
    def search_assets(self, query: str) -> List[Dict]:
        """Search assets by name, description, or tags"""
        matching_assets = []
        query_lower = query.lower()
        
        for source_assets in self.scan_results.values():
            for asset in source_assets:
                if (query_lower in asset.get('name', '').lower() or
                    query_lower in asset.get('description', '').lower() or
                    any(query_lower in tag.lower() for tag in asset.get('tags', []))):
                    matching_assets.append(asset)
        
        return matching_assets
    
    def get_discovery_summary(self) -> Dict:
        """Get summary of the last discovery scan"""
        if not self.scan_results:
            return {"status": "No scans completed yet"}
        
        summary = {
            "last_scan_time": self.last_scan_time.isoformat() if self.last_scan_time else None,
            "total_assets": sum(len(assets) for assets in self.scan_results.values()),
            "sources": {}
        }
        
        for source, assets in self.scan_results.items():
            asset_types = {}
            for asset in assets:
                asset_type = asset.get('type', 'unknown')
                asset_types[asset_type] = asset_types.get(asset_type, 0) + 1
            
            summary["sources"][source] = {
                "total_assets": len(assets),
                "asset_types": asset_types
            }
        
        return summary
    
    async def start_continuous_discovery(self):
        """Start continuous asset discovery based on configured interval"""
        scan_interval = self.config.get('discovery', {}).get('scan_interval', 60) * 60  # Convert to seconds
        
        self.logger.info(f"Starting continuous discovery with {scan_interval/60} minute intervals")
        
        while True:
            try:
                await self.discover_all_assets()
                await asyncio.sleep(scan_interval)
            except KeyboardInterrupt:
                self.logger.info("Continuous discovery stopped by user")
                break
            except Exception as e:
                self.logger.error(f"Error in continuous discovery: {e}")
                await asyncio.sleep(scan_interval)
    
    async def start_file_watcher(self):
        """Start file system watcher for real-time monitoring"""
        try:
            import watchdog
            from watchdog.observers import Observer
            from watchdog.events import FileSystemEventHandler
            
            class AssetEventHandler(FileSystemEventHandler):
                def __init__(self, discovery_engine):
                    self.discovery_engine = discovery_engine
                    self.logger = discovery_engine.logger
                
                def on_created(self, event):
                    if not event.is_directory:
                        self.logger.info(f"New file detected: {event.src_path}")
                        asyncio.create_task(self._process_new_asset(event.src_path))
                
                def on_modified(self, event):
                    if not event.is_directory:
                        self.logger.info(f"File modified: {event.src_path}")
                        asyncio.create_task(self._process_modified_asset(event.src_path))
                
                def on_deleted(self, event):
                    if not event.is_directory:
                        self.logger.info(f"File deleted: {event.src_path}")
                        asyncio.create_task(self._process_deleted_asset(event.src_path))
                
                async def _process_new_asset(self, file_path):
                    """Process newly created asset"""
                    try:
                        from pathlib import Path
                        from connectors.file_system_connector import FileSystemConnector
                        
                        # Create file system connector for single file processing
                        fs_config = self.discovery_engine.config.get('discovery', {}).get('file_system', {})
                        fs_connector = FileSystemConnector(fs_config)
                        
                        # Check if file matches our criteria
                        path_obj = Path(file_path)
                        if self._should_process_file(path_obj, fs_config):
                            asset = fs_connector._create_file_asset(path_obj)
                            if asset:
                                # Extract metadata
                                metadata = self.discovery_engine.metadata_extractor.extract_metadata(asset)
                                asset.update(metadata)
                                
                                # Add to catalog
                                await self.discovery_engine.asset_catalog.add_or_update_asset(asset)
                                self.logger.info(f"Added new asset to catalog: {asset['name']}")
                    except Exception as e:
                        self.logger.error(f"Error processing new asset {file_path}: {e}")
                
                async def _process_modified_asset(self, file_path):
                    """Process modified asset"""
                    try:
                        from pathlib import Path
                        from connectors.file_system_connector import FileSystemConnector
                        
                        # Create file system connector for single file processing
                        fs_config = self.discovery_engine.config.get('discovery', {}).get('file_system', {})
                        fs_connector = FileSystemConnector(fs_config)
                        
                        # Check if file matches our criteria
                        path_obj = Path(file_path)
                        if self._should_process_file(path_obj, fs_config):
                            asset = fs_connector._create_file_asset(path_obj)
                            if asset:
                                # Extract metadata
                                metadata = self.discovery_engine.metadata_extractor.extract_metadata(asset)
                                asset.update(metadata)
                                
                                # Update in catalog
                                await self.discovery_engine.asset_catalog.add_or_update_asset(asset)
                                self.logger.info(f"Updated modified asset in catalog: {asset['name']}")
                    except Exception as e:
                        self.logger.error(f"Error processing modified asset {file_path}: {e}")
                
                async def _process_deleted_asset(self, file_path):
                    """Process deleted asset"""
                    try:
                        # Mark asset as inactive in catalog
                        from pathlib import Path
                        path_obj = Path(file_path)
                        
                        # Find asset by location and mark as inactive
                        assets = self.discovery_engine.asset_catalog.search_assets(path_obj.name)
                        for asset in assets:
                            if asset['location'] == str(path_obj.absolute()):
                                await self.discovery_engine.asset_catalog.mark_assets_inactive([asset['fingerprint']])
                                self.logger.info(f"Marked deleted asset as inactive: {asset['name']}")
                                break
                    except Exception as e:
                        self.logger.error(f"Error processing deleted asset {file_path}: {e}")
                
                def _should_process_file(self, path_obj, fs_config):
                    """Check if file should be processed based on configuration"""
                    try:
                        # Check file size limit
                        max_size = fs_config.get('max_file_size_mb', 1000) * 1024 * 1024
                        if path_obj.stat().st_size > max_size:
                            return False
                        
                        # Check file extensions
                        file_extensions = fs_config.get('file_extensions', [])
                        if file_extensions and path_obj.suffix.lower() not in file_extensions:
                            return False
                        
                        return True
                    except:
                        return False
            
            # Set up file watcher
            event_handler = AssetEventHandler(self)
            observer = Observer()
            
            # Watch all configured scan paths
            scan_paths = self.config.get('discovery', {}).get('file_system', {}).get('scan_paths', [])
            for scan_path in scan_paths:
                if os.path.exists(scan_path):
                    observer.schedule(event_handler, scan_path, recursive=True)
                    self.logger.info(f"Watching directory: {scan_path}")
            
            observer.start()
            self.logger.info("File system watcher started - monitoring for real-time changes")
            
            # Keep the watcher running
            try:
                while True:
                    await asyncio.sleep(1)
            except KeyboardInterrupt:
                observer.stop()
                self.logger.info("File system watcher stopped")
            
            observer.join()
            
        except ImportError:
            self.logger.warning("watchdog library not installed. Install with: pip install watchdog")
            self.logger.info("Falling back to interval-based monitoring only")
        except Exception as e:
            self.logger.error(f"Error starting file watcher: {e}")
            self.logger.info("Falling back to interval-based monitoring only")


# Integration API Functions for Platform Integration
class DataDiscoveryAPI:
    """
    API wrapper for easy integration with main platform
    """
    
    def __init__(self, config_path: str = "config.yaml"):
        self.engine = DataDiscoveryEngine(config_path)
    
    async def scan_all_data_sources(self) -> Dict[str, Any]:
        """
        Main API function to scan all configured data sources
        Returns comprehensive asset information
        """
        try:
            assets = await self.engine.discover_all_assets()
            summary = self.engine.get_discovery_summary()
            
            return {
                "status": "success",
                "summary": summary,
                "assets": assets,
                "timestamp": datetime.now().isoformat()
            }
        except Exception as e:
            return {
                "status": "error",
                "error": str(e),
                "timestamp": datetime.now().isoformat()
            }
    
    def get_asset_inventory(self) -> Dict[str, Any]:
        """Get current asset inventory"""
        return {
            "status": "success",
            "summary": self.engine.get_discovery_summary(),
            "timestamp": datetime.now().isoformat()
        }
    
    def search_data_assets(self, query: str, asset_type: str = None) -> Dict[str, Any]:
        """Search for specific data assets"""
        try:
            if asset_type:
                results = self.engine.get_assets_by_type(asset_type)
                if query:
                    results = [asset for asset in results if query.lower() in asset.get('name', '').lower()]
            else:
                results = self.engine.search_assets(query)
            
            return {
                "status": "success",
                "results": results,
                "count": len(results),
                "timestamp": datetime.now().isoformat()
            }
        except Exception as e:
            return {
                "status": "error",
                "error": str(e),
                "timestamp": datetime.now().isoformat()
            }
    
    async def start_continuous_monitoring(self, real_time: bool = True) -> Dict[str, Any]:
        """
        Start continuous monitoring of data assets
        
        Args:
            real_time: If True, use file system watcher for real-time monitoring
                      If False, use interval-based monitoring only
        """
        try:
            if real_time:
                # Start both file watcher and interval-based monitoring
                self.engine.logger.info("Starting continuous monitoring with real-time file watching")
                await asyncio.gather(
                    self.engine.start_file_watcher(),
                    self.engine.start_continuous_discovery()
                )
            else:
                # Start only interval-based monitoring
                self.engine.logger.info("Starting continuous monitoring with interval-based scanning")
                await self.engine.start_continuous_discovery()
            
            return {
                "status": "success",
                "message": "Continuous monitoring started",
                "real_time": real_time,
                "timestamp": datetime.now().isoformat()
            }
        except Exception as e:
            return {
                "status": "error",
                "error": str(e),
                "timestamp": datetime.now().isoformat()
            }
    
    def stop_monitoring(self) -> Dict[str, Any]:
        """Stop continuous monitoring"""
        try:
            # This would need to be implemented with proper signal handling
            # For now, we'll just return success
            return {
                "status": "success",
                "message": "Monitoring stop requested",
                "timestamp": datetime.now().isoformat()
            }
        except Exception as e:
            return {
                "status": "error",
                "error": str(e),
                "timestamp": datetime.now().isoformat()
            }
    
    def get_connector_config(self, connector_type: str) -> Dict[str, Any]:
        """Get configuration for a specific connector"""
        try:
            config_section = self.engine.config.get('discovery', {}).get(connector_type, {})
            return {
                "status": "success",
                "connector_type": connector_type,
                "config": config_section,
                "enabled": config_section.get('enabled', False),
                "timestamp": datetime.now().isoformat()
            }
        except Exception as e:
            return {
                "status": "error",
                "error": str(e),
                "timestamp": datetime.now().isoformat()
            }
    
    def update_connector_config(self, connector_type: str, config: Dict[str, Any]) -> Dict[str, Any]:
        """Update configuration for a specific connector"""
        try:
            # Update the in-memory config
            if 'discovery' not in self.engine.config:
                self.engine.config['discovery'] = {}
            
            self.engine.config['discovery'][connector_type] = config
            
            # Save to file
            self._save_config()
            
            # Reinitialize connectors if needed
            if config.get('enabled', False):
                self.engine.connectors = self.engine._initialize_connectors()
            
            return {
                "status": "success",
                "message": f"Configuration updated for {connector_type}",
                "connector_type": connector_type,
                "timestamp": datetime.now().isoformat()
            }
        except Exception as e:
            return {
                "status": "error",
                "error": str(e),
                "timestamp": datetime.now().isoformat()
            }
    
    def test_connector_connection(self, connector_type: str) -> Dict[str, Any]:
        """Test connection for a specific connector"""
        try:
            if connector_type in self.engine.connectors:
                connector = self.engine.connectors[connector_type]
                if hasattr(connector, 'test_connection'):
                    try:
                        success = connector.test_connection()
                        if success:
                            return {
                                "status": "success",
                                "connector_type": connector_type,
                                "connection_status": "connected",
                                "timestamp": datetime.now().isoformat()
                            }
                        else:
                            return {
                                "status": "error",
                                "connector_type": connector_type,
                                "connection_status": "failed",
                                "error": "Connection test returned False - check credentials and configuration",
                                "timestamp": datetime.now().isoformat()
                            }
                    except Exception as test_error:
                        return {
                            "status": "error",
                            "connector_type": connector_type,
                            "connection_status": "failed",
                            "error": str(test_error),
                            "timestamp": datetime.now().isoformat()
                        }
                else:
                    return {
                        "status": "warning",
                        "message": f"Connector {connector_type} does not support connection testing",
                        "connector_type": connector_type,
                        "timestamp": datetime.now().isoformat()
                    }
            else:
                return {
                    "status": "error",
                    "error": f"Connector {connector_type} not found or not enabled",
                    "timestamp": datetime.now().isoformat()
                }
        except Exception as e:
            return {
                "status": "error",
                "error": str(e),
                "timestamp": datetime.now().isoformat()
            }
    
    def get_available_connectors(self) -> Dict[str, Any]:
        """Get list of all available connectors with their status"""
        try:
            connectors = {
                "cloud_providers": {
                    "aws": {"name": "Amazon Web Services", "services": ["S3", "RDS", "DynamoDB", "Redshift", "Athena", "Glue"]},
                    "azure": {"name": "Microsoft Azure", "services": ["Blob Storage", "SQL Database", "Cosmos DB", "Synapse"]},
                    "gcp": {"name": "Google Cloud Platform", "services": ["BigQuery", "Cloud Storage", "Cloud SQL", "Dataflow"]},
                    "oracle_cloud": {"name": "Oracle Cloud Infrastructure", "services": ["Object Storage", "Autonomous Database", "MySQL"]},
                    "ibm_cloud": {"name": "IBM Cloud", "services": ["Object Storage", "Db2", "Cloudant", "Watson Discovery"]},
                    "alibaba_cloud": {"name": "Alibaba Cloud", "services": ["OSS", "RDS", "PolarDB", "MongoDB"]}
                },
                "databases": {
                    "postgresql": {"name": "PostgreSQL", "type": "Relational Database"},
                    "mysql": {"name": "MySQL", "type": "Relational Database"},
                    "mongodb": {"name": "MongoDB", "type": "Document Database"},
                    "oracle": {"name": "Oracle Database", "type": "Relational Database"},
                    "sqlserver": {"name": "SQL Server", "type": "Relational Database"},
                    "cassandra": {"name": "Apache Cassandra", "type": "NoSQL Database"},
                    "neo4j": {"name": "Neo4j", "type": "Graph Database"},
                    "redis": {"name": "Redis", "type": "In-Memory Database"},
                    "elasticsearch": {"name": "Elasticsearch", "type": "Search Engine"}
                },
                "data_warehouses": {
                    "snowflake": {"name": "Snowflake", "type": "Cloud Data Warehouse"},
                    "databricks": {"name": "Databricks", "type": "Analytics Platform"},
                    "bigquery": {"name": "BigQuery", "type": "Data Warehouse"},
                    "teradata": {"name": "Teradata", "type": "Data Warehouse"},
                    "redshift": {"name": "Amazon Redshift", "type": "Data Warehouse"},
                    "clickhouse": {"name": "ClickHouse", "type": "Analytics Database"}
                },
                "saas_platforms": {
                    "salesforce": {"name": "Salesforce", "type": "CRM"},
                    "servicenow": {"name": "ServiceNow", "type": "ITSM"},
                    "slack": {"name": "Slack", "type": "Communication"},
                    "jira": {"name": "Jira", "type": "Project Management"},
                    "hubspot": {"name": "HubSpot", "type": "CRM"},
                    "zendesk": {"name": "Zendesk", "type": "Customer Support"}
                },
                "streaming": {
                    "kafka": {"name": "Apache Kafka", "type": "Streaming Platform"},
                    "pulsar": {"name": "Apache Pulsar", "type": "Streaming Platform"},
                    "rabbitmq": {"name": "RabbitMQ", "type": "Message Queue"},
                    "kinesis": {"name": "Amazon Kinesis", "type": "Streaming Service"},
                    "eventhub": {"name": "Azure Event Hub", "type": "Streaming Service"},
                    "pubsub": {"name": "Google Pub/Sub", "type": "Messaging Service"}
                },
                "data_lakes": {
                    "delta_lake": {"name": "Delta Lake", "type": "Data Lake Format"},
                    "iceberg": {"name": "Apache Iceberg", "type": "Table Format"},
                    "hudi": {"name": "Apache Hudi", "type": "Data Lake Framework"},
                    "parquet": {"name": "Parquet Files", "type": "File Format"},
                    "minio": {"name": "MinIO", "type": "Object Storage"},
                    "hdfs": {"name": "Hadoop HDFS", "type": "Distributed Storage"}
                }
            }
            
            # Add status for each connector
            for category in connectors.values():
                for connector_id, connector_info in category.items():
                    config = self.engine.config.get('discovery', {}).get(connector_id, {})
                    connector_info['enabled'] = config.get('enabled', False)
                    connector_info['configured'] = bool(config)
            
            total_connectors = sum(len(category) for category in connectors.values())
            enabled_connectors = sum(
                1 for category in connectors.values() 
                for connector in category.values() 
                if connector.get('enabled', False)
            )
            
            return {
                "status": "success",
                "connectors": connectors,
                "total_connectors": total_connectors,
                "enabled_connectors": enabled_connectors,
                "timestamp": datetime.now().isoformat()
            }
        except Exception as e:
            return {
                "status": "error",
                "error": str(e),
                "timestamp": datetime.now().isoformat()
            }
    
    def get_discovery_status(self) -> Dict[str, Any]:
        """Get current discovery status"""
        try:
            return {
                "status": "success",
                "discovery_status": "idle",
                "last_scan": self.engine.last_scan_time.isoformat() if self.engine.last_scan_time else None,
                "active_connectors": list(self.engine.connectors.keys()),
                "monitoring_enabled": self.engine.config.get('discovery', {}).get('monitoring', {}).get('enabled', False),
                "total_assets": len(self.engine.scan_results) if hasattr(self.engine, 'scan_results') else 0,
                "timestamp": datetime.now().isoformat()
            }
        except Exception as e:
            return {
                "status": "error",
                "error": str(e),
                "timestamp": datetime.now().isoformat()
            }
    
    def get_asset_details(self, asset_name: str) -> Dict[str, Any]:
        """Get detailed information about a specific asset"""
        try:
            # Search for the asset across all sources
            all_assets = []
            if hasattr(self.engine, 'scan_results'):
                for source_assets in self.engine.scan_results.values():
                    all_assets.extend(source_assets)
            
            # Find matching asset
            matching_assets = [asset for asset in all_assets if asset.get('name') == asset_name]
            
            if matching_assets:
                return {
                    "status": "success",
                    "asset": matching_assets[0],
                    "timestamp": datetime.now().isoformat()
                }
            else:
                return {
                    "status": "error",
                    "error": f"Asset '{asset_name}' not found",
                    "timestamp": datetime.now().isoformat()
                }
        except Exception as e:
            return {
                "status": "error",
                "error": str(e),
                "timestamp": datetime.now().isoformat()
            }
    
    def get_system_health(self) -> Dict[str, Any]:
        """Get system health status"""
        try:
            # Count enabled connectors
            enabled_connectors = 0
            total_connectors = 0
            connector_health = {}
            
            for connector_type, config in self.engine.config.get('discovery', {}).items():
                if isinstance(config, dict) and 'enabled' in config:
                    total_connectors += 1
                    is_enabled = config.get('enabled', False)
                    if is_enabled:
                        enabled_connectors += 1
                    
                    connector_health[connector_type] = {
                        'enabled': is_enabled,
                        'configured': bool(config),
                        'status': 'healthy' if is_enabled else 'disabled'
                    }
            
            return {
                "status": "success",
                "system_health": "healthy",
                "connectors": {
                    "total": total_connectors,
                    "enabled": enabled_connectors,
                    "health": connector_health
                },
                "monitoring": {
                    "enabled": self.engine.config.get('discovery', {}).get('monitoring', {}).get('enabled', False),
                    "real_time": self.engine.config.get('discovery', {}).get('monitoring', {}).get('real_time_watching', False)
                },
                "last_scan": self.engine.last_scan_time.isoformat() if self.engine.last_scan_time else None,
                "timestamp": datetime.now().isoformat()
            }
        except Exception as e:
            return {
                "status": "error",
                "error": str(e),
                "timestamp": datetime.now().isoformat()
            }
    
    def _save_config(self):
        """Save current configuration to file"""
        try:
            with open('config.yaml', 'w') as f:
                yaml.dump(self.engine.config, f, default_flow_style=False)
        except Exception as e:
            self.engine.logger.error(f"Error saving config: {e}")


# Main execution function
async def main():
    """Main function for testing the discovery engine"""
    api = DataDiscoveryAPI()
    
    # Run a full discovery scan
    result = await api.scan_all_data_sources()
    print("Discovery Results:", result)


if __name__ == "__main__":
    asyncio.run(main())
