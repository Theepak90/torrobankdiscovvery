"""
Oracle Cloud Connector - Discovers data assets in Oracle Cloud Infrastructure services
"""

from datetime import datetime
from typing import List, Dict, Any
import logging

from .base_connector import BaseConnector


class OracleCloudConnector(BaseConnector):
    """
    Connector for discovering data assets in Oracle Cloud Infrastructure services
    """
    
    def __init__(self, config: Dict[str, Any]):
        super().__init__(config)
        self.services = config.get('services', ['object_storage', 'autonomous_database', 'mysql', 'nosql'])
        self.compartment_id = config.get('compartment_id')
        self.region = config.get('region', 'us-ashburn-1')
        
        # Initialize OCI clients
        try:
            # Note: Requires oci library - pip install oci
            import oci
            self.oci_config = oci.config.from_file()
            self.logger.info("Oracle Cloud credentials initialized successfully")
        except ImportError:
            self.logger.warning("OCI library not installed. Install with: pip install oci")
            self.oci_config = None
        except Exception as e:
            self.logger.error(f"Oracle Cloud credential initialization failed: {e}")
            self.oci_config = None
    
    def discover_assets(self) -> List[Dict[str, Any]]:
        """Discover Oracle Cloud data assets"""
        if not self.oci_config:
            self.logger.error("Oracle Cloud credentials not available")
            return []
        
        self.logger.info("Starting Oracle Cloud asset discovery")
        assets = []
        
        if 'object_storage' in self.services:
            assets.extend(self._discover_object_storage_assets())
        
        if 'autonomous_database' in self.services:
            assets.extend(self._discover_autonomous_database_assets())
        
        if 'mysql' in self.services:
            assets.extend(self._discover_mysql_assets())
        
        if 'nosql' in self.services:
            assets.extend(self._discover_nosql_assets())
        
        self.logger.info(f"Discovered {len(assets)} Oracle Cloud assets")
        return assets
    
    def _discover_object_storage_assets(self) -> List[Dict[str, Any]]:
        """Discover Oracle Cloud Object Storage assets"""
        assets = []
        try:
            import oci
            object_storage_client = oci.object_storage.ObjectStorageClient(self.oci_config)
            namespace = object_storage_client.get_namespace().data
            
            # List buckets
            buckets = object_storage_client.list_buckets(namespace, self.compartment_id).data
            
            for bucket in buckets:
                asset = {
                    'name': bucket.name,
                    'type': 'oci_bucket',
                    'source': 'oracle_object_storage',
                    'location': f"oci://{namespace}/{bucket.name}",
                    'region': self.region,
                    'created_date': bucket.time_created,
                    'size': 0,
                    'metadata': {
                        'service': 'object_storage',
                        'resource_type': 'bucket',
                        'region': self.region,
                        'namespace': namespace
                    }
                }
                assets.append(asset)
                
        except Exception as e:
            self.logger.error(f"Error discovering Oracle Object Storage assets: {e}")
        
        return assets
    
    def _discover_autonomous_database_assets(self) -> List[Dict[str, Any]]:
        """Discover Oracle Autonomous Database assets"""
        assets = []
        try:
            import oci
            database_client = oci.database.DatabaseClient(self.oci_config)
            
            # List autonomous databases
            autonomous_dbs = database_client.list_autonomous_databases(self.compartment_id).data
            
            for db in autonomous_dbs:
                asset = {
                    'name': db.display_name,
                    'type': 'autonomous_database',
                    'source': 'oracle_autonomous_db',
                    'location': f"adb://{db.service_console_url}",
                    'region': self.region,
                    'created_date': db.time_created,
                    'size': db.data_storage_size_in_tbs * 1024 * 1024 * 1024 * 1024 if db.data_storage_size_in_tbs else 0,
                    'metadata': {
                        'service': 'autonomous_database',
                        'resource_type': 'database',
                        'region': self.region,
                        'db_name': db.db_name,
                        'db_workload': db.db_workload,
                        'lifecycle_state': db.lifecycle_state
                    }
                }
                assets.append(asset)
                
        except Exception as e:
            self.logger.error(f"Error discovering Oracle Autonomous Database assets: {e}")
        
        return assets
    
    def _discover_mysql_assets(self) -> List[Dict[str, Any]]:
        """Discover Oracle MySQL Database Service assets"""
        assets = []
        try:
            import oci
            mysql_client = oci.mysql.DbSystemClient(self.oci_config)
            
            # List MySQL DB systems
            db_systems = mysql_client.list_db_systems(self.compartment_id).data
            
            for db_system in db_systems:
                asset = {
                    'name': db_system.display_name,
                    'type': 'mysql_db_system',
                    'source': 'oracle_mysql',
                    'location': f"mysql://{db_system.endpoints[0].hostname if db_system.endpoints else 'unknown'}",
                    'region': self.region,
                    'created_date': db_system.time_created,
                    'size': 0,
                    'metadata': {
                        'service': 'mysql',
                        'resource_type': 'db_system',
                        'region': self.region,
                        'mysql_version': db_system.mysql_version,
                        'lifecycle_state': db_system.lifecycle_state
                    }
                }
                assets.append(asset)
                
        except Exception as e:
            self.logger.error(f"Error discovering Oracle MySQL assets: {e}")
        
        return assets
    
    def _discover_nosql_assets(self) -> List[Dict[str, Any]]:
        """Discover Oracle NoSQL Database assets"""
        assets = []
        try:
            import oci
            nosql_client = oci.nosql.NosqlClient(self.oci_config)
            
            # List NoSQL tables
            tables = nosql_client.list_tables(self.compartment_id).data
            
            for table in tables.items:
                asset = {
                    'name': table.name,
                    'type': 'nosql_table',
                    'source': 'oracle_nosql',
                    'location': f"nosql://{self.region}/{table.name}",
                    'region': self.region,
                    'created_date': table.time_created,
                    'size': 0,
                    'metadata': {
                        'service': 'nosql',
                        'resource_type': 'table',
                        'region': self.region,
                        'lifecycle_state': table.lifecycle_state
                    }
                }
                assets.append(asset)
                
        except Exception as e:
            self.logger.error(f"Error discovering Oracle NoSQL assets: {e}")
        
        return assets
    
    def test_connection(self) -> bool:
        """Test connection to Oracle Cloud Infrastructure"""
        try:
            if not self.oci_config:
                self.logger.error("Oracle Cloud credentials not configured")
                return False
            
            import oci
            
            # Test with a simple API call to verify credentials
            identity_client = oci.identity.IdentityClient(self.oci_config)
            compartments = identity_client.list_compartments(
                compartment_id=self.oci_config.get('tenancy'),
                compartment_id_in_subtree=False,
                access_level="ACCESSIBLE"
            )
            
            self.logger.info("Oracle Cloud connection test successful")
            return True
            
        except Exception as e:
            self.logger.error(f"Oracle Cloud connection test failed: {e}")
            return False
    
    def validate_config(self) -> bool:
        """Validate Oracle Cloud connector configuration"""
        if not self.compartment_id:
            self.logger.error("No Oracle Cloud compartment ID configured")
            return False
        
        if not self.services:
            self.logger.error("No Oracle Cloud services configured")
            return False
        
        return True

