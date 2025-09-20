"""
IBM Cloud Connector - Discovers data assets in IBM Cloud services
"""

from datetime import datetime
from typing import List, Dict, Any
import logging

from .base_connector import BaseConnector


class IBMCloudConnector(BaseConnector):
    """
    Connector for discovering data assets in IBM Cloud services
    """
    
    # Metadata for dynamic discovery
    connector_type = "ibm_cloud"
    connector_name = "IBM Cloud"
    description = "Discover data assets from IBM Cloud services including Object Storage, Db2, Cloudant, and Watson Discovery"
    category = "cloud_providers"
    supported_services = ["Object Storage", "Db2", "Cloudant", "Watson Discovery"]
    required_config_fields = ["api_key"]
    optional_config_fields = ["region", "services"]
    
    def __init__(self, config: Dict[str, Any]):
        super().__init__(config)
        self.services = config.get('services', ['cloud_object_storage', 'db2', 'cloudant', 'watson_discovery'])
        self.api_key = config.get('api_key')
        self.resource_group_id = config.get('resource_group_id')
        self.region = config.get('region', 'us-south')
        
        # Initialize IBM Cloud clients
        try:
            # Note: Requires ibm-cloud-sdk-core - pip install ibm-cloud-sdk-core
            from ibm_cloud_sdk_core.authenticators import IAMAuthenticator
            self.authenticator = IAMAuthenticator(self.api_key) if self.api_key else None
            self.logger.info("IBM Cloud credentials initialized successfully")
        except ImportError:
            self.logger.warning("IBM Cloud SDK not installed. Install with: pip install ibm-cloud-sdk-core")
            self.authenticator = None
        except Exception as e:
            self.logger.error(f"IBM Cloud credential initialization failed: {e}")
            self.authenticator = None
    
    def discover_assets(self) -> List[Dict[str, Any]]:
        """Discover IBM Cloud data assets"""
        if not self.authenticator:
            self.logger.error("IBM Cloud credentials not available")
            return []
        
        self.logger.info("Starting IBM Cloud asset discovery")
        assets = []
        
        if 'cloud_object_storage' in self.services:
            assets.extend(self._discover_cos_assets())
        
        if 'db2' in self.services:
            assets.extend(self._discover_db2_assets())
        
        if 'cloudant' in self.services:
            assets.extend(self._discover_cloudant_assets())
        
        if 'watson_discovery' in self.services:
            assets.extend(self._discover_watson_discovery_assets())
        
        self.logger.info(f"Discovered {len(assets)} IBM Cloud assets")
        return assets
    
    def _discover_cos_assets(self) -> List[Dict[str, Any]]:
        """Discover IBM Cloud Object Storage assets"""
        assets = []
        try:
            from ibm_botocore.client import Config
            import ibm_boto3
            
            cos_client = ibm_boto3.client(
                's3',
                ibm_api_key_id=self.api_key,
                ibm_service_instance_id=self.resource_group_id,
                config=Config(signature_version='oauth'),
                endpoint_url=f'https://s3.{self.region}.cloud-object-storage.appdomain.cloud'
            )
            
            # List buckets
            response = cos_client.list_buckets()
            
            for bucket in response.get('Buckets', []):
                asset = {
                    'name': bucket['Name'],
                    'type': 'cos_bucket',
                    'source': 'ibm_cloud_object_storage',
                    'location': f"cos://{bucket['Name']}",
                    'region': self.region,
                    'created_date': bucket['CreationDate'],
                    'size': 0,
                    'metadata': {
                        'service': 'cloud_object_storage',
                        'resource_type': 'bucket',
                        'region': self.region
                    }
                }
                assets.append(asset)
                
        except Exception as e:
            self.logger.error(f"Error discovering IBM Cloud Object Storage assets: {e}")
        
        return assets
    
    def _discover_db2_assets(self) -> List[Dict[str, Any]]:
        """Discover IBM Db2 on Cloud assets"""
        assets = []
        try:
            import ibm_db
            
            db2_instances = self.config.get('db2_instances', [])
            
            for instance in db2_instances:
                asset = {
                    'name': instance.get('name', 'db2_instance'),
                    'type': 'db2_instance',
                    'source': 'ibm_db2',
                    'location': f"db2://{instance.get('hostname', 'unknown')}",
                    'region': self.region,
                    'created_date': datetime.now(),
                    'size': 0,
                    'metadata': {
                        'service': 'db2',
                        'resource_type': 'database',
                        'region': self.region,
                        'hostname': instance.get('hostname', ''),
                        'port': instance.get('port', 50000)
                    }
                }
                assets.append(asset)
                
        except Exception as e:
            self.logger.error(f"Error discovering IBM Db2 assets: {e}")
        
        return assets
    
    def _discover_cloudant_assets(self) -> List[Dict[str, Any]]:
        """Discover IBM Cloudant assets"""
        assets = []
        try:
            from ibmcloudant.cloudant_v1 import CloudantV1
            from ibm_cloud_sdk_core.authenticators import IAMAuthenticator
            
            service = CloudantV1(
                authenticator=self.authenticator
            )
            
            cloudant_instances = self.config.get('cloudant_instances', [])
            
            for instance_config in cloudant_instances:
                service.set_service_url(instance_config.get('url', ''))
                
                try:
                    # List databases
                    response = service.get_all_dbs()
                    databases = response.get_result()
                    
                    for db_name in databases:
                        asset = {
                            'name': db_name,
                            'type': 'cloudant_database',
                            'source': 'ibm_cloudant',
                            'location': f"cloudant://{instance_config.get('url', '')}/{db_name}",
                            'region': self.region,
                            'created_date': datetime.now(),
                            'size': 0,
                            'metadata': {
                                'service': 'cloudant',
                                'resource_type': 'database',
                                'region': self.region,
                                'instance_url': instance_config.get('url', '')
                            }
                        }
                        assets.append(asset)
                        
                except Exception as e:
                    self.logger.error(f"Error listing Cloudant databases: {e}")
                
        except Exception as e:
            self.logger.error(f"Error discovering IBM Cloudant assets: {e}")
        
        return assets
    
    def _discover_watson_discovery_assets(self) -> List[Dict[str, Any]]:
        """Discover IBM Watson Discovery assets"""
        assets = []
        try:
            from ibm_watson import DiscoveryV2
            
            discovery = DiscoveryV2(
                version='2020-08-30',
                authenticator=self.authenticator
            )
            
            watson_instances = self.config.get('watson_discovery_instances', [])
            
            for instance_config in watson_instances:
                discovery.set_service_url(instance_config.get('url', ''))
                project_id = instance_config.get('project_id', '')
                
                try:
                    # List collections
                    response = discovery.list_collections(project_id=project_id)
                    collections = response.get_result()['collections']
                    
                    for collection in collections:
                        asset = {
                            'name': collection['name'],
                            'type': 'watson_discovery_collection',
                            'source': 'ibm_watson_discovery',
                            'location': f"watson://{project_id}/{collection['collection_id']}",
                            'region': self.region,
                            'created_date': datetime.now(),
                            'size': 0,
                            'metadata': {
                                'service': 'watson_discovery',
                                'resource_type': 'collection',
                                'region': self.region,
                                'collection_id': collection['collection_id'],
                                'project_id': project_id
                            }
                        }
                        assets.append(asset)
                        
                except Exception as e:
                    self.logger.error(f"Error listing Watson Discovery collections: {e}")
                
        except Exception as e:
            self.logger.error(f"Error discovering IBM Watson Discovery assets: {e}")
        
        return assets
    
    def test_connection(self) -> bool:
        """Test connection to IBM Cloud"""
        try:
            if not self.authenticator:
                self.logger.error("IBM Cloud credentials not configured")
                return False
            
            from ibm_cloud_sdk_core.authenticators import IAMAuthenticator
            
            # Test with a simple API call to verify credentials
            # Using Resource Manager API to test connection
            from ibm_cloud_resource_manager import ResourceManagerV2
            
            service = ResourceManagerV2(authenticator=self.authenticator)
            service.set_service_url('https://resource-controller.cloud.ibm.com')
            
            # Try to list resource groups
            response = service.list_resource_groups()
            
            self.logger.info("IBM Cloud connection test successful")
            return True
            
        except Exception as e:
            self.logger.error(f"IBM Cloud connection test failed: {e}")
            return False
    
    def validate_config(self) -> bool:
        """Validate IBM Cloud connector configuration"""
        if not self.api_key:
            self.logger.error("No IBM Cloud API key configured")
            return False
        
        if not self.services:
            self.logger.error("No IBM Cloud services configured")
            return False
        
        return True

