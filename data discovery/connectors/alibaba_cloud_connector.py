"""
Alibaba Cloud Connector - Discovers data assets in Alibaba Cloud services
"""

from datetime import datetime
from typing import List, Dict, Any
import logging

from .base_connector import BaseConnector


class AlibabaCloudConnector(BaseConnector):
    """
    Connector for discovering data assets in Alibaba Cloud services
    """
    
    def __init__(self, config: Dict[str, Any]):
        super().__init__(config)
        self.services = config.get('services', ['oss', 'rds', 'polardb', 'mongodb', 'redis'])
        self.access_key_id = config.get('access_key_id')
        self.access_key_secret = config.get('access_key_secret')
        self.region = config.get('region', 'cn-hangzhou')
        
        # Initialize Alibaba Cloud clients
        try:
            # Note: Requires alibabacloud_oss2 - pip install alibabacloud_oss2
            import oss2
            from alibabacloud_rds20140815.client import Client as RdsClient
            from alibabacloud_tea_openapi import models as open_api_models
            
            # OSS client
            auth = oss2.Auth(self.access_key_id, self.access_key_secret)
            self.oss_service = oss2.Service(auth, f'https://oss-{self.region}.aliyuncs.com')
            
            # RDS client config
            self.rds_config = open_api_models.Config(
                access_key_id=self.access_key_id,
                access_key_secret=self.access_key_secret,
                endpoint=f'https://rds.{self.region}.aliyuncs.com'
            )
            
            self.logger.info("Alibaba Cloud credentials initialized successfully")
        except ImportError:
            self.logger.warning("Alibaba Cloud SDK not installed. Install with: pip install alibabacloud_oss2")
            self.oss_service = None
            self.rds_config = None
        except Exception as e:
            self.logger.error(f"Alibaba Cloud credential initialization failed: {e}")
            self.oss_service = None
            self.rds_config = None
    
    def discover_assets(self) -> List[Dict[str, Any]]:
        """Discover Alibaba Cloud data assets"""
        if not self.oss_service:
            self.logger.error("Alibaba Cloud credentials not available")
            return []
        
        self.logger.info("Starting Alibaba Cloud asset discovery")
        assets = []
        
        if 'oss' in self.services:
            assets.extend(self._discover_oss_assets())
        
        if 'rds' in self.services:
            assets.extend(self._discover_rds_assets())
        
        if 'polardb' in self.services:
            assets.extend(self._discover_polardb_assets())
        
        if 'mongodb' in self.services:
            assets.extend(self._discover_mongodb_assets())
        
        if 'redis' in self.services:
            assets.extend(self._discover_redis_assets())
        
        self.logger.info(f"Discovered {len(assets)} Alibaba Cloud assets")
        return assets
    
    def _discover_oss_assets(self) -> List[Dict[str, Any]]:
        """Discover Alibaba Cloud Object Storage Service assets"""
        assets = []
        try:
            import oss2
            
            # List buckets
            for bucket_info in oss2.BucketIterator(self.oss_service):
                bucket = oss2.Bucket(oss2.Auth(self.access_key_id, self.access_key_secret), 
                                   bucket_info.extranet_endpoint, bucket_info.name)
                
                try:
                    bucket_info_detail = bucket.get_bucket_info()
                    
                    asset = {
                        'name': bucket_info.name,
                        'type': 'oss_bucket',
                        'source': 'alibaba_oss',
                        'location': f"oss://{bucket_info.name}",
                        'region': self.region,
                        'created_date': bucket_info_detail.creation_date,
                        'size': 0,
                        'metadata': {
                            'service': 'oss',
                            'resource_type': 'bucket',
                            'region': self.region,
                            'storage_class': bucket_info_detail.storage_class,
                            'location': bucket_info_detail.location
                        }
                    }
                    assets.append(asset)
                    
                except Exception as e:
                    self.logger.error(f"Error getting bucket info for {bucket_info.name}: {e}")
                
        except Exception as e:
            self.logger.error(f"Error discovering Alibaba Cloud OSS assets: {e}")
        
        return assets
    
    def _discover_rds_assets(self) -> List[Dict[str, Any]]:
        """Discover Alibaba Cloud RDS assets"""
        assets = []
        try:
            from alibabacloud_rds20140815.client import Client as RdsClient
            from alibabacloud_rds20140815 import models as rds_models
            
            rds_client = RdsClient(self.rds_config)
            
            # List RDS instances
            request = rds_models.DescribeDBInstancesRequest()
            response = rds_client.describe_dbinstances(request)
            
            for instance in response.body.items.dbinstance:
                asset = {
                    'name': instance.dbinstance_id,
                    'type': 'rds_instance',
                    'source': 'alibaba_rds',
                    'location': f"rds://{instance.connection_string}",
                    'region': self.region,
                    'created_date': instance.create_time,
                    'size': instance.dbinstance_storage * 1024 * 1024 * 1024 if instance.dbinstance_storage else 0,
                    'metadata': {
                        'service': 'rds',
                        'resource_type': 'database',
                        'region': self.region,
                        'engine': instance.engine,
                        'engine_version': instance.engine_version,
                        'status': instance.dbinstance_status
                    }
                }
                assets.append(asset)
                
        except Exception as e:
            self.logger.error(f"Error discovering Alibaba Cloud RDS assets: {e}")
        
        return assets
    
    def _discover_polardb_assets(self) -> List[Dict[str, Any]]:
        """Discover Alibaba Cloud PolarDB assets"""
        assets = []
        try:
            # This would require PolarDB specific SDK
            # Placeholder for PolarDB discovery logic
            polardb_instances = self.config.get('polardb_instances', [])
            
            for instance in polardb_instances:
                asset = {
                    'name': instance.get('cluster_id', 'polardb_cluster'),
                    'type': 'polardb_cluster',
                    'source': 'alibaba_polardb',
                    'location': f"polardb://{instance.get('endpoint', 'unknown')}",
                    'region': self.region,
                    'created_date': datetime.now(),
                    'size': 0,
                    'metadata': {
                        'service': 'polardb',
                        'resource_type': 'cluster',
                        'region': self.region,
                        'engine': instance.get('engine', ''),
                        'status': instance.get('status', 'UNKNOWN')
                    }
                }
                assets.append(asset)
                
        except Exception as e:
            self.logger.error(f"Error discovering Alibaba Cloud PolarDB assets: {e}")
        
        return assets
    
    def _discover_mongodb_assets(self) -> List[Dict[str, Any]]:
        """Discover Alibaba Cloud MongoDB assets"""
        assets = []
        try:
            # This would require MongoDB specific SDK
            # Placeholder for MongoDB discovery logic
            mongodb_instances = self.config.get('mongodb_instances', [])
            
            for instance in mongodb_instances:
                asset = {
                    'name': instance.get('instance_id', 'mongodb_instance'),
                    'type': 'mongodb_instance',
                    'source': 'alibaba_mongodb',
                    'location': f"mongodb://{instance.get('connection_string', 'unknown')}",
                    'region': self.region,
                    'created_date': datetime.now(),
                    'size': 0,
                    'metadata': {
                        'service': 'mongodb',
                        'resource_type': 'database',
                        'region': self.region,
                        'engine_version': instance.get('engine_version', ''),
                        'status': instance.get('status', 'UNKNOWN')
                    }
                }
                assets.append(asset)
                
        except Exception as e:
            self.logger.error(f"Error discovering Alibaba Cloud MongoDB assets: {e}")
        
        return assets
    
    def _discover_redis_assets(self) -> List[Dict[str, Any]]:
        """Discover Alibaba Cloud Redis assets"""
        assets = []
        try:
            # This would require Redis specific SDK
            # Placeholder for Redis discovery logic
            redis_instances = self.config.get('redis_instances', [])
            
            for instance in redis_instances:
                asset = {
                    'name': instance.get('instance_id', 'redis_instance'),
                    'type': 'redis_instance',
                    'source': 'alibaba_redis',
                    'location': f"redis://{instance.get('connection_string', 'unknown')}",
                    'region': self.region,
                    'created_date': datetime.now(),
                    'size': 0,
                    'metadata': {
                        'service': 'redis',
                        'resource_type': 'cache',
                        'region': self.region,
                        'engine_version': instance.get('engine_version', ''),
                        'status': instance.get('status', 'UNKNOWN')
                    }
                }
                assets.append(asset)
                
        except Exception as e:
            self.logger.error(f"Error discovering Alibaba Cloud Redis assets: {e}")
        
        return assets
    
    def test_connection(self) -> bool:
        """Test connection to Alibaba Cloud"""
        try:
            if not self.oss_service:
                self.logger.error("Alibaba Cloud credentials not configured")
                return False
            
            import oss2
            
            # Test with a simple API call to verify credentials
            # Try to list buckets to test connection
            auth = oss2.Auth(self.access_key_id, self.access_key_secret)
            service = oss2.Service(auth, f'https://oss-{self.region}.aliyuncs.com')
            
            # List buckets to test connection
            buckets = list(oss2.BucketIterator(service))
            
            self.logger.info("Alibaba Cloud connection test successful")
            return True
            
        except Exception as e:
            self.logger.error(f"Alibaba Cloud connection test failed: {e}")
            return False
    
    def validate_config(self) -> bool:
        """Validate Alibaba Cloud connector configuration"""
        if not self.access_key_id or not self.access_key_secret:
            self.logger.error("No Alibaba Cloud access keys configured")
            return False
        
        if not self.services:
            self.logger.error("No Alibaba Cloud services configured")
            return False
        
        return True

