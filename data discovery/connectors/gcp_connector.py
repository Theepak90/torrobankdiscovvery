"""
GCP Connector - Discovers data assets in Google Cloud Platform services
Real implementation with actual Google Cloud API calls
"""

from datetime import datetime
from typing import List, Dict, Any, Optional
import json
import tempfile
import os
import logging
from google.cloud import storage, bigquery
from google.cloud.exceptions import GoogleCloudError, NotFound
from google.oauth2 import service_account
from google.auth import default
from google.auth.exceptions import DefaultCredentialsError

from .base_connector import BaseConnector
class GCPConnector(BaseConnector):
    """
    Connector for discovering data assets in Google Cloud Platform services
    Real implementation with actual Google Cloud API calls
    """
    
    connector_type = "gcp"
    connector_name = "Google Cloud Platform"
    description = "Discover data assets from GCP services including BigQuery, Cloud Storage, Cloud SQL, and Dataflow"
    category = "cloud_providers"
    supported_services = ["BigQuery", "Cloud Storage", "Dataflow", "Pub/Sub", "Firestore"]
    required_config_fields = ["project_id"]
    optional_config_fields = ["credentials_path", "service_account_json", "services", "region"]
    
    def __init__(self, config: Dict[str, Any]):
        super().__init__(config)
        self.services = config.get('services', ['bigquery', 'cloud_storage'])
        self.project_id = config.get('project_id')
        self.service_account_json = config.get('service_account_json')
        self.credentials_path = config.get('credentials_path')
        self.region = config.get('region', 'us-central1')
        
        self.credentials = None
        self.storage_client = None
        self.bigquery_client = None
        
        self._initialize_credentials()
        self._initialize_clients()
    
    def _initialize_credentials(self):
        """Initialize GCP credentials from service account JSON or default credentials"""
        try:
            if self.service_account_json:
                if isinstance(self.service_account_json, str):
                    service_account_info = json.loads(self.service_account_json)
                else:
                    service_account_info = self.service_account_json
                
                self.credentials = service_account.Credentials.from_service_account_info(
                    service_account_info
                )
                self.logger.info("GCP credentials initialized from service account JSON")
                
            elif self.credentials_path and os.path.exists(self.credentials_path):
                self.credentials = service_account.Credentials.from_service_account_file(
                    self.credentials_path
                )
                self.logger.info(f"GCP credentials initialized from file: {self.credentials_path}")
                
            else:
                try:
                    self.credentials, _ = default()
                    self.logger.info("GCP credentials initialized using Application Default Credentials")
                except DefaultCredentialsError:
                    self.logger.warning("No GCP credentials found. Some services may not work.")
                    self.credentials = None
                    
        except Exception as e:
            self.logger.error(f"Failed to initialize GCP credentials: {e}")
            self.credentials = None
    
    def _initialize_clients(self):
        """Initialize GCP service clients"""
        try:
            if self.credentials and self.project_id:
                if 'bigquery' in self.services:
                    self.bigquery_client = bigquery.Client(
                        project=self.project_id,
                        credentials=self.credentials
                    )
                    self.logger.info("BigQuery client initialized")
                
                if 'cloud_storage' in self.services:
                    self.storage_client = storage.Client(
                        project=self.project_id,
                        credentials=self.credentials
                    )
                    self.logger.info("Cloud Storage client initialized")
                
                    
        except Exception as e:
            self.logger.error(f"Failed to initialize GCP clients: {e}")
    
    def discover_assets(self) -> List[Dict[str, Any]]:
        """Discover GCP data assets using real API calls"""
        if not self.credentials or not self.project_id:
            self.logger.error("GCP credentials or project ID not available")
            return []
        
        assets = []
        
        try:
            if 'bigquery' in self.services and self.bigquery_client:
                assets.extend(self._discover_bigquery_assets())
            
            if 'cloud_storage' in self.services and self.storage_client:
                assets.extend(self._discover_cloud_storage_assets())
            
                
        except Exception as e:
            self.logger.error(f"Error discovering GCP assets: {e}")
        
        self.logger.info(f"Discovered {len(assets)} GCP assets")
        return assets
    
    def _discover_bigquery_assets(self) -> List[Dict[str, Any]]:
        """Discover BigQuery datasets, tables, and views"""
        assets = []
        
        try:
            datasets = list(self.bigquery_client.list_datasets())
            
            for dataset in datasets:
                dataset_id = dataset.dataset_id
                dataset_ref = self.bigquery_client.dataset(dataset_id)
                
                try:
                    dataset_obj = self.bigquery_client.get_dataset(dataset_ref)
                    
                    assets.append({
                        'name': f"{self.project_id}.{dataset_id}",
                        'type': 'bigquery_dataset',
                        'source': 'gcp_bigquery',
                        'location': f"bigquery://{self.project_id}/{dataset_id}",
                        'size': 0,  # Datasets don't have size
                        'created_date': dataset_obj.created.isoformat() if dataset_obj.created else None,
                        'modified_date': dataset_obj.modified.isoformat() if dataset_obj.modified else None,
                        'schema': {},
                        'tags': ['gcp', 'bigquery', 'dataset'],
                        'metadata': {
                            'service': 'bigquery',
                            'resource_type': 'dataset',
                            'project_id': self.project_id,
                            'dataset_id': dataset_id,
                            'location': dataset_obj.location,
                            'description': dataset_obj.description or '',
                            'labels': dict(dataset_obj.labels) if dataset_obj.labels else {}
                        }
                    })
                    
                    tables = list(self.bigquery_client.list_tables(dataset_ref))
                    
                    for table in tables:
                        table_id = table.table_id
                        table_ref = dataset_ref.table(table_id)
                        
                        try:
                            table_obj = self.bigquery_client.get_table(table_ref)
                            
                            table_type = 'bigquery_view' if table_obj.table_type == 'VIEW' else 'bigquery_table'
                            
                            schema_info = []
                            if table_obj.schema:
                                for field in table_obj.schema:
                                    schema_info.append({
                                        'name': field.name,
                                        'type': field.field_type,
                                        'mode': field.mode,
                                        'description': field.description or ''
                                    })
                            
                            assets.append({
                                'name': f"{self.project_id}.{dataset_id}.{table_id}",
                                'type': table_type,
                                'source': 'gcp_bigquery',
                                'location': f"bigquery://{self.project_id}/{dataset_id}/{table_id}",
                                'size': table_obj.num_bytes or 0,
                                'created_date': table_obj.created.isoformat() if table_obj.created else None,
                                'modified_date': table_obj.modified.isoformat() if table_obj.modified else None,
                                'schema': {
                                    'fields': schema_info,
                                    'num_rows': table_obj.num_rows or 0,
                                    'num_bytes': table_obj.num_bytes or 0
                                },
                                'tags': ['gcp', 'bigquery', 'table' if table_type == 'bigquery_table' else 'view'],
                                'metadata': {
                                    'service': 'bigquery',
                                    'resource_type': 'table',
                                    'project_id': self.project_id,
                                    'dataset_id': dataset_id,
                                    'table_id': table_id,
                                    'table_type': table_obj.table_type,
                                    'description': table_obj.description or '',
                                    'labels': dict(table_obj.labels) if table_obj.labels else {},
                                    'expiration_time': table_obj.expires.isoformat() if table_obj.expires else None
                                }
                            })
                            
                        except NotFound:
                            self.logger.warning(f"Table {table_id} not found in dataset {dataset_id}")
                        except Exception as e:
                            self.logger.error(f"Error getting table {table_id}: {e}")
                
                except NotFound:
                    self.logger.warning(f"Dataset {dataset_id} not found")
                except Exception as e:
                    self.logger.error(f"Error getting dataset {dataset_id}: {e}")
                    
        except Exception as e:
            self.logger.error(f"Error discovering BigQuery assets: {e}")
        
        return assets
    
    def _discover_cloud_storage_assets(self) -> List[Dict[str, Any]]:
        """Discover Cloud Storage buckets and objects"""
        assets = []
        
        try:
            buckets = list(self.storage_client.list_buckets())
            
            for bucket in buckets:
                bucket_name = bucket.name
                
                assets.append({
                    'name': bucket_name,
                    'type': 'gcs_bucket',
                    'source': 'gcp_cloud_storage',
                    'location': f"gs://{bucket_name}",
                    'size': 0,  # Buckets don't have size
                    'created_date': bucket.time_created.isoformat() if bucket.time_created else None,
                    'modified_date': bucket.updated.isoformat() if bucket.updated else None,
                    'schema': {},
                    'tags': ['gcp', 'cloud_storage', 'bucket'],
                    'metadata': {
                        'service': 'cloud_storage',
                        'resource_type': 'bucket',
                        'project_id': self.project_id,
                        'bucket_name': bucket_name,
                        'location': bucket.location or 'US',
                        'storage_class': bucket.storage_class or 'STANDARD',
                        'labels': dict(bucket.labels) if bucket.labels else {},
                        'versioning_enabled': bucket.versioning_enabled or False,
                        'lifecycle_rules': len(bucket.lifecycle_rules) if bucket.lifecycle_rules else 0
                    }
                })
                
                try:
                    blobs = list(self.storage_client.list_blobs(bucket_name, max_results=1000))
                    
                    for blob in blobs:
                        if blob.size and blob.size > 1024:  # > 1KB
                            file_extension = os.path.splitext(blob.name)[1].lower()
                            
                            assets.append({
                                'name': f"{bucket_name}/{blob.name}",
                                'type': 'gcs_object',
                                'source': 'gcp_cloud_storage',
                                'location': f"gs://{bucket_name}/{blob.name}",
                                'size': blob.size or 0,
                                'created_date': blob.time_created.isoformat() if blob.time_created else None,
                                'modified_date': blob.updated.isoformat() if blob.updated else None,
                                'schema': {},
                                'tags': ['gcp', 'cloud_storage', 'object', file_extension[1:] if file_extension else 'file'],
                                'metadata': {
                                    'service': 'cloud_storage',
                                    'resource_type': 'object',
                                    'project_id': self.project_id,
                                    'bucket_name': bucket_name,
                                    'object_name': blob.name,
                                    'content_type': blob.content_type or 'application/octet-stream',
                                    'storage_class': blob.storage_class or 'STANDARD',
                                    'labels': dict(blob.metadata) if blob.metadata else {},
                                    'md5_hash': blob.md5_hash,
                                    'crc32c': blob.crc32c
                                }
                            })
                            
                except Exception as e:
                    self.logger.error(f"Error listing objects in bucket {bucket_name}: {e}")
                    
        except Exception as e:
            self.logger.error(f"Error discovering Cloud Storage assets: {e}")
        
        return assets
    
    
    def test_connection(self) -> bool:
        """Test connection to GCP services"""
        try:
            if not self.credentials or not self.project_id:
                self.logger.error("GCP credentials or project ID not available")
                return False
            
            connection_successful = False
            
            if 'bigquery' in self.services and self.bigquery_client:
                try:
                    list(self.bigquery_client.list_datasets(max_results=1))
                    self.logger.info("BigQuery connection test successful")
                    connection_successful = True
                except Exception as e:
                    self.logger.error(f"BigQuery connection test failed: {e}")
            
            if 'cloud_storage' in self.services and self.storage_client:
                try:
                    list(self.storage_client.list_buckets(max_results=1))
                    self.logger.info("Cloud Storage connection test successful")
                    connection_successful = True
                except Exception as e:
                    self.logger.error(f"Cloud Storage connection test failed: {e}")
            
            if connection_successful:
                self.logger.info("GCP connection test successful - at least one service is accessible")
                return True
            else:
                self.logger.error("GCP connection test failed - no services are accessible")
                return False
            
        except Exception as e:
            self.logger.error(f"GCP connection test failed: {e}")
            return False
    
    def validate_config(self) -> bool:
        """Validate GCP connector configuration"""
        if not self.project_id:
            self.logger.error("Project ID is required")
            return False
        
        if not self.services:
            self.logger.error("At least one service must be specified")
            return False
        
        if self.service_account_json:
            try:
                if isinstance(self.service_account_json, str):
                    json.loads(self.service_account_json)
                else:
                    required_fields = ['type', 'project_id', 'private_key', 'client_email']
                    for field in required_fields:
                        if field not in self.service_account_json:
                            self.logger.error(f"Service account JSON missing required field: {field}")
                            return False
            except json.JSONDecodeError:
                self.logger.error("Invalid service account JSON format")
                return False
        
        return True
    
    def get_connection_info(self) -> Dict[str, Any]:
        """Get GCP connection information"""
        return {
            'connector_type': self.__class__.__name__,
            'project_id': self.project_id,
            'services': self.services,
            'region': self.region,
            'has_credentials': self.credentials is not None,
            'clients_initialized': {
                'bigquery': self.bigquery_client is not None,
                'cloud_storage': self.storage_client is not None
            }
        }