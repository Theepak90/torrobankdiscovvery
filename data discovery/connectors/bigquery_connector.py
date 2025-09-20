"""
BigQuery Connector - Dedicated connector for Google BigQuery
"""

from datetime import datetime
from typing import List, Dict, Any
import json
import os
from google.cloud import bigquery
from google.cloud.exceptions import GoogleCloudError
from google.oauth2 import service_account

from .base_connector import BaseConnector


class BigQueryConnector(BaseConnector):
    """
    Dedicated connector for Google BigQuery data warehouse
    """
    
    def __init__(self, config: Dict[str, Any]):
        super().__init__(config)
        self.project_id = config.get('project_id')
        self.service_account_json = config.get('service_account_json')
        self.credentials_path = config.get('credentials_path')
        self.dataset_id = config.get('dataset_id')  # Optional specific dataset
        
        # Initialize credentials and client
        self.credentials = None
        self.client = None
        
        self._initialize_credentials()
        self._initialize_client()
    
    def _initialize_credentials(self):
        """Initialize BigQuery credentials from service account JSON"""
        try:
            if self.service_account_json:
                # Parse JSON string into dict
                if isinstance(self.service_account_json, str):
                    service_account_info = json.loads(self.service_account_json)
                else:
                    service_account_info = self.service_account_json
                
                self.credentials = service_account.Credentials.from_service_account_info(service_account_info)
                self.logger.info("BigQuery credentials initialized from service account JSON")
                
            elif self.credentials_path and os.path.exists(self.credentials_path):
                # Use credentials file path
                self.credentials = service_account.Credentials.from_service_account_file(self.credentials_path)
                self.logger.info(f"BigQuery credentials initialized from file: {self.credentials_path}")
                
            else:
                self.logger.warning("No BigQuery credentials provided, will try default credentials")
                self.credentials = None
                
        except Exception as e:
            self.logger.error(f"Failed to initialize BigQuery credentials: {e}")
            self.credentials = None
    
    def _initialize_client(self):
        """Initialize BigQuery client"""
        try:
            if self.credentials:
                self.client = bigquery.Client(project=self.project_id, credentials=self.credentials)
            else:
                self.client = bigquery.Client(project=self.project_id)
            self.logger.info("BigQuery client initialized")
        except Exception as e:
            self.logger.error(f"BigQuery client initialization failed: {e}")
            self.client = None
    
    def discover_assets(self) -> List[Dict[str, Any]]:
        """Discover BigQuery assets"""
        self.logger.info("Starting BigQuery asset discovery")
        assets = []
        
        if not self.client:
            self.logger.error("BigQuery client not initialized")
            return assets
        
        try:
            # Get datasets to scan
            if self.dataset_id:
                # Scan specific dataset
                try:
                    dataset_ref = self.client.dataset(self.dataset_id)
                    dataset = self.client.get_dataset(dataset_ref)
                    datasets = [dataset]
                    self.logger.info(f"Scanning specific dataset: {self.dataset_id}")
                except Exception as e:
                    self.logger.error(f"Failed to get dataset {self.dataset_id}: {e}")
                    return assets
            else:
                # Scan all datasets
                datasets = list(self.client.list_datasets())
                self.logger.info(f"Scanning all datasets in project {self.project_id}")
            
            for dataset in datasets:
                # Add dataset as an asset
                dataset_asset = {
                    'name': dataset.dataset_id,
                    'type': 'bigquery_dataset',
                    'source': 'bigquery',
                    'location': f"bigquery://{self.project_id}/{dataset.dataset_id}",
                    'size': 0,
                    'created_date': dataset.created,
                    'modified_date': dataset.modified,
                    'schema': {},
                    'tags': ['bigquery', 'dataset', 'google_cloud'],
                    'metadata': {
                        'service': 'bigquery',
                        'resource_type': 'dataset',
                        'project_id': self.project_id,
                        'dataset_id': dataset.dataset_id,
                        'location': dataset.location,
                        'description': dataset.description or ''
                    }
                }
                assets.append(dataset_asset)
                
                # Discover tables in this dataset
                try:
                    tables = list(self.client.list_tables(dataset))
                    
                    for table in tables:
                        # Get detailed table info
                        table_ref = self.client.get_table(table)
                        
                        # Determine table type
                        if table_ref.table_type == 'VIEW':
                            table_type = 'bigquery_view'
                        elif table_ref.table_type == 'EXTERNAL':
                            table_type = 'bigquery_external_table'
                        else:
                            table_type = 'bigquery_table'
                        
                        # Build schema information
                        schema_info = {}
                        if table_ref.schema:
                            schema_info = {
                                'columns': [
                                    {
                                        'name': field.name,
                                        'type': field.field_type,
                                        'mode': field.mode,
                                        'description': field.description or ''
                                    }
                                    for field in table_ref.schema
                                ]
                            }
                        
                        table_asset = {
                            'name': f"{dataset.dataset_id}.{table.table_id}",
                            'type': table_type,
                            'source': 'bigquery',
                            'location': f"bigquery://{self.project_id}/{dataset.dataset_id}/{table.table_id}",
                            'size': table_ref.num_bytes or 0,
                            'created_date': table_ref.created,
                            'modified_date': table_ref.modified,
                            'schema': schema_info,
                            'tags': ['bigquery', 'table', 'google_cloud'],
                            'metadata': {
                                'service': 'bigquery',
                                'resource_type': 'table',
                                'project_id': self.project_id,
                                'dataset_id': dataset.dataset_id,
                                'table_id': table.table_id,
                                'table_type': table_ref.table_type,
                                'num_rows': table_ref.num_rows,
                                'num_bytes': table_ref.num_bytes,
                                'location': dataset.location,
                                'description': table_ref.description or '',
                                'labels': dict(table_ref.labels) if table_ref.labels else {}
                            }
                        }
                        assets.append(table_asset)
                        
                except Exception as e:
                    self.logger.warning(f"Error discovering tables in dataset {dataset.dataset_id}: {e}")
            
        except Exception as e:
            self.logger.error(f"Error discovering BigQuery assets: {e}")
        
        self.logger.info(f"Discovered {len(assets)} BigQuery assets")
        return assets
    
    def test_connection(self) -> bool:
        """Test BigQuery connection"""
        try:
            if not self.client:
                self.logger.error("BigQuery client not initialized")
                return False
            
            if not self.project_id:
                self.logger.error("No project ID configured")
                return False
            
            # Test by listing datasets
            try:
                datasets = list(self.client.list_datasets(max_results=1))
                self.logger.info("BigQuery connection test successful")
                return True
            except Exception as e:
                self.logger.error(f"BigQuery connection test failed: {e}")
                return False
                
        except Exception as e:
            self.logger.error(f"BigQuery connection test failed: {e}")
            return False
    
    def validate_config(self) -> bool:
        """Validate BigQuery connector configuration"""
        if not self.project_id:
            self.logger.error("BigQuery project ID not configured")
            return False
        
        if not self.service_account_json and not self.credentials_path:
            self.logger.warning("No BigQuery credentials configured, will try default credentials")
        
        return True
