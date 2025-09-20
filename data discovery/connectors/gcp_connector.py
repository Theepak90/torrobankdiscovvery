"""
GCP Connector - Discovers data assets in Google Cloud Platform services
"""

from datetime import datetime
from typing import List, Dict, Any
import json
import tempfile
import os
from google.cloud import storage, bigquery
from google.cloud.exceptions import GoogleCloudError
from google.oauth2 import service_account

from .base_connector import BaseConnector


class GCPConnector(BaseConnector):
    """
    Connector for discovering data assets in Google Cloud Platform services
    """
    
    def __init__(self, config: Dict[str, Any]):
        super().__init__(config)
        self.services = config.get('services', ['cloud_storage'])
        self.project_id = config.get('project_id')
        self.service_account_json = config.get('service_account_json')
        self.credentials_path = config.get('credentials_path')
        self.dataset_id = config.get('dataset_id')  # Optional dataset ID for BigQuery
        
        # Initialize credentials
        self.credentials = None
        self.storage_client = None
        self.bigquery_client = None
        
        self._initialize_credentials()
        self._initialize_clients()
    
    def _initialize_credentials(self):
        """Initialize GCP credentials from service account JSON"""
        try:
            if self.service_account_json:
                # Parse JSON string into dict
                if isinstance(self.service_account_json, str):
                    service_account_info = json.loads(self.service_account_json)
                else:
                    service_account_info = self.service_account_json
                
                self.credentials = service_account.Credentials.from_service_account_info(service_account_info)
                self.logger.info("GCP credentials initialized from service account JSON")
                
            elif self.credentials_path and os.path.exists(self.credentials_path):
                # Use credentials file path
                self.credentials = service_account.Credentials.from_service_account_file(self.credentials_path)
                self.logger.info(f"GCP credentials initialized from file: {self.credentials_path}")
                
            else:
                self.logger.warning("No GCP credentials provided, will try default credentials")
                self.credentials = None
                
        except Exception as e:
            self.logger.error(f"Failed to initialize GCP credentials: {e}")
            self.credentials = None
    
    def _initialize_clients(self):
        """Initialize GCP service clients"""
        try:
            if 'cloud_storage' in self.services or 'gcs' in self.services:
                if self.credentials:
                    self.storage_client = storage.Client(project=self.project_id, credentials=self.credentials)
                else:
                    self.storage_client = storage.Client(project=self.project_id)
                self.logger.info("GCP Storage client initialized")
            
            if 'bigquery' in self.services:
                if self.credentials:
                    self.bigquery_client = bigquery.Client(project=self.project_id, credentials=self.credentials)
                else:
                    self.bigquery_client = bigquery.Client(project=self.project_id)
                self.logger.info("GCP BigQuery client initialized")
                
        except Exception as e:
            self.logger.error(f"GCP client initialization failed: {e}")
            self.storage_client = None
            self.bigquery_client = None
    
    def discover_assets(self) -> List[Dict[str, Any]]:
        """Discover GCP data assets"""
        self.logger.info("Starting GCP asset discovery")
        assets = []
        
        if 'cloud_storage' in self.services and self.storage_client:
            assets.extend(self._discover_cloud_storage_assets())
        
        if 'bigquery' in self.services:
            assets.extend(self._discover_bigquery_assets())
        
        if 'cloud_sql' in self.services:
            assets.extend(self._discover_cloud_sql_assets())
        
        self.logger.info(f"Discovered {len(assets)} GCP assets")
        return assets
    
    def _discover_cloud_storage_assets(self) -> List[Dict[str, Any]]:
        """Discover Google Cloud Storage assets"""
        assets = []
        
        try:
            # List all buckets
            buckets = self.storage_client.list_buckets()
            
            for bucket in buckets:
                bucket_asset = {
                    'name': bucket.name,
                    'type': 'gcs_bucket',
                    'source': 'gcp_cloud_storage',
                    'location': f"gs://{bucket.name}",
                    'size': 0,
                    'created_date': bucket.time_created,
                    'modified_date': bucket.updated,
                    'schema': {},
                    'tags': ['gcp', 'cloud_storage', 'bucket'],
                    'metadata': {
                        'service': 'cloud_storage',
                        'resource_type': 'bucket',
                        'project_id': self.project_id,
                        'location': bucket.location,
                        'storage_class': bucket.storage_class,
                        'versioning_enabled': bucket.versioning_enabled
                    }
                }
                
                assets.append(bucket_asset)
                
                # Discover objects in bucket
                object_assets = self._discover_gcs_objects(bucket)
                assets.extend(object_assets[:100])  # Limit to first 100 objects per bucket
                
        except GoogleCloudError as e:
            self.logger.error(f"Error discovering Cloud Storage assets: {e}")
        
        return assets
    
    def _discover_gcs_objects(self, bucket) -> List[Dict[str, Any]]:
        """Discover objects in GCS bucket"""
        assets = []
        
        try:
            blobs = bucket.list_blobs(max_results=100)  # Limit for performance
            
            for blob in blobs:
                # Filter for data files
                if not self._is_data_object(blob.name):
                    continue
                
                object_asset = {
                    'name': blob.name.split('/')[-1],  # Just the filename
                    'type': self._determine_gcs_object_type(blob.name),
                    'source': 'gcp_cloud_storage',
                    'location': f"gs://{bucket.name}/{blob.name}",
                    'size': blob.size,
                    'created_date': blob.time_created,
                    'modified_date': blob.updated,
                    'schema': {},
                    'tags': ['gcp', 'cloud_storage', 'object'] + self._get_gcs_object_tags(blob.name),
                    'metadata': {
                        'service': 'cloud_storage',
                        'resource_type': 'object',
                        'bucket': bucket.name,
                        'name': blob.name,
                        'content_type': blob.content_type,
                        'storage_class': blob.storage_class,
                        'etag': blob.etag,
                        'generation': blob.generation
                    }
                }
                
                assets.append(object_asset)
                
        except GoogleCloudError as e:
            self.logger.warning(f"Error listing objects in bucket {bucket.name}: {e}")
        
        return assets
    
    def _discover_bigquery_assets(self) -> List[Dict[str, Any]]:
        """Discover BigQuery assets - COMPLETE discovery"""
        assets = []
        
        if not self.bigquery_client:
            self.logger.warning("BigQuery client not initialized")
            return assets
        
        try:
            # 1. Discover Datasets
            if self.dataset_id:
                # If specific dataset ID is provided, only scan that dataset
                try:
                    dataset_ref = self.bigquery_client.dataset(self.dataset_id)
                    dataset = self.bigquery_client.get_dataset(dataset_ref)
                    datasets = [dataset]
                    self.logger.info(f"Scanning specific dataset: {self.dataset_id}")
                except Exception as e:
                    self.logger.error(f"Failed to get dataset {self.dataset_id}: {e}")
                    return assets
            else:
                # Scan all datasets
                datasets = list(self.bigquery_client.list_datasets())
                self.logger.info(f"Scanning all datasets in project {self.project_id}")
            
            for dataset in datasets:
                dataset_asset = {
                    'name': dataset.dataset_id,
                    'type': 'bigquery_dataset',
                    'source': 'gcp_bigquery',
                    'location': f"bigquery://{self.project_id}/{dataset.dataset_id}",
                    'size': 0,
                    'created_date': dataset.created,
                    'modified_date': dataset.modified,
                    'schema': {},
                    'tags': ['gcp', 'bigquery', 'dataset'],
                    'metadata': {
                        'service': 'bigquery',
                        'resource_type': 'dataset',
                        'project_id': self.project_id,
                        'dataset_id': dataset.dataset_id,
                        'location': dataset.location
                    }
                }
                assets.append(dataset_asset)
                
                # 2. Discover Tables, Views, and External Tables
                table_assets = self._discover_bigquery_tables(bq_client, dataset)
                assets.extend(table_assets)
                
                # 3. Discover Routines (Functions & Procedures)
                routine_assets = self._discover_bigquery_routines(bq_client, dataset)
                assets.extend(routine_assets)
                
                # 4. Discover Models (BigQuery ML)
                model_assets = self._discover_bigquery_models(bq_client, dataset)
                assets.extend(model_assets)
            
            # 5. Discover Scheduled Queries
            scheduled_assets = self._discover_bigquery_scheduled_queries(bq_client)
            assets.extend(scheduled_assets)
            
            # 6. Discover Data Transfers
            transfer_assets = self._discover_bigquery_data_transfers(bq_client)
            assets.extend(transfer_assets)
            
            # 7. Discover Reservations
            reservation_assets = self._discover_bigquery_reservations(bq_client)
            assets.extend(reservation_assets)
                
        except Exception as e:
            self.logger.error(f"Error discovering BigQuery assets: {e}")
        
        return assets
    
    def _discover_bigquery_tables(self, bq_client, dataset) -> List[Dict[str, Any]]:
        """Discover tables in BigQuery dataset"""
        assets = []
        
        try:
            tables = bq_client.list_tables(dataset.reference)
            
            for table in tables:
                table_ref = bq_client.get_table(table.reference)
                
                table_asset = {
                    'name': table.table_id,
                    'type': 'bigquery_table',
                    'source': 'gcp_bigquery',
                    'location': f"bigquery://{self.project_id}/{dataset.dataset_id}/{table.table_id}",
                    'size': table_ref.num_bytes,
                    'created_date': table_ref.created,
                    'modified_date': table_ref.modified,
                    'schema': {
                        'fields': [{'name': field.name, 'type': field.field_type, 'mode': field.mode} 
                                  for field in table_ref.schema]
                    },
                    'tags': ['gcp', 'bigquery', 'table'],
                    'metadata': {
                        'service': 'bigquery',
                        'resource_type': 'table',
                        'project_id': self.project_id,
                        'dataset_id': dataset.dataset_id,
                        'table_id': table.table_id,
                        'table_type': table_ref.table_type,
                        'num_rows': table_ref.num_rows,
                        'num_bytes': table_ref.num_bytes
                    }
                }
                
                assets.append(table_asset)
                
        except Exception as e:
            self.logger.warning(f"Error discovering tables in dataset {dataset.dataset_id}: {e}")
        
        return assets
    
    def _discover_cloud_sql_assets(self) -> List[Dict[str, Any]]:
        """Discover Cloud SQL assets"""
        assets = []
        
        sql_instances = self.config.get('sql_instances', [])
        
        for instance_config in sql_instances:
            try:
                instance_name = instance_config.get('instance_name')
                databases = instance_config.get('databases', [])
                
                for database_name in databases:
                    sql_asset = {
                        'name': database_name,
                        'type': 'cloud_sql_database',
                        'source': 'gcp_cloud_sql',
                        'location': f"cloud-sql://{self.project_id}/{instance_name}/{database_name}",
                        'size': 0,  # Would need to query for actual size
                        'created_date': datetime.now(),  # Would need actual creation date
                        'modified_date': datetime.now(),
                        'schema': {},
                        'tags': ['gcp', 'cloud_sql', 'relational'],
                        'metadata': {
                            'service': 'cloud_sql',
                            'resource_type': 'database',
                            'project_id': self.project_id,
                            'instance_name': instance_name,
                            'database_name': database_name
                        }
                    }
                    
                    assets.append(sql_asset)
                    
            except Exception as e:
                self.logger.error(f"Error discovering Cloud SQL assets: {e}")
        
        return assets
    
    def _is_data_object(self, object_name: str) -> bool:
        """Check if GCS object is a data file"""
        data_extensions = {'.csv', '.json', '.parquet', '.avro', '.orc', '.txt', '.tsv', '.xlsx', '.xml', '.yaml', '.yml'}
        return any(object_name.lower().endswith(ext) for ext in data_extensions)
    
    def _determine_gcs_object_type(self, object_name: str) -> str:
        """Determine GCS object type based on extension"""
        object_name_lower = object_name.lower()
        
        if object_name_lower.endswith('.csv'):
            return 'gcs_csv_file'
        elif object_name_lower.endswith('.json'):
            return 'gcs_json_file'
        elif object_name_lower.endswith('.parquet'):
            return 'gcs_parquet_file'
        elif object_name_lower.endswith(('.xlsx', '.xls')):
            return 'gcs_excel_file'
        elif object_name_lower.endswith('.avro'):
            return 'gcs_avro_file'
        elif object_name_lower.endswith('.orc'):
            return 'gcs_orc_file'
        else:
            return 'gcs_data_file'
    
    def _get_gcs_object_tags(self, object_name: str) -> List[str]:
        """Generate tags for GCS object"""
        tags = []
        
        # Add extension-based tag
        if '.' in object_name:
            ext = object_name.split('.')[-1].lower()
            tags.append(f"ext_{ext}")
        
        # Add path-based tags
        if '/' in object_name:
            path_parts = object_name.split('/')
            for part in path_parts[:-1]:  # Exclude filename
                if part.lower() in ['data', 'raw', 'processed', 'archive', 'backup']:
                    tags.append(f"folder_{part.lower()}")
        
        return tags
    
    def test_connection(self) -> bool:
        """Test GCP connection"""
        try:
            # Test credentials and project access
            if not self.project_id:
                self.logger.error("No project ID configured")
                return False
            
            connection_tested = False
            
            # Test Cloud Storage connection if enabled
            if self.storage_client:
                try:
                    # Test by listing buckets (limited to 1)
                    buckets = list(self.storage_client.list_buckets(max_results=1))
                    self.logger.info("GCP Cloud Storage connection test successful")
                    connection_tested = True
                except Exception as e:
                    self.logger.warning(f"Cloud Storage test failed: {e}")
            
            # Test BigQuery connection if enabled
            if self.bigquery_client:
                try:
                    # Test by listing datasets (limited to 1)
                    datasets = list(self.bigquery_client.list_datasets(max_results=1))
                    self.logger.info("GCP BigQuery connection test successful")
                    connection_tested = True
                except Exception as e:
                    self.logger.warning(f"BigQuery test failed: {e}")
            
            if connection_tested:
                self.logger.info("GCP connection test successful")
                return True
            else:
                error_msg = "No GCP services could be tested - check credentials and project ID"
                self.logger.error(error_msg)
                raise Exception(error_msg)
                
        except Exception as e:
            error_msg = f"GCP connection test failed: {e}"
            self.logger.error(error_msg)
            raise Exception(error_msg)
    
    def validate_config(self) -> bool:
        """Validate GCP connector configuration"""
        if not self.services:
            self.logger.error("No GCP services configured")
            return False
        
        if not self.project_id:
            self.logger.error("No GCP project ID configured")
            return False
        
        return True
    
    def _discover_bigquery_routines(self, bq_client, dataset) -> List[Dict[str, Any]]:
        """Discover BigQuery routines (functions & procedures)"""
        assets = []
        
        try:
            routines = bq_client.list_routines(dataset.reference)
            
            for routine in routines:
                routine_ref = bq_client.get_routine(routine.reference)
                
                routine_asset = {
                    'name': routine.routine_id,
                    'type': 'bigquery_routine',
                    'source': 'gcp_bigquery',
                    'location': f"bigquery://{self.project_id}/{dataset.dataset_id}/{routine.routine_id}",
                    'size': 0,
                    'created_date': routine_ref.created,
                    'modified_date': routine_ref.modified,
                    'schema': {
                        'arguments': [{'name': arg.name, 'data_type': arg.data_type.type_kind} 
                                    for arg in routine_ref.arguments] if routine_ref.arguments else []
                    },
                    'tags': ['gcp', 'bigquery', 'routine'],
                    'metadata': {
                        'service': 'bigquery',
                        'resource_type': 'routine',
                        'project_id': self.project_id,
                        'dataset_id': dataset.dataset_id,
                        'routine_type': routine_ref.routine_type,
                        'language': routine_ref.language,
                        'definition_body': routine_ref.definition_body[:500] + '...' if routine_ref.definition_body and len(routine_ref.definition_body) > 500 else routine_ref.definition_body
                    }
                }
                assets.append(routine_asset)
                
        except Exception as e:
            self.logger.error(f"Error discovering BigQuery routines: {e}")
        
        return assets
    
    def _discover_bigquery_models(self, bq_client, dataset) -> List[Dict[str, Any]]:
        """Discover BigQuery ML models"""
        assets = []
        
        try:
            # List models in dataset
            models = bq_client.list_models(dataset.reference)
            
            for model in models:
                model_ref = bq_client.get_model(model.reference)
                
                model_asset = {
                    'name': model.model_id,
                    'type': 'bigquery_model',
                    'source': 'gcp_bigquery',
                    'location': f"bigquery://{self.project_id}/{dataset.dataset_id}/{model.model_id}",
                    'size': 0,
                    'created_date': model_ref.created,
                    'modified_date': model_ref.modified,
                    'schema': {},
                    'tags': ['gcp', 'bigquery', 'model', 'ml'],
                    'metadata': {
                        'service': 'bigquery',
                        'resource_type': 'model',
                        'project_id': self.project_id,
                        'dataset_id': dataset.dataset_id,
                        'model_type': model_ref.model_type,
                        'training_runs': len(model_ref.training_runs) if model_ref.training_runs else 0,
                        'feature_columns': [col.name for col in model_ref.feature_columns] if model_ref.feature_columns else [],
                        'label_columns': [col.name for col in model_ref.label_columns] if model_ref.label_columns else []
                    }
                }
                assets.append(model_asset)
                
        except Exception as e:
            self.logger.error(f"Error discovering BigQuery models: {e}")
        
        return assets
    
    def _discover_bigquery_scheduled_queries(self, bq_client) -> List[Dict[str, Any]]:
        """Discover BigQuery scheduled queries"""
        assets = []
        
        try:
            from google.cloud import bigquery_datatransfer
            
            transfer_client = bigquery_datatransfer.DataTransferServiceClient()
            project_path = f"projects/{self.project_id}"
            
            # List scheduled queries
            scheduled_queries = transfer_client.list_transfer_configs(parent=project_path)
            
            for config in scheduled_queries:
                if config.data_source_id == 'scheduled_query':
                    asset = {
                        'name': config.display_name,
                        'type': 'bigquery_scheduled_query',
                        'source': 'gcp_bigquery',
                        'location': f"bigquery://{self.project_id}/scheduled_queries/{config.name.split('/')[-1]}",
                        'size': 0,
                        'created_date': config.create_time,
                        'modified_date': config.update_time,
                        'schema': {},
                        'tags': ['gcp', 'bigquery', 'scheduled_query'],
                        'metadata': {
                            'service': 'bigquery',
                            'resource_type': 'scheduled_query',
                            'project_id': self.project_id,
                            'schedule': config.schedule,
                            'state': config.state.name,
                            'destination_dataset_id': config.destination_dataset_id,
                            'query': config.params.get('query', '')[:500] + '...' if config.params.get('query') and len(config.params.get('query', '')) > 500 else config.params.get('query', '')
                        }
                    }
                    assets.append(asset)
                    
        except Exception as e:
            self.logger.error(f"Error discovering BigQuery scheduled queries: {e}")
        
        return assets
    
    def _discover_bigquery_data_transfers(self, bq_client) -> List[Dict[str, Any]]:
        """Discover BigQuery data transfers"""
        assets = []
        
        try:
            from google.cloud import bigquery_datatransfer
            
            transfer_client = bigquery_datatransfer.DataTransferServiceClient()
            project_path = f"projects/{self.project_id}"
            
            # List data transfers
            transfers = transfer_client.list_transfer_configs(parent=project_path)
            
            for transfer in transfers:
                if transfer.data_source_id != 'scheduled_query':  # Skip scheduled queries
                    asset = {
                        'name': transfer.display_name,
                        'type': 'bigquery_data_transfer',
                        'source': 'gcp_bigquery',
                        'location': f"bigquery://{self.project_id}/transfers/{transfer.name.split('/')[-1]}",
                        'size': 0,
                        'created_date': transfer.create_time,
                        'modified_date': transfer.update_time,
                        'schema': {},
                        'tags': ['gcp', 'bigquery', 'data_transfer'],
                        'metadata': {
                            'service': 'bigquery',
                            'resource_type': 'data_transfer',
                            'project_id': self.project_id,
                            'data_source_id': transfer.data_source_id,
                            'destination_dataset_id': transfer.destination_dataset_id,
                            'state': transfer.state.name,
                            'schedule': transfer.schedule
                        }
                    }
                    assets.append(asset)
                    
        except Exception as e:
            self.logger.error(f"Error discovering BigQuery data transfers: {e}")
        
        return assets
    
    def _discover_bigquery_reservations(self, bq_client) -> List[Dict[str, Any]]:
        """Discover BigQuery reservations"""
        assets = []
        
        try:
            from google.cloud import bigquery_reservation
            
            reservation_client = bigquery_reservation.ReservationServiceClient()
            project_path = f"projects/{self.project_id}"
            
            # List reservations
            reservations = reservation_client.list_reservations(parent=project_path)
            
            for reservation in reservations:
                asset = {
                    'name': reservation.name.split('/')[-1],
                    'type': 'bigquery_reservation',
                    'source': 'gcp_bigquery',
                    'location': f"bigquery://{self.project_id}/reservations/{reservation.name.split('/')[-1]}",
                    'size': 0,
                    'created_date': datetime.now(),  # Reservations don't have creation time
                    'modified_date': datetime.now(),
                    'schema': {},
                    'tags': ['gcp', 'bigquery', 'reservation'],
                    'metadata': {
                        'service': 'bigquery',
                        'resource_type': 'reservation',
                        'project_id': self.project_id,
                        'slot_capacity': reservation.slot_capacity,
                        'ignore_idle_slots': reservation.ignore_idle_slots,
                        'location': reservation.location
                    }
                }
                assets.append(asset)
                
        except Exception as e:
            self.logger.error(f"Error discovering BigQuery reservations: {e}")
        
        return assets
