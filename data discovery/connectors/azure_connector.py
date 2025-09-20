"""
Azure Connector - Discovers data assets in Azure services (Blob Storage, SQL Database, Cosmos DB)
"""

from datetime import datetime
from typing import List, Dict, Any
from azure.storage.blob import BlobServiceClient
from azure.identity import DefaultAzureCredential
from azure.core.exceptions import AzureError

from .base_connector import BaseConnector


class AzureConnector(BaseConnector):
    """
    Connector for discovering data assets in Azure services
    """
    
    def __init__(self, config: Dict[str, Any]):
        super().__init__(config)
        self.services = config.get('services', ['blob_storage'])
        
        # Initialize Azure credential
        try:
            self.credential = DefaultAzureCredential()
            self.logger.info("Azure credentials initialized successfully")
        except Exception as e:
            self.logger.error(f"Azure credential initialization failed: {e}")
            self.credential = None
    
    def discover_assets(self) -> List[Dict[str, Any]]:
        """Discover Azure data assets"""
        if not self.credential:
            self.logger.error("Azure credentials not available")
            return []
        
        self.logger.info("Starting Azure asset discovery")
        assets = []
        
        if 'blob_storage' in self.services:
            assets.extend(self._discover_blob_storage_assets())
        
        if 'sql_database' in self.services:
            assets.extend(self._discover_sql_database_assets())
        
        if 'cosmos_db' in self.services:
            assets.extend(self._discover_cosmos_db_assets())
        
        self.logger.info(f"Discovered {len(assets)} Azure assets")
        return assets
    
    def _discover_blob_storage_assets(self) -> List[Dict[str, Any]]:
        """Discover Azure Blob Storage assets"""
        assets = []
        
        storage_accounts = self.config.get('storage_accounts', [])
        
        for storage_account in storage_accounts:
            try:
                account_url = f"https://{storage_account}.blob.core.windows.net"
                blob_service_client = BlobServiceClient(
                    account_url=account_url,
                    credential=self.credential
                )
                
                # List containers
                containers = blob_service_client.list_containers()
                
                for container in containers:
                    container_asset = {
                        'name': container.name,
                        'type': 'azure_blob_container',
                        'source': 'azure_blob_storage',
                        'location': f"https://{storage_account}.blob.core.windows.net/{container.name}",
                        'size': 0,
                        'created_date': container.last_modified,
                        'modified_date': container.last_modified,
                        'schema': {},
                        'tags': ['azure', 'blob_storage', 'container'],
                        'metadata': {
                            'service': 'blob_storage',
                            'resource_type': 'container',
                            'storage_account': storage_account,
                            'public_access': container.public_access
                        }
                    }
                    
                    assets.append(container_asset)
                    
                    # Discover blobs in container
                    blob_assets = self._discover_blobs_in_container(
                        blob_service_client, container.name, storage_account
                    )
                    assets.extend(blob_assets[:50])  # Limit to first 50 blobs per container
                    
            except AzureError as e:
                self.logger.error(f"Error accessing Azure storage account {storage_account}: {e}")
                continue
        
        return assets
    
    def _discover_blobs_in_container(self, blob_service_client, container_name: str, storage_account: str) -> List[Dict[str, Any]]:
        """Discover blobs in Azure container"""
        assets = []
        
        try:
            container_client = blob_service_client.get_container_client(container_name)
            blobs = container_client.list_blobs()
            
            for blob in blobs:
                # Filter for data files
                if not self._is_data_blob(blob.name):
                    continue
                
                blob_asset = {
                    'name': blob.name.split('/')[-1],  # Just the filename
                    'type': self._determine_blob_type(blob.name),
                    'source': 'azure_blob_storage',
                    'location': f"https://{storage_account}.blob.core.windows.net/{container_name}/{blob.name}",
                    'size': blob.size,
                    'created_date': blob.creation_time,
                    'modified_date': blob.last_modified,
                    'schema': {},
                    'tags': ['azure', 'blob_storage', 'blob'] + self._get_blob_tags(blob.name),
                    'metadata': {
                        'service': 'blob_storage',
                        'resource_type': 'blob',
                        'container': container_name,
                        'storage_account': storage_account,
                        'blob_type': blob.blob_type,
                        'content_type': blob.content_settings.content_type if blob.content_settings else None,
                        'etag': blob.etag
                    }
                }
                
                assets.append(blob_asset)
                
        except AzureError as e:
            self.logger.warning(f"Error listing blobs in container {container_name}: {e}")
        
        return assets
    
    def _discover_sql_database_assets(self) -> List[Dict[str, Any]]:
        """Discover Azure SQL Database assets"""
        assets = []
        
        sql_servers = self.config.get('sql_servers', [])
        
        for server_config in sql_servers:
            try:
                server_name = server_config.get('server_name')
                databases = server_config.get('databases', [])
                
                for database_name in databases:
                    db_asset = {
                        'name': database_name,
                        'type': 'azure_sql_database',
                        'source': 'azure_sql_database',
                        'location': f"{server_name}.database.windows.net/{database_name}",
                        'size': 0,  # Would need to query for actual size
                        'created_date': datetime.now(),  # Would need actual creation date
                        'modified_date': datetime.now(),
                        'schema': {},
                        'tags': ['azure', 'sql_database', 'relational'],
                        'metadata': {
                            'service': 'sql_database',
                            'resource_type': 'database',
                            'server_name': server_name,
                            'database_name': database_name
                        }
                    }
                    
                    assets.append(db_asset)
                    
            except Exception as e:
                self.logger.error(f"Error discovering Azure SQL databases: {e}")
        
        return assets
    
    def _discover_cosmos_db_assets(self) -> List[Dict[str, Any]]:
        """Discover Azure Cosmos DB assets"""
        assets = []
        
        cosmos_accounts = self.config.get('cosmos_accounts', [])
        
        for account_config in cosmos_accounts:
            try:
                account_name = account_config.get('account_name')
                databases = account_config.get('databases', [])
                
                for database_config in databases:
                    database_name = database_config.get('name')
                    containers = database_config.get('containers', [])
                    
                    for container_name in containers:
                        cosmos_asset = {
                            'name': f"{database_name}.{container_name}",
                            'type': 'azure_cosmos_container',
                            'source': 'azure_cosmos_db',
                            'location': f"{account_name}.documents.azure.com/{database_name}/{container_name}",
                            'size': 0,  # Would need to query for actual size
                            'created_date': datetime.now(),  # Would need actual creation date
                            'modified_date': datetime.now(),
                            'schema': {},
                            'tags': ['azure', 'cosmos_db', 'nosql'],
                            'metadata': {
                                'service': 'cosmos_db',
                                'resource_type': 'container',
                                'account_name': account_name,
                                'database_name': database_name,
                                'container_name': container_name
                            }
                        }
                        
                        assets.append(cosmos_asset)
                        
            except Exception as e:
                self.logger.error(f"Error discovering Cosmos DB assets: {e}")
        
        return assets
    
    def _is_data_blob(self, blob_name: str) -> bool:
        """Check if Azure blob is a data file"""
        data_extensions = {'.csv', '.json', '.parquet', '.avro', '.orc', '.txt', '.tsv', '.xlsx', '.xml', '.yaml', '.yml'}
        return any(blob_name.lower().endswith(ext) for ext in data_extensions)
    
    def _determine_blob_type(self, blob_name: str) -> str:
        """Determine Azure blob type based on extension"""
        blob_name_lower = blob_name.lower()
        
        if blob_name_lower.endswith('.csv'):
            return 'azure_csv_file'
        elif blob_name_lower.endswith('.json'):
            return 'azure_json_file'
        elif blob_name_lower.endswith('.parquet'):
            return 'azure_parquet_file'
        elif blob_name_lower.endswith(('.xlsx', '.xls')):
            return 'azure_excel_file'
        elif blob_name_lower.endswith('.avro'):
            return 'azure_avro_file'
        elif blob_name_lower.endswith('.orc'):
            return 'azure_orc_file'
        else:
            return 'azure_data_file'
    
    def _get_blob_tags(self, blob_name: str) -> List[str]:
        """Generate tags for Azure blob"""
        tags = []
        
        # Add extension-based tag
        if '.' in blob_name:
            ext = blob_name.split('.')[-1].lower()
            tags.append(f"ext_{ext}")
        
        # Add path-based tags
        if '/' in blob_name:
            path_parts = blob_name.split('/')
            for part in path_parts[:-1]:  # Exclude filename
                if part.lower() in ['data', 'raw', 'processed', 'archive', 'backup']:
                    tags.append(f"folder_{part.lower()}")
        
        return tags
    
    def test_connection(self) -> bool:
        """Test Azure connection"""
        if not self.credential:
            return False
        
        try:
            # Test with a simple credential check
            # In a real implementation, you might test access to a specific resource
            token = self.credential.get_token("https://management.azure.com/.default")
            if token:
                self.logger.info("Azure connection test successful")
                return True
            else:
                return False
        except Exception as e:
            self.logger.error(f"Azure connection test failed: {e}")
            return False
    
    def validate_config(self) -> bool:
        """Validate Azure connector configuration"""
        if not self.services:
            self.logger.error("No Azure services configured")
            return False
        
        return True
