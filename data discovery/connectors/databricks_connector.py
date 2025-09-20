"""
Databricks Connector - Discovers data assets in Databricks workspaces
"""

import requests
from datetime import datetime
from typing import List, Dict, Any, Optional

from .base_connector import BaseConnector


class DatabricksConnector(BaseConnector):
    """
    Connector for discovering data assets in Databricks workspaces
    """
    
    # Metadata for dynamic discovery
    connector_type = "databricks"
    connector_name = "Databricks"
    description = "Discover data assets from Databricks workspace including tables, notebooks, and jobs"
    category = "data_warehouses"
    supported_services = ["Databricks", "Tables", "Notebooks", "Jobs", "Clusters"]
    required_config_fields = ["workspace_url", "access_token"]
    optional_config_fields = ["catalog", "schema", "connection_timeout"]
    
    def __init__(self, config: Dict[str, Any]):
        super().__init__(config)
        self.workspace_url = config.get('workspace_url', '').rstrip('/')
        self.access_token = config.get('access_token', '')
        self.catalog = config.get('catalog', 'main')
        self.schema = config.get('schema', 'default')
        self.connection_timeout = config.get('connection_timeout', 30)
        self.headers = {
            'Authorization': f'Bearer {self.access_token}',
            'Content-Type': 'application/json'
        }
    
    def discover_assets(self) -> List[Dict[str, Any]]:
        """Discover Databricks assets"""
        self.logger.info("Starting Databricks asset discovery")
        assets = []
        
        try:
            # Discover catalogs
            catalogs = self._discover_catalogs()
            assets.extend(catalogs)
            
            # Discover schemas
            schemas = self._discover_schemas()
            assets.extend(schemas)
            
            # Discover tables
            tables = self._discover_tables()
            assets.extend(tables)
            
            # Discover notebooks
            notebooks = self._discover_notebooks()
            assets.extend(notebooks)
            
            # Discover jobs
            jobs = self._discover_jobs()
            assets.extend(jobs)
            
        except Exception as e:
            self.logger.error(f"Error discovering Databricks assets: {e}")
        
        self.logger.info(f"Discovered {len(assets)} Databricks assets")
        return assets
    
    def _discover_catalogs(self) -> List[Dict[str, Any]]:
        """Discover Databricks catalogs"""
        assets = []
        
        try:
            response = requests.get(
                f"{self.workspace_url}/api/2.1/unity-catalog/catalogs",
                headers=self.headers,
                timeout=self.connection_timeout
            )
            response.raise_for_status()
            
            catalogs_data = response.json()
            
            for catalog in catalogs_data.get('catalogs', []):
                asset = {
                    'name': catalog['name'],
                    'type': 'databricks_catalog',
                    'source': 'databricks',
                    'location': f"databricks://{self.workspace_url}/catalog/{catalog['name']}",
                    'size': 0,
                    'created_date': datetime.now().isoformat(),
                    'modified_date': datetime.now().isoformat(),
                    'schema': {},
                    'tags': ['databricks', 'catalog', 'unity-catalog'],
                    'metadata': {
                        'workspace_url': self.workspace_url,
                        'catalog_name': catalog['name'],
                        'comment': catalog.get('comment', ''),
                        'properties': catalog.get('properties', {})
                    }
                }
                assets.append(asset)
                
        except Exception as e:
            self.logger.error(f"Error discovering catalogs: {e}")
        
        return assets
    
    def _discover_schemas(self) -> List[Dict[str, Any]]:
        """Discover Databricks schemas"""
        assets = []
        
        try:
            response = requests.get(
                f"{self.workspace_url}/api/2.1/unity-catalog/schemas",
                headers=self.headers,
                timeout=self.connection_timeout
            )
            response.raise_for_status()
            
            schemas_data = response.json()
            
            for schema in schemas_data.get('schemas', []):
                asset = {
                    'name': f"{schema['catalog_name']}.{schema['name']}",
                    'type': 'databricks_schema',
                    'source': 'databricks',
                    'location': f"databricks://{self.workspace_url}/catalog/{schema['catalog_name']}/schema/{schema['name']}",
                    'size': 0,
                    'created_date': datetime.now().isoformat(),
                    'modified_date': datetime.now().isoformat(),
                    'schema': {},
                    'tags': ['databricks', 'schema', 'unity-catalog'],
                    'metadata': {
                        'workspace_url': self.workspace_url,
                        'catalog_name': schema['catalog_name'],
                        'schema_name': schema['name'],
                        'comment': schema.get('comment', ''),
                        'properties': schema.get('properties', {})
                    }
                }
                assets.append(asset)
                
        except Exception as e:
            self.logger.error(f"Error discovering schemas: {e}")
        
        return assets
    
    def _discover_tables(self) -> List[Dict[str, Any]]:
        """Discover Databricks tables"""
        assets = []
        
        try:
            response = requests.get(
                f"{self.workspace_url}/api/2.1/unity-catalog/tables",
                headers=self.headers,
                timeout=self.connection_timeout
            )
            response.raise_for_status()
            
            tables_data = response.json()
            
            for table in tables_data.get('tables', []):
                # Get table details
                table_details = self._get_table_details(table['full_name'])
                
                asset = {
                    'name': table['full_name'],
                    'type': 'databricks_table',
                    'source': 'databricks',
                    'location': f"databricks://{self.workspace_url}/catalog/{table['catalog_name']}/schema/{table['schema_name']}/table/{table['name']}",
                    'size': table_details.get('storage_location_size', 0),
                    'created_date': datetime.now().isoformat(),
                    'modified_date': datetime.now().isoformat(),
                    'schema': {
                        'columns': table_details.get('columns', []),
                        'column_count': len(table_details.get('columns', []))
                    },
                    'tags': ['databricks', 'table', 'unity-catalog'],
                    'metadata': {
                        'workspace_url': self.workspace_url,
                        'catalog_name': table['catalog_name'],
                        'schema_name': table['schema_name'],
                        'table_name': table['name'],
                        'table_type': table.get('table_type', 'MANAGED'),
                        'data_source_format': table.get('data_source_format', 'DELTA'),
                        'comment': table.get('comment', ''),
                        'properties': table.get('properties', {}),
                        'storage_location': table.get('storage_location', ''),
                        'view_definition': table.get('view_definition', '')
                    }
                }
                assets.append(asset)
                
        except Exception as e:
            self.logger.error(f"Error discovering tables: {e}")
        
        return assets
    
    def _get_table_details(self, full_name: str) -> Dict[str, Any]:
        """Get detailed information about a specific table"""
        try:
            response = requests.get(
                f"{self.workspace_url}/api/2.1/unity-catalog/tables/{full_name}",
                headers=self.headers,
                timeout=self.connection_timeout
            )
            response.raise_for_status()
            
            table_data = response.json()
            
            # Extract column information
            columns = []
            for column in table_data.get('columns', []):
                columns.append({
                    'name': column['name'],
                    'type': column['type_name'],
                    'comment': column.get('comment', ''),
                    'nullable': column.get('nullable', True)
                })
            
            return {
                'columns': columns,
                'storage_location_size': table_data.get('storage_location_size', 0),
                'num_rows': table_data.get('num_rows', 0)
            }
            
        except Exception as e:
            self.logger.warning(f"Error getting table details for {full_name}: {e}")
            return {'columns': [], 'storage_location_size': 0, 'num_rows': 0}
    
    def _discover_notebooks(self) -> List[Dict[str, Any]]:
        """Discover Databricks notebooks"""
        assets = []
        
        try:
            response = requests.get(
                f"{self.workspace_url}/api/2.0/workspace/list",
                headers=self.headers,
                timeout=self.connection_timeout
            )
            response.raise_for_status()
            
            workspace_data = response.json()
            
            for item in workspace_data.get('objects', []):
                if item.get('object_type') == 'NOTEBOOK':
                    asset = {
                        'name': item['path'],
                        'type': 'databricks_notebook',
                        'source': 'databricks',
                        'location': f"databricks://{self.workspace_url}/notebook/{item['path']}",
                        'size': item.get('file_size', 0),
                        'created_date': datetime.now().isoformat(),
                        'modified_date': datetime.now().isoformat(),
                        'schema': {},
                        'tags': ['databricks', 'notebook', 'workspace'],
                        'metadata': {
                            'workspace_url': self.workspace_url,
                            'notebook_path': item['path'],
                            'language': item.get('language', ''),
                            'file_size': item.get('file_size', 0),
                            'object_type': item.get('object_type', '')
                        }
                    }
                    assets.append(asset)
                    
        except Exception as e:
            self.logger.error(f"Error discovering notebooks: {e}")
        
        return assets
    
    def _discover_jobs(self) -> List[Dict[str, Any]]:
        """Discover Databricks jobs"""
        assets = []
        
        try:
            response = requests.get(
                f"{self.workspace_url}/api/2.1/jobs/list",
                headers=self.headers,
                timeout=self.connection_timeout
            )
            response.raise_for_status()
            
            jobs_data = response.json()
            
            for job in jobs_data.get('jobs', []):
                asset = {
                    'name': job['settings'].get('name', f"job_{job['job_id']}"),
                    'type': 'databricks_job',
                    'source': 'databricks',
                    'location': f"databricks://{self.workspace_url}/job/{job['job_id']}",
                    'size': 0,
                    'created_date': datetime.now().isoformat(),
                    'modified_date': datetime.now().isoformat(),
                    'schema': {},
                    'tags': ['databricks', 'job', 'workflow'],
                    'metadata': {
                        'workspace_url': self.workspace_url,
                        'job_id': job['job_id'],
                        'job_name': job['settings'].get('name', ''),
                        'timeout_seconds': job['settings'].get('timeout_seconds', 0),
                        'max_concurrent_runs': job['settings'].get('max_concurrent_runs', 1),
                        'format': job['settings'].get('format', ''),
                        'tasks': len(job['settings'].get('tasks', [])),
                        'created_time': job.get('created_time', 0),
                        'creator_user_name': job.get('creator_user_name', '')
                    }
                }
                assets.append(asset)
                
        except Exception as e:
            self.logger.error(f"Error discovering jobs: {e}")
        
        return assets
    
    def test_connection(self) -> bool:
        """Test Databricks connection"""
        try:
            response = requests.get(
                f"{self.workspace_url}/api/2.0/workspace/list",
                headers=self.headers,
                timeout=10
            )
            response.raise_for_status()
            self.logger.info("Databricks connection test successful")
            return True
        except Exception as e:
            self.logger.error(f"Databricks connection test failed: {e}")
            return False
    
    def validate_config(self) -> bool:
        """Validate Databricks connector configuration"""
        if not self.workspace_url:
            self.logger.error("Workspace URL is required")
            return False
        if not self.access_token:
            self.logger.error("Access token is required")
            return False
        
        return True
    
    def get_connection_info(self) -> Dict[str, Any]:
        """Get connection information"""
        return {
            "workspace_url": self.workspace_url,
            "catalog": self.catalog,
            "schema": self.schema,
            "connection_string": f"databricks://{self.workspace_url}"
        }
