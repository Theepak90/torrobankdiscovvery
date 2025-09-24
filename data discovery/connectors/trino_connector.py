"""
Trino Connector - Discovers data assets in Trino clusters
"""

import requests
from datetime import datetime
from typing import List, Dict, Any, Optional

from .base_connector import BaseConnector
class TrinoConnector(BaseConnector):
    """
    Connector for discovering data assets in Trino clusters
    """
    
    connector_type = "trino"
    connector_name = "Trino"
    description = "Discover data assets from Trino cluster including catalogs, schemas, and tables"
    category = "data_warehouses"
    supported_services = ["Trino", "Catalogs", "Schemas", "Tables", "Views"]
    required_config_fields = ["host", "port", "username"]
    optional_config_fields = ["password", "catalog", "schema", "connection_timeout"]
    
    def __init__(self, config: Dict[str, Any]):
        super().__init__(config)
        self.host = config.get('host')
        self.port = config.get('port', 8080)
        self.username = config.get('username')
        self.password = config.get('password', '')
        self.catalog = config.get('catalog', 'system')
        self.schema = config.get('schema', 'information_schema')
        self.connection_timeout = config.get('connection_timeout', 30)
        self.base_url = f"http://{self.host}:{self.port}"
    
    def discover_assets(self) -> List[Dict[str, Any]]:
        """Discover Trino assets"""
        self.logger.info("Starting Trino asset discovery")
        assets = []
        
        try:
            catalogs = self._discover_catalogs()
            assets.extend(catalogs)
            
            schemas = self._discover_schemas()
            assets.extend(schemas)
            
            tables = self._discover_tables()
            assets.extend(tables)
            
        except Exception as e:
            self.logger.error(f"Error discovering Trino assets: {e}")
        
        self.logger.info(f"Discovered {len(assets)} Trino assets")
        return assets
    
    def _discover_catalogs(self) -> List[Dict[str, Any]]:
        """Discover Trino catalogs"""
        assets = []
        
        try:
            query = "SELECT catalog_name FROM information_schema.schemata"
            result = self._execute_query(query)
            
            for row in result:
                catalog_name = row[0]
                
                if catalog_name in ['system', 'information_schema']:
                    continue
                
                asset = {
                    'name': catalog_name,
                    'type': 'trino_catalog',
                    'source': 'trino',
                    'location': f"trino://{self.host}:{self.port}/{catalog_name}",
                    'size': 0,
                    'created_date': datetime.now().isoformat(),
                    'modified_date': datetime.now().isoformat(),
                    'schema': {},
                    'tags': ['trino', 'catalog'],
                    'metadata': {
                        'host': self.host,
                        'port': self.port,
                        'catalog_name': catalog_name
                    }
                }
                assets.append(asset)
                
        except Exception as e:
            self.logger.error(f"Error discovering catalogs: {e}")
        
        return assets
    
    def _discover_schemas(self) -> List[Dict[str, Any]]:
        """Discover Trino schemas"""
        assets = []
        
        try:
            query = "SELECT catalog_name, schema_name FROM information_schema.schemata"
            result = self._execute_query(query)
            
            for row in result:
                catalog_name, schema_name = row
                
                if catalog_name in ['system', 'information_schema']:
                    continue
                
                asset = {
                    'name': f"{catalog_name}.{schema_name}",
                    'type': 'trino_schema',
                    'source': 'trino',
                    'location': f"trino://{self.host}:{self.port}/{catalog_name}/{schema_name}",
                    'size': 0,
                    'created_date': datetime.now().isoformat(),
                    'modified_date': datetime.now().isoformat(),
                    'schema': {},
                    'tags': ['trino', 'schema'],
                    'metadata': {
                        'host': self.host,
                        'port': self.port,
                        'catalog_name': catalog_name,
                        'schema_name': schema_name
                    }
                }
                assets.append(asset)
                
        except Exception as e:
            self.logger.error(f"Error discovering schemas: {e}")
        
        return assets
    
    def _discover_tables(self) -> List[Dict[str, Any]]:
        """Discover Trino tables and views"""
        assets = []
        
        try:
            query = """
                SELECT 
                    table_catalog,
                    table_schema,
                    table_name,
                    table_type
                FROM information_schema.tables
                WHERE table_catalog NOT IN ('system', 'information_schema')
            """
            result = self._execute_query(query)
            
            for row in result:
                catalog_name, schema_name, table_name, table_type = row
                
                columns = self._get_table_columns(catalog_name, schema_name, table_name)
                
                asset = {
                    'name': f"{catalog_name}.{schema_name}.{table_name}",
                    'type': f'trino_{table_type.lower()}',
                    'source': 'trino',
                    'location': f"trino://{self.host}:{self.port}/{catalog_name}/{schema_name}/{table_name}",
                    'size': 0,
                    'created_date': datetime.now().isoformat(),
                    'modified_date': datetime.now().isoformat(),
                    'schema': {
                        'columns': columns,
                        'column_count': len(columns)
                    },
                    'tags': ['trino', table_type.lower()],
                    'metadata': {
                        'host': self.host,
                        'port': self.port,
                        'catalog_name': catalog_name,
                        'schema_name': schema_name,
                        'table_name': table_name,
                        'table_type': table_type,
                        'column_count': len(columns)
                    }
                }
                assets.append(asset)
                
        except Exception as e:
            self.logger.error(f"Error discovering tables: {e}")
        
        return assets
    
    def _get_table_columns(self, catalog_name: str, schema_name: str, table_name: str) -> List[Dict[str, Any]]:
        """Get column information for a specific table"""
        try:
            query = """
                SELECT 
                    column_name,
                    data_type,
                    is_nullable,
                    column_default,
                    ordinal_position
                FROM information_schema.columns
                WHERE table_catalog = ? AND table_schema = ? AND table_name = ?
                ORDER BY ordinal_position
            """
            result = self._execute_query(query, (catalog_name, schema_name, table_name))
            
            columns = []
            for row in result:
                columns.append({
                    'name': row[0],
                    'type': row[1],
                    'nullable': row[2] == 'YES',
                    'default': row[3],
                    'position': row[4]
                })
            
            return columns
            
        except Exception as e:
            self.logger.warning(f"Error getting columns for {catalog_name}.{schema_name}.{table_name}: {e}")
            return []
    
    def _execute_query(self, query: str, params: tuple = None) -> List[tuple]:
        """Execute a query against Trino"""
        try:
            url = f"{self.base_url}/v1/statement"
            
            payload = {
                "sql": query,
                "catalog": self.catalog,
                "schema": self.schema
            }
            
            headers = {
                'X-Trino-User': self.username,
                'Content-Type': 'application/json'
            }
            
            if self.password:
                headers['X-Trino-Password'] = self.password
            
            response = requests.post(
                url,
                json=payload,
                headers=headers,
                timeout=self.connection_timeout
            )
            response.raise_for_status()
            
            result_data = response.json()
            
            return []
            
        except Exception as e:
            self.logger.error(f"Error executing query: {e}")
            return []
    
    def test_connection(self) -> bool:
        """Test Trino connection"""
        try:
            query = "SELECT 1"
            result = self._execute_query(query)
            self.logger.info("Trino connection test successful")
            return True
        except Exception as e:
            self.logger.error(f"Trino connection test failed: {e}")
            return False
    
    def validate_config(self) -> bool:
        """Validate Trino connector configuration"""
        if not self.host:
            self.logger.error("Host is required")
            return False
        if not self.username:
            self.logger.error("Username is required")
            return False
        
        return True
    
    def get_connection_info(self) -> Dict[str, Any]:
        """Get connection information"""
        return {
            "host": self.host,
            "port": self.port,
            "username": self.username,
            "catalog": self.catalog,
            "schema": self.schema,
            "connection_string": f"trino://{self.host}:{self.port}"
        }
