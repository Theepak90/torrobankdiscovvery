"""
Snowflake Connector - Discovers data assets in Snowflake data warehouses
"""

import snowflake.connector
from datetime import datetime
from typing import List, Dict, Any, Optional

from .base_connector import BaseConnector


class SnowflakeConnector(BaseConnector):
    """
    Connector for discovering data assets in Snowflake data warehouses
    """
    
    # Metadata for dynamic discovery
    connector_type = "snowflake"
    connector_name = "Snowflake"
    description = "Discover data assets from Snowflake data warehouse including databases, schemas, tables, and views"
    category = "data_warehouses"
    supported_services = ["Snowflake", "Databases", "Schemas", "Tables", "Views", "Stages"]
    required_config_fields = ["account", "username", "password"]
    optional_config_fields = ["warehouse", "database", "schema", "role", "connection_timeout"]
    
    def __init__(self, config: Dict[str, Any]):
        super().__init__(config)
        self.account = config.get('account', '')
        self.username = config.get('username', '')
        self.password = config.get('password', '')
        self.warehouse = config.get('warehouse', '')
        self.database = config.get('database', '')
        self.schema = config.get('schema', 'PUBLIC')
        self.role = config.get('role', '')
        self.connection_timeout = config.get('connection_timeout', 30)
    
    def discover_assets(self) -> List[Dict[str, Any]]:
        """Discover Snowflake assets"""
        self.logger.info("Starting Snowflake asset discovery")
        assets = []
        
        try:
            with self._get_connection() as conn:
                cursor = conn.cursor()
                
                # Discover databases
                databases = self._discover_databases(cursor)
                assets.extend(databases)
                
                # Discover schemas
                schemas = self._discover_schemas(cursor)
                assets.extend(schemas)
                
                # Discover tables
                tables = self._discover_tables(cursor)
                assets.extend(tables)
                
                # Discover views
                views = self._discover_views(cursor)
                assets.extend(views)
                
                # Discover stages
                stages = self._discover_stages(cursor)
                assets.extend(stages)
                
        except Exception as e:
            self.logger.error(f"Error discovering Snowflake assets: {e}")
        
        self.logger.info(f"Discovered {len(assets)} Snowflake assets")
        return assets
    
    def _get_connection(self):
        """Get Snowflake connection"""
        conn_params = {
            'user': self.username,
            'password': self.password,
            'account': self.account,
            'warehouse': self.warehouse,
            'database': self.database,
            'schema': self.schema,
            'timeout': self.connection_timeout
        }
        
        if self.role:
            conn_params['role'] = self.role
        
        return snowflake.connector.connect(**conn_params)
    
    def _discover_databases(self, cursor) -> List[Dict[str, Any]]:
        """Discover Snowflake databases"""
        assets = []
        
        try:
            cursor.execute("SHOW DATABASES")
            databases = cursor.fetchall()
            
            for db in databases:
                db_name = db[1]  # Database name is in second column
                
                # Skip system databases
                if db_name.upper() in ['SNOWFLAKE', 'SNOWFLAKE_SAMPLE_DATA']:
                    continue
                
                asset = {
                    'name': db_name,
                    'type': 'snowflake_database',
                    'source': 'snowflake',
                    'location': f"snowflake://{self.account}/{db_name}",
                    'size': 0,
                    'created_date': datetime.now().isoformat(),
                    'modified_date': datetime.now().isoformat(),
                    'schema': {},
                    'tags': ['snowflake', 'database'],
                    'metadata': {
                        'account': self.account,
                        'database_name': db_name,
                        'comment': db[2] if len(db) > 2 else '',
                        'created_on': db[3] if len(db) > 3 else '',
                        'owner': db[4] if len(db) > 4 else ''
                    }
                }
                assets.append(asset)
                
        except Exception as e:
            self.logger.error(f"Error discovering databases: {e}")
        
        return assets
    
    def _discover_schemas(self, cursor) -> List[Dict[str, Any]]:
        """Discover Snowflake schemas"""
        assets = []
        
        try:
            cursor.execute("SHOW SCHEMAS")
            schemas = cursor.fetchall()
            
            for schema in schemas:
                schema_name = schema[1]  # Schema name is in second column
                database_name = schema[2] if len(schema) > 2 else self.database
                
                asset = {
                    'name': f"{database_name}.{schema_name}",
                    'type': 'snowflake_schema',
                    'source': 'snowflake',
                    'location': f"snowflake://{self.account}/{database_name}/{schema_name}",
                    'size': 0,
                    'created_date': datetime.now().isoformat(),
                    'modified_date': datetime.now().isoformat(),
                    'schema': {},
                    'tags': ['snowflake', 'schema'],
                    'metadata': {
                        'account': self.account,
                        'database_name': database_name,
                        'schema_name': schema_name,
                        'comment': schema[3] if len(schema) > 3 else '',
                        'created_on': schema[4] if len(schema) > 4 else '',
                        'owner': schema[5] if len(schema) > 5 else ''
                    }
                }
                assets.append(asset)
                
        except Exception as e:
            self.logger.error(f"Error discovering schemas: {e}")
        
        return assets
    
    def _discover_tables(self, cursor) -> List[Dict[str, Any]]:
        """Discover Snowflake tables"""
        assets = []
        
        try:
            cursor.execute("SHOW TABLES")
            tables = cursor.fetchall()
            
            for table in tables:
                table_name = table[1]  # Table name is in second column
                schema_name = table[2] if len(table) > 2 else self.schema
                database_name = table[3] if len(table) > 3 else self.database
                
                # Get table details
                table_details = self._get_table_details(cursor, database_name, schema_name, table_name)
                
                asset = {
                    'name': f"{database_name}.{schema_name}.{table_name}",
                    'type': 'snowflake_table',
                    'source': 'snowflake',
                    'location': f"snowflake://{self.account}/{database_name}/{schema_name}/{table_name}",
                    'size': table_details.get('bytes', 0),
                    'created_date': datetime.now().isoformat(),
                    'modified_date': datetime.now().isoformat(),
                    'schema': {
                        'columns': table_details.get('columns', []),
                        'column_count': len(table_details.get('columns', []))
                    },
                    'tags': ['snowflake', 'table'],
                    'metadata': {
                        'account': self.account,
                        'database_name': database_name,
                        'schema_name': schema_name,
                        'table_name': table_name,
                        'comment': table[4] if len(table) > 4 else '',
                        'created_on': table[5] if len(table) > 5 else '',
                        'owner': table[6] if len(table) > 6 else '',
                        'rows': table_details.get('rows', 0),
                        'bytes': table_details.get('bytes', 0),
                        'clustering_key': table_details.get('clustering_key', ''),
                        'automatic_clustering': table_details.get('automatic_clustering', 'OFF')
                    }
                }
                assets.append(asset)
                
        except Exception as e:
            self.logger.error(f"Error discovering tables: {e}")
        
        return assets
    
    def _discover_views(self, cursor) -> List[Dict[str, Any]]:
        """Discover Snowflake views"""
        assets = []
        
        try:
            cursor.execute("SHOW VIEWS")
            views = cursor.fetchall()
            
            for view in views:
                view_name = view[1]  # View name is in second column
                schema_name = view[2] if len(view) > 2 else self.schema
                database_name = view[3] if len(view) > 3 else self.database
                
                asset = {
                    'name': f"{database_name}.{schema_name}.{view_name}",
                    'type': 'snowflake_view',
                    'source': 'snowflake',
                    'location': f"snowflake://{self.account}/{database_name}/{schema_name}/{view_name}",
                    'size': 0,
                    'created_date': datetime.now().isoformat(),
                    'modified_date': datetime.now().isoformat(),
                    'schema': {},
                    'tags': ['snowflake', 'view'],
                    'metadata': {
                        'account': self.account,
                        'database_name': database_name,
                        'schema_name': schema_name,
                        'view_name': view_name,
                        'comment': view[4] if len(view) > 4 else '',
                        'created_on': view[5] if len(view) > 5 else '',
                        'owner': view[6] if len(view) > 6 else ''
                    }
                }
                assets.append(asset)
                
        except Exception as e:
            self.logger.error(f"Error discovering views: {e}")
        
        return assets
    
    def _discover_stages(self, cursor) -> List[Dict[str, Any]]:
        """Discover Snowflake stages"""
        assets = []
        
        try:
            cursor.execute("SHOW STAGES")
            stages = cursor.fetchall()
            
            for stage in stages:
                stage_name = stage[1]  # Stage name is in second column
                schema_name = stage[2] if len(stage) > 2 else self.schema
                database_name = stage[3] if len(stage) > 3 else self.database
                
                asset = {
                    'name': f"{database_name}.{schema_name}.{stage_name}",
                    'type': 'snowflake_stage',
                    'source': 'snowflake',
                    'location': f"snowflake://{self.account}/{database_name}/{schema_name}/{stage_name}",
                    'size': 0,
                    'created_date': datetime.now().isoformat(),
                    'modified_date': datetime.now().isoformat(),
                    'schema': {},
                    'tags': ['snowflake', 'stage'],
                    'metadata': {
                        'account': self.account,
                        'database_name': database_name,
                        'schema_name': schema_name,
                        'stage_name': stage_name,
                        'comment': stage[4] if len(stage) > 4 else '',
                        'created_on': stage[5] if len(stage) > 5 else '',
                        'owner': stage[6] if len(stage) > 6 else '',
                        'url': stage[7] if len(stage) > 7 else '',
                        'has_encryption_key': stage[8] if len(stage) > 8 else 'N'
                    }
                }
                assets.append(asset)
                
        except Exception as e:
            self.logger.error(f"Error discovering stages: {e}")
        
        return assets
    
    def _get_table_details(self, cursor, database_name: str, schema_name: str, table_name: str) -> Dict[str, Any]:
        """Get detailed information about a specific table"""
        try:
            # Get table info
            cursor.execute(f"DESCRIBE TABLE {database_name}.{schema_name}.{table_name}")
            columns_info = cursor.fetchall()
            
            columns = []
            for col in columns_info:
                columns.append({
                    'name': col[0],
                    'type': col[1],
                    'kind': col[2],
                    'null': col[3],
                    'default': col[4],
                    'primary_key': col[5],
                    'unique_key': col[6],
                    'check': col[7],
                    'expression': col[8],
                    'comment': col[9]
                })
            
            # Get table stats
            cursor.execute(f"""
                SELECT 
                    ROW_COUNT,
                    BYTES,
                    CLUSTERING_KEY,
                    AUTOMATIC_CLUSTERING
                FROM {database_name}.INFORMATION_SCHEMA.TABLES 
                WHERE TABLE_SCHEMA = '{schema_name}' AND TABLE_NAME = '{table_name}'
            """)
            
            stats = cursor.fetchone()
            
            return {
                'columns': columns,
                'rows': stats[0] if stats else 0,
                'bytes': stats[1] if stats else 0,
                'clustering_key': stats[2] if stats else '',
                'automatic_clustering': stats[3] if stats else 'OFF'
            }
            
        except Exception as e:
            self.logger.warning(f"Error getting table details for {database_name}.{schema_name}.{table_name}: {e}")
            return {'columns': [], 'rows': 0, 'bytes': 0, 'clustering_key': '', 'automatic_clustering': 'OFF'}
    
    def test_connection(self) -> bool:
        """Test Snowflake connection"""
        try:
            with self._get_connection() as conn:
                cursor = conn.cursor()
                cursor.execute("SELECT 1")
                cursor.fetchone()
                self.logger.info("Snowflake connection test successful")
                return True
        except Exception as e:
            self.logger.error(f"Snowflake connection test failed: {e}")
            return False
    
    def validate_config(self) -> bool:
        """Validate Snowflake connector configuration"""
        if not self.account:
            self.logger.error("Account is required")
            return False
        if not self.username:
            self.logger.error("Username is required")
            return False
        if not self.password:
            self.logger.error("Password is required")
            return False
        
        return True
    
    def get_connection_info(self) -> Dict[str, Any]:
        """Get connection information"""
        return {
            "account": self.account,
            "username": self.username,
            "warehouse": self.warehouse,
            "database": self.database,
            "schema": self.schema,
            "role": self.role,
            "connection_string": f"snowflake://{self.account}"
        }
