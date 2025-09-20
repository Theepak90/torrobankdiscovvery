"""
Data Warehouse Connector - Discovers data assets in major data warehouses
"""

from datetime import datetime
from typing import List, Dict, Any, Optional
import logging

from .base_connector import BaseConnector

# Import data warehouse libraries with fallbacks
try:
    import snowflake.connector
except ImportError:
    snowflake = None

try:
    from databricks import sql as databricks_sql
except ImportError:
    databricks_sql = None

try:
    import teradatasql
except ImportError:
    teradatasql = None

try:
    from google.cloud import bigquery
except ImportError:
    bigquery = None

try:
    import redshift_connector
except ImportError:
    redshift_connector = None

try:
    import clickhouse_driver
except ImportError:
    clickhouse_driver = None

try:
    from presto.dbapi import connect as presto_connect
except ImportError:
    presto_connect = None


class DataWarehouseConnector(BaseConnector):
    """
    Connector for discovering data assets in various data warehouses
    """
    
    def __init__(self, config: Dict[str, Any]):
        super().__init__(config)
        self.warehouses = config.get('warehouse_connections', [])
    
    def discover_assets(self) -> List[Dict[str, Any]]:
        """Discover data warehouse assets"""
        self.logger.info("Starting data warehouse asset discovery")
        assets = []
        
        for warehouse_config in self.warehouses:
            warehouse_type = warehouse_config.get('type', '').lower()
            
            try:
                if warehouse_type == 'snowflake':
                    assets.extend(self._discover_snowflake_assets(warehouse_config))
                elif warehouse_type == 'databricks':
                    assets.extend(self._discover_databricks_assets(warehouse_config))
                elif warehouse_type == 'teradata':
                    assets.extend(self._discover_teradata_assets(warehouse_config))
                elif warehouse_type == 'bigquery':
                    assets.extend(self._discover_bigquery_assets(warehouse_config))
                elif warehouse_type == 'redshift':
                    assets.extend(self._discover_redshift_assets(warehouse_config))
                elif warehouse_type == 'clickhouse':
                    assets.extend(self._discover_clickhouse_assets(warehouse_config))
                elif warehouse_type == 'presto':
                    assets.extend(self._discover_presto_assets(warehouse_config))
                elif warehouse_type == 'trino':
                    assets.extend(self._discover_trino_assets(warehouse_config))
                else:
                    self.logger.warning(f"Unsupported data warehouse type: {warehouse_type}")
                    
            except Exception as e:
                self.logger.error(f"Error discovering assets from {warehouse_type} warehouse: {e}")
        
        self.logger.info(f"Discovered {len(assets)} data warehouse assets")
        return assets
    
    def _discover_snowflake_assets(self, config: Dict[str, Any]) -> List[Dict[str, Any]]:
        """Discover Snowflake assets"""
        assets = []
        if not snowflake:
            self.logger.warning("snowflake-connector-python not installed")
            return assets
        
        try:
            conn = snowflake.connector.connect(
                user=config['username'],
                password=config['password'],
                account=config['account'],
                warehouse=config.get('warehouse'),
                database=config.get('database')
            )
            
            cursor = conn.cursor()
            
            # Get databases
            cursor.execute("SHOW DATABASES")
            for row in cursor:
                db_name = row[1]
                
                # Get schemas
                cursor.execute(f"SHOW SCHEMAS IN DATABASE {db_name}")
                for schema_row in cursor:
                    schema_name = schema_row[1]
                    
                    # Get tables
                    cursor.execute(f"SHOW TABLES IN SCHEMA {db_name}.{schema_name}")
                    for table_row in cursor:
                        table_name = table_row[1]
                        
                        asset = {
                            'name': f"{db_name}.{schema_name}.{table_name}",
                            'type': 'snowflake_table',
                            'source': 'snowflake',
                            'location': f"snowflake://{config['account']}/{db_name}/{schema_name}/{table_name}",
                            'created_date': datetime.now(),
                            'size': 0,
                            'metadata': {
                                'warehouse_type': 'snowflake',
                                'database': db_name,
                                'schema': schema_name,
                                'account': config['account']
                            }
                        }
                        assets.append(asset)
            
            conn.close()
            
        except Exception as e:
            self.logger.error(f"Error connecting to Snowflake: {e}")
        
        return assets
    
    def _discover_databricks_assets(self, config: Dict[str, Any]) -> List[Dict[str, Any]]:
        """Discover Databricks assets"""
        assets = []
        if not databricks_sql:
            self.logger.warning("databricks-sql-connector not installed")
            return assets
        
        try:
            connection = databricks_sql.connect(
                server_hostname=config['server_hostname'],
                http_path=config['http_path'],
                access_token=config['access_token']
            )
            
            cursor = connection.cursor()
            
            # Get databases
            cursor.execute("SHOW DATABASES")
            for row in cursor:
                db_name = row[0]
                
                # Get tables
                cursor.execute(f"SHOW TABLES IN {db_name}")
                for table_row in cursor:
                    table_name = table_row[1]
                    
                    asset = {
                        'name': f"{db_name}.{table_name}",
                        'type': 'databricks_table',
                        'source': 'databricks',
                        'location': f"databricks://{config['server_hostname']}/{db_name}/{table_name}",
                        'created_date': datetime.now(),
                        'size': 0,
                        'metadata': {
                            'warehouse_type': 'databricks',
                            'database': db_name,
                            'server_hostname': config['server_hostname']
                        }
                    }
                    assets.append(asset)
            
            connection.close()
            
        except Exception as e:
            self.logger.error(f"Error connecting to Databricks: {e}")
        
        return assets
    
    def _discover_teradata_assets(self, config: Dict[str, Any]) -> List[Dict[str, Any]]:
        """Discover Teradata assets"""
        assets = []
        if not teradatasql:
            self.logger.warning("teradatasql not installed")
            return assets
        
        try:
            conn = teradatasql.connect(
                host=config['host'],
                user=config['username'],
                password=config['password'],
                database=config.get('database', 'DBC')
            )
            
            cursor = conn.cursor()
            
            # Get all tables
            cursor.execute("""
                SELECT DatabaseName, TableName, TableKind 
                FROM DBC.TablesV 
                WHERE TableKind IN ('T', 'V') 
                AND DatabaseName NOT IN ('DBC', 'INFORMATION_SCHEMA', 'DEFINITION_SCHEMA')
            """)
            
            for row in cursor:
                db_name, table_name, table_kind = row
                
                asset = {
                    'name': f"{db_name}.{table_name}",
                    'type': 'teradata_table' if table_kind == 'T' else 'teradata_view',
                    'source': 'teradata',
                    'location': f"teradata://{config['host']}/{db_name}/{table_name}",
                    'created_date': datetime.now(),
                    'size': 0,
                    'metadata': {
                        'warehouse_type': 'teradata',
                        'database': db_name,
                        'table_kind': table_kind,
                        'host': config['host']
                    }
                }
                assets.append(asset)
            
            conn.close()
            
        except Exception as e:
            self.logger.error(f"Error connecting to Teradata: {e}")
        
        return assets
    
    def _discover_bigquery_assets(self, config: Dict[str, Any]) -> List[Dict[str, Any]]:
        """Discover BigQuery assets"""
        assets = []
        if not bigquery:
            self.logger.warning("google-cloud-bigquery not installed")
            return assets
        
        try:
            client = bigquery.Client(project=config['project_id'])
            
            # Get all datasets
            datasets = list(client.list_datasets())
            
            for dataset in datasets:
                # Get tables in dataset
                tables = list(client.list_tables(dataset))
                
                for table in tables:
                    asset = {
                        'name': f"{dataset.dataset_id}.{table.table_id}",
                        'type': 'bigquery_table',
                        'source': 'bigquery',
                        'location': f"bigquery://{config['project_id']}/{dataset.dataset_id}/{table.table_id}",
                        'created_date': table.created,
                        'size': table.num_bytes or 0,
                        'metadata': {
                            'warehouse_type': 'bigquery',
                            'dataset': dataset.dataset_id,
                            'project_id': config['project_id'],
                            'num_rows': table.num_rows or 0,
                            'table_type': table.table_type
                        }
                    }
                    assets.append(asset)
            
        except Exception as e:
            self.logger.error(f"Error connecting to BigQuery: {e}")
        
        return assets
    
    def _discover_redshift_assets(self, config: Dict[str, Any]) -> List[Dict[str, Any]]:
        """Discover Amazon Redshift assets"""
        assets = []
        
        try:
            import psycopg2
            
            conn = psycopg2.connect(
                host=config['host'],
                port=config.get('port', 5439),
                database=config['database'],
                user=config['username'],
                password=config['password']
            )
            
            cursor = conn.cursor()
            
            # Get all tables
            cursor.execute("""
                SELECT schemaname, tablename, tableowner
                FROM pg_tables 
                WHERE schemaname NOT IN ('information_schema', 'pg_catalog')
            """)
            
            for row in cursor:
                schema_name, table_name, table_owner = row
                
                asset = {
                    'name': f"{schema_name}.{table_name}",
                    'type': 'redshift_table',
                    'source': 'redshift',
                    'location': f"redshift://{config['host']}/{config['database']}/{schema_name}/{table_name}",
                    'created_date': datetime.now(),
                    'size': 0,
                    'metadata': {
                        'warehouse_type': 'redshift',
                        'schema': schema_name,
                        'database': config['database'],
                        'owner': table_owner,
                        'host': config['host']
                    }
                }
                assets.append(asset)
            
            conn.close()
            
        except Exception as e:
            self.logger.error(f"Error connecting to Redshift: {e}")
        
        return assets
    
    def _discover_clickhouse_assets(self, config: Dict[str, Any]) -> List[Dict[str, Any]]:
        """Discover ClickHouse assets"""
        assets = []
        if not clickhouse_driver:
            self.logger.warning("clickhouse-driver not installed")
            return assets
        
        try:
            client = clickhouse_driver.Client(
                host=config['host'],
                port=config.get('port', 9000),
                user=config.get('username', 'default'),
                password=config.get('password', ''),
                database=config.get('database', 'default')
            )
            
            # Get all tables
            result = client.execute("SHOW TABLES")
            
            for row in result:
                table_name = row[0]
                
                asset = {
                    'name': table_name,
                    'type': 'clickhouse_table',
                    'source': 'clickhouse',
                    'location': f"clickhouse://{config['host']}/{config.get('database', 'default')}/{table_name}",
                    'created_date': datetime.now(),
                    'size': 0,
                    'metadata': {
                        'warehouse_type': 'clickhouse',
                        'database': config.get('database', 'default'),
                        'host': config['host']
                    }
                }
                assets.append(asset)
            
        except Exception as e:
            self.logger.error(f"Error connecting to ClickHouse: {e}")
        
        return assets
    
    def _discover_presto_assets(self, config: Dict[str, Any]) -> List[Dict[str, Any]]:
        """Discover Presto assets"""
        assets = []
        if not presto_connect:
            self.logger.warning("presto-python-client not installed")
            return assets
        
        try:
            conn = presto_connect(
                host=config['host'],
                port=config.get('port', 8080),
                user=config.get('username', 'presto'),
                catalog=config.get('catalog', 'hive')
            )
            
            cursor = conn.cursor()
            
            # Get schemas
            cursor.execute("SHOW SCHEMAS")
            schemas = [row[0] for row in cursor.fetchall()]
            
            for schema in schemas:
                if schema in ['information_schema', 'sys']:
                    continue
                
                # Get tables in schema
                cursor.execute(f"SHOW TABLES FROM {schema}")
                tables = [row[0] for row in cursor.fetchall()]
                
                for table in tables:
                    asset = {
                        'name': f"{schema}.{table}",
                        'type': 'presto_table',
                        'source': 'presto',
                        'location': f"presto://{config['host']}/{config.get('catalog', 'hive')}/{schema}/{table}",
                        'created_date': datetime.now(),
                        'size': 0,
                        'metadata': {
                            'warehouse_type': 'presto',
                            'catalog': config.get('catalog', 'hive'),
                            'schema': schema,
                            'host': config['host']
                        }
                    }
                    assets.append(asset)
            
        except Exception as e:
            self.logger.error(f"Error connecting to Presto: {e}")
        
        return assets
    
    def _discover_trino_assets(self, config: Dict[str, Any]) -> List[Dict[str, Any]]:
        """Discover Trino assets (same as Presto with different client)"""
        # Trino uses similar protocol to Presto
        return self._discover_presto_assets(config)
    
    def test_connection(self) -> bool:
        """Test data warehouse connections"""
        try:
            if not self.warehouses:
                self.logger.error("No data warehouse connections configured")
                return False
            
            connection_tested = False
            
            for warehouse_config in self.warehouses:
                warehouse_type = warehouse_config.get('type', '').lower()
                
                try:
                    if warehouse_type == 'snowflake':
                        if snowflake:
                            conn = snowflake.connector.connect(
                                user=warehouse_config.get('user'),
                                password=warehouse_config.get('password'),
                                account=warehouse_config.get('account'),
                                warehouse=warehouse_config.get('warehouse'),
                                database=warehouse_config.get('database', 'SNOWFLAKE'),
                                schema=warehouse_config.get('schema', 'INFORMATION_SCHEMA')
                            )
                            # Test with simple query
                            cur = conn.cursor()
                            cur.execute("SELECT CURRENT_VERSION()")
                            cur.fetchone()
                            cur.close()
                            conn.close()
                            self.logger.info("Snowflake connection test successful")
                            connection_tested = True
                        else:
                            self.logger.warning("Snowflake library not available")
                    
                    elif warehouse_type == 'databricks':
                        if databricks_sql:
                            conn = databricks_sql.connect(
                                server_hostname=warehouse_config.get('server_hostname'),
                                http_path=warehouse_config.get('http_path'),
                                access_token=warehouse_config.get('access_token')
                            )
                            # Test with simple query
                            cursor = conn.cursor()
                            cursor.execute("SELECT 1")
                            cursor.fetchone()
                            cursor.close()
                            conn.close()
                            self.logger.info("Databricks connection test successful")
                            connection_tested = True
                        else:
                            self.logger.warning("Databricks library not available")
                    
                    elif warehouse_type == 'bigquery':
                        if bigquery:
                            client = bigquery.Client(
                                project=warehouse_config.get('project_id')
                            )
                            # Test by listing datasets
                            list(client.list_datasets(max_results=1))
                            self.logger.info("BigQuery connection test successful")
                            connection_tested = True
                        else:
                            self.logger.warning("BigQuery library not available")
                    
                    elif warehouse_type == 'redshift':
                        if redshift_connector:
                            conn = redshift_connector.connect(
                                host=warehouse_config.get('host'),
                                port=warehouse_config.get('port', 5439),
                                database=warehouse_config.get('database'),
                                user=warehouse_config.get('user'),
                                password=warehouse_config.get('password')
                            )
                            # Test with simple query
                            cur = conn.cursor()
                            cur.execute("SELECT VERSION()")
                            cur.fetchone()
                            cur.close()
                            conn.close()
                            self.logger.info("Redshift connection test successful")
                            connection_tested = True
                        else:
                            self.logger.warning("Redshift library not available")
                    
                    elif warehouse_type == 'clickhouse':
                        if clickhouse_driver:
                            client = clickhouse_driver.Client(
                                host=warehouse_config.get('host', 'localhost'),
                                port=warehouse_config.get('port', 9000),
                                user=warehouse_config.get('user', 'default'),
                                password=warehouse_config.get('password', ''),
                                database=warehouse_config.get('database', 'default')
                            )
                            # Test with simple query
                            result = client.execute("SELECT version()")
                            self.logger.info("ClickHouse connection test successful")
                            connection_tested = True
                        else:
                            self.logger.warning("ClickHouse library not available")
                    
                except Exception as e:
                    self.logger.warning(f"{warehouse_type.capitalize()} connection test failed: {e}")
            
            if connection_tested:
                self.logger.info("Data warehouse connection test successful")
                return True
            else:
                self.logger.error("No data warehouse connections could be tested")
                return False
                
        except Exception as e:
            self.logger.error(f"Data warehouse connection test failed: {e}")
            return False
    
    def validate_config(self) -> bool:
        """Validate data warehouse connector configuration"""
        if not self.warehouses:
            self.logger.error("No data warehouse connections configured")
            return False
        
        for warehouse_config in self.warehouses:
            if not warehouse_config.get('type'):
                self.logger.error("Data warehouse type not specified in configuration")
                return False
        
        return True

