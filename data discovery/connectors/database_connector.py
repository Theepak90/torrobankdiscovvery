"""
Database Connector - Discovers data assets in various database systems
"""

import pymongo
import psycopg2
import mysql.connector
import sqlite3
from datetime import datetime
from typing import List, Dict, Any, Optional
from sqlalchemy import create_engine, text
from sqlalchemy.exc import SQLAlchemyError

# Additional database imports
try:
    import cx_Oracle
except ImportError:
    cx_Oracle = None

try:
    import pyodbc
except ImportError:
    pyodbc = None

try:
    from cassandra.cluster import Cluster
    from cassandra.auth import PlainTextAuthProvider
except ImportError:
    Cluster = None

try:
    from neo4j import GraphDatabase
except ImportError:
    GraphDatabase = None

try:
    import redis
except ImportError:
    redis = None

try:
    from elasticsearch import Elasticsearch
except ImportError:
    Elasticsearch = None

try:
    import snowflake.connector
except ImportError:
    snowflake = None

from .base_connector import BaseConnector


class DatabaseConnector(BaseConnector):
    """
    Connector for discovering data assets in various database systems
    """
    
    def __init__(self, config: Dict[str, Any]):
        super().__init__(config)
        self.connection_timeout = config.get('connection_timeout', 30)
        self.databases = config.get('database_connections', [])
    
    def discover_assets(self) -> List[Dict[str, Any]]:
        """Discover database assets"""
        self.logger.info("Starting database asset discovery")
        assets = []
        
        for db_config in self.databases:
            db_type = db_config.get('type', '').lower()
            
            try:
                if db_type == 'postgresql':
                    assets.extend(self._discover_postgresql_assets(db_config))
                elif db_type == 'mysql':
                    assets.extend(self._discover_mysql_assets(db_config))
                elif db_type == 'mongodb':
                    assets.extend(self._discover_mongodb_assets(db_config))
                elif db_type == 'sqlite':
                    assets.extend(self._discover_sqlite_assets(db_config))
                elif db_type == 'sqlserver':
                    assets.extend(self._discover_sqlserver_assets(db_config))
                elif db_type == 'oracle':
                    assets.extend(self._discover_oracle_assets(db_config))
                elif db_type == 'cassandra':
                    assets.extend(self._discover_cassandra_assets(db_config))
                elif db_type == 'neo4j':
                    assets.extend(self._discover_neo4j_assets(db_config))
                elif db_type == 'redis':
                    assets.extend(self._discover_redis_assets(db_config))
                elif db_type == 'elasticsearch':
                    assets.extend(self._discover_elasticsearch_assets(db_config))
                elif db_type == 'snowflake':
                    assets.extend(self._discover_snowflake_assets(db_config))
                elif db_type == 'db2':
                    assets.extend(self._discover_db2_assets(db_config))
                elif db_type == 'mariadb':
                    assets.extend(self._discover_mariadb_assets(db_config))
                elif db_type == 'couchdb':
                    assets.extend(self._discover_couchdb_assets(db_config))
                elif db_type == 'influxdb':
                    assets.extend(self._discover_influxdb_assets(db_config))
                else:
                    self.logger.warning(f"Unsupported database type: {db_type}")
                    
            except Exception as e:
                self.logger.error(f"Error discovering assets from {db_type} database: {e}")
        
        self.logger.info(f"Discovered {len(assets)} database assets")
        return assets
    
    def _discover_postgresql_assets(self, config: Dict[str, Any]) -> List[Dict[str, Any]]:
        """Discover PostgreSQL database assets"""
        assets = []
        
        try:
            connection_string = self._build_postgresql_connection_string(config)
            engine = create_engine(connection_string, connect_args={'connect_timeout': self.connection_timeout})
            
            with engine.connect() as conn:
                # Get database info
                db_name = config.get('database', 'postgres')
                
                # Get all tables and views
                query = text("""
                    SELECT 
                        schemaname, 
                        tablename, 
                        'table' as object_type
                    FROM pg_tables 
                    WHERE schemaname NOT IN ('information_schema', 'pg_catalog')
                    UNION ALL
                    SELECT 
                        schemaname, 
                        viewname, 
                        'view' as object_type
                    FROM pg_views 
                    WHERE schemaname NOT IN ('information_schema', 'pg_catalog')
                """)
                
                result = conn.execute(query)
                
                for row in result:
                    schema_name, table_name, object_type = row
                    
                    # Get table/view details
                    table_asset = self._create_postgresql_table_asset(
                        conn, config, db_name, schema_name, table_name, object_type
                    )
                    if table_asset:
                        assets.append(table_asset)
        
        except Exception as e:
            self.logger.error(f"Error discovering PostgreSQL assets: {e}")
        
        return assets
    
    def _create_postgresql_table_asset(self, conn, config: Dict, db_name: str, 
                                     schema_name: str, table_name: str, object_type: str) -> Optional[Dict[str, Any]]:
        """Create asset for PostgreSQL table/view"""
        try:
            # Get column information
            columns_query = text("""
                SELECT 
                    column_name, 
                    data_type, 
                    is_nullable,
                    column_default
                FROM information_schema.columns 
                WHERE table_schema = :schema AND table_name = :table
                ORDER BY ordinal_position
            """)
            
            columns_result = conn.execute(columns_query, {'schema': schema_name, 'table': table_name})
            columns = []
            for col_row in columns_result:
                columns.append({
                    'name': col_row[0],
                    'type': col_row[1],
                    'nullable': col_row[2] == 'YES',
                    'default': col_row[3]
                })
            
            # Get row count (for tables only)
            row_count = 0
            if object_type == 'table':
                try:
                    count_query = text(f'SELECT COUNT(*) FROM "{schema_name}"."{table_name}"')
                    count_result = conn.execute(count_query)
                    row_count = count_result.scalar()
                except:
                    row_count = 0
            
            asset = {
                'name': f"{schema_name}.{table_name}",
                'type': f'postgresql_{object_type}',
                'source': 'postgresql',
                'location': f"postgresql://{config.get('host', 'localhost')}:{config.get('port', 5432)}/{db_name}/{schema_name}/{table_name}",
                'size': row_count * len(columns) * 50 if row_count > 0 else 0,  # Rough estimate
                'created_date': datetime.now(),  # PostgreSQL doesn't store creation time by default
                'modified_date': datetime.now(),
                'schema': {
                    'columns': columns,
                    'column_count': len(columns)
                },
                'tags': ['postgresql', 'database', object_type, schema_name],
                'metadata': {
                    'database_type': 'postgresql',
                    'database_name': db_name,
                    'schema_name': schema_name,
                    'table_name': table_name,
                    'object_type': object_type,
                    'row_count': row_count,
                    'column_count': len(columns),
                    'host': config.get('host', 'localhost'),
                    'port': config.get('port', 5432)
                }
            }
            
            return asset
            
        except Exception as e:
            self.logger.warning(f"Error creating PostgreSQL asset for {schema_name}.{table_name}: {e}")
            return None
    
    def _discover_mysql_assets(self, config: Dict[str, Any]) -> List[Dict[str, Any]]:
        """Discover MySQL database assets"""
        assets = []
        
        try:
            connection_string = self._build_mysql_connection_string(config)
            engine = create_engine(connection_string, connect_args={'connect_timeout': self.connection_timeout})
            
            with engine.connect() as conn:
                db_name = config.get('database')
                
                # Get all tables and views
                query = text("""
                    SELECT 
                        TABLE_NAME, 
                        TABLE_TYPE,
                        TABLE_ROWS,
                        DATA_LENGTH
                    FROM information_schema.TABLES 
                    WHERE TABLE_SCHEMA = :db_name
                """)
                
                result = conn.execute(query, {'db_name': db_name})
                
                for row in result:
                    table_name, table_type, table_rows, data_length = row
                    object_type = 'view' if table_type == 'VIEW' else 'table'
                    
                    # Get column information
                    columns_query = text("""
                        SELECT 
                            COLUMN_NAME, 
                            DATA_TYPE, 
                            IS_NULLABLE,
                            COLUMN_DEFAULT
                        FROM information_schema.COLUMNS 
                        WHERE TABLE_SCHEMA = :db_name AND TABLE_NAME = :table_name
                        ORDER BY ORDINAL_POSITION
                    """)
                    
                    columns_result = conn.execute(columns_query, {'db_name': db_name, 'table_name': table_name})
                    columns = []
                    for col_row in columns_result:
                        columns.append({
                            'name': col_row[0],
                            'type': col_row[1],
                            'nullable': col_row[2] == 'YES',
                            'default': col_row[3]
                        })
                    
                    asset = {
                        'name': table_name,
                        'type': f'mysql_{object_type}',
                        'source': 'mysql',
                        'location': f"mysql://{config.get('host', 'localhost')}:{config.get('port', 3306)}/{db_name}/{table_name}",
                        'size': data_length or 0,
                        'created_date': datetime.now(),
                        'modified_date': datetime.now(),
                        'schema': {
                            'columns': columns,
                            'column_count': len(columns)
                        },
                        'tags': ['mysql', 'database', object_type],
                        'metadata': {
                            'database_type': 'mysql',
                            'database_name': db_name,
                            'table_name': table_name,
                            'object_type': object_type,
                            'row_count': table_rows or 0,
                            'column_count': len(columns),
                            'data_length': data_length or 0,
                            'host': config.get('host', 'localhost'),
                            'port': config.get('port', 3306)
                        }
                    }
                    
                    assets.append(asset)
        
        except Exception as e:
            self.logger.error(f"Error discovering MySQL assets: {e}")
        
        return assets
    
    def _discover_mongodb_assets(self, config: Dict[str, Any]) -> List[Dict[str, Any]]:
        """Discover MongoDB database assets"""
        assets = []
        
        try:
            connection_string = self._build_mongodb_connection_string(config)
            client = pymongo.MongoClient(
                connection_string, 
                serverSelectionTimeoutMS=self.connection_timeout * 1000
            )
            
            # Get database names
            database_names = config.get('databases', client.list_database_names())
            
            for db_name in database_names:
                if db_name in ['admin', 'local', 'config']:  # Skip system databases
                    continue
                
                db = client[db_name]
                collection_names = db.list_collection_names()
                
                for collection_name in collection_names:
                    collection = db[collection_name]
                    
                    # Get collection stats
                    try:
                        stats = db.command("collStats", collection_name)
                        doc_count = stats.get('count', 0)
                        size = stats.get('size', 0)
                    except:
                        doc_count = collection.estimated_document_count()
                        size = 0
                    
                    # Sample document to infer schema
                    schema_info = {}
                    try:
                        sample_doc = collection.find_one()
                        if sample_doc:
                            schema_info = self._infer_mongodb_schema(sample_doc)
                    except:
                        pass
                    
                    asset = {
                        'name': f"{db_name}.{collection_name}",
                        'type': 'mongodb_collection',
                        'source': 'mongodb',
                        'location': f"mongodb://{config.get('host', 'localhost')}:{config.get('port', 27017)}/{db_name}/{collection_name}",
                        'size': size,
                        'created_date': datetime.now(),
                        'modified_date': datetime.now(),
                        'schema': schema_info,
                        'tags': ['mongodb', 'nosql', 'collection'],
                        'metadata': {
                            'database_type': 'mongodb',
                            'database_name': db_name,
                            'collection_name': collection_name,
                            'document_count': doc_count,
                            'size_bytes': size,
                            'host': config.get('host', 'localhost'),
                            'port': config.get('port', 27017)
                        }
                    }
                    
                    assets.append(asset)
            
            client.close()
        
        except Exception as e:
            self.logger.error(f"Error discovering MongoDB assets: {e}")
        
        return assets
    
    def _discover_sqlite_assets(self, config: Dict[str, Any]) -> List[Dict[str, Any]]:
        """Discover SQLite database assets"""
        assets = []
        
        try:
            db_path = config.get('path')
            conn = sqlite3.connect(db_path, timeout=self.connection_timeout)
            cursor = conn.cursor()
            
            # Get all tables
            cursor.execute("SELECT name FROM sqlite_master WHERE type='table' AND name NOT LIKE 'sqlite_%'")
            tables = cursor.fetchall()
            
            for (table_name,) in tables:
                # Get column information
                cursor.execute(f"PRAGMA table_info({table_name})")
                columns_info = cursor.fetchall()
                
                columns = []
                for col_info in columns_info:
                    columns.append({
                        'name': col_info[1],
                        'type': col_info[2],
                        'nullable': not col_info[3],
                        'default': col_info[4],
                        'primary_key': bool(col_info[5])
                    })
                
                # Get row count
                cursor.execute(f"SELECT COUNT(*) FROM {table_name}")
                row_count = cursor.fetchone()[0]
                
                asset = {
                    'name': table_name,
                    'type': 'sqlite_table',
                    'source': 'sqlite',
                    'location': f"sqlite:///{db_path}#{table_name}",
                    'size': row_count * len(columns) * 50,  # Rough estimate
                    'created_date': datetime.now(),
                    'modified_date': datetime.now(),
                    'schema': {
                        'columns': columns,
                        'column_count': len(columns)
                    },
                    'tags': ['sqlite', 'database', 'table'],
                    'metadata': {
                        'database_type': 'sqlite',
                        'database_path': db_path,
                        'table_name': table_name,
                        'row_count': row_count,
                        'column_count': len(columns)
                    }
                }
                
                assets.append(asset)
            
            conn.close()
        
        except Exception as e:
            self.logger.error(f"Error discovering SQLite assets: {e}")
        
        return assets
    
    def _discover_sqlserver_assets(self, config: Dict[str, Any]) -> List[Dict[str, Any]]:
        """Discover SQL Server database assets"""
        assets = []
        
        try:
            connection_string = self._build_sqlserver_connection_string(config)
            engine = create_engine(connection_string, connect_args={'timeout': self.connection_timeout})
            
            with engine.connect() as conn:
                db_name = config.get('database')
                
                # Get all tables and views
                query = text("""
                    SELECT 
                        TABLE_SCHEMA,
                        TABLE_NAME, 
                        TABLE_TYPE
                    FROM INFORMATION_SCHEMA.TABLES 
                    WHERE TABLE_CATALOG = :db_name
                """)
                
                result = conn.execute(query, {'db_name': db_name})
                
                for row in result:
                    schema_name, table_name, table_type = row
                    object_type = 'view' if table_type == 'VIEW' else 'table'
                    
                    # Get column information
                    columns_query = text("""
                        SELECT 
                            COLUMN_NAME, 
                            DATA_TYPE, 
                            IS_NULLABLE,
                            COLUMN_DEFAULT
                        FROM INFORMATION_SCHEMA.COLUMNS 
                        WHERE TABLE_CATALOG = :db_name 
                        AND TABLE_SCHEMA = :schema_name 
                        AND TABLE_NAME = :table_name
                        ORDER BY ORDINAL_POSITION
                    """)
                    
                    columns_result = conn.execute(columns_query, {
                        'db_name': db_name, 
                        'schema_name': schema_name, 
                        'table_name': table_name
                    })
                    
                    columns = []
                    for col_row in columns_result:
                        columns.append({
                            'name': col_row[0],
                            'type': col_row[1],
                            'nullable': col_row[2] == 'YES',
                            'default': col_row[3]
                        })
                    
                    # Get row count for tables
                    row_count = 0
                    if object_type == 'table':
                        try:
                            count_query = text(f'SELECT COUNT(*) FROM [{schema_name}].[{table_name}]')
                            count_result = conn.execute(count_query)
                            row_count = count_result.scalar()
                        except:
                            row_count = 0
                    
                    asset = {
                        'name': f"{schema_name}.{table_name}",
                        'type': f'sqlserver_{object_type}',
                        'source': 'sqlserver',
                        'location': f"sqlserver://{config.get('host', 'localhost')}:{config.get('port', 1433)}/{db_name}/{schema_name}/{table_name}",
                        'size': row_count * len(columns) * 50 if row_count > 0 else 0,
                        'created_date': datetime.now(),
                        'modified_date': datetime.now(),
                        'schema': {
                            'columns': columns,
                            'column_count': len(columns)
                        },
                        'tags': ['sqlserver', 'database', object_type, schema_name],
                        'metadata': {
                            'database_type': 'sqlserver',
                            'database_name': db_name,
                            'schema_name': schema_name,
                            'table_name': table_name,
                            'object_type': object_type,
                            'row_count': row_count,
                            'column_count': len(columns),
                            'host': config.get('host', 'localhost'),
                            'port': config.get('port', 1433)
                        }
                    }
                    
                    assets.append(asset)
        
        except Exception as e:
            self.logger.error(f"Error discovering SQL Server assets: {e}")
        
        return assets
    
    def _build_postgresql_connection_string(self, config: Dict[str, Any]) -> str:
        """Build PostgreSQL connection string"""
        host = config.get('host', 'localhost')
        port = config.get('port', 5432)
        database = config.get('database', 'postgres')
        username = config.get('username', 'postgres')
        password = config.get('password', '')
        
        return f"postgresql://{username}:{password}@{host}:{port}/{database}"
    
    def _build_mysql_connection_string(self, config: Dict[str, Any]) -> str:
        """Build MySQL connection string"""
        host = config.get('host', 'localhost')
        port = config.get('port', 3306)
        database = config.get('database')
        username = config.get('username')
        password = config.get('password', '')
        
        return f"mysql+pymysql://{username}:{password}@{host}:{port}/{database}"
    
    def _build_mongodb_connection_string(self, config: Dict[str, Any]) -> str:
        """Build MongoDB connection string"""
        host = config.get('host', 'localhost')
        port = config.get('port', 27017)
        username = config.get('username', '')
        password = config.get('password', '')
        
        if username and password:
            return f"mongodb://{username}:{password}@{host}:{port}/"
        else:
            return f"mongodb://{host}:{port}/"
    
    def _build_sqlserver_connection_string(self, config: Dict[str, Any]) -> str:
        """Build SQL Server connection string"""
        host = config.get('host', 'localhost')
        port = config.get('port', 1433)
        database = config.get('database')
        username = config.get('username')
        password = config.get('password', '')
        
        return f"mssql+pyodbc://{username}:{password}@{host}:{port}/{database}?driver=ODBC+Driver+17+for+SQL+Server"
    
    def _infer_mongodb_schema(self, document: Dict) -> Dict[str, Any]:
        """Infer schema from MongoDB document"""
        schema = {'fields': []}
        
        def get_type_name(value):
            if isinstance(value, str):
                return 'string'
            elif isinstance(value, int):
                return 'integer'
            elif isinstance(value, float):
                return 'float'
            elif isinstance(value, bool):
                return 'boolean'
            elif isinstance(value, list):
                return 'array'
            elif isinstance(value, dict):
                return 'object'
            else:
                return 'unknown'
        
        for key, value in document.items():
            schema['fields'].append({
                'name': key,
                'type': get_type_name(value),
                'sample_value': str(value)[:100] if not isinstance(value, (dict, list)) else None
            })
        
        return schema
    
    def test_connection(self) -> bool:
        """Test database connections"""
        all_connections_valid = True
        
        for db_config in self.databases:
            db_type = db_config.get('type', '').lower()
            
            try:
                if db_type == 'postgresql':
                    connection_string = self._build_postgresql_connection_string(db_config)
                    engine = create_engine(connection_string, connect_args={'connect_timeout': 5})
                    with engine.connect():
                        pass
                elif db_type == 'mysql':
                    connection_string = self._build_mysql_connection_string(db_config)
                    engine = create_engine(connection_string, connect_args={'connect_timeout': 5})
                    with engine.connect():
                        pass
                elif db_type == 'mongodb':
                    connection_string = self._build_mongodb_connection_string(db_config)
                    client = pymongo.MongoClient(connection_string, serverSelectionTimeoutMS=5000)
                    client.server_info()
                    client.close()
                elif db_type == 'sqlite':
                    db_path = db_config.get('path')
                    conn = sqlite3.connect(db_path, timeout=5)
                    conn.close()
                elif db_type == 'sqlserver':
                    connection_string = self._build_sqlserver_connection_string(db_config)
                    engine = create_engine(connection_string, connect_args={'timeout': 5})
                    with engine.connect():
                        pass
                
                self.logger.info(f"Connection test successful for {db_type}")
                
            except Exception as e:
                self.logger.error(f"Connection test failed for {db_type}: {e}")
                all_connections_valid = False
        
        return all_connections_valid
    
    def validate_config(self) -> bool:
        """Validate database connector configuration"""
        if not self.databases:
            self.logger.error("No database connections configured")
            return False
        
        for db_config in self.databases:
            if not db_config.get('type'):
                self.logger.error("Database type not specified in configuration")
                return False
        
        return True
    
    def _discover_oracle_assets(self, db_config: Dict[str, Any]) -> List[Dict[str, Any]]:
        """Discover Oracle database assets"""
        assets = []
        if not cx_Oracle:
            self.logger.warning("cx_Oracle library not installed. Install with: pip install cx_Oracle")
            return assets
        
        try:
            dsn = cx_Oracle.makedsn(
                db_config['host'],
                db_config.get('port', 1521),
                service_name=db_config.get('service_name', db_config.get('database', ''))
            )
            
            with cx_Oracle.connect(
                db_config['username'],
                db_config['password'],
                dsn
            ) as connection:
                cursor = connection.cursor()
                
                # Get all tables
                cursor.execute("""
                    SELECT table_name, tablespace_name, num_rows 
                    FROM user_tables 
                    ORDER BY table_name
                """)
                
                for table_name, tablespace, num_rows in cursor:
                    asset = {
                        'name': table_name,
                        'type': 'oracle_table',
                        'source': 'oracle_database',
                        'location': f"oracle://{db_config['host']}:{db_config.get('port', 1521)}/{db_config.get('service_name', '')}/{table_name}",
                        'created_date': datetime.now(),
                        'size': 0,
                        'metadata': {
                            'database_type': 'oracle',
                            'tablespace': tablespace,
                            'row_count': num_rows or 0,
                            'host': db_config['host'],
                            'port': db_config.get('port', 1521)
                        }
                    }
                    assets.append(asset)
                    
        except Exception as e:
            self.logger.error(f"Error connecting to Oracle database: {e}")
        
        return assets
    
    def _discover_cassandra_assets(self, db_config: Dict[str, Any]) -> List[Dict[str, Any]]:
        """Discover Cassandra database assets"""
        assets = []
        if not Cluster:
            self.logger.warning("cassandra-driver library not installed. Install with: pip install cassandra-driver")
            return assets
        
        try:
            auth_provider = None
            if db_config.get('username') and db_config.get('password'):
                auth_provider = PlainTextAuthProvider(
                    username=db_config['username'],
                    password=db_config['password']
                )
            
            cluster = Cluster(
                [db_config['host']],
                port=db_config.get('port', 9042),
                auth_provider=auth_provider
            )
            
            session = cluster.connect()
            
            # Get all keyspaces
            keyspaces = session.execute("SELECT keyspace_name FROM system_schema.keyspaces")
            
            for keyspace_row in keyspaces:
                keyspace_name = keyspace_row.keyspace_name
                
                # Skip system keyspaces
                if keyspace_name.startswith('system'):
                    continue
                
                # Get tables in keyspace
                tables = session.execute(f"SELECT table_name FROM system_schema.tables WHERE keyspace_name = '{keyspace_name}'")
                
                for table_row in tables:
                    table_name = table_row.table_name
                    
                    asset = {
                        'name': f"{keyspace_name}.{table_name}",
                        'type': 'cassandra_table',
                        'source': 'cassandra_database',
                        'location': f"cassandra://{db_config['host']}:{db_config.get('port', 9042)}/{keyspace_name}/{table_name}",
                        'created_date': datetime.now(),
                        'size': 0,
                        'metadata': {
                            'database_type': 'cassandra',
                            'keyspace': keyspace_name,
                            'host': db_config['host'],
                            'port': db_config.get('port', 9042)
                        }
                    }
                    assets.append(asset)
            
            cluster.shutdown()
            
        except Exception as e:
            self.logger.error(f"Error connecting to Cassandra database: {e}")
        
        return assets
    
    def _discover_neo4j_assets(self, db_config: Dict[str, Any]) -> List[Dict[str, Any]]:
        """Discover Neo4j database assets"""
        assets = []
        if not GraphDatabase:
            self.logger.warning("neo4j library not installed. Install with: pip install neo4j")
            return assets
        
        try:
            uri = f"bolt://{db_config['host']}:{db_config.get('port', 7687)}"
            driver = GraphDatabase.driver(
                uri,
                auth=(db_config.get('username', 'neo4j'), db_config.get('password', ''))
            )
            
            with driver.session() as session:
                # Get node labels
                result = session.run("CALL db.labels()")
                labels = [record["label"] for record in result]
                
                for label in labels:
                    # Count nodes with this label
                    count_result = session.run(f"MATCH (n:{label}) RETURN count(n) as count")
                    count = count_result.single()["count"]
                    
                    asset = {
                        'name': label,
                        'type': 'neo4j_label',
                        'source': 'neo4j_database',
                        'location': f"neo4j://{db_config['host']}:{db_config.get('port', 7687)}/label/{label}",
                        'created_date': datetime.now(),
                        'size': 0,
                        'metadata': {
                            'database_type': 'neo4j',
                            'node_count': count,
                            'host': db_config['host'],
                            'port': db_config.get('port', 7687)
                        }
                    }
                    assets.append(asset)
            
            driver.close()
            
        except Exception as e:
            self.logger.error(f"Error connecting to Neo4j database: {e}")
        
        return assets
    
    def _discover_redis_assets(self, db_config: Dict[str, Any]) -> List[Dict[str, Any]]:
        """Discover Redis database assets"""
        assets = []
        if not redis:
            self.logger.warning("redis library not installed. Install with: pip install redis")
            return assets
        
        try:
            r = redis.Redis(
                host=db_config['host'],
                port=db_config.get('port', 6379),
                password=db_config.get('password'),
                db=db_config.get('db', 0)
            )
            
            # Get database info
            info = r.info()
            
            # Get key patterns
            keys = r.keys('*')
            key_patterns = {}
            
            for key in keys[:100]:  # Limit to first 100 keys
                key_str = key.decode('utf-8') if isinstance(key, bytes) else str(key)
                pattern = key_str.split(':')[0] if ':' in key_str else 'misc'
                
                if pattern not in key_patterns:
                    key_patterns[pattern] = 0
                key_patterns[pattern] += 1
            
            for pattern, count in key_patterns.items():
                asset = {
                    'name': pattern,
                    'type': 'redis_pattern',
                    'source': 'redis_database',
                    'location': f"redis://{db_config['host']}:{db_config.get('port', 6379)}/{db_config.get('db', 0)}/{pattern}",
                    'created_date': datetime.now(),
                    'size': 0,
                    'metadata': {
                        'database_type': 'redis',
                        'key_count': count,
                        'host': db_config['host'],
                        'port': db_config.get('port', 6379),
                        'db_number': db_config.get('db', 0)
                    }
                }
                assets.append(asset)
            
        except Exception as e:
            self.logger.error(f"Error connecting to Redis database: {e}")
        
        return assets
    
    def _discover_elasticsearch_assets(self, db_config: Dict[str, Any]) -> List[Dict[str, Any]]:
        """Discover Elasticsearch assets"""
        assets = []
        if not Elasticsearch:
            self.logger.warning("elasticsearch library not installed. Install with: pip install elasticsearch")
            return assets
        
        try:
            es = Elasticsearch(
                [{'host': db_config['host'], 'port': db_config.get('port', 9200)}],
                http_auth=(db_config.get('username'), db_config.get('password')) if db_config.get('username') else None
            )
            
            # Get all indices
            indices = es.indices.get_alias("*")
            
            for index_name, index_info in indices.items():
                if index_name.startswith('.'):  # Skip system indices
                    continue
                
                # Get index stats
                stats = es.indices.stats(index=index_name)
                doc_count = stats['indices'][index_name]['total']['docs']['count']
                store_size = stats['indices'][index_name]['total']['store']['size_in_bytes']
                
                asset = {
                    'name': index_name,
                    'type': 'elasticsearch_index',
                    'source': 'elasticsearch',
                    'location': f"elasticsearch://{db_config['host']}:{db_config.get('port', 9200)}/{index_name}",
                    'created_date': datetime.now(),
                    'size': store_size,
                    'metadata': {
                        'database_type': 'elasticsearch',
                        'document_count': doc_count,
                        'host': db_config['host'],
                        'port': db_config.get('port', 9200)
                    }
                }
                assets.append(asset)
            
        except Exception as e:
            self.logger.error(f"Error connecting to Elasticsearch: {e}")
        
        return assets
    
    def _discover_snowflake_assets(self, db_config: Dict[str, Any]) -> List[Dict[str, Any]]:
        """Discover Snowflake database assets"""
        assets = []
        if not snowflake:
            self.logger.warning("snowflake-connector-python library not installed. Install with: pip install snowflake-connector-python")
            return assets
        
        try:
            conn = snowflake.connector.connect(
                user=db_config['username'],
                password=db_config['password'],
                account=db_config['account'],
                warehouse=db_config.get('warehouse'),
                database=db_config.get('database'),
                schema=db_config.get('schema', 'PUBLIC')
            )
            
            cursor = conn.cursor()
            
            # Get all tables
            cursor.execute("SHOW TABLES")
            
            for row in cursor:
                table_name = row[1]  # Table name is in second column
                
                asset = {
                    'name': table_name,
                    'type': 'snowflake_table',
                    'source': 'snowflake_database',
                    'location': f"snowflake://{db_config['account']}/{db_config.get('database', '')}/{db_config.get('schema', 'PUBLIC')}/{table_name}",
                    'created_date': datetime.now(),
                    'size': 0,
                    'metadata': {
                        'database_type': 'snowflake',
                        'account': db_config['account'],
                        'warehouse': db_config.get('warehouse'),
                        'database': db_config.get('database'),
                        'schema': db_config.get('schema', 'PUBLIC')
                    }
                }
                assets.append(asset)
            
            conn.close()
            
        except Exception as e:
            self.logger.error(f"Error connecting to Snowflake database: {e}")
        
        return assets
    
    def _discover_db2_assets(self, db_config: Dict[str, Any]) -> List[Dict[str, Any]]:
        """Discover IBM Db2 database assets"""
        assets = []
        if not pyodbc:
            self.logger.warning("pyodbc library not installed. Install with: pip install pyodbc")
            return assets
        
        try:
            connection_string = f"DRIVER={{IBM DB2 ODBC DRIVER}};DATABASE={db_config['database']};HOSTNAME={db_config['host']};PORT={db_config.get('port', 50000)};PROTOCOL=TCPIP;UID={db_config['username']};PWD={db_config['password']};"
            
            conn = pyodbc.connect(connection_string)
            cursor = conn.cursor()
            
            # Get all tables
            cursor.execute("SELECT TABNAME FROM SYSCAT.TABLES WHERE TYPE='T' AND TABSCHEMA NOT LIKE 'SYS%'")
            
            for row in cursor:
                table_name = row[0]
                
                asset = {
                    'name': table_name,
                    'type': 'db2_table',
                    'source': 'db2_database',
                    'location': f"db2://{db_config['host']}:{db_config.get('port', 50000)}/{db_config['database']}/{table_name}",
                    'created_date': datetime.now(),
                    'size': 0,
                    'metadata': {
                        'database_type': 'db2',
                        'host': db_config['host'],
                        'port': db_config.get('port', 50000),
                        'database': db_config['database']
                    }
                }
                assets.append(asset)
            
            conn.close()
            
        except Exception as e:
            self.logger.error(f"Error connecting to Db2 database: {e}")
        
        return assets
    
    def _discover_mariadb_assets(self, db_config: Dict[str, Any]) -> List[Dict[str, Any]]:
        """Discover MariaDB database assets (uses MySQL connector)"""
        # MariaDB is compatible with MySQL protocol
        return self._discover_mysql_assets(db_config)
    
    def _discover_couchdb_assets(self, db_config: Dict[str, Any]) -> List[Dict[str, Any]]:
        """Discover CouchDB database assets"""
        assets = []
        try:
            import requests
            
            base_url = f"http://{db_config['host']}:{db_config.get('port', 5984)}"
            auth = (db_config.get('username'), db_config.get('password')) if db_config.get('username') else None
            
            # Get all databases
            response = requests.get(f"{base_url}/_all_dbs", auth=auth)
            databases = response.json()
            
            for db_name in databases:
                if db_name.startswith('_'):  # Skip system databases
                    continue
                
                # Get database info
                db_info_response = requests.get(f"{base_url}/{db_name}", auth=auth)
                db_info = db_info_response.json()
                
                asset = {
                    'name': db_name,
                    'type': 'couchdb_database',
                    'source': 'couchdb',
                    'location': f"couchdb://{db_config['host']}:{db_config.get('port', 5984)}/{db_name}",
                    'created_date': datetime.now(),
                    'size': db_info.get('sizes', {}).get('active', 0),
                    'metadata': {
                        'database_type': 'couchdb',
                        'doc_count': db_info.get('doc_count', 0),
                        'host': db_config['host'],
                        'port': db_config.get('port', 5984)
                    }
                }
                assets.append(asset)
            
        except Exception as e:
            self.logger.error(f"Error connecting to CouchDB: {e}")
        
        return assets
    
    def _discover_influxdb_assets(self, db_config: Dict[str, Any]) -> List[Dict[str, Any]]:
        """Discover InfluxDB database assets"""
        assets = []
        try:
            from influxdb import InfluxDBClient
            
            client = InfluxDBClient(
                host=db_config['host'],
                port=db_config.get('port', 8086),
                username=db_config.get('username'),
                password=db_config.get('password')
            )
            
            # Get all databases
            databases = client.get_list_database()
            
            for db_info in databases:
                db_name = db_info['name']
                
                if db_name.startswith('_'):  # Skip system databases
                    continue
                
                client.switch_database(db_name)
                
                # Get measurements (similar to tables)
                measurements = client.get_list_measurements()
                
                for measurement in measurements:
                    measurement_name = measurement['name']
                    
                    asset = {
                        'name': f"{db_name}.{measurement_name}",
                        'type': 'influxdb_measurement',
                        'source': 'influxdb',
                        'location': f"influxdb://{db_config['host']}:{db_config.get('port', 8086)}/{db_name}/{measurement_name}",
                        'created_date': datetime.now(),
                        'size': 0,
                        'metadata': {
                            'database_type': 'influxdb',
                            'database': db_name,
                            'host': db_config['host'],
                            'port': db_config.get('port', 8086)
                        }
                    }
                    assets.append(asset)
            
        except Exception as e:
            self.logger.error(f"Error connecting to InfluxDB: {e}")
        
        return assets
