"""
PostgreSQL Connector - Discovers data assets in PostgreSQL databases
"""

import psycopg2
from datetime import datetime
from typing import List, Dict, Any, Optional
from sqlalchemy import create_engine, text
from sqlalchemy.exc import SQLAlchemyError

from .base_connector import BaseConnector
class PostgreSQLConnector(BaseConnector):
    """
    Connector for discovering data assets in PostgreSQL databases
    """
    
    # Metadata for dynamic discovery
    connector_type = "postgresql"
    connector_name = "PostgreSQL Database"
    description = "Discover data assets from PostgreSQL database including tables, views, and schemas"
    category = "databases"
    supported_services = ["PostgreSQL", "Tables", "Views", "Schemas"]
    required_config_fields = ["host", "username", "password", "database"]
    optional_config_fields = ["port", "connection_timeout"]
    
    def __init__(self, config: Dict[str, Any]):
        super().__init__(config)
        self.host = config.get('host')
        self.port = config.get('port', 5432)
        self.database = config.get('database', 'postgres')
        self.username = config.get('username', 'postgres')
        self.password = config.get('password', '')
        self.connection_timeout = config.get('connection_timeout', 30)
    
    def discover_assets(self) -> List[Dict[str, Any]]:
        """Discover PostgreSQL database assets"""
        self.logger.info("Starting PostgreSQL asset discovery")
        assets = []
        
        try:
            connection_string = self._build_connection_string()
            engine = create_engine(connection_string, connect_args={'connect_timeout': self.connection_timeout})
            
            with engine.connect() as conn:
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
                    
                    table_asset = self._create_table_asset(
                        conn, schema_name, table_name, object_type
                    )
                    if table_asset:
                        assets.append(table_asset)
        
        except Exception as e:
            self.logger.error(f"Error discovering PostgreSQL assets: {e}")
        
        self.logger.info(f"Discovered {len(assets)} PostgreSQL assets")
        return assets
    
    def _create_table_asset(self, conn, schema_name: str, table_name: str, object_type: str) -> Optional[Dict[str, Any]]:
        """Create asset for PostgreSQL table/view"""
        try:
            # Get column information
            columns_query = text("""
                SELECT 
                    column_name, 
                    data_type, 
                    is_nullable,
                    column_default,
                    character_maximum_length,
                    numeric_precision,
                    numeric_scale
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
                    'default': col_row[3],
                    'max_length': col_row[4],
                    'precision': col_row[5],
                    'scale': col_row[6]
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
            
            size_query = text("""
                SELECT pg_size_pretty(pg_total_relation_size(:schema_table)) as size
            """)
            size_result = conn.execute(size_query, {'schema_table': f'"{schema_name}"."{table_name}"'})
            size_info = size_result.scalar()
            
            asset = {
                'name': f"{schema_name}.{table_name}",
                'type': f'postgresql_{object_type}',
                'source': 'postgresql',
                'location': f"postgresql://{self.host}:{self.port}/{self.database}/{schema_name}/{table_name}",
                'size': row_count * len(columns) * 50 if row_count > 0 else 0,  # Rough estimate
                'created_date': datetime.now().isoformat(),
                'modified_date': datetime.now().isoformat(),
                'schema': {
                    'columns': columns,
                    'column_count': len(columns)
                },
                'tags': ['postgresql', 'database', object_type, schema_name],
                'metadata': {
                    'database_type': 'postgresql',
                    'database_name': self.database,
                    'schema_name': schema_name,
                    'table_name': table_name,
                    'object_type': object_type,
                    'row_count': row_count,
                    'column_count': len(columns),
                    'size_info': size_info,
                    'host': self.host,
                    'port': self.port
                }
            }
            
            return asset
            
        except Exception as e:
            self.logger.warning(f"Error creating PostgreSQL asset for {schema_name}.{table_name}: {e}")
            return None
    
    def _build_connection_string(self) -> str:
        """Build PostgreSQL connection string"""
        return f"postgresql://{self.username}:{self.password}@{self.host}:{self.port}/{self.database}"
    
    def test_connection(self) -> bool:
        """Test PostgreSQL connection"""
        try:
            connection_string = self._build_connection_string()
            engine = create_engine(connection_string, connect_args={'connect_timeout': 5})
            with engine.connect():
                self.logger.info("PostgreSQL connection test successful")
                return True
        except Exception as e:
            self.logger.error(f"PostgreSQL connection test failed: {e}")
            return False
    
    def validate_config(self) -> bool:
        """Validate PostgreSQL connector configuration"""
        if not self.host:
            self.logger.error("Host is required")
            return False
        if not self.database:
            self.logger.error("Database name is required")
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
            "host": self.host,
            "port": self.port,
            "database": self.database,
            "username": self.username,
            "connection_string": f"postgresql://{self.host}:{self.port}/{self.database}"
        }
