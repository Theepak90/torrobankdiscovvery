"""
MySQL Connector - Discovers data assets in MySQL databases
"""

import mysql.connector
from datetime import datetime
from typing import List, Dict, Any, Optional
from sqlalchemy import create_engine, text
from sqlalchemy.exc import SQLAlchemyError

from .base_connector import BaseConnector


class MySQLConnector(BaseConnector):
    """
    Connector for discovering data assets in MySQL databases
    """
    
    # Metadata for dynamic discovery
    connector_type = "mysql"
    connector_name = "MySQL Database"
    description = "Discover data assets from MySQL database including tables, views, and schemas"
    category = "databases"
    supported_services = ["MySQL", "Tables", "Views", "Schemas"]
    required_config_fields = ["host", "username", "password", "database"]
    optional_config_fields = ["port", "connection_timeout"]
    
    def __init__(self, config: Dict[str, Any]):
        super().__init__(config)
        self.host = config.get('host')
        self.port = config.get('port', 3306)
        self.database = config.get('database')
        self.username = config.get('username')
        self.password = config.get('password', '')
        self.connection_timeout = config.get('connection_timeout', 30)
    
    def discover_assets(self) -> List[Dict[str, Any]]:
        """Discover MySQL database assets"""
        self.logger.info("Starting MySQL asset discovery")
        assets = []
        
        try:
            connection_string = self._build_connection_string()
            engine = create_engine(connection_string, connect_args={'connect_timeout': self.connection_timeout})
            
            with engine.connect() as conn:
                # Get all tables and views
                query = text("""
                    SELECT 
                        TABLE_NAME, 
                        TABLE_TYPE,
                        TABLE_ROWS,
                        DATA_LENGTH,
                        CREATE_TIME,
                        UPDATE_TIME
                    FROM information_schema.TABLES 
                    WHERE TABLE_SCHEMA = :db_name
                """)
                
                result = conn.execute(query, {'db_name': self.database})
                
                for row in result:
                    table_name, table_type, table_rows, data_length, create_time, update_time = row
                    object_type = 'view' if table_type == 'VIEW' else 'table'
                    
                    # Get column information
                    columns_query = text("""
                        SELECT 
                            COLUMN_NAME, 
                            DATA_TYPE, 
                            IS_NULLABLE,
                            COLUMN_DEFAULT,
                            COLUMN_KEY,
                            EXTRA
                        FROM information_schema.COLUMNS 
                        WHERE TABLE_SCHEMA = :db_name AND TABLE_NAME = :table_name
                        ORDER BY ORDINAL_POSITION
                    """)
                    
                    columns_result = conn.execute(columns_query, {'db_name': self.database, 'table_name': table_name})
                    columns = []
                    for col_row in columns_result:
                        columns.append({
                            'name': col_row[0],
                            'type': col_row[1],
                            'nullable': col_row[2] == 'YES',
                            'default': col_row[3],
                            'key': col_row[4],
                            'extra': col_row[5]
                        })
                    
                    asset = {
                        'name': table_name,
                        'type': f'mysql_{object_type}',
                        'source': 'mysql',
                        'location': f"mysql://{self.host}:{self.port}/{self.database}/{table_name}",
                        'size': data_length or 0,
                        'created_date': create_time.isoformat() if create_time else datetime.now().isoformat(),
                        'modified_date': update_time.isoformat() if update_time else datetime.now().isoformat(),
                        'schema': {
                            'columns': columns,
                            'column_count': len(columns)
                        },
                        'tags': ['mysql', 'database', object_type],
                        'metadata': {
                            'database_type': 'mysql',
                            'database_name': self.database,
                            'table_name': table_name,
                            'object_type': object_type,
                            'row_count': table_rows or 0,
                            'column_count': len(columns),
                            'data_length': data_length or 0,
                            'host': self.host,
                            'port': self.port
                        }
                    }
                    
                    assets.append(asset)
        
        except Exception as e:
            self.logger.error(f"Error discovering MySQL assets: {e}")
        
        self.logger.info(f"Discovered {len(assets)} MySQL assets")
        return assets
    
    def _build_connection_string(self) -> str:
        """Build MySQL connection string"""
        return f"mysql+pymysql://{self.username}:{self.password}@{self.host}:{self.port}/{self.database}"
    
    def test_connection(self) -> bool:
        """Test MySQL connection"""
        try:
            connection_string = self._build_connection_string()
            engine = create_engine(connection_string, connect_args={'connect_timeout': 5})
            with engine.connect():
                self.logger.info("MySQL connection test successful")
                return True
        except Exception as e:
            self.logger.error(f"MySQL connection test failed: {e}")
            return False
    
    def validate_config(self) -> bool:
        """Validate MySQL connector configuration"""
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
            "connection_string": f"mysql://{self.host}:{self.port}/{self.database}"
        }
