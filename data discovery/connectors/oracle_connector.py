"""
Oracle Connector - Discovers data assets in Oracle databases
"""

from datetime import datetime
from typing import List, Dict, Any, Optional

# Oracle imports
try:
    import cx_Oracle
except ImportError:
    cx_Oracle = None

from .base_connector import BaseConnector


class OracleConnector(BaseConnector):
    """
    Connector for discovering data assets in Oracle databases
    """
    
    # Metadata for dynamic discovery
    connector_type = "oracle"
    connector_name = "Oracle Database"
    description = "Discover data assets from Oracle database including tables, views, and schemas"
    category = "databases"
    supported_services = ["Oracle", "Tables", "Views", "Schemas"]
    required_config_fields = ["host", "username", "password", "service_name"]
    optional_config_fields = ["port", "connection_timeout"]
    
    def __init__(self, config: Dict[str, Any]):
        super().__init__(config)
        self.host = config.get('host')
        self.port = config.get('port', 1521)
        self.service_name = config.get('service_name')
        self.username = config.get('username')
        self.password = config.get('password', '')
        self.connection_timeout = config.get('connection_timeout', 30)
    
    def discover_assets(self) -> List[Dict[str, Any]]:
        """Discover Oracle database assets"""
        self.logger.info("Starting Oracle asset discovery")
        assets = []
        
        if not cx_Oracle:
            self.logger.warning("cx_Oracle library not installed. Install with: pip install cx_Oracle")
            return assets
        
        try:
            dsn = cx_Oracle.makedsn(
                self.host,
                self.port,
                service_name=self.service_name
            )
            
            with cx_Oracle.connect(
                self.username,
                self.password,
                dsn
            ) as connection:
                cursor = connection.cursor()
                
                # Get all tables
                cursor.execute("""
                    SELECT 
                        table_name, 
                        tablespace_name, 
                        num_rows,
                        blocks,
                        avg_row_len,
                        last_analyzed
                    FROM user_tables 
                    ORDER BY table_name
                """)
                
                for row in cursor:
                    table_name, tablespace, num_rows, blocks, avg_row_len, last_analyzed = row
                    
                    # Get column information
                    columns_query = """
                        SELECT 
                            column_name,
                            data_type,
                            data_length,
                            data_precision,
                            data_scale,
                            nullable,
                            data_default
                        FROM user_tab_columns 
                        WHERE table_name = :table_name
                        ORDER BY column_id
                    """
                    
                    cursor.execute(columns_query, {'table_name': table_name})
                    columns = []
                    for col_row in cursor:
                        columns.append({
                            'name': col_row[0],
                            'type': col_row[1],
                            'length': col_row[2],
                            'precision': col_row[3],
                            'scale': col_row[4],
                            'nullable': col_row[5] == 'Y',
                            'default': col_row[6]
                        })
                    
                    asset = {
                        'name': table_name,
                        'type': 'oracle_table',
                        'source': 'oracle',
                        'location': f"oracle://{self.host}:{self.port}/{self.service_name}/{table_name}",
                        'size': (blocks or 0) * 8192,  # Oracle block size is typically 8KB
                        'created_date': datetime.now().isoformat(),
                        'modified_date': last_analyzed.isoformat() if last_analyzed else datetime.now().isoformat(),
                        'schema': {
                            'columns': columns,
                            'column_count': len(columns)
                        },
                        'tags': ['oracle', 'database', 'table'],
                        'metadata': {
                            'database_type': 'oracle',
                            'service_name': self.service_name,
                            'table_name': table_name,
                            'tablespace': tablespace,
                            'row_count': num_rows or 0,
                            'column_count': len(columns),
                            'blocks': blocks or 0,
                            'avg_row_len': avg_row_len or 0,
                            'host': self.host,
                            'port': self.port
                        }
                    }
                    assets.append(asset)
                
                # Get all views
                cursor.execute("""
                    SELECT view_name, text
                    FROM user_views
                    ORDER BY view_name
                """)
                
                for row in cursor:
                    view_name, view_text = row
                    
                    # Get column information for view
                    columns_query = """
                        SELECT 
                            column_name,
                            data_type,
                            data_length,
                            data_precision,
                            data_scale,
                            nullable
                        FROM user_tab_columns 
                        WHERE table_name = :view_name
                        ORDER BY column_id
                    """
                    
                    cursor.execute(columns_query, {'view_name': view_name})
                    columns = []
                    for col_row in cursor:
                        columns.append({
                            'name': col_row[0],
                            'type': col_row[1],
                            'length': col_row[2],
                            'precision': col_row[3],
                            'scale': col_row[4],
                            'nullable': col_row[5] == 'Y'
                        })
                    
                    asset = {
                        'name': view_name,
                        'type': 'oracle_view',
                        'source': 'oracle',
                        'location': f"oracle://{self.host}:{self.port}/{self.service_name}/{view_name}",
                        'size': 0,
                        'created_date': datetime.now().isoformat(),
                        'modified_date': datetime.now().isoformat(),
                        'schema': {
                            'columns': columns,
                            'column_count': len(columns)
                        },
                        'tags': ['oracle', 'database', 'view'],
                        'metadata': {
                            'database_type': 'oracle',
                            'service_name': self.service_name,
                            'view_name': view_name,
                            'column_count': len(columns),
                            'view_text': view_text[:500] if view_text else '',  # Truncate for metadata
                            'host': self.host,
                            'port': self.port
                        }
                    }
                    assets.append(asset)
                    
        except Exception as e:
            self.logger.error(f"Error discovering Oracle assets: {e}")
        
        self.logger.info(f"Discovered {len(assets)} Oracle assets")
        return assets
    
    def test_connection(self) -> bool:
        """Test Oracle connection"""
        if not cx_Oracle:
            self.logger.error("cx_Oracle library not installed")
            return False
        
        try:
            dsn = cx_Oracle.makedsn(
                self.host,
                self.port,
                service_name=self.service_name
            )
            
            with cx_Oracle.connect(
                self.username,
                self.password,
                dsn
            ) as connection:
                cursor = connection.cursor()
                cursor.execute("SELECT 1 FROM DUAL")
                cursor.fetchone()
                self.logger.info("Oracle connection test successful")
                return True
        except Exception as e:
            self.logger.error(f"Oracle connection test failed: {e}")
            return False
    
    def validate_config(self) -> bool:
        """Validate Oracle connector configuration"""
        if not self.host:
            self.logger.error("Host is required")
            return False
        if not self.service_name:
            self.logger.error("Service name is required")
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
            "service_name": self.service_name,
            "username": self.username,
            "connection_string": f"oracle://{self.host}:{self.port}/{self.service_name}"
        }
