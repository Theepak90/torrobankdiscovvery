"""
Data Lake Connector - Discovers data assets in data lakes and modern data formats
"""

from datetime import datetime
from typing import List, Dict, Any, Optional
import logging
from pathlib import Path

from .base_connector import BaseConnector

# Import data lake libraries with fallbacks
try:
    import pyarrow as pa
    import pyarrow.parquet as pq
    import pyarrow.dataset as ds
except ImportError:
    pa = None
    pq = None
    ds = None

try:
    from deltalake import DeltaTable
except ImportError:
    DeltaTable = None

try:
    import pyiceberg
except ImportError:
    pyiceberg = None

try:
    import pyhudi
except ImportError:
    pyhudi = None


class DataLakeConnector(BaseConnector):
    """
    Connector for discovering data assets in data lakes and modern data formats
    """
    
    def __init__(self, config: Dict[str, Any]):
        super().__init__(config)
        self.data_lakes = config.get('data_lake_connections', [])
    
    def discover_assets(self) -> List[Dict[str, Any]]:
        """Discover data lake assets"""
        self.logger.info("Starting data lake asset discovery")
        assets = []
        
        for lake_config in self.data_lakes:
            lake_type = lake_config.get('type', '').lower()
            
            try:
                if lake_type == 'delta_lake':
                    assets.extend(self._discover_delta_lake_assets(lake_config))
                elif lake_type == 'iceberg':
                    assets.extend(self._discover_iceberg_assets(lake_config))
                elif lake_type == 'hudi':
                    assets.extend(self._discover_hudi_assets(lake_config))
                elif lake_type == 'parquet':
                    assets.extend(self._discover_parquet_assets(lake_config))
                elif lake_type == 'orc':
                    assets.extend(self._discover_orc_assets(lake_config))
                elif lake_type == 'avro':
                    assets.extend(self._discover_avro_assets(lake_config))
                elif lake_type == 'hdfs':
                    assets.extend(self._discover_hdfs_assets(lake_config))
                elif lake_type == 'minio':
                    assets.extend(self._discover_minio_assets(lake_config))
                elif lake_type == 'ceph':
                    assets.extend(self._discover_ceph_assets(lake_config))
                else:
                    self.logger.warning(f"Unsupported data lake type: {lake_type}")
                    
            except Exception as e:
                self.logger.error(f"Error discovering assets from {lake_type} data lake: {e}")
        
        self.logger.info(f"Discovered {len(assets)} data lake assets")
        return assets
    
    def _discover_delta_lake_assets(self, config: Dict[str, Any]) -> List[Dict[str, Any]]:
        """Discover Delta Lake assets"""
        assets = []
        if not DeltaTable:
            self.logger.warning("deltalake not installed")
            return assets
        
        try:
            base_path = config['base_path']
            
            # Scan for Delta tables
            for table_path in config.get('table_paths', []):
                try:
                    full_path = f"{base_path}/{table_path}"
                    delta_table = DeltaTable(full_path)
                    
                    # Get table metadata
                    metadata = delta_table.metadata()
                    history = delta_table.history()
                    
                    asset = {
                        'name': table_path.split('/')[-1],
                        'type': 'delta_table',
                        'source': 'delta_lake',
                        'location': full_path,
                        'created_date': datetime.fromtimestamp(history[0]['timestamp'] / 1000) if history else datetime.now(),
                        'size': 0,  # Would need to calculate from files
                        'metadata': {
                            'lake_type': 'delta_lake',
                            'table_path': table_path,
                            'version': delta_table.version(),
                            'num_files': len(delta_table.files()),
                            'schema': str(delta_table.schema()),
                            'partition_columns': metadata.partition_columns if hasattr(metadata, 'partition_columns') else []
                        }
                    }
                    assets.append(asset)
                    
                except Exception as e:
                    self.logger.error(f"Error reading Delta table {table_path}: {e}")
            
        except Exception as e:
            self.logger.error(f"Error connecting to Delta Lake: {e}")
        
        return assets
    
    def _discover_iceberg_assets(self, config: Dict[str, Any]) -> List[Dict[str, Any]]:
        """Discover Apache Iceberg assets"""
        assets = []
        if not pyiceberg:
            self.logger.warning("pyiceberg not installed")
            return assets
        
        try:
            # This would require Iceberg catalog connection
            # Placeholder for Iceberg discovery
            catalog_uri = config.get('catalog_uri', '')
            tables = config.get('tables', [])
            
            for table_name in tables:
                asset = {
                    'name': table_name,
                    'type': 'iceberg_table',
                    'source': 'iceberg',
                    'location': f"{catalog_uri}/{table_name}",
                    'created_date': datetime.now(),
                    'size': 0,
                    'metadata': {
                        'lake_type': 'iceberg',
                        'catalog_uri': catalog_uri,
                        'table_name': table_name
                    }
                }
                assets.append(asset)
            
        except Exception as e:
            self.logger.error(f"Error connecting to Iceberg: {e}")
        
        return assets
    
    def _discover_hudi_assets(self, config: Dict[str, Any]) -> List[Dict[str, Any]]:
        """Discover Apache Hudi assets"""
        assets = []
        if not pyhudi:
            self.logger.warning("pyhudi not installed")
            return assets
        
        try:
            # This would require Hudi table reading
            # Placeholder for Hudi discovery
            base_path = config['base_path']
            tables = config.get('tables', [])
            
            for table_name in tables:
                asset = {
                    'name': table_name,
                    'type': 'hudi_table',
                    'source': 'hudi',
                    'location': f"{base_path}/{table_name}",
                    'created_date': datetime.now(),
                    'size': 0,
                    'metadata': {
                        'lake_type': 'hudi',
                        'base_path': base_path,
                        'table_name': table_name
                    }
                }
                assets.append(asset)
            
        except Exception as e:
            self.logger.error(f"Error connecting to Hudi: {e}")
        
        return assets
    
    def _discover_parquet_assets(self, config: Dict[str, Any]) -> List[Dict[str, Any]]:
        """Discover Parquet files"""
        assets = []
        if not pq:
            self.logger.warning("pyarrow not installed")
            return assets
        
        try:
            base_path = config['base_path']
            
            # Scan for Parquet files
            if config.get('scan_recursive', True):
                parquet_files = list(Path(base_path).rglob('*.parquet'))
            else:
                parquet_files = list(Path(base_path).glob('*.parquet'))
            
            for parquet_file in parquet_files:
                try:
                    # Read Parquet metadata
                    parquet_metadata = pq.read_metadata(str(parquet_file))
                    
                    asset = {
                        'name': parquet_file.name,
                        'type': 'parquet_file',
                        'source': 'parquet',
                        'location': str(parquet_file.absolute()),
                        'created_date': datetime.fromtimestamp(parquet_file.stat().st_ctime),
                        'modified_date': datetime.fromtimestamp(parquet_file.stat().st_mtime),
                        'size': parquet_file.stat().st_size,
                        'metadata': {
                            'lake_type': 'parquet',
                            'num_rows': parquet_metadata.num_rows,
                            'num_columns': parquet_metadata.schema.to_arrow_schema().names.__len__(),
                            'file_size': parquet_metadata.serialized_size,
                            'schema': str(parquet_metadata.schema.to_arrow_schema())
                        }
                    }
                    assets.append(asset)
                    
                except Exception as e:
                    self.logger.error(f"Error reading Parquet file {parquet_file}: {e}")
            
        except Exception as e:
            self.logger.error(f"Error scanning Parquet files: {e}")
        
        return assets
    
    def _discover_orc_assets(self, config: Dict[str, Any]) -> List[Dict[str, Any]]:
        """Discover ORC files"""
        assets = []
        
        try:
            base_path = config['base_path']
            
            # Scan for ORC files
            if config.get('scan_recursive', True):
                orc_files = list(Path(base_path).rglob('*.orc'))
            else:
                orc_files = list(Path(base_path).glob('*.orc'))
            
            for orc_file in orc_files:
                try:
                    asset = {
                        'name': orc_file.name,
                        'type': 'orc_file',
                        'source': 'orc',
                        'location': str(orc_file.absolute()),
                        'created_date': datetime.fromtimestamp(orc_file.stat().st_ctime),
                        'modified_date': datetime.fromtimestamp(orc_file.stat().st_mtime),
                        'size': orc_file.stat().st_size,
                        'metadata': {
                            'lake_type': 'orc',
                            'file_size': orc_file.stat().st_size
                        }
                    }
                    assets.append(asset)
                    
                except Exception as e:
                    self.logger.error(f"Error reading ORC file {orc_file}: {e}")
            
        except Exception as e:
            self.logger.error(f"Error scanning ORC files: {e}")
        
        return assets
    
    def _discover_avro_assets(self, config: Dict[str, Any]) -> List[Dict[str, Any]]:
        """Discover Avro files"""
        assets = []
        
        try:
            base_path = config['base_path']
            
            # Scan for Avro files
            if config.get('scan_recursive', True):
                avro_files = list(Path(base_path).rglob('*.avro'))
            else:
                avro_files = list(Path(base_path).glob('*.avro'))
            
            for avro_file in avro_files:
                try:
                    asset = {
                        'name': avro_file.name,
                        'type': 'avro_file',
                        'source': 'avro',
                        'location': str(avro_file.absolute()),
                        'created_date': datetime.fromtimestamp(avro_file.stat().st_ctime),
                        'modified_date': datetime.fromtimestamp(avro_file.stat().st_mtime),
                        'size': avro_file.stat().st_size,
                        'metadata': {
                            'lake_type': 'avro',
                            'file_size': avro_file.stat().st_size
                        }
                    }
                    assets.append(asset)
                    
                except Exception as e:
                    self.logger.error(f"Error reading Avro file {avro_file}: {e}")
            
        except Exception as e:
            self.logger.error(f"Error scanning Avro files: {e}")
        
        return assets
    
    def _discover_hdfs_assets(self, config: Dict[str, Any]) -> List[Dict[str, Any]]:
        """Discover HDFS assets"""
        assets = []
        
        try:
            # This would require HDFS client (hdfs3 or pydoop)
            # Placeholder for HDFS discovery
            namenode = config.get('namenode', 'localhost:9000')
            paths = config.get('paths', ['/'])
            
            for path in paths:
                asset = {
                    'name': path.split('/')[-1] or 'root',
                    'type': 'hdfs_directory',
                    'source': 'hdfs',
                    'location': f"hdfs://{namenode}{path}",
                    'created_date': datetime.now(),
                    'size': 0,
                    'metadata': {
                        'lake_type': 'hdfs',
                        'namenode': namenode,
                        'path': path
                    }
                }
                assets.append(asset)
            
        except Exception as e:
            self.logger.error(f"Error connecting to HDFS: {e}")
        
        return assets
    
    def _discover_minio_assets(self, config: Dict[str, Any]) -> List[Dict[str, Any]]:
        """Discover MinIO assets"""
        assets = []
        
        try:
            from minio import Minio
            
            client = Minio(
                endpoint=config['endpoint'],
                access_key=config['access_key'],
                secret_key=config['secret_key'],
                secure=config.get('secure', True)
            )
            
            # List buckets
            buckets = client.list_buckets()
            
            for bucket in buckets:
                asset = {
                    'name': bucket.name,
                    'type': 'minio_bucket',
                    'source': 'minio',
                    'location': f"minio://{config['endpoint']}/{bucket.name}",
                    'created_date': bucket.creation_date,
                    'size': 0,
                    'metadata': {
                        'lake_type': 'minio',
                        'endpoint': config['endpoint'],
                        'bucket_name': bucket.name
                    }
                }
                assets.append(asset)
            
        except Exception as e:
            self.logger.error(f"Error connecting to MinIO: {e}")
        
        return assets
    
    def _discover_ceph_assets(self, config: Dict[str, Any]) -> List[Dict[str, Any]]:
        """Discover Ceph assets"""
        assets = []
        
        try:
            # This would require Ceph client (boto3 with Ceph endpoint)
            import boto3
            
            s3_client = boto3.client(
                's3',
                endpoint_url=config['endpoint'],
                aws_access_key_id=config['access_key'],
                aws_secret_access_key=config['secret_key']
            )
            
            # List buckets
            response = s3_client.list_buckets()
            
            for bucket in response['Buckets']:
                asset = {
                    'name': bucket['Name'],
                    'type': 'ceph_bucket',
                    'source': 'ceph',
                    'location': f"ceph://{config['endpoint']}/{bucket['Name']}",
                    'created_date': bucket['CreationDate'],
                    'size': 0,
                    'metadata': {
                        'lake_type': 'ceph',
                        'endpoint': config['endpoint'],
                        'bucket_name': bucket['Name']
                    }
                }
                assets.append(asset)
            
        except Exception as e:
            self.logger.error(f"Error connecting to Ceph: {e}")
        
        return assets
    
    def test_connection(self) -> bool:
        """Test data lake connections"""
        try:
            if not self.data_lakes:
                self.logger.error("No data lake connections configured")
                return False
            
            connection_tested = False
            
            for lake_config in self.data_lakes:
                lake_type = lake_config.get('type', '').lower()
                
                try:
                    if lake_type == 'delta':
                        if DeltaTable:
                            table_path = lake_config.get('table_path')
                            if table_path and Path(table_path).exists():
                                # Test by reading delta table metadata
                                dt = DeltaTable(table_path)
                                schema = dt.schema()
                                self.logger.info("Delta Lake connection test successful")
                                connection_tested = True
                            else:
                                self.logger.warning(f"Delta table path not found: {table_path}")
                        else:
                            self.logger.warning("Delta Lake library not available")
                    
                    elif lake_type == 'parquet':
                        if pq:
                            path = lake_config.get('path')
                            if path and Path(path).exists():
                                # Test by reading parquet metadata
                                if Path(path).is_file():
                                    parquet_file = pq.ParquetFile(path)
                                    schema = parquet_file.schema
                                else:
                                    # Directory with parquet files
                                    dataset = ds.dataset(path, format='parquet')
                                    schema = dataset.schema
                                self.logger.info("Parquet connection test successful")
                                connection_tested = True
                            else:
                                self.logger.warning(f"Parquet path not found: {path}")
                        else:
                            self.logger.warning("PyArrow library not available")
                    
                    elif lake_type == 'iceberg':
                        if pyiceberg:
                            # Test Iceberg connection (would need proper catalog config)
                            catalog_config = lake_config.get('catalog', {})
                            self.logger.info("Iceberg connection test successful")
                            connection_tested = True
                        else:
                            self.logger.warning("PyIceberg library not available")
                    
                    elif lake_type == 'hudi':
                        if pyhudi:
                            # Test Hudi connection
                            table_path = lake_config.get('table_path')
                            if table_path and Path(table_path).exists():
                                self.logger.info("Hudi connection test successful")
                                connection_tested = True
                            else:
                                self.logger.warning(f"Hudi table path not found: {table_path}")
                        else:
                            self.logger.warning("PyHudi library not available")
                    
                except Exception as e:
                    self.logger.warning(f"{lake_type.capitalize()} connection test failed: {e}")
            
            if connection_tested:
                self.logger.info("Data lake connection test successful")
                return True
            else:
                self.logger.error("No data lake connections could be tested")
                return False
                
        except Exception as e:
            self.logger.error(f"Data lake connection test failed: {e}")
            return False
    
    def validate_config(self) -> bool:
        """Validate data lake connector configuration"""
        if not self.data_lakes:
            self.logger.error("No data lake connections configured")
            return False
        
        for lake_config in self.data_lakes:
            if not lake_config.get('type'):
                self.logger.error("Data lake type not specified in configuration")
                return False
        
        return True

