"""
Data Lake Connector - Discovers data assets in data lakes and modern data formats
"""

from datetime import datetime
from typing import List, Dict, Any, Optional
import logging
from pathlib import Path

from .base_connector import BaseConnector

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
    
    connector_type = "data_lakes"
    connector_name = "Data Lakes"
    description = "Discover data assets from data lakes including Delta Lake, Iceberg, Hudi, Parquet, MinIO, HDFS, and Ceph"
    category = "data_lakes"
    supported_services = ["Delta Lake", "Iceberg", "Hudi", "Parquet", "MinIO", "HDFS", "Ceph"]
    required_config_fields = ["data_lake_connections"]
    optional_config_fields = ["connection_timeout"]
    
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
            
            for table_path in config.get('table_paths', []):
                try:
                    full_path = f"{base_path}/{table_path}"
                    delta_table = DeltaTable(full_path)
                    
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
        
        try:
            from pyiceberg.catalog import load_catalog
            from pyiceberg.exceptions import NoSuchTableError
            
            catalog_config = config.get('catalog', {})
            catalog_type = catalog_config.get('type', 'rest')
            
            if catalog_type == 'rest':
                catalog = load_catalog(
                    name="rest_catalog",
                    **{
                        "type": "rest",
                        "uri": catalog_config.get('uri', 'http://localhost:8181'),
                        "credential": catalog_config.get('credential', ''),
                        "token": catalog_config.get('token', '')
                    }
                )
            elif catalog_type == 'hive':
                catalog = load_catalog(
                    name="hive_catalog",
                    **{
                        "type": "hive",
                        "uri": catalog_config.get('uri', 'thrift://localhost:9083'),
                        "warehouse": catalog_config.get('warehouse', '/warehouse')
                    }
                )
            elif catalog_type == 'glue':
                catalog = load_catalog(
                    name="glue_catalog",
                    **{
                        "type": "glue",
                        "region_name": catalog_config.get('region', 'us-east-1')
                    }
                )
            else:
                self.logger.warning(f"Unsupported Iceberg catalog type: {catalog_type}")
                return assets
            
            try:
                namespaces = catalog.list_namespaces()
                
                for namespace in namespaces:
                    try:
                        tables = catalog.list_tables(namespace)
                        
                        for table_identifier in tables:
                            try:
                                table = catalog.load_table(table_identifier)
                                
                                asset = {
                                    'name': f"{'.'.join(table_identifier.namespace)}.{table_identifier.name}",
                                    'type': 'iceberg_table',
                                    'source': 'iceberg',
                                    'location': table.location(),
                                    'created_date': datetime.now(),
                                    'size': 0,  # Would need to scan files for size
                                    'schema': {
                                        'columns': [
                                            {
                                                'name': field.name,
                                                'type': str(field.field_type),
                                                'required': field.required,
                                                'doc': field.doc
                                            }
                                            for field in table.schema().fields
                                        ]
                                    },
                                    'metadata': {
                                        'lake_type': 'iceberg',
                                        'catalog_type': catalog_type,
                                        'namespace': '.'.join(table_identifier.namespace),
                                        'table_name': table_identifier.name,
                                        'format_version': table.format_version,
                                        'current_snapshot_id': table.current_snapshot().snapshot_id if table.current_snapshot() else None,
                                        'partition_spec': str(table.spec()) if table.spec() else None
                                    }
                                }
                                assets.append(asset)
                                
                            except NoSuchTableError:
                                self.logger.warning(f"Table {table_identifier} not found")
                            except Exception as e:
                                self.logger.error(f"Error loading Iceberg table {table_identifier}: {e}")
                    except Exception as e:
                        self.logger.error(f"Error listing tables in namespace {namespace}: {e}")
            except Exception as e:
                self.logger.error(f"Error listing namespaces: {e}")
                
        except ImportError:
            self.logger.warning("pyiceberg not installed. Install with: pip install pyiceberg")
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
        
        try:
            from pyspark.sql import SparkSession
            
            spark = SparkSession.builder \
                .appName("HudiDiscovery") \
                .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer") \
                .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.hudi.catalog.HoodieCatalog") \
                .config("spark.sql.extensions", "org.apache.hudi.SparkSQLExtension") \
                .getOrCreate()
            
            base_path = config.get('base_path', '')
            table_paths = config.get('table_paths', [])
            
            if not table_paths and base_path:
                from pathlib import Path
                base_dir = Path(base_path)
                
                if base_dir.exists():
                    for item in base_dir.rglob('.hoodie'):
                        if item.is_dir():
                            table_path = str(item.parent)
                            table_paths.append(table_path)
            
            for table_path in table_paths:
                try:
                    hudi_df = spark.read.format("hudi").load(table_path)
                    
                    table_name = Path(table_path).name
                    
                    schema_info = {
                        'columns': [
                            {
                                'name': field.name,
                                'type': str(field.dataType),
                                'nullable': field.nullable
                            }
                            for field in hudi_df.schema.fields
                        ]
                    }
                    
                    try:
                        row_count = hudi_df.count()
                    except:
                        row_count = 0
                    
                    asset = {
                        'name': table_name,
                        'type': 'hudi_table',
                        'source': 'hudi',
                        'location': table_path,
                        'created_date': datetime.now(),
                        'size': row_count * len(schema_info['columns']) * 100,  # Rough estimate
                        'schema': schema_info,
                        'metadata': {
                            'lake_type': 'hudi',
                            'base_path': base_path,
                            'table_path': table_path,
                            'table_name': table_name,
                            'row_count': row_count,
                            'column_count': len(schema_info['columns'])
                        }
                    }
                    assets.append(asset)
                    
                except Exception as e:
                    self.logger.error(f"Error reading Hudi table {table_path}: {e}")
            
            spark.stop()
            
        except ImportError:
            self.logger.warning("PySpark not installed. Install with: pip install pyspark")
            base_path = config.get('base_path', '')
            
            if base_path:
                from pathlib import Path
                base_dir = Path(base_path)
                
                if base_dir.exists():
                    for item in base_dir.rglob('.hoodie'):
                        if item.is_dir():
                            table_path = str(item.parent)
                            table_name = item.parent.name
                            
                            asset = {
                                'name': table_name,
                                'type': 'hudi_table',
                                'source': 'hudi',
                                'location': table_path,
                                'created_date': datetime.fromtimestamp(item.stat().st_ctime),
                                'size': 0,
                                'metadata': {
                                    'lake_type': 'hudi',
                                    'base_path': base_path,
                                    'table_path': table_path,
                                    'table_name': table_name
                                }
                            }
                            assets.append(asset)
            else:
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
            
            if config.get('scan_recursive', True):
                parquet_files = list(Path(base_path).rglob('*.parquet'))
            else:
                parquet_files = list(Path(base_path).glob('*.parquet'))
            
            for parquet_file in parquet_files:
                try:
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
            from hdfs import InsecureClient
            
            namenode_url = config.get('namenode_url', 'http://localhost:9870')
            user = config.get('user', 'hdfs')
            
            client = InsecureClient(namenode_url, user=user)
            
            paths = config.get('paths', ['/'])
            
            for path in paths:
                try:
                    file_list = client.list(path, status=True)
                    
                    for file_name, file_status in file_list:
                        file_path = f"{path.rstrip('/')}/{file_name}"
                        
                        is_directory = file_status['type'] == 'DIRECTORY'
                        
                        asset = {
                            'name': file_name,
                            'type': 'hdfs_directory' if is_directory else 'hdfs_file',
                            'source': 'hdfs',
                            'location': f"hdfs://{namenode_url.replace('http://', '')}{file_path}",
                            'created_date': datetime.fromtimestamp(file_status['modificationTime'] / 1000),
                            'modified_date': datetime.fromtimestamp(file_status['modificationTime'] / 1000),
                            'size': file_status['length'],
                            'metadata': {
                                'lake_type': 'hdfs',
                                'namenode_url': namenode_url,
                                'path': file_path,
                                'owner': file_status['owner'],
                                'group': file_status['group'],
                                'permission': file_status['permission'],
                                'replication': file_status.get('replication', 0),
                                'block_size': file_status.get('blockSize', 0)
                            }
                        }
                        assets.append(asset)
                        
                        if is_directory and config.get('recursive', False):
                            try:
                                sub_assets = self._discover_hdfs_assets({
                                    **config,
                                    'paths': [file_path],
                                    'recursive': False  # Avoid infinite recursion
                                })
                                assets.extend(sub_assets)
                            except Exception as e:
                                self.logger.warning(f"Error scanning HDFS subdirectory {file_path}: {e}")
                
                except Exception as e:
                    self.logger.error(f"Error listing HDFS path {path}: {e}")
            
        except ImportError:
            self.logger.warning("hdfs library not installed. Install with: pip install hdfs")
            try:
                import subprocess
                import json
                
                namenode = config.get('namenode', 'localhost:9000')
                paths = config.get('paths', ['/'])
                
                for path in paths:
                    try:
                        result = subprocess.run(
                            ['hdfs', 'dfs', '-ls', '-R', path],
                            capture_output=True,
                            text=True,
                            timeout=30
                        )
                        
                        if result.returncode == 0:
                            lines = result.stdout.strip().split('\n')
                            for line in lines:
                                if line.startswith('d') or line.startswith('-'):
                                    parts = line.split()
                                    if len(parts) >= 8:
                                        permissions = parts[0]
                                        owner = parts[2]
                                        group = parts[3]
                                        size = int(parts[4]) if parts[4].isdigit() else 0
                                        file_path = parts[7]
                                        file_name = file_path.split('/')[-1]
                                        
                                        asset = {
                                            'name': file_name,
                                            'type': 'hdfs_directory' if permissions.startswith('d') else 'hdfs_file',
                                            'source': 'hdfs',
                                            'location': f"hdfs://{namenode}{file_path}",
                                            'created_date': datetime.now(),
                                            'size': size,
                                            'metadata': {
                                                'lake_type': 'hdfs',
                                                'namenode': namenode,
                                                'path': file_path,
                                                'owner': owner,
                                                'group': group,
                                                'permissions': permissions
                                            }
                                        }
                                        assets.append(asset)
                        
                    except subprocess.TimeoutExpired:
                        self.logger.error(f"HDFS command timeout for path {path}")
                    except Exception as e:
                        self.logger.error(f"Error running HDFS command for path {path}: {e}")
                        
            except Exception as e:
                self.logger.warning(f"HDFS command line tool not available: {e}")
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
        """Discover Ceph assets using S3-compatible API"""
        assets = []
        
        try:
            import boto3
            from botocore.exceptions import ClientError
            
            s3_client = boto3.client(
                's3',
                endpoint_url=config['endpoint'],
                aws_access_key_id=config['access_key'],
                aws_secret_access_key=config['secret_key'],
                verify=config.get('verify_ssl', True)
            )
            
            response = s3_client.list_buckets()
            
            for bucket in response['Buckets']:
                bucket_name = bucket['Name']
                
                bucket_asset = {
                    'name': bucket_name,
                    'type': 'ceph_bucket',
                    'source': 'ceph',
                    'location': f"ceph://{config['endpoint']}/{bucket_name}",
                    'created_date': bucket['CreationDate'],
                    'size': 0,
                    'metadata': {
                        'lake_type': 'ceph',
                        'endpoint': config['endpoint'],
                        'bucket_name': bucket_name
                    }
                }
                
                try:
                    location = s3_client.get_bucket_location(Bucket=bucket_name)
                    bucket_asset['metadata']['location_constraint'] = location.get('LocationConstraint')
                    
                    try:
                        policy = s3_client.get_bucket_policy(Bucket=bucket_name)
                        bucket_asset['metadata']['has_policy'] = True
                    except ClientError:
                        bucket_asset['metadata']['has_policy'] = False
                    
                    try:
                        versioning = s3_client.get_bucket_versioning(Bucket=bucket_name)
                        bucket_asset['metadata']['versioning_status'] = versioning.get('Status', 'Disabled')
                    except ClientError:
                        bucket_asset['metadata']['versioning_status'] = 'Unknown'
                        
                except Exception as e:
                    self.logger.warning(f"Error getting Ceph bucket metadata for {bucket_name}: {e}")
                
                assets.append(bucket_asset)
                
                try:
                    objects_response = s3_client.list_objects_v2(
                        Bucket=bucket_name,
                        MaxKeys=config.get('max_objects_per_bucket', 100)
                    )
                    
                    total_size = 0
                    object_count = 0
                    
                    for obj in objects_response.get('Contents', []):
                        object_key = obj['Key']
                        
                        if self._is_data_file(object_key):
                            object_asset = {
                                'name': object_key.split('/')[-1],
                                'type': 'ceph_object',
                                'source': 'ceph',
                                'location': f"ceph://{config['endpoint']}/{bucket_name}/{object_key}",
                                'created_date': obj['LastModified'],
                                'modified_date': obj['LastModified'],
                                'size': obj['Size'],
                                'metadata': {
                                    'lake_type': 'ceph',
                                    'endpoint': config['endpoint'],
                                    'bucket_name': bucket_name,
                                    'object_key': object_key,
                                    'storage_class': obj.get('StorageClass', 'STANDARD'),
                                    'etag': obj.get('ETag', '').strip('"')
                                }
                            }
                            assets.append(object_asset)
                            
                        total_size += obj['Size']
                        object_count += 1
                    
                    bucket_asset['size'] = total_size
                    bucket_asset['metadata']['object_count'] = object_count
                    
                except Exception as e:
                    self.logger.warning(f"Error listing objects in Ceph bucket {bucket_name}: {e}")
            
        except ImportError:
            self.logger.warning("boto3 library not installed. Install with: pip install boto3")
        except Exception as e:
            self.logger.error(f"Error connecting to Ceph: {e}")
        
        return assets
    
    def _is_data_file(self, key: str) -> bool:
        """Check if object key represents a data file"""
        data_extensions = {'.csv', '.json', '.parquet', '.avro', '.orc', '.txt', '.tsv', '.xlsx', '.xml', '.yaml', '.yml'}
        return any(key.lower().endswith(ext) for ext in data_extensions)
    
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
                                if Path(path).is_file():
                                    parquet_file = pq.ParquetFile(path)
                                    schema = parquet_file.schema
                                else:
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
                            catalog_config = lake_config.get('catalog', {})
                            self.logger.info("Iceberg connection test successful")
                            connection_tested = True
                        else:
                            self.logger.warning("PyIceberg library not available")
                    
                    elif lake_type == 'hudi':
                        if pyhudi:
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
