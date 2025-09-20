"""
AWS Connector - Discovers data assets in AWS services (S3, RDS, DynamoDB, Redshift)
"""

import boto3
from datetime import datetime
from typing import List, Dict, Any
from botocore.exceptions import ClientError, NoCredentialsError

from .base_connector import BaseConnector


class AWSConnector(BaseConnector):
    """
    Connector for discovering data assets in AWS services
    """
    
    # Metadata for dynamic discovery
    connector_type = "aws"
    connector_name = "Amazon Web Services"
    description = "Discover data assets from AWS services including S3, RDS, DynamoDB, Redshift, Athena, and Glue"
    category = "cloud_providers"
    supported_services = ["S3", "RDS", "DynamoDB", "Redshift", "Athena", "Glue"]
    required_config_fields = ["region"]
    optional_config_fields = ["access_key", "secret_key", "services"]
    
    def __init__(self, config: Dict[str, Any]):
        super().__init__(config)
        self.regions = config.get('regions', ['us-east-1'])
        self.services = config.get('services', [
            's3', 'rds', 'dynamodb', 'redshift', 'athena', 'glue', 
            'emr', 'kinesis', 'msk', 'documentdb', 'neptune', 
            'timestream', 'qldb', 'opensearch', 'elasticache'
        ])
        
        # Initialize AWS session
        try:
            self.session = boto3.Session()
            self.logger.info("AWS session initialized successfully")
        except NoCredentialsError:
            self.logger.error("AWS credentials not found")
            self.session = None
    
    def discover_assets(self) -> List[Dict[str, Any]]:
        """Discover AWS data assets"""
        if not self.session:
            self.logger.error("AWS session not available")
            return []
        
        self.logger.info("Starting AWS asset discovery")
        assets = []
        
        for region in self.regions:
            self.logger.info(f"Scanning AWS region: {region}")
            
            if 's3' in self.services:
                assets.extend(self._discover_s3_assets(region))
            
            if 'rds' in self.services:
                assets.extend(self._discover_rds_assets(region))
            
            if 'dynamodb' in self.services:
                assets.extend(self._discover_dynamodb_assets(region))
            
            if 'redshift' in self.services:
                assets.extend(self._discover_redshift_assets(region))
            
            if 'athena' in self.services:
                assets.extend(self._discover_athena_assets(region))
            
            if 'glue' in self.services:
                assets.extend(self._discover_glue_assets(region))
            
            if 'emr' in self.services:
                assets.extend(self._discover_emr_assets(region))
            
            if 'kinesis' in self.services:
                assets.extend(self._discover_kinesis_assets(region))
            
            if 'msk' in self.services:
                assets.extend(self._discover_msk_assets(region))
            
            if 'documentdb' in self.services:
                assets.extend(self._discover_documentdb_assets(region))
            
            if 'neptune' in self.services:
                assets.extend(self._discover_neptune_assets(region))
            
            if 'timestream' in self.services:
                assets.extend(self._discover_timestream_assets(region))
            
            if 'qldb' in self.services:
                assets.extend(self._discover_qldb_assets(region))
            
            if 'opensearch' in self.services:
                assets.extend(self._discover_opensearch_assets(region))
            
            if 'elasticache' in self.services:
                assets.extend(self._discover_elasticache_assets(region))
        
        self.logger.info(f"Discovered {len(assets)} AWS assets")
        return assets
    
    def _discover_s3_assets(self, region: str) -> List[Dict[str, Any]]:
        """Discover S3 buckets and objects"""
        assets = []
        
        try:
            s3_client = self.session.client('s3', region_name=region)
            
            # List all buckets
            response = s3_client.list_buckets()
            
            for bucket in response.get('Buckets', []):
                bucket_name = bucket['Name']
                
                try:
                    # Get bucket location
                    location_response = s3_client.get_bucket_location(Bucket=bucket_name)
                    bucket_region = location_response.get('LocationConstraint') or 'us-east-1'
                    
                    # Only process buckets in the current region
                    if bucket_region != region and region != 'us-east-1':
                        continue
                    
                    # Create bucket asset
                    bucket_asset = {
                        'name': bucket_name,
                        'type': 's3_bucket',
                        'source': 'aws_s3',
                        'location': f"s3://{bucket_name}",
                        'region': bucket_region,
                        'created_date': bucket['CreationDate'],
                        'modified_date': bucket['CreationDate'],
                        'size': 0,
                        'schema': {},
                        'tags': ['aws', 's3', 'bucket'],
                        'metadata': {
                            'service': 's3',
                            'resource_type': 'bucket',
                            'region': bucket_region
                        }
                    }
                    
                    # Get bucket metadata
                    self._enrich_s3_bucket_metadata(s3_client, bucket_asset, bucket_name)
                    assets.append(bucket_asset)
                    
                    # Discover objects in bucket (sample)
                    object_assets = self._discover_s3_objects(s3_client, bucket_name, bucket_region)
                    assets.extend(object_assets[:100])  # Limit to first 100 objects per bucket
                    
                except ClientError as e:
                    self.logger.warning(f"Error accessing S3 bucket {bucket_name}: {e}")
                    continue
                    
        except ClientError as e:
            self.logger.error(f"Error discovering S3 assets in region {region}: {e}")
        
        return assets
    
    def _discover_s3_objects(self, s3_client, bucket_name: str, region: str) -> List[Dict[str, Any]]:
        """Discover objects in S3 bucket"""
        assets = []
        
        try:
            paginator = s3_client.get_paginator('list_objects_v2')
            page_iterator = paginator.paginate(Bucket=bucket_name, MaxKeys=100)
            
            for page in page_iterator:
                for obj in page.get('Contents', []):
                    # Filter for data files
                    key = obj['Key']
                    if not self._is_data_file(key):
                        continue
                    
                    object_asset = {
                        'name': key.split('/')[-1],  # Just the filename
                        'type': self._determine_s3_object_type(key),
                        'source': 'aws_s3',
                        'location': f"s3://{bucket_name}/{key}",
                        'region': region,
                        'size': obj['Size'],
                        'created_date': obj['LastModified'],
                        'modified_date': obj['LastModified'],
                        'schema': {},
                        'tags': ['aws', 's3', 'object'] + self._get_s3_object_tags(key),
                        'metadata': {
                            'service': 's3',
                            'resource_type': 'object',
                            'bucket': bucket_name,
                            'key': key,
                            'region': region,
                            'storage_class': obj.get('StorageClass', 'STANDARD'),
                            'etag': obj.get('ETag', '').strip('"')
                        }
                    }
                    
                    assets.append(object_asset)
                    
        except ClientError as e:
            self.logger.warning(f"Error listing objects in bucket {bucket_name}: {e}")
        
        return assets
    
    def _discover_rds_assets(self, region: str) -> List[Dict[str, Any]]:
        """Discover RDS databases"""
        assets = []
        
        try:
            rds_client = self.session.client('rds', region_name=region)
            
            # Discover RDS instances
            response = rds_client.describe_db_instances()
            
            for db_instance in response.get('DBInstances', []):
                db_asset = {
                    'name': db_instance['DBInstanceIdentifier'],
                    'type': 'rds_database',
                    'source': 'aws_rds',
                    'location': db_instance.get('Endpoint', {}).get('Address', ''),
                    'region': region,
                    'size': db_instance.get('AllocatedStorage', 0) * 1024 * 1024 * 1024,  # Convert GB to bytes
                    'created_date': db_instance.get('InstanceCreateTime'),
                    'modified_date': db_instance.get('InstanceCreateTime'),
                    'schema': {},
                    'tags': ['aws', 'rds', 'database', db_instance.get('Engine', '')],
                    'metadata': {
                        'service': 'rds',
                        'resource_type': 'db_instance',
                        'engine': db_instance.get('Engine'),
                        'engine_version': db_instance.get('EngineVersion'),
                        'instance_class': db_instance.get('DBInstanceClass'),
                        'status': db_instance.get('DBInstanceStatus'),
                        'port': db_instance.get('Endpoint', {}).get('Port'),
                        'multi_az': db_instance.get('MultiAZ'),
                        'region': region
                    }
                }
                
                assets.append(db_asset)
                
        except ClientError as e:
            self.logger.error(f"Error discovering RDS assets in region {region}: {e}")
        
        return assets
    
    def _discover_dynamodb_assets(self, region: str) -> List[Dict[str, Any]]:
        """Discover DynamoDB tables"""
        assets = []
        
        try:
            dynamodb_client = self.session.client('dynamodb', region_name=region)
            
            # List tables
            response = dynamodb_client.list_tables()
            
            for table_name in response.get('TableNames', []):
                try:
                    # Get table description
                    table_desc = dynamodb_client.describe_table(TableName=table_name)
                    table_info = table_desc['Table']
                    
                    table_asset = {
                        'name': table_name,
                        'type': 'dynamodb_table',
                        'source': 'aws_dynamodb',
                        'location': f"dynamodb://{region}/{table_name}",
                        'region': region,
                        'size': table_info.get('TableSizeBytes', 0),
                        'created_date': table_info.get('CreationDateTime'),
                        'modified_date': table_info.get('CreationDateTime'),
                        'schema': {
                            'key_schema': table_info.get('KeySchema', []),
                            'attribute_definitions': table_info.get('AttributeDefinitions', [])
                        },
                        'tags': ['aws', 'dynamodb', 'nosql'],
                        'metadata': {
                            'service': 'dynamodb',
                            'resource_type': 'table',
                            'status': table_info.get('TableStatus'),
                            'item_count': table_info.get('ItemCount', 0),
                            'provisioned_throughput': table_info.get('ProvisionedThroughput', {}),
                            'billing_mode': table_info.get('BillingModeSummary', {}).get('BillingMode'),
                            'region': region
                        }
                    }
                    
                    assets.append(table_asset)
                    
                except ClientError as e:
                    self.logger.warning(f"Error describing DynamoDB table {table_name}: {e}")
                    continue
                    
        except ClientError as e:
            self.logger.error(f"Error discovering DynamoDB assets in region {region}: {e}")
        
        return assets
    
    def _discover_redshift_assets(self, region: str) -> List[Dict[str, Any]]:
        """Discover Redshift clusters"""
        assets = []
        
        try:
            redshift_client = self.session.client('redshift', region_name=region)
            
            # Describe clusters
            response = redshift_client.describe_clusters()
            
            for cluster in response.get('Clusters', []):
                cluster_asset = {
                    'name': cluster['ClusterIdentifier'],
                    'type': 'redshift_cluster',
                    'source': 'aws_redshift',
                    'location': f"{cluster.get('Endpoint', {}).get('Address', '')}:{cluster.get('Endpoint', {}).get('Port', 5439)}",
                    'region': region,
                    'size': cluster.get('TotalStorageCapacityInMegaBytes', 0) * 1024 * 1024,  # Convert MB to bytes
                    'created_date': cluster.get('ClusterCreateTime'),
                    'modified_date': cluster.get('ClusterCreateTime'),
                    'schema': {},
                    'tags': ['aws', 'redshift', 'datawarehouse'],
                    'metadata': {
                        'service': 'redshift',
                        'resource_type': 'cluster',
                        'status': cluster.get('ClusterStatus'),
                        'node_type': cluster.get('NodeType'),
                        'number_of_nodes': cluster.get('NumberOfNodes'),
                        'database_name': cluster.get('DBName'),
                        'master_username': cluster.get('MasterUsername'),
                        'region': region
                    }
                }
                
                assets.append(cluster_asset)
                
        except ClientError as e:
            self.logger.error(f"Error discovering Redshift assets in region {region}: {e}")
        
        return assets
    
    def _enrich_s3_bucket_metadata(self, s3_client, bucket_asset: Dict, bucket_name: str):
        """Enrich S3 bucket with additional metadata"""
        try:
            # Get bucket policy
            try:
                policy_response = s3_client.get_bucket_policy(Bucket=bucket_name)
                bucket_asset['metadata']['has_policy'] = True
            except ClientError:
                bucket_asset['metadata']['has_policy'] = False
            
            # Get bucket encryption
            try:
                encryption_response = s3_client.get_bucket_encryption(Bucket=bucket_name)
                bucket_asset['metadata']['encrypted'] = True
                bucket_asset['metadata']['encryption_config'] = encryption_response.get('ServerSideEncryptionConfiguration')
            except ClientError:
                bucket_asset['metadata']['encrypted'] = False
            
            # Get bucket versioning
            try:
                versioning_response = s3_client.get_bucket_versioning(Bucket=bucket_name)
                bucket_asset['metadata']['versioning_status'] = versioning_response.get('Status', 'Disabled')
            except ClientError:
                bucket_asset['metadata']['versioning_status'] = 'Unknown'
            
        except Exception as e:
            self.logger.warning(f"Error enriching S3 bucket metadata for {bucket_name}: {e}")
    
    def _is_data_file(self, key: str) -> bool:
        """Check if S3 object is a data file"""
        data_extensions = {'.csv', '.json', '.parquet', '.avro', '.orc', '.txt', '.tsv', '.xlsx', '.xml', '.yaml', '.yml'}
        return any(key.lower().endswith(ext) for ext in data_extensions)
    
    def _determine_s3_object_type(self, key: str) -> str:
        """Determine S3 object type based on extension"""
        key_lower = key.lower()
        
        if key_lower.endswith('.csv'):
            return 's3_csv_file'
        elif key_lower.endswith('.json'):
            return 's3_json_file'
        elif key_lower.endswith('.parquet'):
            return 's3_parquet_file'
        elif key_lower.endswith(('.xlsx', '.xls')):
            return 's3_excel_file'
        elif key_lower.endswith('.avro'):
            return 's3_avro_file'
        elif key_lower.endswith('.orc'):
            return 's3_orc_file'
        else:
            return 's3_data_file'
    
    def _get_s3_object_tags(self, key: str) -> List[str]:
        """Generate tags for S3 object"""
        tags = []
        
        # Add extension-based tag
        if '.' in key:
            ext = key.split('.')[-1].lower()
            tags.append(f"ext_{ext}")
        
        # Add path-based tags
        if '/' in key:
            path_parts = key.split('/')
            for part in path_parts[:-1]:  # Exclude filename
                if part.lower() in ['data', 'raw', 'processed', 'archive', 'backup']:
                    tags.append(f"folder_{part.lower()}")
        
        return tags
    
    def test_connection(self) -> bool:
        """Test AWS connection"""
        if not self.session:
            return False
        
        try:
            # Test with a simple STS call to get caller identity
            sts_client = self.session.client('sts')
            sts_client.get_caller_identity()
            self.logger.info("AWS connection test successful")
            return True
        except Exception as e:
            self.logger.error(f"AWS connection test failed: {e}")
            return False
    
    def validate_config(self) -> bool:
        """Validate AWS connector configuration"""
        if not self.regions:
            self.logger.error("No AWS regions configured")
            return False
        
        if not self.services:
            self.logger.error("No AWS services configured")
            return False
        
        # AWS credentials are typically handled by boto3 through environment variables,
        # IAM roles, or credential files, so we don't need to validate specific fields
        # But we can check if boto3 can create a session
        try:
            import boto3
            session = boto3.Session()
            # Test if we can get credentials
            credentials = session.get_credentials()
            if not credentials:
                self.logger.warning("No AWS credentials found. Make sure AWS credentials are configured via environment variables, IAM roles, or credential files.")
                return False
        except Exception as e:
            self.logger.error(f"Error validating AWS credentials: {e}")
            return False
        
        return True
    
    def _discover_athena_assets(self, region: str) -> List[Dict[str, Any]]:
        """Discover Amazon Athena assets"""
        assets = []
        try:
            athena_client = self.session.client('athena', region_name=region)
            
            # List workgroups
            response = athena_client.list_work_groups()
            for workgroup in response.get('WorkGroups', []):
                asset = {
                    'name': workgroup['Name'],
                    'type': 'athena_workgroup',
                    'source': 'aws_athena',
                    'location': f"athena://{region}/{workgroup['Name']}",
                    'region': region,
                    'created_date': workgroup.get('CreationTime', datetime.now()),
                    'size': 0,
                    'metadata': {
                        'service': 'athena',
                        'resource_type': 'workgroup',
                        'region': region,
                        'state': workgroup.get('State', 'UNKNOWN')
                    }
                }
                assets.append(asset)
                
        except Exception as e:
            self.logger.error(f"Error discovering Athena assets in {region}: {e}")
        
        return assets
    
    def _discover_glue_assets(self, region: str) -> List[Dict[str, Any]]:
        """Discover AWS Glue assets"""
        assets = []
        try:
            glue_client = self.session.client('glue', region_name=region)
            
            # List databases
            response = glue_client.get_databases()
            for database in response.get('DatabaseList', []):
                asset = {
                    'name': database['Name'],
                    'type': 'glue_database',
                    'source': 'aws_glue',
                    'location': f"glue://{region}/{database['Name']}",
                    'region': region,
                    'created_date': database.get('CreateTime', datetime.now()),
                    'size': 0,
                    'metadata': {
                        'service': 'glue',
                        'resource_type': 'database',
                        'region': region,
                        'description': database.get('Description', '')
                    }
                }
                assets.append(asset)
                
                # List tables in database
                try:
                    tables_response = glue_client.get_tables(DatabaseName=database['Name'])
                    for table in tables_response.get('TableList', []):
                        table_asset = {
                            'name': f"{database['Name']}.{table['Name']}",
                            'type': 'glue_table',
                            'source': 'aws_glue',
                            'location': f"glue://{region}/{database['Name']}/{table['Name']}",
                            'region': region,
                            'created_date': table.get('CreateTime', datetime.now()),
                            'size': 0,
                            'metadata': {
                                'service': 'glue',
                                'resource_type': 'table',
                                'region': region,
                                'database': database['Name'],
                                'columns': len(table.get('StorageDescriptor', {}).get('Columns', []))
                            }
                        }
                        assets.append(table_asset)
                except Exception as e:
                    self.logger.error(f"Error listing tables for database {database['Name']}: {e}")
                    
        except Exception as e:
            self.logger.error(f"Error discovering Glue assets in {region}: {e}")
        
        return assets
    
    def _discover_emr_assets(self, region: str) -> List[Dict[str, Any]]:
        """Discover Amazon EMR assets"""
        assets = []
        try:
            emr_client = self.session.client('emr', region_name=region)
            
            # List clusters
            response = emr_client.list_clusters()
            for cluster in response.get('Clusters', []):
                asset = {
                    'name': cluster['Name'],
                    'type': 'emr_cluster',
                    'source': 'aws_emr',
                    'location': f"emr://{region}/{cluster['Id']}",
                    'region': region,
                    'created_date': cluster.get('Status', {}).get('Timeline', {}).get('CreationDateTime', datetime.now()),
                    'size': 0,
                    'metadata': {
                        'service': 'emr',
                        'resource_type': 'cluster',
                        'region': region,
                        'cluster_id': cluster['Id'],
                        'state': cluster.get('Status', {}).get('State', 'UNKNOWN')
                    }
                }
                assets.append(asset)
                
        except Exception as e:
            self.logger.error(f"Error discovering EMR assets in {region}: {e}")
        
        return assets
    
    def _discover_kinesis_assets(self, region: str) -> List[Dict[str, Any]]:
        """Discover Amazon Kinesis assets"""
        assets = []
        try:
            kinesis_client = self.session.client('kinesis', region_name=region)
            
            # List streams
            response = kinesis_client.list_streams()
            for stream_name in response.get('StreamNames', []):
                try:
                    stream_info = kinesis_client.describe_stream(StreamName=stream_name)
                    stream_desc = stream_info['StreamDescription']
                    
                    asset = {
                        'name': stream_name,
                        'type': 'kinesis_stream',
                        'source': 'aws_kinesis',
                        'location': f"kinesis://{region}/{stream_name}",
                        'region': region,
                        'created_date': stream_desc.get('StreamCreationTimestamp', datetime.now()),
                        'size': 0,
                        'metadata': {
                            'service': 'kinesis',
                            'resource_type': 'stream',
                            'region': region,
                            'status': stream_desc.get('StreamStatus', 'UNKNOWN'),
                            'shards': len(stream_desc.get('Shards', []))
                        }
                    }
                    assets.append(asset)
                except Exception as e:
                    self.logger.error(f"Error describing Kinesis stream {stream_name}: {e}")
                
        except Exception as e:
            self.logger.error(f"Error discovering Kinesis assets in {region}: {e}")
        
        return assets
    
    def _discover_msk_assets(self, region: str) -> List[Dict[str, Any]]:
        """Discover Amazon MSK (Managed Streaming for Kafka) assets"""
        assets = []
        try:
            msk_client = self.session.client('kafka', region_name=region)
            
            # List clusters
            response = msk_client.list_clusters()
            for cluster in response.get('ClusterInfoList', []):
                asset = {
                    'name': cluster['ClusterName'],
                    'type': 'msk_cluster',
                    'source': 'aws_msk',
                    'location': f"msk://{region}/{cluster['ClusterArn']}",
                    'region': region,
                    'created_date': cluster.get('CreationTime', datetime.now()),
                    'size': 0,
                    'metadata': {
                        'service': 'msk',
                        'resource_type': 'cluster',
                        'region': region,
                        'cluster_arn': cluster['ClusterArn'],
                        'state': cluster.get('State', 'UNKNOWN')
                    }
                }
                assets.append(asset)
                
        except Exception as e:
            self.logger.error(f"Error discovering MSK assets in {region}: {e}")
        
        return assets
    
    def _discover_documentdb_assets(self, region: str) -> List[Dict[str, Any]]:
        """Discover Amazon DocumentDB assets"""
        assets = []
        try:
            docdb_client = self.session.client('docdb', region_name=region)
            
            # List clusters
            response = docdb_client.describe_db_clusters()
            for cluster in response.get('DBClusters', []):
                asset = {
                    'name': cluster['DBClusterIdentifier'],
                    'type': 'documentdb_cluster',
                    'source': 'aws_documentdb',
                    'location': f"docdb://{cluster['Endpoint']}",
                    'region': region,
                    'created_date': cluster.get('ClusterCreateTime', datetime.now()),
                    'size': 0,
                    'metadata': {
                        'service': 'documentdb',
                        'resource_type': 'cluster',
                        'region': region,
                        'engine': cluster.get('Engine', ''),
                        'status': cluster.get('Status', 'UNKNOWN')
                    }
                }
                assets.append(asset)
                
        except Exception as e:
            self.logger.error(f"Error discovering DocumentDB assets in {region}: {e}")
        
        return assets
    
    def _discover_neptune_assets(self, region: str) -> List[Dict[str, Any]]:
        """Discover Amazon Neptune assets"""
        assets = []
        try:
            neptune_client = self.session.client('neptune', region_name=region)
            
            # List clusters
            response = neptune_client.describe_db_clusters()
            for cluster in response.get('DBClusters', []):
                asset = {
                    'name': cluster['DBClusterIdentifier'],
                    'type': 'neptune_cluster',
                    'source': 'aws_neptune',
                    'location': f"neptune://{cluster['Endpoint']}",
                    'region': region,
                    'created_date': cluster.get('ClusterCreateTime', datetime.now()),
                    'size': 0,
                    'metadata': {
                        'service': 'neptune',
                        'resource_type': 'cluster',
                        'region': region,
                        'engine': cluster.get('Engine', ''),
                        'status': cluster.get('Status', 'UNKNOWN')
                    }
                }
                assets.append(asset)
                
        except Exception as e:
            self.logger.error(f"Error discovering Neptune assets in {region}: {e}")
        
        return assets
    
    def _discover_timestream_assets(self, region: str) -> List[Dict[str, Any]]:
        """Discover Amazon Timestream assets"""
        assets = []
        try:
            timestream_client = self.session.client('timestream-query', region_name=region)
            
            # List databases
            response = timestream_client.list_databases()
            for database in response.get('Databases', []):
                asset = {
                    'name': database['DatabaseName'],
                    'type': 'timestream_database',
                    'source': 'aws_timestream',
                    'location': f"timestream://{region}/{database['DatabaseName']}",
                    'region': region,
                    'created_date': database.get('CreationTime', datetime.now()),
                    'size': 0,
                    'metadata': {
                        'service': 'timestream',
                        'resource_type': 'database',
                        'region': region
                    }
                }
                assets.append(asset)
                
        except Exception as e:
            self.logger.error(f"Error discovering Timestream assets in {region}: {e}")
        
        return assets
    
    def _discover_qldb_assets(self, region: str) -> List[Dict[str, Any]]:
        """Discover Amazon QLDB assets"""
        assets = []
        try:
            qldb_client = self.session.client('qldb', region_name=region)
            
            # List ledgers
            response = qldb_client.list_ledgers()
            for ledger in response.get('Ledgers', []):
                asset = {
                    'name': ledger['Name'],
                    'type': 'qldb_ledger',
                    'source': 'aws_qldb',
                    'location': f"qldb://{region}/{ledger['Name']}",
                    'region': region,
                    'created_date': ledger.get('CreationDateTime', datetime.now()),
                    'size': 0,
                    'metadata': {
                        'service': 'qldb',
                        'resource_type': 'ledger',
                        'region': region,
                        'state': ledger.get('State', 'UNKNOWN')
                    }
                }
                assets.append(asset)
                
        except Exception as e:
            self.logger.error(f"Error discovering QLDB assets in {region}: {e}")
        
        return assets
    
    def _discover_opensearch_assets(self, region: str) -> List[Dict[str, Any]]:
        """Discover Amazon OpenSearch assets"""
        assets = []
        try:
            opensearch_client = self.session.client('opensearch', region_name=region)
            
            # List domains
            response = opensearch_client.list_domain_names()
            for domain in response.get('DomainNames', []):
                try:
                    domain_info = opensearch_client.describe_domain(DomainName=domain['DomainName'])
                    domain_status = domain_info['DomainStatus']
                    
                    asset = {
                        'name': domain['DomainName'],
                        'type': 'opensearch_domain',
                        'source': 'aws_opensearch',
                        'location': f"opensearch://{domain_status.get('Endpoint', '')}",
                        'region': region,
                        'created_date': domain_status.get('Created', datetime.now()),
                        'size': 0,
                        'metadata': {
                            'service': 'opensearch',
                            'resource_type': 'domain',
                            'region': region,
                            'engine_version': domain_status.get('EngineVersion', ''),
                            'processing': domain_status.get('Processing', False)
                        }
                    }
                    assets.append(asset)
                except Exception as e:
                    self.logger.error(f"Error describing OpenSearch domain {domain['DomainName']}: {e}")
                
        except Exception as e:
            self.logger.error(f"Error discovering OpenSearch assets in {region}: {e}")
        
        return assets
    
    def _discover_elasticache_assets(self, region: str) -> List[Dict[str, Any]]:
        """Discover Amazon ElastiCache assets"""
        assets = []
        try:
            elasticache_client = self.session.client('elasticache', region_name=region)
            
            # List cache clusters
            response = elasticache_client.describe_cache_clusters()
            for cluster in response.get('CacheClusters', []):
                asset = {
                    'name': cluster['CacheClusterId'],
                    'type': 'elasticache_cluster',
                    'source': 'aws_elasticache',
                    'location': f"elasticache://{region}/{cluster['CacheClusterId']}",
                    'region': region,
                    'created_date': cluster.get('CacheClusterCreateTime', datetime.now()),
                    'size': 0,
                    'metadata': {
                        'service': 'elasticache',
                        'resource_type': 'cluster',
                        'region': region,
                        'engine': cluster.get('Engine', ''),
                        'status': cluster.get('CacheClusterStatus', 'UNKNOWN')
                    }
                }
                assets.append(asset)
                
        except Exception as e:
            self.logger.error(f"Error discovering ElastiCache assets in {region}: {e}")
        
        return assets
