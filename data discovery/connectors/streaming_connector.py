"""
Streaming Connector - Discovers data assets in streaming platforms
"""

from datetime import datetime
from typing import List, Dict, Any, Optional
import logging

from .base_connector import BaseConnector

# Import streaming libraries with fallbacks
try:
    from kafka import KafkaConsumer, KafkaAdminClient
    from kafka.admin import ConfigResource, ConfigResourceType
except ImportError:
    KafkaConsumer = None
    KafkaAdminClient = None

try:
    import pulsar
except ImportError:
    pulsar = None

try:
    import pika
except ImportError:
    pika = None

try:
    import boto3
except ImportError:
    boto3 = None

try:
    from azure.eventhub import EventHubConsumerClient
    from azure.servicebus import ServiceBusClient
except ImportError:
    EventHubConsumerClient = None
    ServiceBusClient = None


class StreamingConnector(BaseConnector):
    """
    Connector for discovering data assets in streaming platforms
    """
    
    def __init__(self, config: Dict[str, Any]):
        super().__init__(config)
        self.streaming_platforms = config.get('streaming_connections', [])
    
    def discover_assets(self) -> List[Dict[str, Any]]:
        """Discover streaming platform assets"""
        self.logger.info("Starting streaming platform asset discovery")
        assets = []
        
        for streaming_config in self.streaming_platforms:
            platform_type = streaming_config.get('type', '').lower()
            
            try:
                if platform_type == 'kafka':
                    assets.extend(self._discover_kafka_assets(streaming_config))
                elif platform_type == 'pulsar':
                    assets.extend(self._discover_pulsar_assets(streaming_config))
                elif platform_type == 'rabbitmq':
                    assets.extend(self._discover_rabbitmq_assets(streaming_config))
                elif platform_type == 'kinesis':
                    assets.extend(self._discover_kinesis_assets(streaming_config))
                elif platform_type == 'eventhub':
                    assets.extend(self._discover_eventhub_assets(streaming_config))
                elif platform_type == 'servicebus':
                    assets.extend(self._discover_servicebus_assets(streaming_config))
                elif platform_type == 'pubsub':
                    assets.extend(self._discover_pubsub_assets(streaming_config))
                elif platform_type == 'nats':
                    assets.extend(self._discover_nats_assets(streaming_config))
                elif platform_type == 'redis_streams':
                    assets.extend(self._discover_redis_streams_assets(streaming_config))
                else:
                    self.logger.warning(f"Unsupported streaming platform type: {platform_type}")
                    
            except Exception as e:
                self.logger.error(f"Error discovering assets from {platform_type} platform: {e}")
        
        self.logger.info(f"Discovered {len(assets)} streaming platform assets")
        return assets
    
    def _discover_kafka_assets(self, config: Dict[str, Any]) -> List[Dict[str, Any]]:
        """Discover Apache Kafka assets"""
        assets = []
        if not KafkaAdminClient:
            self.logger.warning("kafka-python not installed")
            return assets
        
        try:
            admin_client = KafkaAdminClient(
                bootstrap_servers=config['bootstrap_servers'],
                security_protocol=config.get('security_protocol', 'PLAINTEXT'),
                sasl_mechanism=config.get('sasl_mechanism'),
                sasl_plain_username=config.get('username'),
                sasl_plain_password=config.get('password')
            )
            
            # Get topic metadata
            metadata = admin_client.describe_topics()
            
            for topic_name, topic_metadata in metadata.items():
                asset = {
                    'name': topic_name,
                    'type': 'kafka_topic',
                    'source': 'kafka',
                    'location': f"kafka://{config['bootstrap_servers']}/{topic_name}",
                    'created_date': datetime.now(),
                    'size': 0,
                    'metadata': {
                        'platform_type': 'kafka',
                        'partitions': len(topic_metadata.partitions),
                        'replication_factor': len(topic_metadata.partitions[0].replicas) if topic_metadata.partitions else 0,
                        'bootstrap_servers': config['bootstrap_servers']
                    }
                }
                assets.append(asset)
            
            admin_client.close()
            
        except Exception as e:
            self.logger.error(f"Error connecting to Kafka: {e}")
        
        return assets
    
    def _discover_pulsar_assets(self, config: Dict[str, Any]) -> List[Dict[str, Any]]:
        """Discover Apache Pulsar assets"""
        assets = []
        if not pulsar:
            self.logger.warning("pulsar-client not installed")
            return assets
        
        try:
            client = pulsar.Client(
                service_url=config['service_url'],
                authentication=pulsar.AuthenticationToken(config.get('token')) if config.get('token') else None
            )
            
            # Get topics (this would require admin API)
            # Placeholder for topic discovery
            topics = config.get('topics', [])
            
            for topic in topics:
                asset = {
                    'name': topic,
                    'type': 'pulsar_topic',
                    'source': 'pulsar',
                    'location': f"pulsar://{config['service_url']}/{topic}",
                    'created_date': datetime.now(),
                    'size': 0,
                    'metadata': {
                        'platform_type': 'pulsar',
                        'service_url': config['service_url']
                    }
                }
                assets.append(asset)
            
            client.close()
            
        except Exception as e:
            self.logger.error(f"Error connecting to Pulsar: {e}")
        
        return assets
    
    def _discover_rabbitmq_assets(self, config: Dict[str, Any]) -> List[Dict[str, Any]]:
        """Discover RabbitMQ assets"""
        assets = []
        if not pika:
            self.logger.warning("pika not installed")
            return assets
        
        try:
            credentials = pika.PlainCredentials(
                config.get('username', 'guest'),
                config.get('password', 'guest')
            )
            
            connection = pika.BlockingConnection(
                pika.ConnectionParameters(
                    host=config['host'],
                    port=config.get('port', 5672),
                    credentials=credentials
                )
            )
            
            channel = connection.channel()
            
            # Get queue information (this requires management plugin)
            # Placeholder for queue discovery
            queues = config.get('queues', [])
            
            for queue in queues:
                asset = {
                    'name': queue,
                    'type': 'rabbitmq_queue',
                    'source': 'rabbitmq',
                    'location': f"rabbitmq://{config['host']}/{queue}",
                    'created_date': datetime.now(),
                    'size': 0,
                    'metadata': {
                        'platform_type': 'rabbitmq',
                        'host': config['host'],
                        'port': config.get('port', 5672)
                    }
                }
                assets.append(asset)
            
            connection.close()
            
        except Exception as e:
            self.logger.error(f"Error connecting to RabbitMQ: {e}")
        
        return assets
    
    def _discover_kinesis_assets(self, config: Dict[str, Any]) -> List[Dict[str, Any]]:
        """Discover Amazon Kinesis assets"""
        assets = []
        if not boto3:
            self.logger.warning("boto3 not installed")
            return assets
        
        try:
            kinesis_client = boto3.client(
                'kinesis',
                region_name=config.get('region', 'us-east-1'),
                aws_access_key_id=config.get('access_key_id'),
                aws_secret_access_key=config.get('secret_access_key')
            )
            
            # List streams
            response = kinesis_client.list_streams()
            
            for stream_name in response['StreamNames']:
                try:
                    stream_info = kinesis_client.describe_stream(StreamName=stream_name)
                    stream_desc = stream_info['StreamDescription']
                    
                    asset = {
                        'name': stream_name,
                        'type': 'kinesis_stream',
                        'source': 'kinesis',
                        'location': f"kinesis://{config.get('region', 'us-east-1')}/{stream_name}",
                        'created_date': stream_desc.get('StreamCreationTimestamp', datetime.now()),
                        'size': 0,
                        'metadata': {
                            'platform_type': 'kinesis',
                            'status': stream_desc.get('StreamStatus', 'UNKNOWN'),
                            'shards': len(stream_desc.get('Shards', [])),
                            'region': config.get('region', 'us-east-1')
                        }
                    }
                    assets.append(asset)
                    
                except Exception as e:
                    self.logger.error(f"Error describing Kinesis stream {stream_name}: {e}")
            
        except Exception as e:
            self.logger.error(f"Error connecting to Kinesis: {e}")
        
        return assets
    
    def _discover_eventhub_assets(self, config: Dict[str, Any]) -> List[Dict[str, Any]]:
        """Discover Azure Event Hub assets"""
        assets = []
        if not EventHubConsumerClient:
            self.logger.warning("azure-eventhub not installed")
            return assets
        
        try:
            # Get event hubs (this would require management API)
            # Placeholder for event hub discovery
            event_hubs = config.get('event_hubs', [])
            
            for hub_name in event_hubs:
                asset = {
                    'name': hub_name,
                    'type': 'eventhub_hub',
                    'source': 'eventhub',
                    'location': f"eventhub://{config.get('namespace', '')}/{hub_name}",
                    'created_date': datetime.now(),
                    'size': 0,
                    'metadata': {
                        'platform_type': 'eventhub',
                        'namespace': config.get('namespace', ''),
                        'connection_string': config.get('connection_string', '')[:20] + '...' if config.get('connection_string') else ''
                    }
                }
                assets.append(asset)
            
        except Exception as e:
            self.logger.error(f"Error connecting to Event Hub: {e}")
        
        return assets
    
    def _discover_servicebus_assets(self, config: Dict[str, Any]) -> List[Dict[str, Any]]:
        """Discover Azure Service Bus assets"""
        assets = []
        if not ServiceBusClient:
            self.logger.warning("azure-servicebus not installed")
            return assets
        
        try:
            # Get queues and topics (this would require management API)
            # Placeholder for service bus discovery
            queues = config.get('queues', [])
            topics = config.get('topics', [])
            
            for queue_name in queues:
                asset = {
                    'name': queue_name,
                    'type': 'servicebus_queue',
                    'source': 'servicebus',
                    'location': f"servicebus://{config.get('namespace', '')}/{queue_name}",
                    'created_date': datetime.now(),
                    'size': 0,
                    'metadata': {
                        'platform_type': 'servicebus',
                        'resource_type': 'queue',
                        'namespace': config.get('namespace', '')
                    }
                }
                assets.append(asset)
            
            for topic_name in topics:
                asset = {
                    'name': topic_name,
                    'type': 'servicebus_topic',
                    'source': 'servicebus',
                    'location': f"servicebus://{config.get('namespace', '')}/{topic_name}",
                    'created_date': datetime.now(),
                    'size': 0,
                    'metadata': {
                        'platform_type': 'servicebus',
                        'resource_type': 'topic',
                        'namespace': config.get('namespace', '')
                    }
                }
                assets.append(asset)
            
        except Exception as e:
            self.logger.error(f"Error connecting to Service Bus: {e}")
        
        return assets
    
    def _discover_pubsub_assets(self, config: Dict[str, Any]) -> List[Dict[str, Any]]:
        """Discover Google Cloud Pub/Sub assets"""
        assets = []
        
        try:
            from google.cloud import pubsub_v1
            
            publisher = pubsub_v1.PublisherClient()
            project_path = publisher.common_project_path(config['project_id'])
            
            # List topics
            topics = publisher.list_topics(request={"project": project_path})
            
            for topic in topics:
                topic_name = topic.name.split('/')[-1]
                
                asset = {
                    'name': topic_name,
                    'type': 'pubsub_topic',
                    'source': 'pubsub',
                    'location': f"pubsub://{config['project_id']}/{topic_name}",
                    'created_date': datetime.now(),
                    'size': 0,
                    'metadata': {
                        'platform_type': 'pubsub',
                        'project_id': config['project_id'],
                        'full_name': topic.name
                    }
                }
                assets.append(asset)
            
        except Exception as e:
            self.logger.error(f"Error connecting to Pub/Sub: {e}")
        
        return assets
    
    def _discover_nats_assets(self, config: Dict[str, Any]) -> List[Dict[str, Any]]:
        """Discover NATS assets"""
        assets = []
        
        try:
            # This would require NATS client
            # Placeholder for NATS discovery
            subjects = config.get('subjects', [])
            
            for subject in subjects:
                asset = {
                    'name': subject,
                    'type': 'nats_subject',
                    'source': 'nats',
                    'location': f"nats://{config.get('server', 'localhost:4222')}/{subject}",
                    'created_date': datetime.now(),
                    'size': 0,
                    'metadata': {
                        'platform_type': 'nats',
                        'server': config.get('server', 'localhost:4222')
                    }
                }
                assets.append(asset)
            
        except Exception as e:
            self.logger.error(f"Error connecting to NATS: {e}")
        
        return assets
    
    def _discover_redis_streams_assets(self, config: Dict[str, Any]) -> List[Dict[str, Any]]:
        """Discover Redis Streams assets"""
        assets = []
        
        try:
            import redis
            
            r = redis.Redis(
                host=config['host'],
                port=config.get('port', 6379),
                password=config.get('password'),
                db=config.get('db', 0)
            )
            
            # Get all keys that might be streams
            keys = r.keys('*')
            
            for key in keys:
                try:
                    key_type = r.type(key).decode('utf-8')
                    if key_type == 'stream':
                        key_str = key.decode('utf-8') if isinstance(key, bytes) else str(key)
                        
                        # Get stream info
                        info = r.xinfo_stream(key)
                        
                        asset = {
                            'name': key_str,
                            'type': 'redis_stream',
                            'source': 'redis_streams',
                            'location': f"redis://{config['host']}/{config.get('db', 0)}/{key_str}",
                            'created_date': datetime.now(),
                            'size': info.get('length', 0),
                            'metadata': {
                                'platform_type': 'redis_streams',
                                'length': info.get('length', 0),
                                'groups': info.get('groups', 0),
                                'host': config['host'],
                                'db': config.get('db', 0)
                            }
                        }
                        assets.append(asset)
                        
                except Exception as e:
                    self.logger.error(f"Error checking Redis key {key}: {e}")
            
        except Exception as e:
            self.logger.error(f"Error connecting to Redis Streams: {e}")
        
        return assets
    
    def test_connection(self) -> bool:
        """Test streaming platform connections"""
        try:
            if not self.streaming_platforms:
                self.logger.error("No streaming platforms configured")
                return False
            
            connection_tested = False
            
            for stream_config in self.streaming_platforms:
                platform_type = stream_config.get('type', '').lower()
                
                try:
                    if platform_type == 'kafka':
                        if KafkaAdminClient:
                            admin_client = KafkaAdminClient(
                                bootstrap_servers=stream_config.get('bootstrap_servers', ['localhost:9092']),
                                client_id='data_discovery_test'
                            )
                            # Test by listing topics
                            metadata = admin_client.list_topics()
                            self.logger.info("Kafka connection test successful")
                            connection_tested = True
                        else:
                            self.logger.warning("Kafka library not available")
                    
                    elif platform_type == 'pulsar':
                        if pulsar:
                            client = pulsar.Client(stream_config.get('service_url', 'pulsar://localhost:6650'))
                            # Test connection
                            client.close()
                            self.logger.info("Pulsar connection test successful")
                            connection_tested = True
                        else:
                            self.logger.warning("Pulsar library not available")
                    
                    elif platform_type == 'rabbitmq':
                        if pika:
                            credentials = pika.PlainCredentials(
                                stream_config.get('username', 'guest'),
                                stream_config.get('password', 'guest')
                            )
                            connection = pika.BlockingConnection(
                                pika.ConnectionParameters(
                                    host=stream_config.get('host', 'localhost'),
                                    port=stream_config.get('port', 5672),
                                    credentials=credentials
                                )
                            )
                            connection.close()
                            self.logger.info("RabbitMQ connection test successful")
                            connection_tested = True
                        else:
                            self.logger.warning("RabbitMQ library not available")
                    
                    elif platform_type == 'kinesis':
                        if boto3:
                            kinesis_client = boto3.client(
                                'kinesis',
                                aws_access_key_id=stream_config.get('access_key'),
                                aws_secret_access_key=stream_config.get('secret_key'),
                                region_name=stream_config.get('region', 'us-east-1')
                            )
                            # Test by listing streams
                            kinesis_client.list_streams(Limit=1)
                            self.logger.info("Kinesis connection test successful")
                            connection_tested = True
                        else:
                            self.logger.warning("AWS SDK not available")
                    
                except Exception as e:
                    self.logger.warning(f"{platform_type.capitalize()} connection test failed: {e}")
            
            if connection_tested:
                self.logger.info("Streaming connection test successful")
                return True
            else:
                self.logger.error("No streaming platforms could be tested")
                return False
                
        except Exception as e:
            self.logger.error(f"Streaming connection test failed: {e}")
            return False
    
    def validate_config(self) -> bool:
        """Validate streaming connector configuration"""
        if not self.streaming_platforms:
            self.logger.error("No streaming platform connections configured")
            return False
        
        for streaming_config in self.streaming_platforms:
            if not streaming_config.get('type'):
                self.logger.error("Streaming platform type not specified in configuration")
                return False
        
        return True

