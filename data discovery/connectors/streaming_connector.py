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
    
    # Metadata for dynamic discovery
    connector_type = "streaming"
    connector_name = "Streaming Platforms"
    description = "Discover streaming data assets from Kafka, Pulsar, RabbitMQ, Kinesis, Event Hub, Pub/Sub, and NATS"
    category = "streaming"
    supported_services = ["Kafka", "Pulsar", "RabbitMQ", "Kinesis", "Event Hub", "Pub/Sub", "NATS"]
    required_config_fields = ["streaming_connections"]
    optional_config_fields = ["connection_timeout"]
    
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
            
            # Try to discover topics using admin API
            try:
                from pulsar import Client, AuthenticationToken
                admin_client = pulsar.Admin(
                    service_url=config['service_url'],
                    authentication=AuthenticationToken(config.get('token')) if config.get('token') else None
                )
                
                topics = admin_client.topics()
                for topic in topics:
                    try:
                        stats = admin_client.topics().get_stats(topic)
                        
                        asset = {
                            'name': topic.split('/')[-1],  # Just the topic name
                            'type': 'pulsar_topic',
                            'source': 'pulsar',
                            'location': f"pulsar://{config['service_url']}/{topic}",
                            'created_date': datetime.now(),
                            'size': stats.get('msgInCounter', 0),
                            'metadata': {
                                'platform_type': 'pulsar',
                                'service_url': config['service_url'],
                                'full_topic_name': topic,
                                'producers': stats.get('producers', []),
                                'subscriptions': list(stats.get('subscriptions', {}).keys()),
                                'msg_in_rate': stats.get('msgInRate', 0),
                                'msg_out_rate': stats.get('msgOutRate', 0)
                            }
                        }
                        assets.append(asset)
                    except Exception as e:
                        self.logger.warning(f"Error getting stats for topic {topic}: {e}")
                        # Add basic topic info without stats
                        asset = {
                            'name': topic.split('/')[-1],
                            'type': 'pulsar_topic',
                            'source': 'pulsar',
                            'location': f"pulsar://{config['service_url']}/{topic}",
                            'created_date': datetime.now(),
                            'size': 0,
                            'metadata': {
                                'platform_type': 'pulsar',
                                'service_url': config['service_url'],
                                'full_topic_name': topic
                            }
                        }
                        assets.append(asset)
                
                admin_client.close()
                
            except ImportError:
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
            
            # Try to use management API first
            try:
                import requests
                from requests.auth import HTTPBasicAuth
                
                management_port = config.get('management_port', 15672)
                management_url = f"http://{config['host']}:{management_port}/api"
                
                response = requests.get(
                    f"{management_url}/queues",
                    auth=HTTPBasicAuth(config.get('username', 'guest'), config.get('password', 'guest')),
                    timeout=10
                )
                
                if response.status_code == 200:
                    queues_data = response.json()
                    
                    for queue in queues_data:
                        asset = {
                            'name': queue['name'],
                            'type': 'rabbitmq_queue',
                            'source': 'rabbitmq',
                            'location': f"rabbitmq://{config['host']}/{queue['name']}",
                            'created_date': datetime.fromtimestamp(queue.get('created_at', 0) / 1000) if queue.get('created_at') else datetime.now(),
                            'size': queue.get('messages', 0),
                            'metadata': {
                                'platform_type': 'rabbitmq',
                                'host': config['host'],
                                'port': config.get('port', 5672),
                                'vhost': queue.get('vhost', '/'),
                                'durable': queue.get('durable', False),
                                'auto_delete': queue.get('auto_delete', False),
                                'messages_ready': queue.get('messages_ready', 0),
                                'messages_unacknowledged': queue.get('messages_unacknowledged', 0),
                                'consumers': queue.get('consumers', 0),
                                'state': queue.get('state', 'unknown')
                            }
                        }
                        assets.append(asset)
                
                response = requests.get(
                    f"{management_url}/exchanges",
                    auth=HTTPBasicAuth(config.get('username', 'guest'), config.get('password', 'guest')),
                    timeout=10
                )
                
                if response.status_code == 200:
                    exchanges_data = response.json()
                    
                    for exchange in exchanges_data:
                        if not exchange['name'].startswith('amq.'):  # Skip system exchanges
                            asset = {
                                'name': exchange['name'],
                                'type': 'rabbitmq_exchange',
                                'source': 'rabbitmq',
                                'location': f"rabbitmq://{config['host']}/exchanges/{exchange['name']}",
                                'created_date': datetime.now(),
                                'size': 0,
                                'metadata': {
                                    'platform_type': 'rabbitmq',
                                    'host': config['host'],
                                    'port': config.get('port', 5672),
                                    'vhost': exchange.get('vhost', '/'),
                                    'type': exchange.get('type', 'direct'),
                                    'durable': exchange.get('durable', False),
                                    'auto_delete': exchange.get('auto_delete', False),
                                    'internal': exchange.get('internal', False)
                                }
                            }
                            assets.append(asset)
                
            except ImportError:
                self.logger.warning("requests library not available for RabbitMQ management API")
            except Exception as e:
                self.logger.warning(f"RabbitMQ management API not available: {e}")
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
            from azure.eventhub import EventHubConsumerClient
            from azure.identity import DefaultAzureCredential
            
            try:
                credential = DefaultAzureCredential()
                from azure.mgmt.eventhub import EventHubManagementClient
                
                subscription_id = config.get('subscription_id')
                resource_group = config.get('resource_group')
                namespace_name = config.get('namespace')
                
                if all([subscription_id, resource_group, namespace_name]):
                    eventhub_client = EventHubManagementClient(credential, subscription_id)
                    
                    event_hubs = eventhub_client.event_hubs.list_by_namespace(
                        resource_group, namespace_name
                    )
                    
                    for hub in event_hubs:
                        asset = {
                            'name': hub.name,
                            'type': 'eventhub_hub',
                            'source': 'eventhub',
                            'location': f"eventhub://{namespace_name}/{hub.name}",
                            'created_date': hub.created_at if hasattr(hub, 'created_at') else datetime.now(),
                            'size': 0,
                            'metadata': {
                                'platform_type': 'eventhub',
                                'namespace': namespace_name,
                                'partition_count': hub.partition_count if hasattr(hub, 'partition_count') else 0,
                                'message_retention_in_days': hub.message_retention_in_days if hasattr(hub, 'message_retention_in_days') else 0
                            }
                        }
                        assets.append(asset)
                else:
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
                        
            except ImportError:
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
            from azure.servicebus import ServiceBusClient
            from azure.identity import DefaultAzureCredential
            
            try:
                credential = DefaultAzureCredential()
                from azure.mgmt.servicebus import ServiceBusManagementClient
                
                subscription_id = config.get('subscription_id')
                resource_group = config.get('resource_group')
                namespace_name = config.get('namespace')
                
                if all([subscription_id, resource_group, namespace_name]):
                    servicebus_client = ServiceBusManagementClient(credential, subscription_id)
                    
                    queues = servicebus_client.queues.list_by_namespace(
                        resource_group, namespace_name
                    )
                    
                    for queue in queues:
                        asset = {
                            'name': queue.name,
                            'type': 'servicebus_queue',
                            'source': 'servicebus',
                            'location': f"servicebus://{namespace_name}/{queue.name}",
                            'created_date': queue.created_at if hasattr(queue, 'created_at') else datetime.now(),
                            'size': 0,
                            'metadata': {
                                'platform_type': 'servicebus',
                                'resource_type': 'queue',
                                'namespace': namespace_name,
                                'max_size_in_megabytes': queue.max_size_in_megabytes if hasattr(queue, 'max_size_in_megabytes') else 0,
                                'default_message_time_to_live': queue.default_message_time_to_live if hasattr(queue, 'default_message_time_to_live') else None
                            }
                        }
                        assets.append(asset)
                    
                    topics = servicebus_client.topics.list_by_namespace(
                        resource_group, namespace_name
                    )
                    
                    for topic in topics:
                        asset = {
                            'name': topic.name,
                            'type': 'servicebus_topic',
                            'source': 'servicebus',
                            'location': f"servicebus://{namespace_name}/{topic.name}",
                            'created_date': topic.created_at if hasattr(topic, 'created_at') else datetime.now(),
                            'size': 0,
                            'metadata': {
                                'platform_type': 'servicebus',
                                'resource_type': 'topic',
                                'namespace': namespace_name,
                                'max_size_in_megabytes': topic.max_size_in_megabytes if hasattr(topic, 'max_size_in_megabytes') else 0,
                                'default_message_time_to_live': topic.default_message_time_to_live if hasattr(topic, 'default_message_time_to_live') else None
                            }
                        }
                        assets.append(asset)
                else:
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
                        
            except ImportError:
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
            import nats
            import asyncio
            
            server = config.get('server', 'nats://localhost:4222')
            
            async def discover_nats_assets():
                try:
                    nc = await nats.connect(server)
                    
                    server_info = nc.server_info
                    
                    # Get subject information (this requires NATS monitoring)
                    try:
                        # Try to get subject statistics if monitoring is enabled
                        subjects = config.get('subjects', [])
                        
                        for subject in subjects:
                            asset = {
                                'name': subject,
                                'type': 'nats_subject',
                                'source': 'nats',
                                'location': f"nats://{server}/{subject}",
                                'created_date': datetime.now(),
                                'size': 0,
                                'metadata': {
                                    'platform_type': 'nats',
                                    'server': server,
                                    'server_version': server_info.get('version', 'unknown'),
                                    'server_id': server_info.get('server_id', 'unknown'),
                                    'go_version': server_info.get('go_version', 'unknown')
                                }
                            }
                            assets.append(asset)
                    
                    except Exception as e:
                        self.logger.warning(f"Could not get NATS subject info: {e}")
                        subjects = config.get('subjects', [])
                        for subject in subjects:
                            asset = {
                                'name': subject,
                                'type': 'nats_subject',
                                'source': 'nats',
                                'location': f"nats://{server}/{subject}",
                                'created_date': datetime.now(),
                                'size': 0,
                                'metadata': {
                                    'platform_type': 'nats',
                                    'server': server
                                }
                            }
                            assets.append(asset)
                    
                    await nc.close()
                    
                except Exception as e:
                    self.logger.error(f"Error connecting to NATS: {e}")
                    subjects = config.get('subjects', [])
                    for subject in subjects:
                        asset = {
                            'name': subject,
                            'type': 'nats_subject',
                            'source': 'nats',
                            'location': f"nats://{server}/{subject}",
                            'created_date': datetime.now(),
                            'size': 0,
                            'metadata': {
                                'platform_type': 'nats',
                                'server': server
                            }
                        }
                        assets.append(asset)
            
            asyncio.run(discover_nats_assets())
            
        except ImportError:
            self.logger.warning("nats-py library not installed. Install with: pip install nats-py")
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
            
            keys = r.keys('*')
            
            for key in keys:
                try:
                    key_type = r.type(key).decode('utf-8')
                    if key_type == 'stream':
                        key_str = key.decode('utf-8') if isinstance(key, bytes) else str(key)
                        
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
                            metadata = admin_client.list_topics()
                            self.logger.info("Kafka connection test successful")
                            connection_tested = True
                        else:
                            self.logger.warning("Kafka library not available")
                    
                    elif platform_type == 'pulsar':
                        if pulsar:
                            client = pulsar.Client(stream_config.get('service_url', 'pulsar://localhost:6650'))
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
