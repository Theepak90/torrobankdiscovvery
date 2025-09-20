# Data Discovery System

A comprehensive data discovery system that automatically scans and catalogs data assets across on-premise, cloud, and file system environments.

## Features

- **Multi-Source Discovery**: Automatically discovers data assets from:
  - Local and network file systems
  - **Network Storage**: NAS, SFTP, SMB/CIFS, FTP, NFS
  - AWS (S3, RDS, DynamoDB, Redshift)
  - Azure (Blob Storage, SQL Database, Cosmos DB)
  - Google Cloud Platform (Cloud Storage, BigQuery, Cloud SQL)
  - Various databases (PostgreSQL, MySQL, MongoDB, SQLite, SQL Server)

- **Intelligent Metadata Extraction**: 
  - Schema detection and analysis
  - Data quality metrics
  - PII detection and privacy risk assessment
  - Business context inference

- **Asset Cataloging**: 
  - SQLite-based catalog with full search capabilities
  - Asset history tracking and change detection
  - Export capabilities (JSON, CSV)

- **Continuous Monitoring**: 
  - Real-time file system watching with instant detection
  - Interval-based scanning for comprehensive coverage
  - Automatic asset catalog updates as files change
  - Background monitoring service for 24/7 operation

- **Platform Integration Ready**: 
  - Clean API interface for easy integration
  - Async/await support for high performance
  - Configurable scanning intervals and concurrent processing

## Quick Start

### 1. Installation

```bash
pip install -r requirements.txt
```

### 2. Configuration

Copy the example configuration and customize for your environment:

```bash
cp example_config.yaml config.yaml
```

Edit `config.yaml` to configure your data sources:

```yaml
discovery:
  file_system:
    enabled: true
    scan_paths:
      - "/your/data/path"
      - "/another/data/location"
    file_extensions:
      - ".csv"
      - ".json"
      - ".xlsx"
      - ".parquet"
  
  databases:
    enabled: true
    database_connections:
      - type: "postgresql"
        host: "localhost"
        database: "mydatabase"
        username: "myuser"
        password: "mypassword"
```

### 3. Basic Usage

```python
from data_discovery_engine import DataDiscoveryAPI
import asyncio

async def discover_data():
    # Initialize the discovery API
    api = DataDiscoveryAPI("config.yaml")
    
    # Run a full discovery scan
    result = await api.scan_all_data_sources()
    
    if result['status'] == 'success':
        print(f"Discovered {result['summary']['total_assets']} assets")
        
        # Search for specific assets
        csv_files = api.search_data_assets("", asset_type="csv_file")
        print(f"Found {csv_files['count']} CSV files")
    
# Run the discovery
asyncio.run(discover_data())
```

### 4. Continuous Monitoring

```python
# Start continuous monitoring with real-time file watching
api = DataDiscoveryAPI("config.yaml")
await api.start_continuous_monitoring(real_time=True)

# Or use the monitoring service
python monitor_service.py --config config.yaml
```

## Platform Integration

### For Senior Developers - Integration Guide

The system is designed for easy integration with your main platform. Here are the key integration points:

#### 1. Main API Class

```python
from data_discovery_engine import DataDiscoveryAPI

# Initialize once in your platform
discovery_api = DataDiscoveryAPI("config.yaml")
```

#### 2. Core Integration Functions

```python
# Full discovery scan - call this from your platform's data refresh jobs
async def platform_data_scan():
    result = await discovery_api.scan_all_data_sources()
    return result

# Get current inventory - for dashboard displays
def get_data_inventory():
    return discovery_api.get_asset_inventory()

# Search functionality - for user search features
def search_platform_data(query, filters=None):
    asset_type = filters.get('type') if filters else None
    return discovery_api.search_data_assets(query, asset_type)
```

#### 3. Integration with Web Frameworks

```python
# Flask example
@app.route('/api/data/scan', methods=['POST'])
async def trigger_scan():
    result = await discovery_api.scan_all_data_sources()
    return jsonify(result)

# FastAPI example
@app.post("/api/data/scan")
async def trigger_scan():
    result = await discovery_api.scan_all_data_sources()
    return result
```

#### 4. Database Integration

The system uses SQLite for its catalog, but you can easily integrate with your platform's database:

```python
# Store discovery results in your platform database
async def store_discovery_results(discovery_result):
    # Extract relevant data
    total_assets = discovery_result['summary']['total_assets']
    sources = list(discovery_result['summary']['sources'].keys())
    
    # Store in your platform's database
    await your_db.execute(
        "INSERT INTO data_discovery_runs (timestamp, assets, sources) VALUES (?, ?, ?)",
        (datetime.now(), total_assets, ','.join(sources))
    )
```

## Architecture

### Core Components

1. **DataDiscoveryEngine**: Main orchestrator that coordinates all discovery activities
2. **Connectors**: Modular connectors for different data sources (file system, AWS, Azure, GCP, databases)
3. **MetadataExtractor**: Extracts and enriches metadata for discovered assets
4. **AssetCatalog**: Manages the catalog of discovered assets with search and history capabilities

### Connector Framework

Each connector implements the `BaseConnector` interface:

```python
class MyCustomConnector(BaseConnector):
    def discover_assets(self) -> List[Dict[str, Any]]:
        # Your discovery logic here
        pass
    
    def test_connection(self) -> bool:
        # Connection test logic
        pass
```

## Configuration Reference

### File System Configuration

```yaml
discovery:
  file_system:
    enabled: true
    scan_paths: ["/data", "/home/user/documents"]
    file_extensions: [".csv", ".json", ".xlsx", ".parquet"]
    max_file_size_mb: 1000
```

### Network Storage Configuration

```yaml
discovery:
  network:
    enabled: true
    network_sources:
      # SFTP
      - type: "sftp"
        host: "sftp.example.com"
        port: 22
        username: "myuser"
        password: "mypassword"
        scan_paths: ["/data", "/reports"]
      
      # SMB/NAS
      - type: "smb"
        host: "nas.example.com"
        share: "data"
        username: "myuser"
        password: "mypassword"
        domain: "MYDOMAIN"
        scan_paths: ["/", "/backups"]
      
      # FTP
      - type: "ftp"
        host: "ftp.example.com"
        username: "myuser"
        password: "mypassword"
        scan_paths: ["/pub/data"]
      
      # NFS (requires local mount)
      - type: "nfs"
        server: "nfs.example.com"
        export: "/export/data"
        mount_point: "/mnt/nfs_data"
        scan_paths: ["/"]
```

### Database Configuration

```yaml
discovery:
  databases:
    enabled: true
    database_connections:
      - type: "postgresql"
        host: "localhost"
        port: 5432
        database: "mydatabase"
        username: "myuser"
        password: "mypassword"
```

### Cloud Provider Configuration

```yaml
discovery:
  aws:
    enabled: true
    regions: ["us-east-1", "us-west-2"]
    services: ["s3", "rds", "dynamodb"]
  
  azure:
    enabled: true
    services: ["blob_storage", "sql_database"]
    storage_accounts: ["mystorageaccount"]
  
  gcp:
    enabled: true
    project_id: "my-project"
    services: ["cloud_storage", "bigquery"]
```

## API Reference

### DataDiscoveryAPI Methods

- `scan_all_data_sources()`: Perform full discovery scan
- `get_asset_inventory()`: Get current asset inventory summary
- `search_data_assets(query, asset_type=None)`: Search for specific assets

### Asset Dictionary Format

Each discovered asset follows this standardized format:

```python
{
    'name': 'asset_name',
    'type': 'asset_type',  # e.g., 'csv_file', 'postgresql_table'
    'source': 'source_system',  # e.g., 'file_system', 'aws_s3'
    'location': 'full_path_or_uri',
    'size': 1024,  # Size in bytes
    'created_date': datetime,
    'modified_date': datetime,
    'schema': {...},  # Schema information
    'tags': [...],  # Generated tags
    'metadata': {...}  # Rich metadata including quality metrics, PII detection
}
```

## Monitoring and Logging

The system provides comprehensive logging:

```yaml
logging:
  level: "INFO"
  file: "logs/data_discovery.log"
  max_size_mb: 100
```

Log levels: DEBUG, INFO, WARNING, ERROR

## Performance Considerations

- **Concurrent Scanning**: Configurable concurrent workers for parallel processing
- **File Size Limits**: Configurable limits to avoid processing very large files
- **Sampling**: Configurable sample sizes for metadata extraction
- **Incremental Updates**: Asset fingerprinting for efficient incremental updates

## Network Discovery Features

- **SFTP Discovery**: Secure file transfer protocol with key-based or password authentication
- **SMB/NAS Discovery**: Windows file shares and Network Attached Storage
- **FTP Discovery**: File Transfer Protocol for legacy systems
- **NFS Discovery**: Network File System for Unix/Linux environments
- **Automatic Protocol Detection**: Handles different file types across all network protocols
- **Connection Pooling**: Efficient connection management for large network scans

## Security Features

- **PII Detection**: Automatic detection of personally identifiable information
- **Privacy Risk Assessment**: Risk level classification (low, medium, high)
- **Sensitive Column Detection**: Pattern-based detection of sensitive data columns
- **Access Control Ready**: Designed to work with your platform's access control system
- **Secure Network Access**: Encrypted connections for SFTP and secure authentication for all protocols

## Extending the System

### Adding New Connectors

1. Create a new connector class inheriting from `BaseConnector`
2. Implement required methods: `discover_assets()`, `test_connection()`
3. Add configuration section to `config.yaml`
4. Register in `DataDiscoveryEngine`

### Custom Metadata Extractors

Extend the `MetadataExtractor` class to add custom metadata extraction logic:

```python
class CustomMetadataExtractor(MetadataExtractor):
    def extract_metadata(self, asset):
        metadata = super().extract_metadata(asset)
        # Add your custom metadata logic
        metadata['custom_field'] = self.extract_custom_data(asset)
        return metadata
```

## Troubleshooting

### Common Issues

1. **Permission Errors**: Ensure the system has read access to configured scan paths
2. **Database Connection Failures**: Verify database credentials and network connectivity
3. **Cloud Authentication**: Ensure proper cloud provider credentials are configured
4. **Large File Processing**: Adjust `max_file_size_mb` if encountering memory issues

### Debug Mode

Enable debug logging for detailed troubleshooting:

```yaml
logging:
  level: "DEBUG"
```

## License

This data discovery system is designed for integration with your platform. Please ensure compliance with your organization's data governance and privacy policies when deploying.
