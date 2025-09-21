# Data Discovery System - Enterprise UI

## 🎯 Complete Enterprise Data Discovery Platform

This is a **complete enterprise-grade data discovery system** with a modern web UI, supporting **120+ connectors** across all major cloud providers, databases, data warehouses, SaaS platforms, and streaming systems.

## 🚀 Quick Start

### 1. Install Dependencies
```bash
pip install -r requirements.txt
```

### 2. Start the System
```bash
python start_ui.py
```

### 3. Access the Dashboard
- **Main Dashboard:** http://your-server:8000
- **API Documentation:** http://your-server:8000/docs
- **Interactive API:** http://your-server:8000/redoc

## 📊 What You Get

### ✅ **Complete Web Interface**
- **Modern Dashboard** with real-time statistics
- **Connector Management** - Configure all 120+ connectors via UI
- **Asset Browser** - Search and explore discovered data assets
- **Discovery Controls** - Start/stop discovery scans
- **Monitoring Dashboard** - Real-time monitoring controls
- **System Health** - Complete system status overview

### ✅ **Enterprise Features**
- **120+ Data Connectors** (Cloud, Database, SaaS, Streaming)
- **Real-time Monitoring** with file watching
- **Advanced Search** across all assets
- **Asset Metadata** extraction and cataloging
- **Configuration Management** via web interface
- **REST API** for integration with other systems

### ✅ **Supported Data Sources**
- **Cloud Providers:** AWS, Azure, GCP, Oracle Cloud, IBM Cloud, Alibaba Cloud
- **Databases:** PostgreSQL, MySQL, MongoDB, Oracle, SQL Server, Cassandra, Neo4j, Redis, Elasticsearch
- **Data Warehouses:** Snowflake, Databricks, BigQuery, Teradata, Redshift, ClickHouse
- **SaaS Platforms:** Salesforce, ServiceNow, Slack, Jira, HubSpot, Zendesk, Teams
- **Streaming:** Kafka, Pulsar, RabbitMQ, Kinesis, Event Hub, Pub/Sub
- **Data Lakes:** Delta Lake, Iceberg, Hudi, Parquet, MinIO, HDFS

## 🎛️ Dashboard Features

### 1. **System Overview**
- Total assets discovered
- Active connectors count  
- Last scan timestamp
- Monitoring status

### 2. **Connector Management**
- View all 120+ available connectors
- Enable/disable connectors via UI
- Configure connection settings
- Test connections
- Organized by categories (Cloud, Database, SaaS, etc.)

### 3. **Asset Discovery**
- Browse all discovered assets
- Search assets by name, type, source
- Filter by asset type and source
- View detailed asset metadata
- Export asset information

### 4. **Discovery Controls**
- Start full discovery across all sources
- Scan specific data sources
- View discovery status and progress
- Schedule discovery scans

### 5. **Monitoring Dashboard**
- Start/stop continuous monitoring
- Real-time file watching
- Configure monitoring settings
- View monitoring status

## 🔧 Configuration

### BigQuery Example
1. Go to **Connectors** tab
2. Find **Google Cloud Platform** in Cloud Providers
3. Click to configure
4. Enable the connector
5. Enter your Project ID
6. Set credentials path to your service account JSON
7. Select BigQuery service
8. Save configuration
9. Test connection

### Other Connectors
Each connector has its own configuration form in the UI. Simply:
1. Navigate to the appropriate category
2. Click on the connector
3. Fill in the required fields
4. Test the connection
5. Save and enable

## 📡 API Integration

### REST API Endpoints
```bash
# System Health
GET /api/system/health

# Connectors
GET /api/connectors
POST /api/config/{connector_type}
GET /api/config/{connector_type}

# Discovery
POST /api/discovery/scan
GET /api/discovery/status

# Assets
GET /api/assets
POST /api/assets/search
GET /api/assets/by-type/{type}

# Monitoring
POST /api/monitoring/start
POST /api/monitoring/stop
```

### Integration Example
```python
import requests

# Start discovery
response = requests.post('http://your-server:8000/api/discovery/scan')

# Get assets
assets = requests.get('http://your-server:8000/api/assets').json()

# Search assets
search_results = requests.post(
    'http://your-server:8000/api/assets/search',
    json={'query': 'customer_data'}
).json()
```

## 🏗️ Architecture

```
┌─────────────────┐    ┌─────────────────┐    ┌─────────────────┐
│   Web UI        │    │   FastAPI       │    │   Discovery     │
│   (Frontend)    │◄──►│   (Web API)     │◄──►│   Engine        │
│                 │    │                 │    │                 │
│ • Dashboard     │    │ • REST API      │    │ • 120+ Connectors│
│ • Config Forms  │    │ • Config Mgmt   │    │ • Asset Catalog │
│ • Asset Browser │    │ • Discovery API │    │ • Metadata      │
│ • Monitoring    │    │ • Asset API     │    │ • Monitoring    │
└─────────────────┘    └─────────────────┘    └─────────────────┘
```

## 📂 File Structure

```
data discovery/
├── web_api.py              # FastAPI web server
├── data_discovery_engine.py # Core discovery engine (enhanced)
├── config.yaml             # Master configuration
├── start_ui.py             # Quick start script
├── ui/
│   ├── index.html          # Main dashboard
│   └── static/
│       ├── style.css       # UI styling
│       └── app.js          # Frontend JavaScript
├── connectors/             # 120+ connector implementations
├── metadata/               # Metadata extraction
└── utils/                  # Utilities and helpers
```

## 🎯 For Integration

### Button Integration Example
```javascript
// Start discovery from your platform
function startDataDiscovery() {
    fetch('http://your-server:8000/api/discovery/scan', {
        method: 'POST'
    }).then(response => response.json())
      .then(data => console.log('Discovery started:', data));
}

// Get discovered assets
function getAssets() {
    fetch('http://your-server:8000/api/assets')
        .then(response => response.json())
        .then(assets => displayAssets(assets));
}
```

### Configuration via API
```python
# Configure BigQuery connector programmatically
import requests

config = {
    "enabled": True,
    "config": {
        "project_id": "your-project-id",
        "credentials_path": "service-account.json",
        "services": ["bigquery"]
    }
}

response = requests.post(
    'http://your-server:8000/api/config/gcp',
    json=config
)
```

## 🔐 Security Notes

- **Credentials:** Store service account keys securely
- **Network:** Consider running on internal network only
- **Access:** Add authentication if needed
- **HTTPS:** Use reverse proxy for production

## 🎉 Ready for Enterprise!

This system now provides:
- ✅ **Complete Web UI** for non-technical users
- ✅ **120+ Connectors** rivaling Collibra/Atlan  
- ✅ **REST API** for platform integration
- ✅ **Real-time Monitoring** capabilities
- ✅ **Enterprise Configuration** management
- ✅ **Modern Dashboard** with statistics
- ✅ **Easy Integration** via buttons and APIs

**Your senior can now build buttons in the main platform that call the REST API endpoints to trigger discovery, get assets, and manage connectors!**
