# Google Cloud Platform (GCP) Integration

## 🚀 Real GCP API Integration

The GCP connector now uses **real Google Cloud APIs** to discover data assets from your GCP projects. No more mock data - this is the real deal!

## 📋 Supported Services

### ✅ BigQuery
- **Datasets**: Discover all BigQuery datasets in your project
- **Tables**: List all tables with schema information, row counts, and sizes
- **Views**: Discover BigQuery views with their definitions
- **Metadata**: Creation dates, labels, descriptions, and more

### ✅ Cloud Storage
- **Buckets**: List all Cloud Storage buckets
- **Objects**: Discover files and objects in buckets (with size filtering)
- **Metadata**: Storage class, location, versioning, lifecycle rules

### 🔄 Coming Soon
- **Cloud SQL**: Database instances and databases
- **Pub/Sub**: Topics and subscriptions
- **Firestore**: Collections and documents

## 🔧 Configuration

### Required Fields
- `project_id`: Your GCP project ID

### Optional Fields
- `services`: List of services to discover (default: `["bigquery", "cloud_storage"]`)
- `region`: GCP region (default: `"us-central1"`)
- `credentials_path`: Path to service account JSON file
- `service_account_json`: Service account JSON as string or dict

## 🔐 Authentication

The connector supports multiple authentication methods:

### 1. Application Default Credentials (Recommended)
```bash
gcloud auth application-default login
```

### 2. Service Account File
```bash
export GOOGLE_APPLICATION_CREDENTIALS="/path/to/service-account.json"
```

### 3. Service Account JSON in Config
```json
{
  "project_id": "your-project-id",
  "service_account_json": {
    "type": "service_account",
    "project_id": "your-project-id",
    "private_key_id": "...",
    "private_key": "...",
    "client_email": "...",
    "client_id": "...",
    "auth_uri": "https://accounts.google.com/o/oauth2/auth",
    "token_uri": "https://oauth2.googleapis.com/token"
  }
}
```

## 🚀 Usage

### Via API
```bash
curl -X POST http://localhost:8000/api/connectors/gcp/add \
  -H "Content-Type: application/json" \
  -d '{
    "enabled": true,
    "config": {
      "project_id": "your-project-id",
      "services": ["bigquery", "cloud_storage"],
      "region": "us-central1"
    }
  }'
```

### Via UI
1. Open the Data Discovery UI at `http://localhost:8000`
2. Go to Connectors section
3. Find "Google Cloud Platform" connector
4. Click "Configure"
5. Enter your project ID and select services
6. Click "Add Connector"

## 📊 Discovered Assets

### BigQuery Assets
```json
{
  "name": "project-id.dataset-id.table-id",
  "type": "bigquery_table",
  "source": "gcp_bigquery",
  "location": "bigquery://project-id/dataset-id/table-id",
  "size": 1048576,
  "created_date": "2024-01-15T10:30:00Z",
  "modified_date": "2024-12-01T14:20:00Z",
  "schema": {
    "fields": [
      {
        "name": "column1",
        "type": "STRING",
        "mode": "NULLABLE",
        "description": "Description of column1"
      }
    ],
    "num_rows": 1000,
    "num_bytes": 1048576
  },
  "tags": ["gcp", "bigquery", "table"],
  "metadata": {
    "service": "bigquery",
    "resource_type": "table",
    "project_id": "project-id",
    "dataset_id": "dataset-id",
    "table_id": "table-id",
    "table_type": "TABLE",
    "description": "Table description",
    "labels": {"environment": "production"},
    "expiration_time": "2025-01-01T00:00:00Z"
  }
}
```

### Cloud Storage Assets
```json
{
  "name": "bucket-name/path/to/file.csv",
  "type": "gcs_object",
  "source": "gcp_cloud_storage",
  "location": "gs://bucket-name/path/to/file.csv",
  "size": 2048,
  "created_date": "2024-01-15T10:30:00Z",
  "modified_date": "2024-12-01T14:20:00Z",
  "schema": {},
  "tags": ["gcp", "cloud_storage", "object", "csv"],
  "metadata": {
    "service": "cloud_storage",
    "resource_type": "object",
    "project_id": "project-id",
    "bucket_name": "bucket-name",
    "object_name": "path/to/file.csv",
    "content_type": "text/csv",
    "storage_class": "STANDARD",
    "labels": {"environment": "production"},
    "md5_hash": "abc123...",
    "crc32c": "def456..."
  }
}
```

## 🔍 Testing

### Test Connection
```bash
curl -X POST http://localhost:8000/api/config/gcp/test
```

### Discover Assets
```bash
curl -X POST http://localhost:8000/api/discovery/test/gcp
```

### Run Test Script
```bash
python test_gcp_integration.py
```

## 🛠️ Development

### Adding New Services
To add support for new GCP services:

1. Add the service to `supported_services` list
2. Add client initialization in `_initialize_clients()`
3. Add discovery method `_discover_{service}_assets()`
4. Add connection test in `test_connection()`
5. Update the `discover_assets()` method

### Example: Adding Pub/Sub Support
```python
# In __init__
self.pubsub_client = None

# In _initialize_clients
if 'pubsub' in self.services:
    self.pubsub_client = pubsub_v1.PublisherClient(credentials=self.credentials)

# In discover_assets
if 'pubsub' in self.services and self.pubsub_client:
    assets.extend(self._discover_pubsub_assets())

# Add discovery method
def _discover_pubsub_assets(self):
    # Implementation here
    pass
```

## 🔒 Security

- **Credentials**: Never commit service account keys to version control
- **IAM**: Use least-privilege IAM roles
- **Environment**: Use environment variables for sensitive data
- **Audit**: Enable Cloud Audit Logs for API access

## 📈 Performance

- **Pagination**: Large result sets are paginated
- **Filtering**: Small files (< 1KB) are filtered out
- **Caching**: Results can be cached for better performance
- **Rate Limiting**: Respects GCP API rate limits

## 🐛 Troubleshooting

### Common Issues

1. **Authentication Error**
   ```
   Error: No GCP credentials found
   Solution: Run `gcloud auth application-default login`
   ```

2. **Project Not Found**
   ```
   Error: Project 'your-project-id' not found
   Solution: Verify project ID and permissions
   ```

3. **Permission Denied**
   ```
   Error: Permission denied
   Solution: Check IAM roles and permissions
   ```

### Required IAM Roles

- **BigQuery**: `BigQuery Data Viewer`, `BigQuery Metadata Viewer`
- **Cloud Storage**: `Storage Object Viewer`, `Storage Bucket Reader`
- **Cloud SQL**: `Cloud SQL Viewer` (when implemented)

## 🎯 Next Steps

1. **Set up authentication** using one of the methods above
2. **Configure the connector** with your project ID
3. **Test the connection** to verify credentials
4. **Discover assets** to see your GCP data
5. **Monitor and manage** through the UI

## 📚 Resources

- [Google Cloud Authentication](https://cloud.google.com/docs/authentication)
- [BigQuery Python Client](https://cloud.google.com/bigquery/docs/reference/libraries#client-libraries)
- [Cloud Storage Python Client](https://cloud.google.com/storage/docs/reference/libraries#client-libraries)
- [GCP IAM Roles](https://cloud.google.com/iam/docs/understanding-roles)

---

**🎉 Happy Data Discovering with Google Cloud Platform!**
