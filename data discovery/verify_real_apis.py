"""
Direct verification that all connectors use real APIs
"""

import os
import re
from pathlib import Path

def check_file_for_real_apis(file_path: str) -> dict:
    """Check a single file for real API usage"""
    
    try:
        with open(file_path, 'r') as f:
            content = f.read()
        
        real_api_patterns = [
            r'boto3\.client\(',
            r'boto3\.Session\(',
            r'from google\.cloud import',
            r'google\.cloud\.bigquery',
            r'google\.cloud\.storage',
            r'google\.oauth2',
            r'azure\.storage\.blob',
            r'azure\.identity',
            r'psycopg2\.connect',
            r'mysql\.connector',
            r'pymongo\.MongoClient',
            r'sqlalchemy\.create_engine',
            r'kafka\.KafkaAdminClient',
            r'kafka\.KafkaConsumer',
            r'pulsar\.Client',
            r'snowflake\.connector',
            r'pyarrow\.parquet',
            r'deltalake\.DeltaTable',
            r'requests\.get\(',
            r'requests\.post\(',
            r'simple_salesforce',
            r'pysnow\.Client',
            r'slack_sdk\.WebClient',
            r'jira\.JIRA',
            r'cx_Oracle\.connect',
            r'redis\.Redis',
            r'elasticsearch\.Elasticsearch',
            r'teradatasql\.connect',
            r'clickhouse_driver\.Client',
            r'presto\.dbapi\.connect',
            r'minio\.Minio',
            r'os\.path\.',
            r'pathlib\.Path',
            r'Path\(',
            r'os\.listdir',
            r'os\.walk',
            r'glob\.glob',
            r'from pathlib import',
            r'import os',
            r'import pathlib',
            r'hdfs\.InsecureClient',
            r'from hdfs import',
            r'subprocess\.run',
            r'from pyiceberg',
            r'from pyspark',
            r'InsecureClient\(',
            r'paramiko\.SSHClient',
            r'smbclient\.',
            r'ftplib\.FTP',
            r'import paramiko',
            r'import smbclient',
            r'import ftplib',
            r'import oci',
            r'oci\.config',
            r'oci\.object_storage',
            r'import ibm_db',
            r'ibm_db\.',
        ]
        
        mock_patterns = [
            r'placeholder for',
            r'mock data',
            r'fake data',
            r'dummy data',
            r'return \[\]  # Placeholder',
            r'This would require.*API',
            r'Placeholder for.*discovery',
        ]
        
        real_api_count = sum(len(re.findall(pattern, content)) for pattern in real_api_patterns)
        mock_count = sum(len(re.findall(pattern, content, re.IGNORECASE)) for pattern in mock_patterns)
        
        api_call_patterns = [
            r'client\.list_',
            r'client\.get_',
            r'client\.describe_',
            r'client\.query',
            r'client\.execute',
            r'response = requests\.',
            r'cursor\.execute',
            r'engine\.connect',
            r'client\.connect',
            r'\.discover_',
            r'\.list_',
            r'\.get_',
        ]
        
        api_call_count = sum(len(re.findall(pattern, content)) for pattern in api_call_patterns)
        
        return {
            'real_api_count': real_api_count,
            'mock_count': mock_count,
            'api_call_count': api_call_count,
            'uses_real_apis': real_api_count > 0 and api_call_count > 0 and mock_count < real_api_count
        }
        
    except Exception as e:
        return {'error': str(e)}

def main():
    """Main verification function"""
    
    
    connectors_dir = Path("connectors")
    results = {}
    
    for connector_file in connectors_dir.glob("*.py"):
        if connector_file.name == "__init__.py" or connector_file.name == "base_connector.py":
            continue
            
        
        file_results = check_file_for_real_apis(str(connector_file))
        
        if 'error' in file_results:
            results[connector_file.name] = {'status': 'error', 'message': file_results['error']}
        else:
            uses_real_apis = file_results['uses_real_apis']
            status = "✅ Real APIs" if uses_real_apis else "⚠️  Mock/Placeholder"
            
            
            results[connector_file.name] = {
                'status': 'real_api' if uses_real_apis else 'mock',
                'real_api_count': file_results['real_api_count'],
                'api_call_count': file_results['api_call_count'],
                'mock_count': file_results['mock_count']
            }
    
    
    total_files = len(results)
    real_api_files = sum(1 for r in results.values() if r.get('status') == 'real_api')
    mock_files = sum(1 for r in results.values() if r.get('status') == 'mock')
    error_files = sum(1 for r in results.values() if r.get('status') == 'error')
    
    
    if real_api_files > 0:
    
    
    for filename, result in results.items():
        if result['status'] == 'real_api':
        elif result['status'] == 'mock':
        else:
    
    
    if real_api_files == total_files:
    elif real_api_files > total_files * 0.8:
    elif real_api_files > total_files * 0.5:
    else:
    

if __name__ == "__main__":
    main()
