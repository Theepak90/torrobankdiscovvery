#!/usr/bin/env python3
"""
Test script for real GCP integration
Shows how to use the GCP connector with actual Google Cloud credentials
"""

import json
import os
from connectors.gcp_connector import GCPConnector

def test_gcp_connector_with_credentials():
    """Test GCP connector with real credentials"""
    print("🚀 Testing Real GCP Integration")
    print("=" * 50)
    
    # Test 1: With service account JSON (if available)
    print("\n1. Testing with Service Account JSON:")
    
    # Check if service account file exists
    service_account_file = os.path.expanduser("~/.config/gcloud/application_default_credentials.json")
    if os.path.exists(service_account_file):
        print(f"   Found credentials file: {service_account_file}")
        
        config = {
            "project_id": "your-gcp-project-id",  # Replace with your actual project ID
            "services": ["bigquery", "cloud_storage"],
            "region": "us-central1"
        }
        
        try:
            connector = GCPConnector(config)
            print(f"   ✅ GCP Connector initialized successfully")
            print(f"   Project ID: {connector.project_id}")
            print(f"   Services: {connector.services}")
            print(f"   Has credentials: {connector.credentials is not None}")
            
            # Test connection
            if connector.test_connection():
                print("   ✅ Connection test successful")
                
                # Discover assets
                print("   🔍 Discovering assets...")
                assets = connector.discover_assets()
                print(f"   Found {len(assets)} assets")
                
                # Show sample assets
                for i, asset in enumerate(assets[:3]):  # Show first 3 assets
                    print(f"   Asset {i+1}: {asset['name']} ({asset['type']})")
                    
            else:
                print("   ❌ Connection test failed")
                
        except Exception as e:
            print(f"   ❌ Error: {e}")
    else:
        print("   ⚠️  No service account credentials found")
        print("   To test with real credentials:")
        print("   1. Run: gcloud auth application-default login")
        print("   2. Or set GOOGLE_APPLICATION_CREDENTIALS environment variable")
        print("   3. Or provide service_account_json in config")
    
    # Test 2: With environment variable
    print("\n2. Testing with Environment Variable:")
    if os.getenv('GOOGLE_APPLICATION_CREDENTIALS'):
        print(f"   Found GOOGLE_APPLICATION_CREDENTIALS: {os.getenv('GOOGLE_APPLICATION_CREDENTIALS')}")
    else:
        print("   ⚠️  GOOGLE_APPLICATION_CREDENTIALS not set")
    
    # Test 3: Show configuration options
    print("\n3. Configuration Options:")
    print("   Required:")
    print("   - project_id: Your GCP project ID")
    print("   Optional:")
    print("   - services: ['bigquery', 'cloud_storage']")
    print("   - region: 'us-central1'")
    print("   - credentials_path: Path to service account JSON file")
    print("   - service_account_json: Service account JSON as string/dict")
    
    # Test 4: Show API usage
    print("\n4. API Usage:")
    print("   curl -X POST http://localhost:8000/api/connectors/gcp/add \\")
    print("     -H 'Content-Type: application/json' \\")
    print("     -d '{")
    print('       "enabled": true,')
    print('       "config": {')
    print('         "project_id": "your-project-id",')
    print('         "services": ["bigquery", "cloud_storage"],')
    print('         "region": "us-central1"')
    print('       }')
    print('     }')
    
    print("\n" + "=" * 50)
    print("✅ GCP integration test completed!")

if __name__ == "__main__":
    test_gcp_connector_with_credentials()
