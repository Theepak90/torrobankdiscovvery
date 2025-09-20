#!/usr/bin/env python3
"""
Test script to demonstrate the dynamic nature of the data discovery system
"""

import requests
import json
import time

BASE_URL = "http://localhost:8000"

def test_dynamic_connectors():
    """Test the dynamic connector system"""
    print("🚀 Testing Dynamic Data Discovery System")
    print("=" * 50)
    
    # 1. Get available connectors
    print("\n1. Available Connectors:")
    response = requests.get(f"{BASE_URL}/api/connectors")
    if response.status_code == 200:
        data = response.json()
        print(f"   Total connectors: {data['total_connectors']}")
        print(f"   Enabled connectors: {data['enabled_connectors']}")
        print(f"   Categories: {list(data['connectors'].keys())}")
        
        for category, connectors in data['connectors'].items():
            if connectors:
                print(f"   {category}: {connectors}")
    
    # 2. Get connector details
    print("\n2. Connector Details:")
    for connector_type in ['aws', 'gcp', 'file_system']:
        response = requests.get(f"{BASE_URL}/api/connectors/{connector_type}")
        if response.status_code == 200:
            data = response.json()
            info = data['connector_info']
            print(f"   {info['name']}: {info['description']}")
            print(f"     Services: {info['services']}")
            print(f"     Required: {info['required_config']}")
    
    # 3. Test system health
    print("\n3. System Health:")
    response = requests.get(f"{BASE_URL}/api/system/health")
    if response.status_code == 200:
        data = response.json()
        print(f"   Status: {data['status']}")
        print(f"   Total connectors: {data['total_connectors']}")
        print(f"   Connected: {data['connected_connectors']}")
        print(f"   Total assets: {data['total_assets']}")
    
    # 4. Test adding a connector dynamically (this will fail without credentials)
    print("\n4. Testing Dynamic Connector Addition:")
    try:
        response = requests.post(
            f"{BASE_URL}/api/connectors/aws/add",
            json={
                "enabled": True,
                "config": {
                    "region": "us-east-1",
                    "services": ["s3", "rds"]
                }
            }
        )
        if response.status_code == 200:
            print("   ✅ Successfully added AWS connector")
        else:
            print(f"   ❌ Failed to add AWS connector: {response.text}")
    except Exception as e:
        print(f"   ❌ Error adding connector: {e}")
    
    # 5. Test connector reload
    print("\n5. Testing Connector Reload:")
    response = requests.post(f"{BASE_URL}/api/connectors/reload")
    if response.status_code == 200:
        print("   ✅ Connectors reloaded successfully")
    else:
        print(f"   ❌ Failed to reload connectors: {response.text}")
    
    print("\n" + "=" * 50)
    print("✅ Dynamic system test completed!")

if __name__ == "__main__":
    test_dynamic_connectors()
