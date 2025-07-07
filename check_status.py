#!/usr/bin/env python3
import requests

# Cloud endpoint from log.json
CLOUD_URL = "https://lws-data-pipeline.blackglacier-1e24db99.westus2.azurecontainerapps.io"

try:
    # Check health first
    health_response = requests.get(f'{CLOUD_URL}/health')
    if health_response.status_code == 200:
        health_data = health_response.json()
        print(f"🌐 Cloud Container Status: {health_data['status']}")
        print(f"📦 Service: {health_data['service']}")
        print(f"🔢 Version: {health_data['version']}")
        print(f"🌍 Environment: {health_data['environment']}")
        print()
    
    # Check pipeline status
    response = requests.get(f'{CLOUD_URL}/api/pipeline/status')
    if response.status_code == 200:
        data = response.json()
        print(f"📊 Pipeline Status: {data['status']}")
        if data.get('logs'):
            print(f"📝 Latest log: {data['logs'][-1]['message']}")
        else:
            print("📝 No logs available")
        
        # Check for active jobs
        if data.get('active_jobs'):
            print(f"🚀 Active Jobs: {len(data['active_jobs'])}")
            for job in data['active_jobs']:
                print(f"   - Job ID: {job['id']}, Status: {job['status']}")
    else:
        print(f"❌ Pipeline status check failed: {response.status_code}")
        
except Exception as e:
    print(f"❌ Error: {e}") 