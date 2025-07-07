#!/usr/bin/env python3
"""
Test script to trigger and monitor pipeline execution
"""

import requests
import json
import time

def test_pipeline():
    """Test the pipeline execution through Flask API"""
    
    # Update to use Azure Container App URL
    base_url = "http://lws-data-pipeline.blackglacier-1e24db99.westus2.azurecontainerapps.io"
    
    print("🚀 Testing LWS CloudPipe v2 Pipeline")
    print("=" * 50)
    
    # Check health
    print("1. Checking Flask application health...")
    try:
        health_response = requests.get(f"{base_url}/health")
        if health_response.status_code == 200:
            health_data = health_response.json()
            print(f"✅ Flask app healthy: {health_data['service']} v{health_data['version']}")
        else:
            print(f"❌ Flask app not healthy: {health_response.status_code}")
            return
    except Exception as e:
        print(f"❌ Cannot connect to Flask app: {e}")
        return
    
    # Check available scripts
    print("\n2. Checking available pipeline scripts...")
    try:
        scripts_response = requests.get(f"{base_url}/api/pipeline/scripts")
        if scripts_response.status_code == 200:
            scripts_data = scripts_response.json()
            print(f"✅ Found {scripts_data['count']} pipeline scripts:")
            for script in scripts_data['scripts']:
                print(f"   • {script['name']} ({script['size']} bytes)")
        else:
            print(f"❌ Cannot get scripts: {scripts_response.status_code}")
            return
    except Exception as e:
        print(f"❌ Error getting scripts: {e}")
        return
    
    # Trigger pipeline
    print("\n3. Triggering pipeline execution...")
    try:
        pipeline_response = requests.post(
            f"{base_url}/api/pipeline/run",
            json={"script": "load_from_azure.py"}
        )
        if pipeline_response.status_code == 200:
            pipeline_data = pipeline_response.json()
            print(f"✅ Pipeline started: {pipeline_data['job_id']}")
            job_id = pipeline_data['job_id']
        else:
            print(f"❌ Cannot start pipeline: {pipeline_response.status_code}")
            return
    except Exception as e:
        print(f"❌ Error starting pipeline: {e}")
        return
    
    # Monitor pipeline status
    print("\n4. Monitoring pipeline execution...")
    max_wait_time = 900  # 15 minutes
    start_time = time.time()
    
    while time.time() - start_time < max_wait_time:
        try:
            status_response = requests.get(f"{base_url}/api/pipeline/status")
            if status_response.status_code == 200:
                status_data = status_response.json()
                current_status = status_data['status']
                
                print(f"   Status: {current_status.upper()}")
                
                # Show latest log entry
                if status_data['logs']:
                    latest_log = status_data['logs'][-1]
                    print(f"   Latest: {latest_log['message']}")
                
                if current_status == 'completed':
                    print("✅ Pipeline completed successfully!")
                    break
                elif current_status == 'failed':
                    print("❌ Pipeline failed!")
                    # Show error details
                    for log in status_data['logs']:
                        if log['level'] == 'ERROR':
                            print(f"   Error: {log['message']}")
                    break
                elif current_status == 'running':
                    print("   ⏳ Pipeline is running...")
                
                time.sleep(10)  # Wait 10 seconds before next check
            else:
                print(f"❌ Cannot get status: {status_response.status_code}")
                break
        except Exception as e:
            print(f"❌ Error monitoring pipeline: {e}")
            break
    
    # Final status check
    print("\n5. Final pipeline status:")
    try:
        final_status_response = requests.get(f"{base_url}/api/pipeline/status")
        if final_status_response.status_code == 200:
            final_status = final_status_response.json()
            print(f"   Status: {final_status['status'].upper()}")
            print(f"   Last Run: {final_status['last_run']}")
            print(f"   Total Logs: {len(final_status['logs'])}")
            
            # Show all logs
            print("\n   Pipeline Logs:")
            for log in final_status['logs']:
                level_emoji = "✅" if log['level'] == 'INFO' else "❌"
                print(f"   {level_emoji} [{log['timestamp']}] {log['message']}")
        else:
            print(f"❌ Cannot get final status: {final_status_response.status_code}")
    except Exception as e:
        print(f"❌ Error getting final status: {e}")
    
    print("\n" + "=" * 50)
    print("🎯 Pipeline test completed!")

if __name__ == "__main__":
    test_pipeline() 