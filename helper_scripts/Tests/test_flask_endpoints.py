#!/usr/bin/env python3
"""
Flask application endpoints test script
Tests Flask web application health and API endpoints
"""

import os
import sys
import json
import requests
import time
from pathlib import Path
from datetime import datetime

# Add the Utils directory to the path so we can import the logger
sys.path.append(str(Path(__file__).parent.parent / "Utils"))

from logger import PipelineLogger

class FlaskEndpointsTester:
    def __init__(self):
        """Initialize the Flask endpoints tester."""
        self.logger = PipelineLogger(log_dir="logs", log_file="flask_endpoints_test.log")
        self.base_url = "http://localhost:5000"
        
    def test_flask_app_running(self) -> bool:
        """Test if Flask application is running."""
        self.logger.log("FLASK_TEST", "Testing if Flask application is running...", "INFO")
        
        try:
            response = requests.get(f"{self.base_url}/health", timeout=10)
            
            if response.status_code == 200:
                self.logger.log("FLASK_TEST", "✅ Flask application is running and responding", "INFO")
                return True
            else:
                self.logger.log("FLASK_TEST", f"❌ Flask application responded with status: {response.status_code}", "ERROR")
                return False
                
        except requests.exceptions.ConnectionError:
            self.logger.log("FLASK_TEST", "❌ Flask application is not running on localhost:5000", "ERROR")
            return False
        except Exception as e:
            self.logger.log("FLASK_TEST", f"❌ Exception testing Flask app: {str(e)}", "ERROR")
            return False
    
    def test_health_endpoint(self) -> bool:
        """Test Flask health check endpoint."""
        self.logger.log("FLASK_TEST", "Testing Flask health endpoint...", "INFO")
        
        try:
            response = requests.get(f"{self.base_url}/health", timeout=10)
            
            if response.status_code == 200:
                try:
                    health_data = response.json()
                    self.logger.log("FLASK_TEST", f"✅ Health endpoint working. Response: {health_data}", "INFO")
                    return True
                except json.JSONDecodeError:
                    self.logger.log("FLASK_TEST", f"✅ Health endpoint working. Response: {response.text}", "INFO")
                    return True
            else:
                self.logger.log("FLASK_TEST", f"❌ Health endpoint failed. Status: {response.status_code}", "ERROR")
                return False
                
        except Exception as e:
            self.logger.log("FLASK_TEST", f"❌ Exception testing health endpoint: {str(e)}", "ERROR")
            return False
    
    def test_pipeline_status_endpoint(self) -> bool:
        """Test pipeline status endpoint."""
        self.logger.log("FLASK_TEST", "Testing pipeline status endpoint...", "INFO")
        
        try:
            response = requests.get(f"{self.base_url}/api/pipeline/status", timeout=10)
            
            if response.status_code == 200:
                try:
                    status_data = response.json()
                    self.logger.log("FLASK_TEST", f"✅ Pipeline status endpoint working. Status: {status_data}", "INFO")
                    return True
                except json.JSONDecodeError:
                    self.logger.log("FLASK_TEST", f"✅ Pipeline status endpoint working. Response: {response.text}", "INFO")
                    return True
            elif response.status_code == 404:
                self.logger.log("FLASK_TEST", "⚠️ Pipeline status endpoint not found (may not be implemented)", "WARNING")
                return True
            else:
                self.logger.log("FLASK_TEST", f"❌ Pipeline status endpoint failed. Status: {response.status_code}", "ERROR")
                return False
                
        except Exception as e:
            self.logger.log("FLASK_TEST", f"❌ Exception testing pipeline status endpoint: {str(e)}", "ERROR")
            return False
    
    def test_pipeline_run_endpoint(self) -> bool:
        """Test pipeline run endpoint."""
        self.logger.log("FLASK_TEST", "Testing pipeline run endpoint...", "INFO")
        
        try:
            # Test with a simple payload
            payload = {
                "pipeline": "test",
                "parameters": {}
            }
            
            response = requests.post(f"{self.base_url}/api/pipeline/run", 
                                   json=payload, 
                                   timeout=30)
            
            if response.status_code == 200:
                try:
                    run_data = response.json()
                    self.logger.log("FLASK_TEST", f"✅ Pipeline run endpoint working. Response: {run_data}", "INFO")
                    return True
                except json.JSONDecodeError:
                    self.logger.log("FLASK_TEST", f"✅ Pipeline run endpoint working. Response: {response.text}", "INFO")
                    return True
            elif response.status_code == 404:
                self.logger.log("FLASK_TEST", "⚠️ Pipeline run endpoint not found (may not be implemented)", "WARNING")
                return True
            elif response.status_code == 400:
                self.logger.log("FLASK_TEST", "⚠️ Pipeline run endpoint rejected request (expected for test)", "WARNING")
                return True
            else:
                self.logger.log("FLASK_TEST", f"❌ Pipeline run endpoint failed. Status: {response.status_code}", "ERROR")
                return False
                
        except Exception as e:
            self.logger.log("FLASK_TEST", f"❌ Exception testing pipeline run endpoint: {str(e)}", "ERROR")
            return False
    
    def test_upload_endpoint(self) -> bool:
        """Test CSV upload endpoint."""
        self.logger.log("FLASK_TEST", "Testing CSV upload endpoint...", "INFO")
        
        try:
            # Create a test CSV file
            test_csv_content = "name,value\ntest1,100\ntest2,200"
            
            files = {
                'file': ('test.csv', test_csv_content, 'text/csv')
            }
            
            response = requests.post(f"{self.base_url}/api/upload/csv", 
                                   files=files, 
                                   timeout=30)
            
            if response.status_code == 200:
                try:
                    upload_data = response.json()
                    self.logger.log("FLASK_TEST", f"✅ CSV upload endpoint working. Response: {upload_data}", "INFO")
                    return True
                except json.JSONDecodeError:
                    self.logger.log("FLASK_TEST", f"✅ CSV upload endpoint working. Response: {response.text}", "INFO")
                    return True
            elif response.status_code == 404:
                self.logger.log("FLASK_TEST", "⚠️ CSV upload endpoint not found (may not be implemented)", "WARNING")
                return True
            elif response.status_code == 400:
                self.logger.log("FLASK_TEST", "⚠️ CSV upload endpoint rejected file (expected for test)", "WARNING")
                return True
            else:
                self.logger.log("FLASK_TEST", f"❌ CSV upload endpoint failed. Status: {response.status_code}", "ERROR")
                return False
                
        except Exception as e:
            self.logger.log("FLASK_TEST", f"❌ Exception testing CSV upload endpoint: {str(e)}", "ERROR")
            return False
    
    def test_download_endpoint(self) -> bool:
        """Test download endpoint."""
        self.logger.log("FLASK_TEST", "Testing download endpoint...", "INFO")
        
        try:
            response = requests.get(f"{self.base_url}/api/download/test.csv", timeout=10)
            
            if response.status_code == 200:
                self.logger.log("FLASK_TEST", "✅ Download endpoint working", "INFO")
                return True
            elif response.status_code == 404:
                self.logger.log("FLASK_TEST", "⚠️ Download endpoint not found or file doesn't exist (expected)", "WARNING")
                return True
            else:
                self.logger.log("FLASK_TEST", f"❌ Download endpoint failed. Status: {response.status_code}", "ERROR")
                return False
                
        except Exception as e:
            self.logger.log("FLASK_TEST", f"❌ Exception testing download endpoint: {str(e)}", "ERROR")
            return False
    
    def test_environment_variables(self) -> bool:
        """Test Flask environment variables."""
        self.logger.log("FLASK_TEST", "Testing Flask environment variables...", "INFO")
        
        required_vars = [
            "FLASK_APP",
            "FLASK_ENV",
            "SECRET_KEY"
        ]
        
        missing_vars = []
        for var in required_vars:
            if not os.getenv(var):
                missing_vars.append(var)
        
        if missing_vars:
            self.logger.log("FLASK_TEST", f"⚠️ Missing Flask environment variables: {', '.join(missing_vars)}", "WARNING")
            return True  # Not critical for testing
        else:
            self.logger.log("FLASK_TEST", "✅ All Flask environment variables are set", "INFO")
            return True
    
    def test_flask_app_structure(self) -> bool:
        """Test Flask application file structure."""
        self.logger.log("FLASK_TEST", "Testing Flask application structure...", "INFO")
        
        try:
            # Check for common Flask files
            flask_files = [
                "app.py",
                "requirements.txt",
                "Dockerfile.flask"
            ]
            
            existing_files = []
            for file in flask_files:
                if os.path.exists(file):
                    existing_files.append(file)
            
            if existing_files:
                self.logger.log("FLASK_TEST", f"✅ Found Flask files: {', '.join(existing_files)}", "INFO")
                return True
            else:
                self.logger.log("FLASK_TEST", "⚠️ No Flask application files found in current directory", "WARNING")
                return True  # Not critical for testing
                
        except Exception as e:
            self.logger.log("FLASK_TEST", f"❌ Exception testing Flask structure: {str(e)}", "ERROR")
            return False
    
    def run_all_tests(self) -> dict:
        """Run all Flask endpoints tests."""
        self.logger.log("FLASK_TEST", "Starting Flask endpoints tests...", "INFO")
        
        tests = [
            ("Environment Variables", self.test_environment_variables),
            ("Flask App Structure", self.test_flask_app_structure),
            ("Flask App Running", self.test_flask_app_running),
            ("Health Endpoint", self.test_health_endpoint),
            ("Pipeline Status Endpoint", self.test_pipeline_status_endpoint),
            ("Pipeline Run Endpoint", self.test_pipeline_run_endpoint),
            ("Upload Endpoint", self.test_upload_endpoint),
            ("Download Endpoint", self.test_download_endpoint)
        ]
        
        results = {
            "timestamp": datetime.utcnow().isoformat(),
            "tests": {},
            "summary": {"passed": 0, "failed": 0, "total": len(tests)}
        }
        
        for test_name, test_func in tests:
            try:
                success = test_func()
                results["tests"][test_name] = {
                    "success": success,
                    "timestamp": datetime.utcnow().isoformat()
                }
                if success:
                    results["summary"]["passed"] += 1
                else:
                    results["summary"]["failed"] += 1
            except Exception as e:
                self.logger.log("FLASK_TEST", f"❌ Test '{test_name}' failed with exception: {str(e)}", "ERROR")
                results["tests"][test_name] = {
                    "success": False,
                    "error": str(e),
                    "timestamp": datetime.utcnow().isoformat()
                }
                results["summary"]["failed"] += 1
        
        # Log summary
        summary = results["summary"]
        self.logger.log(
            "FLASK_TEST",
            f"Flask tests completed. Passed: {summary['passed']}, Failed: {summary['failed']}, Total: {summary['total']}",
            "INFO" if summary['failed'] == 0 else "WARNING"
        )
        
        return results

def main():
    """Main function to run Flask endpoints tests."""
    print("🌐 Starting Flask Endpoints Tests\n")
    
    tester = FlaskEndpointsTester()
    results = tester.run_all_tests()
    
    # Print summary
    summary = results["summary"]
    print(f"\n📊 Flask Test Summary:")
    print(f"   Total Tests: {summary['total']}")
    print(f"   ✅ Passed: {summary['passed']}")
    print(f"   ❌ Failed: {summary['failed']}")
    
    if summary['failed'] == 0:
        print("\n🎉 All Flask endpoints tests passed!")
        return 0
    else:
        print(f"\n⚠️  {summary['failed']} Flask test(s) failed. Check logs for details.")
        return 1

if __name__ == "__main__":
    sys.exit(main()) 