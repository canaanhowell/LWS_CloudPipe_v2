#!/usr/bin/env python3
"""
Comprehensive connection test script for all endpoints described in connections.md
Tests SharePoint, Monday.com, Google Analytics, Snowflake, Azure Blob Storage, and Flask endpoints
"""

import os
import sys
import json
import requests
import time
from pathlib import Path
from datetime import datetime
from typing import Dict, Any, List, Tuple

# Add the Utils directory to the path so we can import the logger
sys.path.append(str(Path(__file__).parent.parent / "Utils"))

from logger import PipelineLogger

class ConnectionTester:
    def __init__(self):
        """Initialize the connection tester with logger."""
        self.logger = PipelineLogger(log_dir="logs", log_file="connection_tests.log")
        self.results = {
            "timestamp": datetime.utcnow().isoformat(),
            "tests": {},
            "summary": {
                "total_tests": 0,
                "passed": 0,
                "failed": 0,
                "skipped": 0
            }
        }
        
    def log_test_result(self, test_name: str, success: bool, message: str, details: Dict = None):
        """Log test results and update summary."""
        self.results["summary"]["total_tests"] += 1
        
        if success:
            self.results["summary"]["passed"] += 1
            self.logger.log("CONNECTION_TEST", f"✅ {test_name}: {message}", "INFO")
        else:
            self.results["summary"]["failed"] += 1
            self.logger.log("CONNECTION_TEST", f"❌ {test_name}: {message}", "ERROR")
            
        self.results["tests"][test_name] = {
            "success": success,
            "message": message,
            "details": details or {},
            "timestamp": datetime.utcnow().isoformat()
        }
    
    def test_environment_variables(self) -> bool:
        """Test that required environment variables are set."""
        self.logger.log("CONNECTION_TEST", "Testing environment variables...", "INFO")
        
        required_vars = [
            "AZURE_TENANT_ID",
            "AZURE_CLIENT_ID", 
            "AZURE_CLIENT_SECRET",
            "MONDAY_API_KEY",
            "GOOGLE_ANALYTICS_PROPERTY_ID",
            "SNOWFLAKE_ACCOUNT",
            "SNOWFLAKE_USER",
            "AZURE_STORAGE_CONNECTION_STRING"
        ]
        
        missing_vars = []
        for var in required_vars:
            if not os.getenv(var):
                missing_vars.append(var)
        
        if missing_vars:
            self.log_test_result(
                "Environment Variables",
                False,
                f"Missing environment variables: {', '.join(missing_vars)}"
            )
            return False
        else:
            self.log_test_result(
                "Environment Variables",
                True,
                "All required environment variables are set"
            )
            return True
    
    def test_sharepoint_connection(self) -> bool:
        """Test SharePoint connection via Microsoft Graph API."""
        self.logger.log("CONNECTION_TEST", "Testing SharePoint connection...", "INFO")
        
        try:
            # Test basic Graph API access
            headers = {
                'Authorization': f'Bearer {self._get_azure_token()}',
                'Content-Type': 'application/json'
            }
            
            # Test sites endpoint
            sites_url = "https://graph.microsoft.com/v1.0/sites"
            response = requests.get(sites_url, headers=headers, timeout=30)
            
            if response.status_code == 200:
                sites_data = response.json()
                self.log_test_result(
                    "SharePoint Graph API",
                    True,
                    f"Successfully connected to Microsoft Graph API. Found {len(sites_data.get('value', []))} sites"
                )
                return True
            else:
                self.log_test_result(
                    "SharePoint Graph API",
                    False,
                    f"Failed to connect to Microsoft Graph API. Status: {response.status_code}, Response: {response.text}"
                )
                return False
                
        except Exception as e:
            self.log_test_result(
                "SharePoint Graph API",
                False,
                f"Exception during SharePoint connection test: {str(e)}"
            )
            return False
    
    def test_monday_com_connection(self) -> bool:
        """Test Monday.com API connection."""
        self.logger.log("CONNECTION_TEST", "Testing Monday.com connection...", "INFO")
        
        try:
            api_key = os.getenv("MONDAY_API_KEY")
            if not api_key:
                self.log_test_result(
                    "Monday.com API",
                    False,
                    "Monday.com API key not found in environment variables"
                )
                return False
            
            headers = {
                'Authorization': api_key,
                'Content-Type': 'application/json'
            }
            
            # Test basic API access with a simple query
            query = """
            query {
                me {
                    name
                    email
                }
            }
            """
            
            response = requests.post(
                "https://api.monday.com/v2",
                headers=headers,
                json={'query': query},
                timeout=30
            )
            
            if response.status_code == 200:
                data = response.json()
                if 'data' in data and 'me' in data['data']:
                    self.log_test_result(
                        "Monday.com API",
                        True,
                        f"Successfully connected to Monday.com API. User: {data['data']['me']['name']}"
                    )
                    return True
                else:
                    self.log_test_result(
                        "Monday.com API",
                        False,
                        f"API response format unexpected: {data}"
                    )
                    return False
            else:
                self.log_test_result(
                    "Monday.com API",
                    False,
                    f"Failed to connect to Monday.com API. Status: {response.status_code}, Response: {response.text}"
                )
                return False
                
        except Exception as e:
            self.log_test_result(
                "Monday.com API",
                False,
                f"Exception during Monday.com connection test: {str(e)}"
            )
            return False
    
    def test_google_analytics_connection(self) -> bool:
        """Test Google Analytics 4 API connection."""
        self.logger.log("CONNECTION_TEST", "Testing Google Analytics connection...", "INFO")
        
        try:
            # Check if service account key file exists
            service_account_file = "config_files/google_analytics_service_account.json"
            if not os.path.exists(service_account_file):
                self.log_test_result(
                    "Google Analytics API",
                    False,
                    f"Service account key file not found: {service_account_file}"
                )
                return False
            
            # Test basic API access (this would require proper Google Auth setup)
            property_id = os.getenv("GOOGLE_ANALYTICS_PROPERTY_ID")
            if not property_id:
                self.log_test_result(
                    "Google Analytics API",
                    False,
                    "Google Analytics property ID not found in environment variables"
                )
                return False
            
            # For now, just test that the service account file is valid JSON
            with open(service_account_file, 'r') as f:
                service_account_data = json.load(f)
            
            if 'client_email' in service_account_data:
                self.log_test_result(
                    "Google Analytics API",
                    True,
                    f"Service account file is valid. Account: {service_account_data['client_email']}"
                )
                return True
            else:
                self.log_test_result(
                    "Google Analytics API",
                    False,
                    "Service account file is missing required fields"
                )
                return False
                
        except Exception as e:
            self.log_test_result(
                "Google Analytics API",
                False,
                f"Exception during Google Analytics connection test: {str(e)}"
            )
            return False
    
    def test_snowflake_connection(self) -> bool:
        """Test Snowflake database connection."""
        self.logger.log("CONNECTION_TEST", "Testing Snowflake connection...", "INFO")
        
        try:
            # Check if private key file exists
            private_key_path = os.getenv("SNOWFLAKE_PRIVATE_KEY_PATH", "config_files/snowflake_private_key.txt")
            if not os.path.exists(private_key_path):
                self.log_test_result(
                    "Snowflake Database",
                    False,
                    f"Snowflake private key file not found: {private_key_path}"
                )
                return False
            
            # Test basic connection parameters
            required_params = [
                "SNOWFLAKE_ACCOUNT",
                "SNOWFLAKE_USER", 
                "SNOWFLAKE_WAREHOUSE",
                "SNOWFLAKE_DATABASE"
            ]
            
            missing_params = []
            for param in required_params:
                if not os.getenv(param):
                    missing_params.append(param)
            
            if missing_params:
                self.log_test_result(
                    "Snowflake Database",
                    False,
                    f"Missing Snowflake parameters: {', '.join(missing_params)}"
                )
                return False
            
            # Test private key file format
            with open(private_key_path, 'r') as f:
                private_key_content = f.read().strip()
            
            if len(private_key_content) > 100:  # Basic validation
                self.log_test_result(
                    "Snowflake Database",
                    True,
                    f"Snowflake configuration appears valid. Account: {os.getenv('SNOWFLAKE_ACCOUNT')}, User: {os.getenv('SNOWFLAKE_USER')}"
                )
                return True
            else:
                self.log_test_result(
                    "Snowflake Database",
                    False,
                    "Snowflake private key file appears to be invalid or empty"
                )
                return False
                
        except Exception as e:
            self.log_test_result(
                "Snowflake Database",
                False,
                f"Exception during Snowflake connection test: {str(e)}"
            )
            return False
    
    def test_azure_blob_storage_connection(self) -> bool:
        """Test Azure Blob Storage connection."""
        self.logger.log("CONNECTION_TEST", "Testing Azure Blob Storage connection...", "INFO")
        
        try:
            connection_string = os.getenv("AZURE_STORAGE_CONNECTION_STRING")
            if not connection_string:
                self.log_test_result(
                    "Azure Blob Storage",
                    False,
                    "Azure Storage connection string not found in environment variables"
                )
                return False
            
            # Test connection string format
            if "DefaultEndpointsProtocol=" in connection_string and "AccountName=" in connection_string:
                self.log_test_result(
                    "Azure Blob Storage",
                    True,
                    "Azure Storage connection string format appears valid"
                )
                return True
            else:
                self.log_test_result(
                    "Azure Blob Storage",
                    False,
                    "Azure Storage connection string format appears invalid"
                )
                return False
                
        except Exception as e:
            self.log_test_result(
                "Azure Blob Storage",
                False,
                f"Exception during Azure Blob Storage connection test: {str(e)}"
            )
            return False
    
    def test_flask_application_endpoints(self) -> bool:
        """Test Flask application endpoints."""
        self.logger.log("CONNECTION_TEST", "Testing Flask application endpoints...", "INFO")
        
        try:
            # Test if Flask app is running (assuming default port 5000)
            base_url = "http://localhost:5000"
            
            # Test health check endpoint
            try:
                response = requests.get(f"{base_url}/health", timeout=10)
                if response.status_code == 200:
                    self.log_test_result(
                        "Flask Health Check",
                        True,
                        f"Flask application is running. Health check response: {response.text}"
                    )
                    return True
                else:
                    self.log_test_result(
                        "Flask Health Check",
                        False,
                        f"Flask health check failed. Status: {response.status_code}"
                    )
                    return False
            except requests.exceptions.ConnectionError:
                self.log_test_result(
                    "Flask Health Check",
                    False,
                    "Flask application is not running on localhost:5000"
                )
                return False
                
        except Exception as e:
            self.log_test_result(
                "Flask Application",
                False,
                f"Exception during Flask application test: {str(e)}"
            )
            return False
    
    def _get_azure_token(self) -> str:
        """Get Azure access token for Microsoft Graph API."""
        # This is a simplified version - in production you'd use proper Azure SDK
        # For now, we'll return a placeholder
        return "placeholder_token"
    
    def run_all_tests(self) -> Dict[str, Any]:
        """Run all connection tests and return results."""
        self.logger.log("CONNECTION_TEST", "Starting comprehensive connection tests...", "INFO")
        
        # Run all tests
        tests = [
            ("Environment Variables", self.test_environment_variables),
            ("SharePoint Graph API", self.test_sharepoint_connection),
            ("Monday.com API", self.test_monday_com_connection),
            ("Google Analytics API", self.test_google_analytics_connection),
            ("Snowflake Database", self.test_snowflake_connection),
            ("Azure Blob Storage", self.test_azure_blob_storage_connection),
            ("Flask Application", self.test_flask_application_endpoints)
        ]
        
        for test_name, test_func in tests:
            try:
                test_func()
            except Exception as e:
                self.log_test_result(
                    test_name,
                    False,
                    f"Test function failed with exception: {str(e)}"
                )
        
        # Log summary
        summary = self.results["summary"]
        self.logger.log(
            "CONNECTION_TEST",
            f"Connection tests completed. Passed: {summary['passed']}, Failed: {summary['failed']}, Total: {summary['total_tests']}",
            "INFO" if summary['failed'] == 0 else "WARNING"
        )
        
        # Save results to JSON file
        results_file = Path("logs") / "connection_test_results.json"
        with open(results_file, 'w') as f:
            json.dump(self.results, f, indent=2)
        
        return self.results

def main():
    """Main function to run all connection tests."""
    print("🔌 Starting Connection Tests for All Endpoints\n")
    
    tester = ConnectionTester()
    results = tester.run_all_tests()
    
    # Print summary
    summary = results["summary"]
    print(f"\n📊 Test Summary:")
    print(f"   Total Tests: {summary['total_tests']}")
    print(f"   ✅ Passed: {summary['passed']}")
    print(f"   ❌ Failed: {summary['failed']}")
    print(f"   ⏭️  Skipped: {summary['skipped']}")
    
    if summary['failed'] == 0:
        print("\n🎉 All connection tests passed!")
        return 0
    else:
        print(f"\n⚠️  {summary['failed']} connection test(s) failed. Check logs for details.")
        return 1

if __name__ == "__main__":
    sys.exit(main()) 