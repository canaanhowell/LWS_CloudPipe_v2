#!/usr/bin/env python3
"""
Monday.com connection test script
Tests Monday.com API connection and GraphQL queries
"""

import os
import sys
import json
import requests
from pathlib import Path
from datetime import datetime

# Add the Utils directory to the path so we can import the logger
sys.path.append(str(Path(__file__).parent.parent / "Utils"))

from logger import PipelineLogger

class MondayConnectionTester:
    def __init__(self):
        """Initialize the Monday.com connection tester."""
        self.logger = PipelineLogger(log_dir="logs", log_file="monday_connection_test.log")
        
    def test_environment_variables(self) -> bool:
        """Test that Monday.com environment variables are set."""
        self.logger.log("MONDAY_TEST", "Testing Monday.com environment variables...", "INFO")
        
        required_vars = [
            "MONDAY_API_KEY",
            "SEAL_RESI_BOARD_ID",
            "SEAL_COMM_SALES_BOARD_ID",
            "SEAL_COMM_PM_BOARD_ID"
        ]
        
        missing_vars = []
        for var in required_vars:
            if not os.getenv(var):
                missing_vars.append(var)
        
        if missing_vars:
            self.logger.log("MONDAY_TEST", f"❌ Missing environment variables: {', '.join(missing_vars)}", "ERROR")
            return False
        else:
            self.logger.log("MONDAY_TEST", "✅ All Monday.com environment variables are set", "INFO")
            return True
    
    def test_api_key_format(self) -> bool:
        """Test Monday.com API key format."""
        self.logger.log("MONDAY_TEST", "Testing Monday.com API key format...", "INFO")
        
        try:
            api_key = os.getenv("MONDAY_API_KEY")
            if not api_key:
                self.logger.log("MONDAY_TEST", "❌ Monday.com API key not found", "ERROR")
                return False
            
            # Monday.com API keys are typically long strings
            if len(api_key) > 20:
                self.logger.log("MONDAY_TEST", "✅ Monday.com API key format appears valid", "INFO")
                return True
            else:
                self.logger.log("MONDAY_TEST", "❌ Monday.com API key appears too short", "ERROR")
                return False
                
        except Exception as e:
            self.logger.log("MONDAY_TEST", f"❌ Exception during API key format test: {str(e)}", "ERROR")
            return False
    
    def test_api_endpoint_access(self) -> bool:
        """Test Monday.com API endpoint access."""
        self.logger.log("MONDAY_TEST", "Testing Monday.com API endpoint access...", "INFO")
        
        try:
            api_key = os.getenv("MONDAY_API_KEY")
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
                    user_name = data['data']['me']['name']
                    self.logger.log("MONDAY_TEST", f"✅ Successfully connected to Monday.com API. User: {user_name}", "INFO")
                    return True
                else:
                    self.logger.log("MONDAY_TEST", f"❌ API response format unexpected: {data}", "ERROR")
                    return False
            elif response.status_code == 401:
                self.logger.log("MONDAY_TEST", "❌ Monday.com API authentication failed (invalid API key)", "ERROR")
                return False
            else:
                self.logger.log("MONDAY_TEST", f"❌ API endpoint test failed. Status: {response.status_code}, Response: {response.text}", "ERROR")
                return False
                
        except Exception as e:
            self.logger.log("MONDAY_TEST", f"❌ Exception during API endpoint test: {str(e)}", "ERROR")
            return False
    
    def test_board_access(self) -> bool:
        """Test access to specific Monday.com boards."""
        self.logger.log("MONDAY_TEST", "Testing Monday.com board access...", "INFO")
        
        try:
            api_key = os.getenv("MONDAY_API_KEY")
            board_ids = [
                os.getenv("SEAL_RESI_BOARD_ID"),
                os.getenv("SEAL_COMM_SALES_BOARD_ID"),
                os.getenv("SEAL_COMM_PM_BOARD_ID")
            ]
            
            headers = {
                'Authorization': api_key,
                'Content-Type': 'application/json'
            }
            
            accessible_boards = []
            
            for board_id in board_ids:
                if not board_id:
                    continue
                    
                query = f"""
                query {{
                    boards(ids: [{board_id}]) {{
                        id
                        name
                        state
                    }}
                }}
                """
                
                response = requests.post(
                    "https://api.monday.com/v2",
                    headers=headers,
                    json={'query': query},
                    timeout=30
                )
                
                if response.status_code == 200:
                    data = response.json()
                    if 'data' in data and 'boards' in data['data'] and data['data']['boards']:
                        board_name = data['data']['boards'][0]['name']
                        accessible_boards.append(f"{board_name} (ID: {board_id})")
                    else:
                        self.logger.log("MONDAY_TEST", f"⚠️ Board {board_id} not accessible or not found", "WARNING")
                else:
                    self.logger.log("MONDAY_TEST", f"⚠️ Failed to access board {board_id}. Status: {response.status_code}", "WARNING")
            
            if accessible_boards:
                self.logger.log("MONDAY_TEST", f"✅ Successfully accessed {len(accessible_boards)} boards: {', '.join(accessible_boards)}", "INFO")
                return True
            else:
                self.logger.log("MONDAY_TEST", "❌ No boards accessible", "ERROR")
                return False
                
        except Exception as e:
            self.logger.log("MONDAY_TEST", f"❌ Exception during board access test: {str(e)}", "ERROR")
            return False
    
    def test_graphql_queries(self) -> bool:
        """Test GraphQL query functionality."""
        self.logger.log("MONDAY_TEST", "Testing Monday.com GraphQL queries...", "INFO")
        
        try:
            api_key = os.getenv("MONDAY_API_KEY")
            board_id = os.getenv("SEAL_RESI_BOARD_ID")
            
            if not board_id:
                self.logger.log("MONDAY_TEST", "⚠️ No board ID available for GraphQL query test", "WARNING")
                return True
            
            headers = {
                'Authorization': api_key,
                'Content-Type': 'application/json'
            }
            
            # Test a more complex query
            query = f"""
            query {{
                boards(ids: [{board_id}]) {{
                    id
                    name
                    items {{
                        id
                        name
                        column_values {{
                            id
                            title
                            value
                        }}
                    }}
                }}
            }}
            """
            
            response = requests.post(
                "https://api.monday.com/v2",
                headers=headers,
                json={'query': query},
                timeout=30
            )
            
            if response.status_code == 200:
                data = response.json()
                if 'data' in data and 'boards' in data['data']:
                    self.logger.log("MONDAY_TEST", "✅ GraphQL queries working correctly", "INFO")
                    return True
                else:
                    self.logger.log("MONDAY_TEST", f"❌ GraphQL query response format unexpected: {data}", "ERROR")
                    return False
            else:
                self.logger.log("MONDAY_TEST", f"❌ GraphQL query failed. Status: {response.status_code}", "ERROR")
                return False
                
        except Exception as e:
            self.logger.log("MONDAY_TEST", f"❌ Exception during GraphQL query test: {str(e)}", "ERROR")
            return False
    
    def run_all_tests(self) -> dict:
        """Run all Monday.com connection tests."""
        self.logger.log("MONDAY_TEST", "Starting Monday.com connection tests...", "INFO")
        
        tests = [
            ("Environment Variables", self.test_environment_variables),
            ("API Key Format", self.test_api_key_format),
            ("API Endpoint Access", self.test_api_endpoint_access),
            ("Board Access", self.test_board_access),
            ("GraphQL Queries", self.test_graphql_queries)
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
                self.logger.log("MONDAY_TEST", f"❌ Test '{test_name}' failed with exception: {str(e)}", "ERROR")
                results["tests"][test_name] = {
                    "success": False,
                    "error": str(e),
                    "timestamp": datetime.utcnow().isoformat()
                }
                results["summary"]["failed"] += 1
        
        # Log summary
        summary = results["summary"]
        self.logger.log(
            "MONDAY_TEST",
            f"Monday.com tests completed. Passed: {summary['passed']}, Failed: {summary['failed']}, Total: {summary['total']}",
            "INFO" if summary['failed'] == 0 else "WARNING"
        )
        
        return results

def main():
    """Main function to run Monday.com connection tests."""
    print("📅 Starting Monday.com Connection Tests\n")
    
    tester = MondayConnectionTester()
    results = tester.run_all_tests()
    
    # Print summary
    summary = results["summary"]
    print(f"\n📊 Monday.com Test Summary:")
    print(f"   Total Tests: {summary['total']}")
    print(f"   ✅ Passed: {summary['passed']}")
    print(f"   ❌ Failed: {summary['failed']}")
    
    if summary['failed'] == 0:
        print("\n🎉 All Monday.com connection tests passed!")
        return 0
    else:
        print(f"\n⚠️  {summary['failed']} Monday.com test(s) failed. Check logs for details.")
        return 1

if __name__ == "__main__":
    sys.exit(main()) 