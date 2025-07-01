#!/usr/bin/env python3
"""
Snowflake connection test script
Tests Snowflake database connection and basic operations
"""

import os
import sys
import json
import base64
from pathlib import Path
from datetime import datetime

# Add the Utils directory to the path so we can import the logger
sys.path.append(str(Path(__file__).parent.parent / "Utils"))

from logger import PipelineLogger

class SnowflakeConnectionTester:
    def __init__(self):
        """Initialize the Snowflake connection tester."""
        self.logger = PipelineLogger(log_dir="logs", log_file="snowflake_connection_test.log")
        
    def test_environment_variables(self) -> bool:
        """Test that Snowflake environment variables are set."""
        self.logger.log("SNOWFLAKE_TEST", "Testing Snowflake environment variables...", "INFO")
        
        required_vars = [
            "SNOWFLAKE_ACCOUNT",
            "SNOWFLAKE_USER",
            "SNOWFLAKE_WAREHOUSE",
            "SNOWFLAKE_DATABASE"
        ]
        
        missing_vars = []
        for var in required_vars:
            if not os.getenv(var):
                missing_vars.append(var)
        
        if missing_vars:
            self.logger.log("SNOWFLAKE_TEST", f"❌ Missing environment variables: {', '.join(missing_vars)}", "ERROR")
            return False
        else:
            self.logger.log("SNOWFLAKE_TEST", "✅ All Snowflake environment variables are set", "INFO")
            return True
    
    def test_private_key_file(self) -> bool:
        """Test Snowflake private key file existence and format."""
        self.logger.log("SNOWFLAKE_TEST", "Testing Snowflake private key file...", "INFO")
        
        try:
            private_key_path = os.getenv("SNOWFLAKE_PRIVATE_KEY_PATH", "config_files/snowflake_private_key.txt")
            
            if not os.path.exists(private_key_path):
                self.logger.log("SNOWFLAKE_TEST", f"❌ Snowflake private key file not found: {private_key_path}", "ERROR")
                return False
            
            # Read and validate the private key
            with open(private_key_path, 'r') as f:
                private_key_content = f.read().strip()
            
            if not private_key_content:
                self.logger.log("SNOWFLAKE_TEST", "❌ Snowflake private key file is empty", "ERROR")
                return False
            
            # Basic validation - check if it looks like a base64 DER key
            if len(private_key_content) < 100:
                self.logger.log("SNOWFLAKE_TEST", "❌ Snowflake private key appears too short", "ERROR")
                return False
            
            # Try to decode as base64 to validate format
            try:
                base64.b64decode(private_key_content)
                self.logger.log("SNOWFLAKE_TEST", "✅ Snowflake private key file format appears valid", "INFO")
                return True
            except Exception as e:
                self.logger.log("SNOWFLAKE_TEST", f"❌ Snowflake private key is not valid base64: {str(e)}", "ERROR")
                return False
                
        except Exception as e:
            self.logger.log("SNOWFLAKE_TEST", f"❌ Exception during private key file test: {str(e)}", "ERROR")
            return False
    
    def test_connection_parameters(self) -> bool:
        """Test Snowflake connection parameters."""
        self.logger.log("SNOWFLAKE_TEST", "Testing Snowflake connection parameters...", "INFO")
        
        try:
            account = os.getenv("SNOWFLAKE_ACCOUNT")
            user = os.getenv("SNOWFLAKE_USER")
            warehouse = os.getenv("SNOWFLAKE_WAREHOUSE")
            database = os.getenv("SNOWFLAKE_DATABASE")
            
            # Validate account format (should contain organization-account)
            if not account or '.' not in account:
                self.logger.log("SNOWFLAKE_TEST", "❌ Snowflake account format appears invalid", "ERROR")
                return False
            
            # Validate user format (should be email)
            if not user or '@' not in user:
                self.logger.log("SNOWFLAKE_TEST", "❌ Snowflake user format appears invalid", "ERROR")
                return False
            
            # Validate warehouse and database names
            if not warehouse or not database:
                self.logger.log("SNOWFLAKE_TEST", "❌ Snowflake warehouse or database not specified", "ERROR")
                return False
            
            self.logger.log("SNOWFLAKE_TEST", f"✅ Snowflake connection parameters valid. Account: {account}, User: {user}", "INFO")
            return True
            
        except Exception as e:
            self.logger.log("SNOWFLAKE_TEST", f"❌ Exception during connection parameters test: {str(e)}", "ERROR")
            return False
    
    def test_snowflake_connector_import(self) -> bool:
        """Test if Snowflake connector can be imported."""
        self.logger.log("SNOWFLAKE_TEST", "Testing Snowflake connector import...", "INFO")
        
        try:
            import snowflake.connector
            self.logger.log("SNOWFLAKE_TEST", "✅ Snowflake connector successfully imported", "INFO")
            return True
        except ImportError:
            self.logger.log("SNOWFLAKE_TEST", "❌ Snowflake connector not installed. Install with: pip install snowflake-connector-python", "ERROR")
            return False
        except Exception as e:
            self.logger.log("SNOWFLAKE_TEST", f"❌ Exception importing Snowflake connector: {str(e)}", "ERROR")
            return False
    
    def test_database_connection(self) -> bool:
        """Test actual database connection (if connector is available)."""
        self.logger.log("SNOWFLAKE_TEST", "Testing Snowflake database connection...", "INFO")
        
        try:
            # Try to import the connector
            try:
                import snowflake.connector
            except ImportError:
                self.logger.log("SNOWFLAKE_TEST", "⚠️ Skipping database connection test - connector not installed", "WARNING")
                return True
            
            # Test connection parameters
            account = os.getenv("SNOWFLAKE_ACCOUNT")
            user = os.getenv("SNOWFLAKE_USER")
            warehouse = os.getenv("SNOWFLAKE_WAREHOUSE")
            database = os.getenv("SNOWFLAKE_DATABASE")
            private_key_path = os.getenv("SNOWFLAKE_PRIVATE_KEY_PATH", "config_files/snowflake_private_key.txt")
            
            if not all([account, user, warehouse, database, os.path.exists(private_key_path)]):
                self.logger.log("SNOWFLAKE_TEST", "⚠️ Skipping database connection test - missing parameters", "WARNING")
                return True
            
            # Read private key
            with open(private_key_path, 'r') as f:
                private_key = f.read().strip()
            
            # Test connection
            try:
                conn = snowflake.connector.connect(
                    account=account,
                    user=user,
                    private_key=private_key,
                    warehouse=warehouse,
                    database=database
                )
                
                # Test a simple query
                cursor = conn.cursor()
                cursor.execute("SELECT CURRENT_VERSION()")
                version = cursor.fetchone()[0]
                
                cursor.close()
                conn.close()
                
                self.logger.log("SNOWFLAKE_TEST", f"✅ Successfully connected to Snowflake. Version: {version}", "INFO")
                return True
                
            except Exception as e:
                self.logger.log("SNOWFLAKE_TEST", f"❌ Database connection failed: {str(e)}", "ERROR")
                return False
                
        except Exception as e:
            self.logger.log("SNOWFLAKE_TEST", f"❌ Exception during database connection test: {str(e)}", "ERROR")
            return False
    
    def test_database_structure(self) -> bool:
        """Test database structure and access to required databases."""
        self.logger.log("SNOWFLAKE_TEST", "Testing Snowflake database structure...", "INFO")
        
        try:
            # Try to import the connector
            try:
                import snowflake.connector
            except ImportError:
                self.logger.log("SNOWFLAKE_TEST", "⚠️ Skipping database structure test - connector not installed", "WARNING")
                return True
            
            # Test access to required databases
            required_databases = ["LWS", "SEAL", "SHARED_DIMENSIONS"]
            
            account = os.getenv("SNOWFLAKE_ACCOUNT")
            user = os.getenv("SNOWFLAKE_USER")
            private_key_path = os.getenv("SNOWFLAKE_PRIVATE_KEY_PATH", "config_files/snowflake_private_key.txt")
            
            if not all([account, user, os.path.exists(private_key_path)]):
                self.logger.log("SNOWFLAKE_TEST", "⚠️ Skipping database structure test - missing parameters", "WARNING")
                return True
            
            # Read private key
            with open(private_key_path, 'r') as f:
                private_key = f.read().strip()
            
            try:
                conn = snowflake.connector.connect(
                    account=account,
                    user=user,
                    private_key=private_key
                )
                
                cursor = conn.cursor()
                
                # Test database access
                accessible_databases = []
                for db in required_databases:
                    try:
                        cursor.execute(f"SHOW DATABASES LIKE '{db}'")
                        result = cursor.fetchone()
                        if result:
                            accessible_databases.append(db)
                    except Exception:
                        pass
                
                cursor.close()
                conn.close()
                
                if accessible_databases:
                    self.logger.log("SNOWFLAKE_TEST", f"✅ Accessible databases: {', '.join(accessible_databases)}", "INFO")
                    return True
                else:
                    self.logger.log("SNOWFLAKE_TEST", "⚠️ No required databases accessible", "WARNING")
                    return True
                    
            except Exception as e:
                self.logger.log("SNOWFLAKE_TEST", f"❌ Database structure test failed: {str(e)}", "ERROR")
                return False
                
        except Exception as e:
            self.logger.log("SNOWFLAKE_TEST", f"❌ Exception during database structure test: {str(e)}", "ERROR")
            return False
    
    def run_all_tests(self) -> dict:
        """Run all Snowflake connection tests."""
        self.logger.log("SNOWFLAKE_TEST", "Starting Snowflake connection tests...", "INFO")
        
        tests = [
            ("Environment Variables", self.test_environment_variables),
            ("Private Key File", self.test_private_key_file),
            ("Connection Parameters", self.test_connection_parameters),
            ("Snowflake Connector Import", self.test_snowflake_connector_import),
            ("Database Connection", self.test_database_connection),
            ("Database Structure", self.test_database_structure)
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
                self.logger.log("SNOWFLAKE_TEST", f"❌ Test '{test_name}' failed with exception: {str(e)}", "ERROR")
                results["tests"][test_name] = {
                    "success": False,
                    "error": str(e),
                    "timestamp": datetime.utcnow().isoformat()
                }
                results["summary"]["failed"] += 1
        
        # Log summary
        summary = results["summary"]
        self.logger.log(
            "SNOWFLAKE_TEST",
            f"Snowflake tests completed. Passed: {summary['passed']}, Failed: {summary['failed']}, Total: {summary['total']}",
            "INFO" if summary['failed'] == 0 else "WARNING"
        )
        
        return results

def main():
    """Main function to run Snowflake connection tests."""
    print("❄️ Starting Snowflake Connection Tests\n")
    
    tester = SnowflakeConnectionTester()
    results = tester.run_all_tests()
    
    # Print summary
    summary = results["summary"]
    print(f"\n📊 Snowflake Test Summary:")
    print(f"   Total Tests: {summary['total']}")
    print(f"   ✅ Passed: {summary['passed']}")
    print(f"   ❌ Failed: {summary['failed']}")
    
    if summary['failed'] == 0:
        print("\n🎉 All Snowflake connection tests passed!")
        return 0
    else:
        print(f"\n⚠️  {summary['failed']} Snowflake test(s) failed. Check logs for details.")
        return 1

if __name__ == "__main__":
    sys.exit(main()) 