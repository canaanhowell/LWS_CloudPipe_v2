#!/usr/bin/env python3
"""
Master connection test runner
Runs all individual connection tests and provides a comprehensive summary
"""

import os
import sys
import json
import subprocess
from pathlib import Path
from datetime import datetime

# Add the Utils directory to the path so we can import the logger
sys.path.append(str(Path(__file__).parent.parent / "Utils"))

from logger import PipelineLogger

class MasterConnectionTester:
    def __init__(self):
        """Initialize the master connection tester."""
        self.logger = PipelineLogger(log_dir="logs", log_file="master_connection_tests.log")
        self.test_scripts = [
            "test_connections.py",
            "test_sharepoint_connection.py", 
            "test_monday_connection.py",
            "test_snowflake_connection.py",
            "test_flask_endpoints.py"
        ]
        
    def run_individual_test(self, script_name: str) -> dict:
        """Run an individual connection test script."""
        self.logger.log("MASTER_TEST", f"Running {script_name}...", "INFO")
        
        try:
            script_path = Path(__file__).parent / script_name
            
            if not script_path.exists():
                return {
                    "success": False,
                    "error": f"Script not found: {script_name}",
                    "timestamp": datetime.utcnow().isoformat()
                }
            
            # Run the test script
            result = subprocess.run(
                [sys.executable, str(script_path)],
                capture_output=True,
                text=True,
                timeout=300  # 5 minute timeout
            )
            
            success = result.returncode == 0
            
            return {
                "success": success,
                "return_code": result.returncode,
                "stdout": result.stdout,
                "stderr": result.stderr,
                "timestamp": datetime.utcnow().isoformat()
            }
            
        except subprocess.TimeoutExpired:
            return {
                "success": False,
                "error": f"Test timed out after 5 minutes: {script_name}",
                "timestamp": datetime.utcnow().isoformat()
            }
        except Exception as e:
            return {
                "success": False,
                "error": f"Exception running {script_name}: {str(e)}",
                "timestamp": datetime.utcnow().isoformat()
            }
    
    def run_all_tests(self) -> dict:
        """Run all connection tests and compile results."""
        self.logger.log("MASTER_TEST", "Starting master connection test suite...", "INFO")
        
        results = {
            "timestamp": datetime.utcnow().isoformat(),
            "tests": {},
            "summary": {
                "total_tests": len(self.test_scripts),
                "passed": 0,
                "failed": 0,
                "skipped": 0
            }
        }
        
        for script_name in self.test_scripts:
            test_result = self.run_individual_test(script_name)
            results["tests"][script_name] = test_result
            
            if test_result["success"]:
                results["summary"]["passed"] += 1
                self.logger.log("MASTER_TEST", f"✅ {script_name} completed successfully", "INFO")
            else:
                results["summary"]["failed"] += 1
                self.logger.log("MASTER_TEST", f"❌ {script_name} failed: {test_result.get('error', 'Unknown error')}", "ERROR")
        
        # Log summary
        summary = results["summary"]
        self.logger.log(
            "MASTER_TEST",
            f"Master test suite completed. Passed: {summary['passed']}, Failed: {summary['failed']}, Total: {summary['total_tests']}",
            "INFO" if summary['failed'] == 0 else "WARNING"
        )
        
        return results
    
    def generate_report(self, results: dict) -> str:
        """Generate a comprehensive test report."""
        summary = results["summary"]
        
        report = f"""
# Connection Test Report

**Generated:** {results['timestamp']}
**Total Tests:** {summary['total_tests']}
**Passed:** {summary['passed']}
**Failed:** {summary['failed']}
**Success Rate:** {(summary['passed'] / summary['total_tests'] * 100):.1f}%

## Test Results

"""
        
        for script_name, test_result in results["tests"].items():
            status = "✅ PASS" if test_result["success"] else "❌ FAIL"
            report += f"### {script_name}\n"
            report += f"**Status:** {status}\n"
            report += f"**Timestamp:** {test_result['timestamp']}\n"
            
            if not test_result["success"]:
                report += f"**Error:** {test_result.get('error', 'Unknown error')}\n"
            
            if test_result.get("stdout"):
                report += f"**Output:**\n```\n{test_result['stdout'][:500]}...\n```\n"
            
            report += "\n"
        
        return report
    
    def save_results(self, results: dict):
        """Save test results to JSON file."""
        results_file = Path("logs") / "master_connection_test_results.json"
        with open(results_file, 'w') as f:
            json.dump(results, f, indent=2)
        
        # Also save human-readable report
        report = self.generate_report(results)
        report_file = Path("logs") / "connection_test_report.md"
        with open(report_file, 'w') as f:
            f.write(report)

def main():
    """Main function to run all connection tests."""
    print("🚀 Starting Master Connection Test Suite\n")
    
    tester = MasterConnectionTester()
    results = tester.run_all_tests()
    
    # Save results
    tester.save_results(results)
    
    # Print summary
    summary = results["summary"]
    print(f"\n📊 Master Test Summary:")
    print(f"   Total Tests: {summary['total_tests']}")
    print(f"   ✅ Passed: {summary['passed']}")
    print(f"   ❌ Failed: {summary['failed']}")
    print(f"   📈 Success Rate: {(summary['passed'] / summary['total_tests'] * 100):.1f}%")
    
    print(f"\n📁 Results saved to:")
    print(f"   - logs/master_connection_test_results.json")
    print(f"   - logs/connection_test_report.md")
    
    if summary['failed'] == 0:
        print("\n🎉 All connection tests passed!")
        return 0
    else:
        print(f"\n⚠️  {summary['failed']} test(s) failed. Check the detailed report for more information.")
        return 1

if __name__ == "__main__":
    sys.exit(main()) 