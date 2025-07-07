#!/usr/bin/env python3
"""
LWS CloudPipe v2 - Data Query & CSV Cleaning Orchestration Script

This script orchestrates the data query and CSV cleaning modules of the data pipeline:
1. Runs data_query.py to extract data from all endpoints
2. Runs csv_cleaner.py to clean and merge the extracted data
3. Provides comprehensive logging and error handling

Modified to run data query and CSV cleaning stages for local testing.
"""

import os
import sys
import subprocess
import time
from datetime import datetime
from pathlib import Path
import json

# Add helper_scripts to path for logging
sys.path.append(str(Path(__file__).parent / "helper_scripts" / "Utils"))
from logger import pipeline_logger, log

class PipelineOrchestrator:
    def __init__(self):
        """Initialize the pipeline orchestrator."""
        self.base_dir = Path(__file__).parent
        self.start_time = datetime.now()
        
        # Pipeline results tracking
        self.results = {
            "orchestration_start": self.start_time.isoformat(),
            "stages": {},
            "overall_success": False,
            "total_stages": 3,  # Run data query, CSV cleaner, and schema sync
            "successful_stages": 0,
            "failed_stages": 0
        }
        
        log("ORCHESTRATOR", "Pipeline orchestrator initialized", "INFO")
    
    def run_data_query(self) -> bool:
        """Run the data query pipeline to extract data from all endpoints."""
        log("ORCHESTRATOR", "Starting Stage 1: Data Query Pipeline", "INFO")
        
        try:
            # Path to data_query.py
            data_query_script = self.base_dir / "pipeline_scripts" / "data_query.py"
            
            if not data_query_script.exists():
                log("ORCHESTRATOR", f"❌ Data query script not found: {data_query_script}", "ERROR")
                return False
            
            log("ORCHESTRATOR", f"Executing: {data_query_script}", "INFO")
            
            # Run the data query script
            result = subprocess.run(
                [sys.executable, str(data_query_script)],
                capture_output=True,
                text=True,
                cwd=self.base_dir
            )
            
            # Log the output
            if result.stdout:
                log("ORCHESTRATOR", f"Data query stdout: {result.stdout}", "INFO")
            if result.stderr:
                log("ORCHESTRATOR", f"Data query stderr: {result.stderr}", "WARNING")
            
            # Check if successful
            if result.returncode == 0:
                log("ORCHESTRATOR", "Stage 1: Data Query Pipeline completed successfully", "INFO")
                self.results["stages"]["data_query"] = {
                    "status": "success",
                    "return_code": result.returncode,
                    "timestamp": datetime.now().isoformat()
                }
                return True
            else:
                log("ORCHESTRATOR", f"❌ Stage 1: Data Query Pipeline failed with return code {result.returncode}", "ERROR")
                self.results["stages"]["data_query"] = {
                    "status": "failed",
                    "return_code": result.returncode,
                    "error": result.stderr,
                    "timestamp": datetime.now().isoformat()
                }
                return False
                
        except Exception as e:
            log("ORCHESTRATOR", f"❌ Stage 1: Exception during data query: {str(e)}", "ERROR")
            self.results["stages"]["data_query"] = {
                "status": "error",
                "error": str(e),
                "timestamp": datetime.now().isoformat()
            }
            return False
    
    def run_csv_cleaner(self) -> bool:
        """Run the CSV cleaner to process and merge the extracted data."""
        log("ORCHESTRATOR", "Starting Stage 2: CSV Cleaning Pipeline", "INFO")
        
        try:
            # Path to csv_cleaner.py
            csv_cleaner_script = self.base_dir / "pipeline_scripts" / "csv_cleaner.py"
            
            if not csv_cleaner_script.exists():
                log("ORCHESTRATOR", f"❌ CSV cleaner script not found: {csv_cleaner_script}", "ERROR")
                return False
            
            log("ORCHESTRATOR", f"Executing: {csv_cleaner_script}", "INFO")
            
            # Run the CSV cleaner script
            result = subprocess.run(
                [sys.executable, str(csv_cleaner_script)],
                capture_output=True,
                text=True,
                cwd=self.base_dir
            )
            
            # Log the output
            if result.stdout:
                log("ORCHESTRATOR", f"CSV cleaner stdout: {result.stdout}", "INFO")
            if result.stderr:
                log("ORCHESTRATOR", f"CSV cleaner stderr: {result.stderr}", "WARNING")
            
            # Check if successful
            if result.returncode == 0:
                log("ORCHESTRATOR", "Stage 2: CSV Cleaning Pipeline completed successfully", "INFO")
                self.results["stages"]["csv_cleaner"] = {
                    "status": "success",
                    "return_code": result.returncode,
                    "timestamp": datetime.now().isoformat()
                }
                return True
            else:
                log("ORCHESTRATOR", f"❌ Stage 2: CSV Cleaning Pipeline failed with return code {result.returncode}", "ERROR")
                self.results["stages"]["csv_cleaner"] = {
                    "status": "failed",
                    "return_code": result.returncode,
                    "error": result.stderr,
                    "timestamp": datetime.now().isoformat()
                }
                return False
                
        except Exception as e:
            log("ORCHESTRATOR", f"❌ Stage 2: Exception during CSV cleaning: {str(e)}", "ERROR")
            self.results["stages"]["csv_cleaner"] = {
                "status": "error",
                "error": str(e),
                "timestamp": datetime.now().isoformat()
            }
            return False
    
    def run_schema_sync(self) -> bool:
        """Run the schema synchronization pipeline to sync Snowflake table schemas with CSV structures."""
        log("ORCHESTRATOR", "Starting Stage 3: Schema Synchronization Pipeline", "INFO")
        
        try:
            # Path to schema_sync_pipeline.py
            schema_sync_script = self.base_dir / "pipeline_scripts" / "schema_sync_pipeline.py"
            
            if not schema_sync_script.exists():
                log("ORCHESTRATOR", f"❌ Schema sync script not found: {schema_sync_script}", "ERROR")
                return False
            
            log("ORCHESTRATOR", f"Executing: {schema_sync_script}", "INFO")
            
            # Run the schema sync script
            result = subprocess.run(
                [sys.executable, str(schema_sync_script)],
                capture_output=True,
                text=True,
                cwd=self.base_dir
            )
            
            # Log the output
            if result.stdout:
                log("ORCHESTRATOR", f"Schema sync stdout: {result.stdout}", "INFO")
            if result.stderr:
                log("ORCHESTRATOR", f"Schema sync stderr: {result.stderr}", "WARNING")
            
            # Check if successful
            if result.returncode == 0:
                log("ORCHESTRATOR", "Stage 3: Schema Synchronization Pipeline completed successfully", "INFO")
                self.results["stages"]["schema_sync"] = {
                    "status": "success",
                    "return_code": result.returncode,
                    "timestamp": datetime.now().isoformat()
                }
                return True
            else:
                log("ORCHESTRATOR", f"❌ Stage 3: Schema Synchronization Pipeline failed with return code {result.returncode}", "ERROR")
                self.results["stages"]["schema_sync"] = {
                    "status": "failed",
                    "return_code": result.returncode,
                    "error": result.stderr,
                    "timestamp": datetime.now().isoformat()
                }
                return False
                
        except Exception as e:
            log("ORCHESTRATOR", f"❌ Stage 3: Exception during schema synchronization: {str(e)}", "ERROR")
            self.results["stages"]["schema_sync"] = {
                "status": "error",
                "error": str(e),
                "timestamp": datetime.now().isoformat()
            }
            return False
    
    def run_external_storage_verification(self) -> bool:
        """Run the external storage integration verification."""
        log("ORCHESTRATOR", "Starting Stage 5: External Storage Integration Verification", "INFO")
        
        try:
            # Path to verify_external_storage_integration.py
            external_storage_verify_script = self.base_dir / "pipeline_scripts" / "verify_external_storage_integration.py"
            
            if not external_storage_verify_script.exists():
                log("ORCHESTRATOR", f"❌ External storage verification script not found: {external_storage_verify_script}", "ERROR")
                return False
            
            log("ORCHESTRATOR", f"Executing: {external_storage_verify_script}", "INFO")
            
            # Run the external storage verification script
            result = subprocess.run(
                [sys.executable, str(external_storage_verify_script)],
                capture_output=True,
                text=True,
                cwd=self.base_dir
            )
            
            # Log the output
            if result.stdout:
                log("ORCHESTRATOR", f"External storage verification stdout: {result.stdout}", "INFO")
            if result.stderr:
                log("ORCHESTRATOR", f"External storage verification stderr: {result.stderr}", "WARNING")
            
            # Check if successful
            if result.returncode == 0:
                log("ORCHESTRATOR", "Stage 5: External Storage Integration Verification completed successfully", "INFO")
                self.results["stages"]["external_storage_verification"] = {
                    "status": "success",
                    "return_code": result.returncode,
                    "timestamp": datetime.now().isoformat()
                }
                return True
            else:
                log("ORCHESTRATOR", f"❌ Stage 5: External Storage Integration Verification failed with return code {result.returncode}", "ERROR")
                self.results["stages"]["external_storage_verification"] = {
                    "status": "failed",
                    "return_code": result.returncode,
                    "error": result.stderr,
                    "timestamp": datetime.now().isoformat()
                }
                return False
                
        except Exception as e:
            log("ORCHESTRATOR", f"❌ Stage 5: Exception during external storage verification: {str(e)}", "ERROR")
            self.results["stages"]["external_storage_verification"] = {
                "status": "error",
                "error": str(e),
                "timestamp": datetime.now().isoformat()
            }
            return False
    
    def verify_output_files(self) -> dict:
        """Verify that expected output files were created."""
        log("ORCHESTRATOR", "Verifying output files", "INFO")
        
        csv_dir = self.base_dir / "data" / "csv"
        verification_results = {
            "raw_files": [],
            "cleaned_files": [],
            "total_files": 0
        }
        
        if csv_dir.exists():
            # Check for raw files
            raw_files = list(csv_dir.glob("*_raw.csv"))
            verification_results["raw_files"] = [f.name for f in raw_files]
            
            # Check for cleaned files (based on table_mapping.json naming)
            expected_cleaned_files = [
                "LWS.PUBLIC.PROJECTS.csv",
                "LWS.PUBLIC.SERVICE.csv", 
                "LWS.PUBLIC.SUNGROW.csv",
                "SEAL.PUBLIC.RESI.csv",
                "SEAL.PUBLIC.SEAL_COMM_MERGED.csv",
                "LWS.PUBLIC.GOOGLE_ANALYTICS.csv"
            ]
            
            cleaned_files = []
            for expected_file in expected_cleaned_files:
                file_path = csv_dir / expected_file
                if file_path.exists():
                    cleaned_files.append(expected_file)
                    log("ORCHESTRATOR", f"Found cleaned file: {expected_file}", "INFO")
                else:
                    log("ORCHESTRATOR", f"Missing cleaned file: {expected_file}", "WARNING")
            
            verification_results["cleaned_files"] = cleaned_files
            verification_results["total_files"] = len(raw_files) + len(cleaned_files)
        
        return verification_results
    
    def run_pipeline(self) -> dict:
        """Run the data query, CSV cleaning, and schema sync pipeline."""
        log("ORCHESTRATOR", "Starting LWS CloudPipe v2 - Data Query, CSV Cleaning & Schema Sync", "INFO")
        log("ORCHESTRATOR", "=" * 60, "INFO")
        
        # Update total stages count
        self.results["total_stages"] = 3  # Data query, CSV cleaner, and schema sync
        
        # Stage 1: Data Query
        stage1_success = self.run_data_query()
        if stage1_success:
            self.results["successful_stages"] += 1
        else:
            self.results["failed_stages"] += 1
        
        # Brief pause between stages
        time.sleep(2)
        
        # Stage 2: CSV Cleaning
        stage2_success = self.run_csv_cleaner()
        if stage2_success:
            self.results["successful_stages"] += 1
        else:
            self.results["failed_stages"] += 1
        
        # Brief pause between stages
        time.sleep(2)
        
        # Stage 3: Schema Sync
        stage3_success = self.run_schema_sync()
        if stage3_success:
            self.results["successful_stages"] += 1
        else:
            self.results["failed_stages"] += 1
        
        # Verify output files (both raw and cleaned files)
        verification_results = self.verify_output_files()
        self.results["verification"] = verification_results
        
        # Finalize results
        self.results["orchestration_end"] = datetime.now().isoformat()
        self.results["duration_seconds"] = (datetime.now() - self.start_time).total_seconds()
        self.results["overall_success"] = self.results["successful_stages"] == 3  # All three stages must succeed
        
        # Log final results
        self.log_final_results()
        
        return self.results
    
    def log_final_results(self):
        """Log the final pipeline results."""
        log("ORCHESTRATOR", "=" * 60, "INFO")
        log("ORCHESTRATOR", "PIPELINE ORCHESTRATION COMPLETED", "INFO")
        log("ORCHESTRATOR", f"Total Duration: {self.results['duration_seconds']:.2f} seconds", "INFO")
        log("ORCHESTRATOR", f"Stages Completed: {self.results['successful_stages']}/{self.results['total_stages']}", "INFO")
        
        if self.results["overall_success"]:
            log("ORCHESTRATOR", "ALL STAGES COMPLETED SUCCESSFULLY!", "INFO")
        else:
            log("ORCHESTRATOR", f"{self.results['failed_stages']} stage(s) failed", "WARNING")
        
        # Log file verification results
        verification = self.results.get("verification", {})
        log("ORCHESTRATOR", f"Raw files found: {len(verification.get('raw_files', []))}", "INFO")
        log("ORCHESTRATOR", f"Cleaned files found: {len(verification.get('cleaned_files', []))}", "INFO")
        log("ORCHESTRATOR", f"Total files: {verification.get('total_files', 0)}", "INFO")
        
        # Log stage details
        for stage_name, stage_result in self.results["stages"].items():
            status_symbol = "SUCCESS" if stage_result["status"] == "success" else "FAILED"
            log("ORCHESTRATOR", f"{stage_name}: {stage_result['status']} ({status_symbol})", "INFO")
        
        log("ORCHESTRATOR", "NOTE: Data query, CSV cleaning, and schema sync stages were executed", "INFO")
        
        log("ORCHESTRATOR", "=" * 60, "INFO")

def main():
    """Main entry point for the orchestration script."""
    try:
        orchestrator = PipelineOrchestrator()
        results = orchestrator.run_pipeline()
        
        # Print summary to console
        print("\n" + "="*60)
        print("LWS CLOUDPIPE v2 - DATA QUERY, CSV CLEANING & SCHEMA SYNC SUMMARY")
        print("="*60)
        print(f"Start Time: {results['orchestration_start']}")
        print(f"End Time: {results['orchestration_end']}")
        print(f"Duration: {results['duration_seconds']:.2f} seconds")
        print(f"Stages Completed: {results['successful_stages']}/{results['total_stages']}")
        print(f"Overall Success: {'YES' if results['overall_success'] else 'NO'}")
        
        print("\nStage Results:")
        for stage_name, stage_result in results["stages"].items():
            status_symbol = "SUCCESS" if stage_result["status"] == "success" else "FAILED"
            print(f"  {stage_name}: {stage_result['status']} ({status_symbol})")
        
        print("\nFile Verification:")
        verification = results.get("verification", {})
        print(f"  Raw files: {len(verification.get('raw_files', []))}")
        print(f"  Cleaned files: {len(verification.get('cleaned_files', []))}")
        print(f"  Total files: {verification.get('total_files', 0)}")
        
        if verification.get("raw_files"):
            print("\n  Raw files created:")
            for file in verification["raw_files"]:
                print(f"    {file}")
        
        if verification.get("cleaned_files"):
            print("\n  Cleaned files created:")
            for file in verification["cleaned_files"]:
                print(f"    {file}")
        
        print("="*60)
        
        # Return appropriate exit code
        return 0 if results['overall_success'] else 1
        
    except Exception as e:
        log("ORCHESTRATOR", f"Critical orchestration error: {str(e)}", "CRITICAL")
        print(f"\n❌ Critical error: {str(e)}")
        return 1

if __name__ == "__main__":
    sys.exit(main()) 