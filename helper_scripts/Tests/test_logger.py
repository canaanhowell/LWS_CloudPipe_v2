#!/usr/bin/env python3
"""
Test script for the pipeline logger functionality.
"""

import sys
import os
from pathlib import Path

# Add the Utils directory to the path so we can import the logger
sys.path.append(str(Path(__file__).parent.parent / "Utils"))

from logger import PipelineLogger, log

def test_basic_logging():
    """Test basic logging functionality."""
    print("Testing basic logging...")
    
    # Test the legacy log function
    log("TEST", "This is a test message", "INFO")
    log("TEST", "This is a warning message", "WARNING")
    log("TEST", "This is an error message", "ERROR")
    
    # Test invalid log level
    log("TEST", "This should default to INFO", "INVALID_LEVEL")
    
    print("✓ Basic logging test completed")

def test_pipeline_logger():
    """Test the PipelineLogger class."""
    print("Testing PipelineLogger class...")
    
    # Create a test logger
    test_logger = PipelineLogger(log_dir="logs", log_file="test_pipeline.log")
    
    # Test different log levels
    test_logger.log("DATA_EXTRACTION", "Starting data extraction from Azure", "INFO")
    test_logger.log("DATA_TRANSFORMATION", "Transforming data format", "DEBUG")
    test_logger.log("DATA_LOADING", "Loading data to Snowflake", "WARNING")
    test_logger.log("PIPELINE", "Pipeline completed successfully", "INFO")
    
    print("✓ PipelineLogger test completed")

def test_json_logging():
    """Test JSON logging functionality."""
    print("Testing JSON logging...")
    
    test_logger = PipelineLogger(log_dir="logs", log_file="test_pipeline.log")
    
    # Test JSON logging
    test_data = {
        "records_processed": 1000,
        "processing_time": 45.2,
        "status": "completed",
        "errors": []
    }
    
    test_logger.log_json("DATA_PROCESSING", test_data, "INFO")
    
    print("✓ JSON logging test completed")

def test_progress_logging():
    """Test progress logging functionality."""
    print("Testing progress logging...")
    
    test_logger = PipelineLogger(log_dir="logs", log_file="test_pipeline.log")
    
    # Test progress logging
    test_logger.log_progress("DATA_EXTRACTION", 50, 100, "Processing records")
    test_logger.log_progress("DATA_EXTRACTION", 100, 100, "Extraction completed")
    
    print("✓ Progress logging test completed")

def test_error_handling():
    """Test error handling in the logger."""
    print("Testing error handling...")
    
    # Test with invalid log directory (should create it)
    test_logger = PipelineLogger(log_dir="logs/test_subdir", log_file="error_test.log")
    test_logger.log("ERROR_TEST", "Testing error handling", "INFO")
    
    print("✓ Error handling test completed")

def main():
    """Run all logger tests."""
    print("🧪 Starting Logger Tests\n")
    
    try:
        test_basic_logging()
        test_pipeline_logger()
        test_json_logging()
        test_progress_logging()
        test_error_handling()
        
        print("\n✅ All logger tests completed successfully!")
        print("\n📁 Check the following files for test results:")
        print("   - logs/test_pipeline.log")
        print("   - logs/log.json")
        print("   - logs/progress.md")
        print("   - logs/test_subdir/error_test.log")
        
    except Exception as e:
        print(f"\n❌ Logger test failed: {str(e)}")
        sys.exit(1)

if __name__ == "__main__":
    main() 