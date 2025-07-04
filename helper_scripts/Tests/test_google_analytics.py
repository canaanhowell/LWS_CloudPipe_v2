#!/usr/bin/env python3
"""
Simple test script for Google Analytics connection
"""

import os
import sys
import json
from pathlib import Path
from datetime import datetime, timedelta

# Add helper_scripts to path
sys.path.append(str(Path(__file__).parent / "helper_scripts" / "Utils"))
from logger import log

def test_google_analytics():
    """Test Google Analytics connection and data retrieval."""
    try:
        log("GOOGLE_TEST", "Starting Google Analytics test", "INFO")
        
        # Import Google Analytics modules
        from google.analytics.data_v1beta import BetaAnalyticsDataClient
        from google.analytics.data_v1beta.types import RunReportRequest, DateRange, Metric, Dimension
        from google.oauth2 import service_account
        
        # Load credentials
        base_dir = Path(__file__).parent
        cred_file = base_dir / "settings.json"
        service_account_path = base_dir / "config_files" / "google_analytics_service_account.json"
        
        if not cred_file.exists():
            log("GOOGLE_TEST", "Missing settings.json file", "ERROR")
            return False
            
        if not service_account_path.exists():
            log("GOOGLE_TEST", "Missing google_analytics_service_account.json file", "ERROR")
            return False
        
        # Load credentials
        with open(cred_file, "r") as f:
            credentials = json.load(f)
        
        property_id = credentials.get("GOOGLE_ANALYTICS_PROPERTY_ID")
        if not property_id:
            log("GOOGLE_TEST", "Missing GOOGLE_ANALYTICS_PROPERTY_ID", "ERROR")
            return False
        
        log("GOOGLE_TEST", f"Using property ID: {property_id}", "INFO")
        
        # Initialize Google Analytics client
        log("GOOGLE_TEST", "Initializing Google Analytics client...", "INFO")
        credentials = service_account.Credentials.from_service_account_file(
            str(service_account_path),
            scopes=["https://www.googleapis.com/auth/analytics.readonly"]
        )
        
        client = BetaAnalyticsDataClient(credentials=credentials)
        log("GOOGLE_TEST", "✅ Google Analytics client initialized successfully", "INFO")
        
        # Define date range (last 12 months for comprehensive data)
        end_date = datetime.now().date()
        start_date = end_date - timedelta(days=365)
        
        log("GOOGLE_TEST", f"Querying data from {start_date} to {end_date}", "INFO")
        
        # Create a simple report request
        request = RunReportRequest(
            property=f"properties/{property_id}",
            date_ranges=[DateRange(start_date=start_date.isoformat(), end_date=end_date.isoformat())],
            metrics=[
                Metric(name="totalUsers"),
                Metric(name="sessions"),
                Metric(name="screenPageViews")
            ],
            dimensions=[
                Dimension(name="date")
            ]
        )
        
        # Run the report
        log("GOOGLE_TEST", "Running analytics report...", "INFO")
        response = client.run_report(request)
        
        # Process results
        log("GOOGLE_TEST", f"✅ Report completed successfully", "INFO")
        log("GOOGLE_TEST", f"Retrieved {len(response.rows)} rows of data", "INFO")
        
        # Display sample data
        if response.rows:
            log("GOOGLE_TEST", "Sample data:", "INFO")
            for i, row in enumerate(response.rows[:3]):  # Show first 3 rows
                date = row.dimension_values[0].value
                users = row.metric_values[0].value
                sessions = row.metric_values[1].value
                pageviews = row.metric_values[2].value
                log("GOOGLE_TEST", f"  {date}: {users} users, {sessions} sessions, {pageviews} pageviews", "INFO")
        else:
            log("GOOGLE_TEST", "⚠️ No data returned for the specified date range", "WARNING")
        
        log("GOOGLE_TEST", "🎉 Google Analytics test completed successfully!", "INFO")
        return True
        
    except Exception as e:
        log("GOOGLE_TEST", f"❌ Error testing Google Analytics: {str(e)}", "ERROR")
        return False

if __name__ == "__main__":
    success = test_google_analytics()
    if success:
        print("\n✅ Google Analytics test PASSED!")
    else:
        print("\n❌ Google Analytics test FAILED!")
        sys.exit(1) 