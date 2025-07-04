#!/usr/bin/env python3
"""
Test Monday.com API connection and board queries
"""

import os
import sys
import json
import requests
from pathlib import Path

# Add helper_scripts to path
sys.path.append(str(Path(__file__).parent / "helper_scripts" / "Utils"))
from logger import log

def test_monday_board_queries():
    """Test Monday.com API connection and board queries."""
    try:
        log("MONDAY_TEST", "Starting Monday.com board query test", "INFO")
        
        # Load credentials
        base_dir = Path(__file__).parent
        cred_file = base_dir / "settings.json"
        
        if not cred_file.exists():
            log("MONDAY_TEST", "Missing settings.json file", "ERROR")
            return False
        
        with open(cred_file, "r") as f:
            credentials = json.load(f)
        
        api_key = credentials.get("MONDAY_API_KEY")
        if not api_key:
            log("MONDAY_TEST", "Missing MONDAY_API_KEY", "ERROR")
            return False
        
        headers = {"Authorization": api_key, "Content-Type": "application/json"}
        
        # Step 1: Query for all boards (id, name)
        boards_query = """
        query {
            boards {
                id
                name
            }
        }
        """
        response = requests.post(
            "https://api.monday.com/v2",
            json={"query": boards_query},
            headers=headers,
            timeout=15
        )
        log("MONDAY_TEST", f"Boards query status: {response.status_code}", "INFO")
        if response.status_code == 200:
            data = response.json()
            boards = data.get("data", {}).get("boards", [])
            log("MONDAY_TEST", f"Boards found: {len(boards)}", "INFO")
            for b in boards[:3]:
                log("MONDAY_TEST", f"Board: {b['id']} - {b['name']}", "INFO")
            if boards:
                board_id = boards[0]['id']
                # Step 2: Query for items on the first board using the new API structure
                items_query = """
                query ($id: [ID!], $cursor: String) {
                    boards(ids: $id) {
                        name
                        items_page(limit: 100, cursor: $cursor) {
                            cursor
                            items {
                                id
                                name
                                column_values {
                                    id
                                    text
                                    value
                                    column {
                                        title
                                    }
                                }
                            }
                        }
                    }
                }
                """
                
                variables = {
                    "id": [board_id],
                    "cursor": None
                }
                
                response2 = requests.post(
                    "https://api.monday.com/v2",
                    json={"query": items_query, "variables": variables},
                    headers=headers,
                    timeout=15
                )
                log("MONDAY_TEST", f"Items query status: {response2.status_code}", "INFO")
                if response2.status_code == 200:
                    data2 = response2.json()
                    items = data2.get("data", {}).get("boards", [{}])[0].get("items", [])
                    log("MONDAY_TEST", f"Items found: {len(items)}", "INFO")
                    if items:
                        log("MONDAY_TEST", f"Sample item: {json.dumps(items[0], indent=2)}", "INFO")
                    # Step 2: Introspect the Board type for available fields
                    introspect_query = f"""
                    query {{
                        boards(ids: [{board_id}]) {{
                            __typename
                            id
                            name
                            # Try to list all possible fields
                        }}
                    }}
                    """
                    response3 = requests.post(
                        "https://api.monday.com/v2",
                        json={"query": introspect_query},
                        headers=headers,
                        timeout=15
                    )
                    log("MONDAY_TEST", f"Introspect query status: {response3.status_code}", "INFO")
                    log("MONDAY_TEST", f"Introspect response: {response3.text}", "INFO")
                    return True
                else:
                    log("MONDAY_TEST", f"❌ Items query failed: {response2.text}", "ERROR")
                    return False
            else:
                log("MONDAY_TEST", "No boards found.", "ERROR")
                return False
        else:
            log("MONDAY_TEST", f"❌ Boards query failed: {response.text}", "ERROR")
            return False
    except Exception as e:
        log("MONDAY_TEST", f"❌ Error testing Monday.com board queries: {str(e)}", "ERROR")
        return False

if __name__ == "__main__":
    success = test_monday_board_queries()
    if success:
        print("\n✅ Monday.com board query test PASSED!")
    else:
        print("\n❌ Monday.com board query test FAILED!")
        sys.exit(1) 