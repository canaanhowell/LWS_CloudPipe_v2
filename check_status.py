#!/usr/bin/env python3
import requests

try:
    response = requests.get('http://localhost:5000/api/pipeline/status')
    data = response.json()
    print(f"Status: {data['status']}")
    if data['logs']:
        print(f"Latest log: {data['logs'][-1]['message']}")
    else:
        print("No logs available")
except Exception as e:
    print(f"Error: {e}") 