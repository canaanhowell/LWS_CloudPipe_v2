import requests
import json

url = 'https://lws-data-pipeline.blackglacier-1e24db99.westus2.azurecontainerapps.io/api/pipeline/run'
response = requests.post(url, json={})

with open('pipeline_api_response.json', 'w', encoding='utf-8') as f:
    f.write(response.text)

print('Status:', response.status_code) 