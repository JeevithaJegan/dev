import os
import requests
import json

def lambda_handler(event, context):
    airflow_api_url = os.environ.get('AIRFLOW_API_URL')
    s3_bucket = os.environ.get('S3_BUCKET')
    s3_key = os.environ.get('S3_KEY')

    headers = {'Content-Type': 'application/json'}
    data = {
        'conf': {
            's3_bucket': s3_bucket,
            's3_key': s3_key
        }
    }

    response = requests.post(airflow_api_url, data=json.dumps(data), headers=headers, auth=('Admin', 'Admin'))

    if response.status_code == 200:
        print("DAG run triggered successfully")
    else:
        print("Failed to trigger DAG run")
        print(f"Response status code: {response.status_code}")
        print(f"Response content: {response.text}")
