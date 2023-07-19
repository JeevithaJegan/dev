import requests
import json

livy_url = "http://ec2-54-253-147-229.ap-southeast-2.compute.amazonaws.com:8998/batches"

spark_job_config = {
    "file": "s3://jeevithalandingzonebucket/spark-job.py",
    "args": [],
    "conf": {
        "spark.executor.memory": "2g",
        "spark.executor.cores": "2"
    }
}

response = requests.post(livy_url, data=json.dumps(spark_job_config), headers={"Content-Type": "application/json"})


if response.status_code == 201:
    print("Spark job submitted successfully.")
else:
    print("Failed to submit Spark job. Status code:", response.status_code)
    print("Error message:", response.text)
