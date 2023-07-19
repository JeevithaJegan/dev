from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.operators.bash_operator import BashOperator
from datetime import datetime
import shutil
import requests
import json
import os

def read_config_file():
    spark_config_path = "s3://jeevithalandingzonebucket/spark.config" 
    config_json_path = "s3://jeevithalandingzonebucket/config.json" 
    with open(spark_config_path, 'r') as spark_config_file:
        spark_config = spark_config_file.read()

    with open(config_json_path, 'r') as config_json_file:
        config_json = config_json_file.read()
        config = json.loads(config_json)
    print("spark.config content:")
    print(spark_config)
    print("\nconfig.json content:")
    print(config_json)

def copy_raw_data():
    landing_zone_path = config["paths"]["landing_zone_path"] 
    raw_zone_path = config["paths"]["raw_zone_path"]

    try:
        shutil.copytree(landing_zone_path, raw_zone_path)
        print("Raw data copied successfully!")
    except FileNotFoundError:
        print("Landing zone directory not found.")
    except FileExistsError:
        print("Raw zone directory already exists.")
    except Exception as e:
        print(f"Error occurred while copying raw data: {str(e)}")

def pre_validation():
    raw_zone_path = config["paths"]["raw_zone_path"]

    try:
        file_count = len(os.listdir(raw_zone_path))
        if file_count > 0:
            print("Pre-validation successful. Raw data exists.")
        else:
            print("Pre-validation failed. No raw data files found.")
    except FileNotFoundError:
        print("Raw zone directory not found.")
    except Exception as e:
        print(f"Error occurred during pre-validation: {str(e)}")

def submit_emr_spark_job():
    livy_endpoint = "http://ec2-54-253-147-229.ap-southeast-2.compute.amazonaws.com:8998/batches" 
    spark_job_file = "s3://jeevithalandingzonebucket/spark-job.py" 

    spark_job_payload = {
        'file': [spark_job_file]
    }

    try:
        response = requests.post(f'{livy_endpoint}/batches', data=json.dumps(spark_job_payload),
                                 headers={'Content-Type': 'application/json'})

        if response.status_code == 201:
            response_json = response.json()
            batch_id = response_json['id']
            print(f"EMR-Spark job submitted successfully. Batch ID: {batch_id}")
        else:
            print(f"Failed to submit EMR-Spark job. Status code: {response.status_code}")

    except requests.exceptions.RequestException as e:
        print(f"Error occurred while submitting EMR-Spark job: {str(e)}")

def copy_transformed_data():
    transformed_data_actives = mask_columns(transformed_data_actives, mask_fields)
    transformed_data_viewership = mask_columns(transformed_data_viewership, mask_fields)

    transformed_data_actives = cast_columns(transformed_data_actives, cast_fields, casting_type)
    transformed_data_viewership = cast_columns(transformed_data_viewership, cast_fields, casting_type)
    staging_zone_path = config["paths"]["staging_zone_path"]

    try:
        shutil.copytree(transformed_data_path, staging_zone_path)
        print("Transformed data copied to staging zone successfully!")
    except FileNotFoundError:
        print("Transformed data directory not found.")
    except FileExistsError:
        print("Staging zone directory already exists.")
    except Exception as e:
        print(f"Error occurred while copying transformed data: {str(e)}")

def post_validation():
    staging_zone_path = config["paths"]["staging_zone_path"]

    try:
        file_count = len(os.listdir(staging_zone_path))
        if file_count > 0:
            print("Post-validation successful. Staged data exists.")
        else:
            print("Post-validation failed. No staged data files found.")
    except FileNotFoundError:
        print("Staging zone directory not found.")
    except Exception as e:
        print(f"Error occurred during post-validation: {str(e)}")

default_args = {
    'start_date': datetime(2023, 6, 28),
    'schedule_interval': None,
    'catchup': True
}

with DAG('data_processing_dag', description='Data Processing DAG', default_args=default_args) as dag:
    task_read_config_file = PythonOperator(
        task_id='read_config_file',
        python_callable=read_config_file
    )

    task_copy_raw_data = PythonOperator(
        task_id='copy_raw_data',
        python_callable=copy_raw_data
    )

    task_pre_validation = PythonOperator(
        task_id='pre_validation',
        python_callable=pre_validation
    )

    task_submit_emr_spark_job = PythonOperator(
        task_id='submit_emr_spark_job',
        python_callable=submit_emr_spark_job
    )

    task_copy_transformed_data = PythonOperator(
        task_id='copy_transformed_data',
        python_callable=copy_transformed_data
    )

    task_post_validation = PythonOperator(
        task_id='post_validation',
        python_callable=post_validation
    )

    # Define the task dependencies
    task_read_config_file >> task_copy_raw_data >> task_pre_validation
    task_pre_validation >> task_submit_emr_spark_job >> task_copy_transformed_data
    task_copy_transformed_data >> task_post_validation
