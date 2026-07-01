from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator
from airflow.models import Variable
from datetime import datetime, timedelta
from pathlib import Path
import urllib.request
import json
import os

# Securely grab the key from your Airflow UI Variable (Admin -> Variables)
API_KEY = Variable.get("unsplash_api_key", default_var=None)

def download_func():
    if not API_KEY:
        raise ValueError("Missing 'unsplash_api_key' Variable in Airflow Admin UI!")

    # Read the JSON file downloaded by the BashOperator task
    with open('/tmp/images.json', 'r') as f:
        data = json.load(f)

    # Handle both single dict response and list responses gracefully
    api_data = [data] if isinstance(data, dict) else data
    
    # Create the internal images directory inside the container
    save_dir = Path('/opt/airflow/images')
    save_dir.mkdir(parents=True, exist_ok=True)
    
    print(f"----- Pipeline Status: Processing {len(api_data)} items -----")
    
    for item in api_data:
        if isinstance(item, dict) and 'urls' in item:
            img_url = item['urls']['small']
            img_id = item['id']
            target_file = save_dir / f"{img_id}.jpg"
            
            print(f"----- Target located! Downloading image ID: {img_id}")
            try:
                urllib.request.urlretrieve(img_url, target_file)
                print(f"File saved successfully to {target_file}")
            except Exception as e:
                print(f"Failed to download image {img_id}: {e}")
        else:
            print(f"Skipping invalid item or API error response: {item}")


# Define default arguments for the tasks inside the DAG
default_args = {
    'owner': 'airflow',
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

with DAG(
    dag_id='kitten_collector',
    start_date=datetime(2026, 1, 1),
    catchup=False,
    schedule_interval='@daily',
    default_args=default_args,  # Pass the retries here instead!
) as dag:
    
    # Task 1: Fetch random kitten metadata from Unsplash using curl
    fetch_images = BashOperator(
        task_id='fetch_images',
        bash_command='curl -o /tmp/images.json "https://api.unsplash.com/photos/random?count=5&query=kitten&client_id=$UNSPLASH_KEY"',
        env={'UNSPLASH_KEY': API_KEY}
    )

    # Task 2: Parse the metadata JSON and download the images
    save_images = PythonOperator(
        task_id='save_images',
        python_callable=download_func
    )

    # Task 3: Output success confirmation
    notify = BashOperator(
        task_id='notify',
        bash_command='echo "Kitten pictures downloaded successfully!"'
    )

    # Task 4: Write task to file
    write_to_file = BashOperator(
        task_id='write_to_file',
        bash_command='mkdir -p /tmp/airflow && echo "=== Kitten Storage State Snapshot: $(date) ===" > /tmp/airflow/kitten_state.txt && ls -la /opt/airflow/images >> /tmp/airflow/kitten_state.txt'
    )
    # Define the execution pipeline order
    fetch_images >> save_images >> [notify, write_to_file]