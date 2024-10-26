from airflow import DAG  # Importing DAG to define workflow structure in Airflow
from datetime import timedelta, datetime  # Importing timedelta and datetime for timing and scheduling
from airflow.operators.python import PythonOperator  # PythonOperator for running Python functions in Airflow
from airflow.operators.bash_operator import BashOperator  # BashOperator to execute bash commands in Airflow
import json  # JSON library to handle JSON files and responses
import requests  # Requests library for making HTTP requests to external APIs
from airflow.providers.amazon.aws.sensors.s3 import S3KeySensor
from airflow.providers.amazon.aws.transfers.s3_to_redshift import S3ToRedshiftOperator

# Load API configuration from JSON file
with open('/home/ubuntu/airflow/config_api.json', "r") as config_file:
    api_host_key = json.load(config_file)  # Load the API key and other details into api_host_key

# Get current datetime and format it as a string for unique file naming
now = datetime.now()  # Fetch current datetime
dt_now_string = now.strftime("%d%m%Y%H%M%S")  # Format datetime as string for file name

s3_bucket = 'final-transformed-data-zone-csv-bucket'

# Define function to extract Zillow data
def extract_zillow_data(**kwargs):
    url = kwargs['url']  # Extract URL for the API request
    headers = kwargs['headers']  # Extract headers for API authentication
    querystring = kwargs['querystring']  # Extract query string parameters for the API request
    dt_string = kwargs['date_string']  # Extract formatted date string for file naming
    response = requests.get(url, headers=headers, params=querystring)  # Make API request
    response_data = response.json()  # Parse response to JSON

    # Specify output file path with date string for unique naming
    output_file_path = f"/home/ubuntu/airflow/response_data_{dt_string}.json"  
    file_str = f"response_data_{dt_string}.csv"  # Define CSV filename for potential downstream tasks

    # Save JSON response data to file
    with open(output_file_path, "w") as output_file:
        json.dump(response_data, output_file, indent=4)  # Write JSON data to file with indentation
    output_list = [output_file_path, file_str]  # Output file path list for use in next tasks
    return output_list  # Return list with JSON file path and CSV filename

# Set default arguments for the DAG
default_args = {
    'owner': 'airflow',  # DAG owner name
    'depends_on_past': False,  # Task does not depend on previous runs
    'start_date': datetime(2023, 8, 8),  # DAG start date
    'email': ['dukenurrein@gmail.com'],  # Email for notifications
    'email_on_failure': False,  # Disable email on task failure
    'email_on_rety': False,  # Disable email on task retry
    'retries': 2,  # Number of retries on failure
    'retry_delay': timedelta(seconds=15)  # Delay between retries
}

# Define the DAG with daily scheduling and no backfill for missed runs
with DAG('zillowanalytics_dag',
         default_args=default_args,
         schedule_interval='@daily',  # Set to run daily
         catchup=False) as dag:  # catchup=False to skip runs that were missed

    # Task to extract data from Zillow API
    extract_zillow_data_var = PythonOperator(
        task_id='tsk_extract_zillow_data_var',  # Task ID
        python_callable=extract_zillow_data,  # Python function to execute
        op_kwargs={'url': 'https://zillow56.p.rapidapi.com/search',
                   'querystring': {"location": "houston, tx"},  # Query location for Zillow data
                   'headers': api_host_key,  # API headers
                   'date_string': dt_now_string}  # Pass formatted date string
    )

    # Task to load the extracted data into S3
    load_to_s3 = BashOperator(
        task_id='tsk_load_to_s3',  # Unique identifier for this task
        bash_command='aws s3 mv {{ti.xcom_pull("tsk_extract_zillow_data_var")[0]}} s3://zillowapi-bucket/',  
    # Bash command to move the extracted JSON file (retrieved from XCom) to the specified S3 bucket
)

# Sensor task to check if the file is available in the S3 bucket
is_file_in_s3_available = S3KeySensor(
    task_id='tsk_is_file_in_s3_available',  # Unique identifier for the sensor task
    bucket_key='{{ti.xcom.pull("tsk_extract_zillow_data_var")[1]}}',  # The S3 key (file path) retrieved from XCom
    bucket_name=s3_bucket,  # Name of the S3 bucket
    aws_conn_id='aws_s3_conn',  # AWS connection ID for authentication
    wildcard_match=False,  # Exact match for the S3 key (no wildcards)
    timeout=30,  # Time to wait before failing the sensor if the file is not found
    poke_interval=5,  # Interval (in seconds) between consecutive checks for the file in S3
)

# Task to transfer data from S3 to Redshift
transfer_s3_to_RedShift = S3ToRedshiftOperator(
    task_id='tsk_s3_to_redshift',  # Unique identifier for this task
    aws_conn_id='aws_s3_conn',  # AWS connection ID for accessing S3
    redshift_conn_id='conn_id_redshift',  # Connection ID for Redshift
    s3_bucket=s3_bucket,  # Name of the S3 bucket where the file is stored
    s3_key='{{ti.xcom.pull("tsk_extract_zillow_data_var")[1]}}',  # The S3 key (file path) retrieved from XCom
    schema="PUBLIC",  # Schema in Redshift where the data will be loaded
    table="zillowdata",  # Table name in Redshift to load the data into
    copy_options=["csv INGNOREHEADER 1"]  # COPY command options for Redshift (ignore the first row as header)
)


    # Define task sequence: extract data task followed by load to S3 task followed by S3 monitoring and subsequent loading to RedShift
    extract_zillow_data_var >> load_to_s3 >> is_file_in_s3_available >> transfer_s3_to_RedShift
