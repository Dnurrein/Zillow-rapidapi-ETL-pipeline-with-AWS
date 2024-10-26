import boto3
import json
import pandas as pd

# Initialize an S3 client to interact with AWS S3
s3_client = boto3.client('s3')

def lambda_handler(event, context):
    # Lambda function handler that will be triggered when an S3 event occurs
    # The event parameter contains details about the file uploaded to S3
    # The context parameter contains runtime information from Lambda

    # Extract the source bucket name and the key (file name) from the event triggered by S3
    source_bucket = event['Records'][0]['s3']['bucket']['name']
    object_key = event['Records'][0]['s3']['object']['key']

    # Define the target bucket where the transformed CSV file will be saved
    target_bucket = 'final-transformed-data-zone-csv-bucket'
    
    # Create a target file name by removing the last 5 characters (assuming '.json')
    target_file_name = object_key[:-5]
    print(target_file_name)  # Debugging: print the target file name

    # Waiter to ensure that the object exists in S3 before proceeding (optional)
    # waiter = s3_client.get_waiter('object_exists')
    # waiter.wait(Bucket=source_bucket, Key=object_key)

    # Retrieve the JSON object from the source bucket in S3
    response = s3_client.get_object(Bucket=source_bucket, Key=object_key)
    
    # Read and decode the file content from bytes to string
    data = response['Body'].read().decode('utf-8')
    
    # Parse the string as JSON
    data = json.loads(data)

    # Initialize an empty list to store the results from the JSON data
    f = []

    # Loop through the 'results' section of the JSON object and append each item to the list
    for i in data['results']:
        f.append(i)
    
    # Convert the list into a pandas DataFrame for easier data manipulation
    df = pd.DataFrame(f)

    # Specify the columns that are required in the final CSV
    selected_columns = ['bathrooms', 'bedrooms', 'city', 
                        'homeStatus', 'homeType', 'livingArea', 
                        'price', 'rentZestimate', 'zipcode']

    # Select only the specified columns from the DataFrame
    df = df[selected_columns]
    print(df)  # Debugging: print the DataFrame to verify the selected columns

    # Convert the DataFrame to CSV format without including the index column
    csv_data = df.to_csv(index=False)

    # Define the bucket and file name for the target CSV file in S3
    bucket_name = target_bucket
    object_key = f"{target_file_name}.csv"  # Add .csv extension to the file name
    
    # Upload the CSV file to the target S3 bucket
    s3_client.put_object(Bucket=bucket_name, Key=object_key, Body=csv_data)

    # Return a success response
    return {
        'statusCode': 200,
        'body': json.dumps('CSV S3 upload complete!')
    }
