import boto3
import json

# Initialize an S3 client to interact with AWS S3 services
s3_client = boto3.client('s3')

def lambda_handler(event, context):
    # Lambda function handler to process S3 event notifications and copy files

    # Retrieve the source bucket name from the event that triggered the Lambda function
    source_bucket = event['Records'][0]['s3']['bucket']['name']
    
    # Retrieve the object key (file name/path) from the event
    object_key = event['Records'][0]['s3']['object']['key']

    # Define the target bucket where the file will be copied
    target_bucket = 'copy-of-raw-json-staging-bucket'

    # Define the source location (bucket and key) for the copy operation
    copy_source = {'Bucket': source_bucket, 'Key': object_key}

    # Uncomment these lines if you need to wait until the object exists in the source bucket
    # waiter = s3_client.get_waiter('object exists')
    # waiter.wait(Bucket=source_bucket, Key=object_key)

    # Copy the object from the source bucket to the target bucket with the same key (file name/path)
    s3_client.copy_object(Bucket=target_bucket, Key=object_key, CopySource=copy_source)

    # Return a success message upon completion of the copy operation
    return {
        'statusCode': 200,
        'body': json.dumps('Copy completed successfully!')
    }
