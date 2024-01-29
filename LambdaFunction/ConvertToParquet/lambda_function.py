import os
import boto3
import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
from botocore.exceptions import NoCredentialsError


def lambda_handler(event, context):
    # Get the S3 bucket and key from the event
    print("Processing started")
    s3_bucket = 'iatademoritu'
    s3_key = 'unzipped/2m Sales Records.csv'

    # Download CSV file from S3
    s3 = boto3.client('s3')
    csv_object = s3.get_object(Bucket=s3_bucket, Key=s3_key)
    #csv_content = csv_object['Body'].read().decode('utf-8')

    # Convert CSV to Parquet
    df = pd.read_csv(csv_object['Body'])
    df.columns = map(str.lower, df.columns.str.replace(' ', '_'))
    table = pa.Table.from_pandas(df)

    print("Writing Output parquet files to tmp storage")
    tmp_output_path = '/tmp/pq'
    pq.write_to_dataset(table=table,root_path=tmp_output_path,partition_cols=['country'])
    print("Writing Output file to tmp storage completed")

    print("Uploading Converted Parquet files to S3")

    s3_folder = 'curated/'

    try:
        
        # Upload the entire local folder to S3
        for root, dirs, files in os.walk(tmp_output_path):
            for file in files:
                local_path = os.path.join(root, file)
                s3_key = os.path.relpath(local_path, tmp_output_path)
                s3_path = os.path.join(s3_folder, s3_key).replace("\\", "/") # Replace backslashes with forward slashes for Windows paths

                # Upload the file to S3
                s3.upload_file(local_path, s3_bucket, s3_path)

        print(f"Upload successful. Local folder '{tmp_output_path}' uploaded to S3 folder '{s3_folder}' in bucket '{s3_bucket}'.")
    except NoCredentialsError:
        print("AWS credentials not available.")
    print("Upload Successful")

    print("Deleting tmp/pq data")

    remove_non_empty_folder(tmp_output_path)

    print("Processing completed")

    

    return {
    'statusCode': 200,
    'body': 'Conversion and partitioning complete.'
    }

def remove_non_empty_folder(folder_path):
    try:
        # List all files and subdirectories in the folder
        entries = os.listdir(folder_path)

        # Iterate through each entry and remove it
        for entry in entries:
            entry_path = os.path.join(folder_path, entry)
            if os.path.isdir(entry_path):
                remove_non_empty_folder(entry_path) # Recursively remove subdirectories
            else:
                os.remove(entry_path) # Remove files

        # Remove the empty folder
        os.rmdir(folder_path)
        print(f"Folder '{folder_path}' deleted successfully.")
    except FileNotFoundError:
        print(f"Folder '{folder_path}' not found.")
    except Exception as e:
        print(f"Error deleting folder: {e}")



