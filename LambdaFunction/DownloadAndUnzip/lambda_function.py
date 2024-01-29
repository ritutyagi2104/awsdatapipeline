import json
import boto3
import zipfile
import requests

def lambda_handler(event, context):
    # Create connection to S3
    print("Starting execution of lambda function DownloadAndUnzip")
    fileUrl = 'https://awsdpsrc.s3.eu-central-1.amazonaws.com/raw/2m+Sales+Records.zip'
    download_location = '/tmp/rawdata.zip'

    print("Downloading file from HTTP Endpoint")
    response = requests.get(fileUrl)
    with open(download_location, 'wb') as file:
        file.write(response.content)
    print("File download completed")
    print(download_location)
    
    unzip_location = '/tmp/unzippedfile'
    print("Unzipping file")
    with zipfile.ZipFile(download_location, 'r') as zip_ref:
        zip_ref.extractall(unzip_location)  
    print("Unzipping completed") 
    
    bucket_name = 'iatademoritu'
    s3 = boto3.client('s3')
    
    unzipped_object_key = 'unzipped/2m Sales Records.csv'
    print("Uploading Unzipped file to S3")
    s3.upload_file(unzip_location+"/2m Sales Records.csv", bucket_name, unzipped_object_key)
    print("Upload completed")

    raw_data_archive = 'archive/2m Sales Records.zip'
    print("Uploading Raw Data to Archive in S3")
    s3.upload_file(download_location, bucket_name, raw_data_archive)
    print("Upload completed")

    print("Completed execution of lambda function DownloadAndUnzip")
    
    return {
        'statusCode': 200,
        'body': json.dumps('DownloadAndUnzip Completed!')
    }
