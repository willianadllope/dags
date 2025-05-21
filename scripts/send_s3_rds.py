import boto3
import config as cfg
import os

awss3_key = os.environ.get('access_key')
awss3_secret = os.environ.get('secret_secret')


session = boto3.Session( aws_access_key_id=awss3_key, aws_secret_access_key=awss3_secret)
s3 = session.resource('s3')

def list_files_in_s3_bucket(bucket_name, prefix=''):
    s3_client = boto3.client('s3')
    try:
        response = s3_client.list_objects_v2(Bucket=bucket_name, Prefix=prefix)
        if 'Contents' in response:
            for obj in response['Contents']:
                print(obj['Key'])
        else:
            print(f"No files found in bucket '{bucket_name}' with prefix '{prefix}'.")
    except Exception as e:
        print(f"An error occurred: {e}")

# Example usage:
bucket_name = 'systaxbackuprds/'  # Replace with your bucket name
list_files_in_s3_bucket(bucket_name)

# To list files with a specific prefix (e.g., in a folder):
prefix = 'pgentreganew/tabelao'  # Replace with your prefix
#list_files_in_s3_bucket(bucket_name, prefix)


