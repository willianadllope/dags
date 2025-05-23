import psycopg2
import pandas as pd
import config as cfg
import boto3
import sys
import os
import shutil
from time import time
from datetime import datetime

pgentrega = config.pgentrega
pastas = config.pastas 

con = psycopg2.connect(database=pgentrega['DATABASE'], user=pgentrega['UID'], password=pgentrega['PWD'], host=pgentrega['SERVER'], port=pgentrega['PORT'])

#awss3_key = os.environ.get('access_key')
#awss3_secret = os.environ.get('secret_secret')

def list_files_in_s3_bucket(bucket_name, prefix=''):
    s3_client = boto3.client('s3')
    files = []
    try:
        response = s3_client.list_objects_v2(Bucket=bucket_name, Prefix=prefix)
        if 'Contents' in response:
            for obj in response['Contents']:
                files.append(obj['Key'])
            return files
        else:
            print(f"No files found in bucket '{bucket_name}' with prefix '{prefix}'.")
    except Exception as e:
        print(f"An error occurred: {e}")

bucket_name = 'systaxbackuprds'  
prefix = 'pgentreganew/tabelao'  # Replace with your prefix
bucket_files = list_files_in_s3_bucket(bucket_name, prefix)

index = 0


print("Inicio: ",datetime.now())


for file in bucket_files:
    if index==0:
        print("primeiro arquivo:", file)
        cursor = con.cursor()
        comando = "select fc_carrega_csv('usuarios','usuarios','"+file+"',1::int);"
        cursor.execute(comando)
        con.commit()
        cursor.close()
        print("--------------------------------------------")
    else:
        print(file)
    index = index + 1



print(" ")
print("Fim: ",datetime.now())