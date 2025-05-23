import psycopg2
import pandas as pd
import config as cfg
import boto3
import sys
import os
import shutil
from time import time
from datetime import datetime

pgentrega = cfg.pgentrega
pastas = cfg.pastas 

con = psycopg2.connect(database='snowflake', user=pgentrega['UID'], password=pgentrega['PWD'], host=pgentrega['SERVER'], port=pgentrega['PORT'])

#awss3_key = os.environ.get('access_key')
#awss3_secret = os.environ.get('secret_secret')

def list_files_in_s3_bucket(bucket_name, prefix=''):
    s3_client = boto3.client('s3')
    files = []
    try:
        response = s3_client.list_objects_v2(Bucket=bucket_name, Prefix=prefix)
        if 'Contents' in response:
            for obj in response['Contents']:
                f=obj['Key'].replace(prefix+'/','')
                files.append(f)
            return files
        else:
            print(f"No files found in bucket '{bucket_name}' with prefix '{prefix}'.")
    except Exception as e:
        print(f"An error occurred: {e}")

print("Inicio: ",datetime.now())



bucket_name = 'systaxbackuprds'  
prefix = 'pgentreganew/usuarios'  # Replace with your prefix
bucket_files = list_files_in_s3_bucket(bucket_name, prefix)

index = 0
for file in bucket_files:
    if index==0:
        print("primeiro arquivo:", file)
    else:
        print(file)

    cursor = con.cursor()
    comando = "select public.fc_carrega_csv('usuarios','usuarios','"+file+"',"+(index==0 and "0" or "1")+"::int);"
    cursor.execute(comando)
    con.commit()
    cursor.close()

    index = index + 1



print(" ")
print("Fim: ",datetime.now())