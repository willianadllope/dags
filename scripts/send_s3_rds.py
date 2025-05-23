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

# pasta_dentro_do_bucket : tabela_no_rds
buckets_tables_csv =	{
  "cean_relacionado": "cean_relacionado",
  "clientes": "clientes",
  "config_super_condensado": "config_super_condensado",
  "config": "config",
  "custom_prod_rel_cigarros": "custom_prod_rel_cigarros",
  "licencas_controle": "licencas_controle",
  "usuario_clientes": "usuario_clientes",
  "custom_prod": "custom_prod",  
  #"cache_condensado": "cache_condensado",
  #"tabelao": "tabelao",
  "usuarios": "usuarios"
}
bucket_name = 'systaxbackuprds'
for buckets in buckets_tables_csv:
    print("-----------------------------")
    print("Diretorio: ",buckets)
    prefix = 'pgentreganew/'+buckets+'/'  # Replace with your prefix
    bucket_files = list_files_in_s3_bucket(bucket_name, prefix)
    index = 0
    for file in bucket_files:
        if index==0:
            print("primeiro arquivo: ", file)
        else:
            print(file)

        cursor = con.cursor()
        comando = "select public.fc_carrega_csv('"+buckets_tables_csv[buckets]+"','"+buckets+"','"+file+"',"+(index==0 and "1" or "0")+"::int);"
        print(comando)
        cursor.execute(comando)
        con.commit()
        cursor.close()

        index = index + 1
    del bucket_files


print(" ")
print("Fim: ",datetime.now())