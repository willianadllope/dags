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

con = psycopg2.connect(database='systax', user=pgentrega['UID'], password=pgentrega['PWD'], host=pgentrega['SERVER'], port=pgentrega['PORT'])

#awss3_key = os.environ.get('access_key')
#awss3_secret = os.environ.get('secret_secret')

def list_files_in_s3_bucket(bucket_name, prefix=''):
    s3_client = boto3.client('s3')
    files = []
    try:
        response = s3_client.list_objects_v2(Bucket=bucket_name, Prefix=prefix)
        if 'Contents' in response:
            for obj in response['Contents']:
                f=obj['Key'].replace(prefix,'')
                files.append(f)
            return files
        else:
            print(f"No files found in bucket '{bucket_name}' with prefix '{prefix}'.")
    except Exception as e:
        print(f"An error occurred: {e}")

filtrar = ""
if len(sys.argv)  >= 2:
    filtrar = sys.argv[1]


print("Inicio: ",datetime.now())

# pasta_dentro_do_bucket : tabela_no_rds
buckets_tables_csv =	{
  "usuarios": "usuarios",
  "clientes": "clientes",
  "config": "config",
  "licencas_controle": "licencas_controle",
  "usuario_clientes": "usuario_clientes",
  "cean_relacionado": "cean_relacionado",
  "custom_prod": "custom_prod",
  "custom_prod_rel_cigarros": "custom_prod_rel_cigarros",
  "config_super_condensado": "config_super_condensado",
  "tabelao": "tabelao",
  "cache_condensado": "cache_condensado",
}
bucket_name = 'systaxbackuprds'
for buckets in buckets_tables_csv:
    if filtrar != "" and buckets!=filtrar:
        continue
    print("-----------------------------")
    print("=====",datetime.now())
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
        ## comando = "select public.fc_carrega_csv_new(buckets_tables_csv[buckets],'"+buckets+"','"+file+"',"+(index==0 and "1" or "0")+"::int);"
        comando = "select public.fc_carrega_csv_new('teste_"+buckets_tables_csv[buckets]+"_copia','"+file+"',"+(index==0 and "1" or "0")+"::int,'"+bucket_name+"','pgentreganew/"+buckets+"','us-east-1');"
        print(comando)
        cursor.execute(comando)
        con.commit()
        cursor.close()

        index = index + 1
    del bucket_files
    print("=====",datetime.now())


print(" ")
print("Fim: ",datetime.now())