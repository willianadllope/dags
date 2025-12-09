import sys
import shutil
from time import time
import boto3
import os
from botocore.exceptions import NoCredentialsError, PartialCredentialsError, ClientError
from sqlalchemy import create_engine
from datetime import datetime
import pandas as pd
import config

# --- Configurações ---
BUCKET_NAME = 'csvvertex'
LOCAL_DIRECTORY = '/csvpautas'
FILE_EXTENSION = '.csv'
db = config.prod01sql

print(f"DATABASE:{db['DATABASE']}")

engine = create_engine(f"mssql+pymssql://{db['UID']}:{db['PWD']}@{db['SERVER']}:{db['PORT']}/{db['DATABASE']}")
con = engine.connect()

print(engine)
def get_file_csv_created():
    con = engine.connect()
    df = pd.read_sql("SELECT TOP 1 id, id_controle, arquivo FROM vertex_pauta.dbo.log_arquivo_csv_pautas (nolock) WHERE etapa='gerado' ORDER BY ID", con)
    for index,row in df.iterrows():
        arquivo = row['arquivo'];
    print("arquivo:",arquivo)
    return arquivo

# O nome do arquivo a ser baixado é o segundo elemento da lista sys.argv
#file_to_download = get_file_csv_created()

def set_file_downloaded(arquivo):
    con = engine.connect()
    cursor = con.cursor()
    comando = f"UPDATE vertex_pauta.dbo.log_arquivo_csv_pautas SET etapa='downloaded' WHERE arquivo = '{arquivo}';"
    cursor.execute(comando)
    con.commit()
    cursor.close()

def download_single_file(bucket_name, file_key, local_dir):
    """
    Baixa um único arquivo especificado (file_key) de um bucket S3 
    para um diretório local.
    """
    
    local_file_path = os.path.join(local_dir, os.path.basename(file_key))
    print(f"Tentando baixar o arquivo '{file_key}' do bucket '{bucket_name}'...")
    print(f"Caminho local de destino: {local_file_path}")
    
    # 1. Cria a sessão e o cliente S3
    try:
        s3 = boto3.client('s3')
    except (NoCredentialsError, PartialCredentialsError) as e:
        print(f"❌ Erro de credenciais: {e}")
        print("Verifique se suas credenciais AWS estão configuradas.")
        return False
    except Exception as e:
        print(f"❌ Ocorreu um erro ao criar o cliente S3: {e}")
        return False

    # 2. Cria o diretório local se não existir
    if not os.path.exists(local_dir):
        os.makedirs(local_dir)
        print(f"Diretório local '{local_dir}' criado.")
    
    # 3. Baixa o arquivo
    try:
        # Verifica se o arquivo existe antes de tentar o download (opcional, mas útil)
        s3.head_object(Bucket=bucket_name, Key=file_key)
        
        # O método download_file baixa o objeto para o caminho local especificado
        s3.download_file(bucket_name, file_key, local_file_path)
        
        print("\n--- Concluído ---")
        print(f"✅ Arquivo baixado com sucesso: **{local_file_path}**")
        return True
        
    except ClientError as e:
        if e.response['Error']['Code'] == '404' or e.response['Error']['Code'] == 'NoSuchKey':
            print(f"❌ Erro: O arquivo '{file_key}' **não foi encontrado** no bucket '{bucket_name}'.")
        elif e.response['Error']['Code'] == 'AccessDenied':
            print(f"❌ Erro: Acesso negado. Verifique as permissões para o bucket/arquivo.")
        else:
            print(f"❌ Erro ao baixar '{file_key}': {e}")
        return False
    except Exception as e:
        print(f"❌ Erro desconhecido ao baixar '{file_key}': {e}")
        return False


if __name__ == "__main__":
    
    #download_single_file(BUCKET_NAME, file_to_download, LOCAL_DIRECTORY)
    print('EXECUTOU')

    #set_file_downloaded(file_to_download)