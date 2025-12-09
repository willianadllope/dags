import os
import sys
import time
import shutil
import boto3
from datetime import datetime
from botocore.exceptions import NoCredentialsError, PartialCredentialsError, ClientError
from sqlalchemy import create_engine
import pandas as pd
import config

# ===============================================
# CONFIGURAÇÕES
# ===============================================

ROLE_1_ARN = "arn:aws:iam::939357810902:role/RoleS3VertexPautasCSV"
ROLE_2_ARN = "arn:aws:iam::954235624237:role/sandbox-trm-pauta-content-s3-systax-us-east-2"

SESSION_NAME_ROLE1 = "upload-session"
SESSION_NAME_ROLE2 = "upload-session"

BUCKET_NAME = "sandbox-trm-pauta-content-us-east-2"
BUCKET_NAME = "systaxlinks"

BASE_PROFILE = "systax"
LOCAL_DIRECTORY = '/csvpautas'

db = config.prod01sql

def get_file_csv_downloaded():
    engine = create_engine(f"mssql+pymssql://{db['UID']}:{db['PWD']}@{db['SERVER']}:{db['PORT']}/{db['DATABASE']}")
    con = engine.connect()
    arquivo = ""
    df = pd.read_sql("SELECT TOP 1 id, id_controle, arquivo FROM vertex_pauta.dbo.log_arquivo_csv_pautas (nolock) WHERE etapa='downloaded' ORDER BY ID", con)
    for index,row in df.iterrows():
        arquivo = row['arquivo'];
    return arquivo

def set_file_uploaded(arquivo):
    engine = create_engine(f"mssql+pymssql://{db['UID']}:{db['PWD']}@{db['SERVER']}:{db['PORT']}/{db['DATABASE']}")
    con2 = engine.raw_connection()
    cursor = con2.cursor()
    comando = f"UPDATE vertex_pauta.dbo.log_arquivo_csv_pautas SET etapa='uploaded' WHERE arquivo = '{arquivo}';"
    cursor.execute(comando)
    con2.commit()
    cursor.close()

arquivo_local = get_file_csv_downloaded()
LOCAL_FILE = f"{LOCAL_DIRECTORY}/{arquivo_local}"
OBJECT_KEY = arquivo_local
# ===============================================
# FUNÇÃO PARA ASSUMIR UMA ROLE COM CREDENCIAIS ESPECÍFICAS
# ===============================================

def assume_role(role_arn, session_name, base_session=None):
    """
    base_session = boto3.Session(...) com credenciais já assumidas
                 = None -> usar credenciais padrão (~/.aws/credentials)
    """
    try:
        if base_session:
            sts = base_session.client("sts")
        else:
            base_session = boto3.Session(profile_name=BASE_PROFILE)
            sts = base_session.client("sts")

        print(f"\nAssumindo role: {role_arn} ...")

        response = sts.assume_role(
            RoleArn=role_arn,
            RoleSessionName=session_name
        )

        creds = response["Credentials"]

        # Retorna uma nova sessão usando as credenciais temporárias
        return boto3.Session(
            aws_access_key_id     = creds["AccessKeyId"],
            aws_secret_access_key = creds["SecretAccessKey"],
            aws_session_token     = creds["SessionToken"]
        )
    except Exception as e:
        print("\n❌ ERRO ao assumir a role:")
        print(f"Role ARN: {role_arn}")
        print(f"Mensagem completa da AWS:\n{e}\n")
        print(boto3.Session(session_name).get_credentials().get_frozen_credentials())
        raise


# ===============================================
# 1. ASSUME ROLE 1
# ===============================================
def assume_role_systax():
    session_role1 = assume_role(
        role_arn=ROLE_1_ARN,
        session_name=SESSION_NAME_ROLE1,
        base_session=None  # usa credenciais padrão da máquina
    )
    print(session_role1)
    print("Role 1 assumida com sucesso!")
    s3 = session_role1.client("s3")
    s3.upload_file(LOCAL_FILE, BUCKET_NAME, OBJECT_KEY)
    print("\nUpload concluído com sucesso!")        
    return session_role1

def assume_role_vertex(session_role1):
    # ===============================================
    # 2. ASSUME ROLE 2 (USANDO CREDENCIAIS DA ROLE 1)
    # ===============================================
    session_role2 = assume_role(
        role_arn=ROLE_2_ARN,
        session_name=SESSION_NAME_ROLE2,
        base_session=session_role1
    )
    print("Role 2 assumida com sucesso!")
    s3 = session_role2.client("s3")
    s3.upload_file(LOCAL_FILE, BUCKET_NAME, OBJECT_KEY)
    print("\nUpload concluído com sucesso!")    

# ===============================================
# 3. UPLOAD PARA O S3 USANDO A SEGUNDA ROLE
# ===============================================

if __name__ == "__main__":
    print("INICIO")
    formatted_datetime = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    print(formatted_datetime)
    
    if arquivo_local != "":
        print(f"\nRealizando upload: {LOCAL_FILE} -> s3://{BUCKET_NAME}/{OBJECT_KEY}")
        sessao_systax = assume_role_systax()
        #assume_role_vertex(sessao_systax)
        

    formatted_datetime = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    print(formatted_datetime)
    print("FIM")
