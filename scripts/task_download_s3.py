import boto3
import os
from botocore.exceptions import NoCredentialsError, PartialCredentialsError, ClientError

# --- Configurações ---
BUCKET_NAME = 'csvvertex'
LOCAL_DIRECTORY = '/csvpautas'
FILE_EXTENSION = '.csv'

def download_csv_files(bucket_name, local_dir, file_ext):
    """
    Baixa todos os arquivos com a extensão especificada de um bucket S3 
    para um diretório local.
    """
    print(f"Iniciando o download do bucket '{bucket_name}'...")
    
    # 1. Cria a sessão e o cliente S3
    try:
        s3 = boto3.client('s3')
    except (NoCredentialsError, PartialCredentialsError) as e:
        print(f"❌ Erro de credenciais: {e}")
        print("Verifique se suas credenciais AWS estão configuradas.")
        return
    except Exception as e:
        print(f"❌ Ocorreu um erro ao criar o cliente S3: {e}")
        return

    # 2. Cria o diretório local se não existir
    if not os.path.exists(local_dir):
        os.makedirs(local_dir)
        print(f"Diretório local '{local_dir}' criado.")
    else:
        print(f"Diretório local '{local_dir}' já existe.")

    # 3. Lista os objetos no bucket
    try:
        response = s3.list_objects_v2(Bucket=bucket_name)
    except ClientError as e:
        if e.response['Error']['Code'] == 'NoSuchBucket':
            print(f"❌ Erro: O bucket '{bucket_name}' não existe.")
        elif e.response['Error']['Code'] == 'AccessDenied':
            print(f"❌ Erro: Acesso negado ao bucket '{bucket_name}'.")
        else:
            print(f"❌ Erro ao listar objetos: {e}")
        return
    
    # Verifica se há conteúdo no bucket
    if 'Contents' not in response:
        print(f"⚠️ O bucket '{bucket_name}' está vazio ou não contém objetos.")
        return

    # 4. Filtra e baixa os arquivos .csv
    download_count = 0
    
    for obj in response['Contents']:
        object_key = obj['Key']
        
        # Verifica se o arquivo termina com a extensão desejada
        if object_key.lower().endswith(file_ext):
            local_file_path = os.path.join(local_dir, os.path.basename(object_key))
            
            try:
                # O método download_file baixa o objeto para o caminho local especificado
                s3.download_file(bucket_name, object_key, local_file_path)
                print(f"✅ Baixado: s3://{bucket_name}/{object_key} -> {local_file_path}")
                download_count += 1
            except ClientError as e:
                print(f"❌ Erro ao baixar '{object_key}': {e}")
            except Exception as e:
                print(f"❌ Erro desconhecido ao baixar '{object_key}': {e}")

    # 5. Conclusão
    print(f"\n--- Concluído ---")
    print(f"Total de arquivos {file_ext} baixados: **{download_count}**")
    print(f"Local salvo: **{os.path.abspath(local_dir)}**")


if __name__ == "__main__":
    download_csv_files(BUCKET_NAME, LOCAL_DIRECTORY, FILE_EXTENSION)