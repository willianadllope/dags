import boto3
import os

def baixar_arquivos_do_bucket(bucket_name, destino_local, prefixo=''):
    # Inicializa o cliente do S3
    s3 = boto3.client('s3')
    
    # Lista os objetos no bucket
    resposta = s3.list_objects_v2(Bucket=bucket_name, Prefix=prefixo)

    if 'Contents' not in resposta:
        print("Nenhum arquivo encontrado no bucket.")
        return

    for obj in resposta['Contents']:
        chave = obj['Key']
        caminho_destino = os.path.join(destino_local, chave)

        # Cria diretórios intermediários se necessário
        os.makedirs(os.path.dirname(caminho_destino), exist_ok=True)

        # Baixa o arquivo
        print(f"Baixando {chave} para {caminho_destino}...")
        s3.download_file(bucket_name, chave, caminho_destino)

    print("Download concluído.")

# Exemplo de uso
bucket = 'systaxbackuprds'
diretorio_destino = '/parquet2/csv/usuarios/'
prefixo_opcional = 'pgentreganew/usuarios/'

baixar_arquivos_do_bucket(bucket, diretorio_destino, prefixo_opcional)
