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
        arquivo = chave.replace(prefixo,'')
        print("Chave:",chave)
        print("Destino_local:",destino_local)
        print("Arquivo:",arquivo)
        # Cria diretórios intermediários se necessário
        os.makedirs(os.path.dirname(destino_local), exist_ok=True)

        # Baixa o arquivo
        destino_local = destino_local+arquivo
        print(f"Baixando {chave} para {destino_local}...")
        s3.download_file(bucket_name, chave, destino_local)

    print("Download concluído.")

# Exemplo de uso
bucket = 'systaxbackuprds'
diretorio_destino = '/parquet2/csv/usuarios/'
prefixo_opcional = 'pgentreganew/usuarios/'

baixar_arquivos_do_bucket(bucket, diretorio_destino, prefixo_opcional)
