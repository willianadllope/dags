import boto3
import os
import sys

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

import os

def converter_diretorio_utf8_para_win1252(diretorio_entrada, diretorio_saida, extensao='.csv'):
    # Cria o diretório de saída se não existir
    os.makedirs(diretorio_saida, exist_ok=True)

    # Percorre os arquivos do diretório
    for nome_arquivo in os.listdir(diretorio_entrada):
        if nome_arquivo.endswith(extensao):
            caminho_entrada = os.path.join(diretorio_entrada, nome_arquivo)
            caminho_saida = os.path.join(diretorio_saida, nome_arquivo)

            try:
                with open(caminho_entrada, 'r', encoding='utf-8') as f_in:
                    conteudo = f_in.read()

                with open(caminho_saida, 'w', encoding='windows-1252', errors='replace') as f_out:
                    f_out.write(conteudo)

                print(f"Convertido: {nome_arquivo}")
            except Exception as e:
                print(f"Erro ao converter {nome_arquivo}: {e}")

# Exemplo de uso
tabela = ""
if len(sys.argv)  >= 2:
    tabela = sys.argv[1]

bucket = 'systaxbackuprds'
diretorio_local = '/parquet2/csv/'+tabela+'/'
diretorio_saida = '/parquet2/convertido/'+tabela+'/'
prefixo_opcional = 'pgentreganew/'+tabela+'/'

baixar_arquivos_do_bucket(bucket, diretorio_local, prefixo_opcional)

converter_diretorio_utf8_para_win1252(diretorio_local, diretorio_saida)