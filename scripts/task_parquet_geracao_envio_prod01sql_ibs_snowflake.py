import config
import queriesibs
import sys
import os
import shutil
from time import time

from sqlalchemy import create_engine, text
from datetime import datetime
import pandas as pd
import urllib as ul
import snowflake as sf

from snowflake import connector
import ClassConnectSnowflake as ConxSnow


db = config.prod01sqldev
cfg = config.snowibs
sql_queries = queriesibs.sql_queries
pastas = config.diretorios

props = ul.parse.quote_plus("DRIVER={SQL Server Native Client 11.0};"
                                "SERVER=" + db['SERVER'] + ";"
                                "DATABASE=" + db['DATABASE'] + ";"
                                "uid="+db['UID']+";pwd="+db['PWD']+";"
                                "Encrypt=yes;TrustServerCertificate=yes;")
con = create_engine(f"mssql+pymssql://{db['UID']}:{db['PWD']}@{db['SERVER']}:{db['PORT']}/{db['DATABASE']}")

connectSnowflake = ConxSnow.ConnectSnowflake(cfg)
private_key_bytes = connectSnowflake.get_value_key()

connSnow = sf.connector.connect(
    user=cfg['user'],
    account=cfg['account'],
    private_key=private_key_bytes,
    warehouse=cfg['warehouse'],
    database=cfg['database'],
    schema=cfg['schema'],
    role=cfg['role']
)

def export_query_to_parquet(sql,tipo, fileprefix, limit, strposicao='001'):
    """ export data from given table to parquet """
    time_step = time()
    print("Let's export", fileprefix)
    lines = 0
    print("SQL: "+sql)
    for i, df in enumerate(pd.read_sql(sql, con, chunksize=limit)):
		# by chunk of 1M rows if needed
        t_step = time()
        current_date = datetime.now()
        formatted_previous_day = current_date.strftime("%Y%m%d%H%M%S")
        file_name = pastas['ibs']+tipo+'/'+fileprefix+'/'+fileprefix + '_'+strposicao + '_'+str(i) +'_'+ formatted_previous_day+'.parquet'   
        df.to_parquet(file_name, index=False)
        lines += df.shape[0]
        print('  ', file_name, df.shape[0], f'lines ({round(time() - t_step, 2)}s)')
    print("  ", lines, f"lines exported {'' if i==0 else f' in {i} files'} ({round(time() - time_step, 2)}s)")

def delete_files_directory(tipo, diretorio):
    # Specify the path of the directory to be deleted
    directory_path = pastas['ibs']+tipo+'/'+diretorio
    # Check if the directory exists before attempting to delete it
    if os.path.exists(directory_path):
        shutil.rmtree(directory_path)
        print(f"The directory {directory_path} has been deleted.")
    else:
        print(f"The directory {directory_path} does not exist.")     
    os.makedirs(directory_path)

def send_parquet_snowflake(tipo, tabela):
    # populate the file_name and stage_name with the arguments
    file_name = pastas['ibs']+tipo+'/'+tabela+'/*'
    STAGE_SCHEMA = 'STAGING'
    #STAGE_SCHEMA = 'FULL'
    #if(tipo=='INCREMENTAL'):
    #    STAGE_SCHEMA = 'INCREMENTAL'
    stage_name = 'COCKPIT.'+STAGE_SCHEMA+'.STAGE_FILES_IBS/'+tabela+'/'
    cs = connSnow.cursor()
    print('Enviando '+tabela)
    try:
        cs.execute(f"PUT file://{file_name} @{stage_name} auto_compress=false")
        print(cs.fetchall())
    finally:
        cs.close()
    print('Enviado '+tabela)

def main():
    posicao = 1
    posicao_final = 500
    stringposicao = '001'
    if len(sys.argv)  >= 2:
      findTabela = sys.argv[1]
      if findTabela != 'ALL':
        indexAtual = 0
        index = 0
        for consulta in sql_queries:
          if (consulta.tabela==findTabela):
            index = indexAtual
          indexAtual = indexAtual + 1
        sqls = [
          sql_queries[index]
        ]
      else:
        sqls = sql_queries
    else:
      sqls = sql_queries

    if len(sys.argv)  >= 3:
      tipoExecucao = sys.argv[2] # FULL | INCREMENTAL
    tipoExecucao = tipoExecucao.upper()
    if len(sys.argv)  >= 4:
      posicao = int(sys.argv[3]) # inicio do loop
    if len(sys.argv)  >= 5:
      posicao_final = int(sys.argv[4]) # fim do loop
    print('TIPO: ' + tipoExecucao)
    for consulta in sqls:
        cons = consulta.consulta
        delete_files_directory(tipoExecucao, consulta.tabela)
        export_query_to_parquet(cons, tipoExecucao, consulta.tabela, consulta.limite)
        send_parquet_snowflake(tipoExecucao, consulta.tabela)
## exemplos:
## parquet_geracao_envio.py ALL FULL 1 500
## parquet_geracao_envio.py tributos_internos_cache incremental 1 500
## parquet_geracao_envio.py tributos_internos_cache FULL 999  => somente envio da tributos_internos_cache
## parquet_geracao_envio.py tributos_internos_cache incremental 999  => somente envio da tributos_internos_cache

### ANTES DA EXECUCAO, chamar:
###  call staging.pr_carga_inicial_limpa_arquivos();

if __name__ == "__main__":
    print('INICIO: '+datetime.now().strftime("%Y-%m-%d %H %M %S"))
    main()
    print('FIM: '+datetime.now().strftime("%Y-%m-%d %H %M %S"))

### APOS A EXECUCAO, chamar:
###  call staging.pr_carregar_tabelas_ibs();
###  call staging.pr_carregar_tabelas_ibs_producao();