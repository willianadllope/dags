import config
import snowflake as sf
from snowflake import connector
import sys
import glob
from time import time
from datetime import datetime

cfg = config.snowtabelao
pastas = config.pastas
reverse = 0

if len(sys.argv)  >= 2:
    tabela = sys.argv[1]
if len(sys.argv)  >= 3:
    tipoExecucao = sys.argv[2] # FULL | INCREMENTAL      
if len(sys.argv) >= 4:
    reverse = sys.argv[3]

conn = sf.connector.connect(
    user=cfg['user'],
    password=cfg['password'],
    account=cfg['account'],
    warehouse=cfg['warehouse'],
    database=cfg['database'],
    schema=tipoExecucao
)

# populate the file_name and stage_name with the arguments
file_name =tipoExecucao+'/'+tabela+'/*'
stage_name = 'DB_TABELAO.'+tipoExecucao+'.STAGE_FILES_TABELAO/tabelao/'+tabela+'/'
print('stage: ' +stage_name)


cs = conn.cursor()
print('cursor aberto')

# parquet_prod01sql stage_files_snowflake_tabelao COPY INTO dbo.interno_cean_relacionado from @stage_files_snowflake_tabelao/tabelao/cean_relacionado/ FILE_FORMAT = (TYPE = CSV FIELD_DELIMITER = '|' ) MATCH_BY_COLUMN_NAME=CASE_INSENSITIVE
print('Pasta: '+pastas['parquet']+tipoExecucao+'/'+tabela+'/*.parquet')

arquivos = [f for f in glob.glob(pastas['parquet']+tipoExecucao+'/'+tabela+'/*.parquet')]

if(reverse=="1"):
    arquivos.sort(reverse=True)
    print("reverse")
else:
    arquivos.sort()

try:
    for arq in arquivos:
        print('INICIO: '+datetime.now().strftime("%Y-%m-%d %H %M %S"))
        print(arq)
        cs.execute(f"PUT file://{arq} @{stage_name} auto_compress=false")
        print('FIM: '+datetime.now().strftime("%Y-%m-%d %H %M %S"))
        print("----------------")
finally:
    cs.close()      


print('cursor fechado')    