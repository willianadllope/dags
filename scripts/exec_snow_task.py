import snowflake as sf
from snowflake import connector
import config
import sys
import pandas
import time 

#print('iniciou')

cfg = config.snowtabelao

conn = sf.connector.connect(
    user=cfg['user'],
    password=cfg['password'],
    account=cfg['account'],
    warehouse=cfg['warehouse'],
    database=cfg['database'],
    schema=cfg['schema']
)

schema = 'full'
task = 'task_teste_inicio'
filtro = 'task_teste'
param1 = ''
param2 = ''

if len(sys.argv)  >= 2:
    schema = sys.argv[1]
if len(sys.argv)  >= 3:
    task = sys.argv[2]
if len(sys.argv)  >= 4:
    filtro = sys.argv[3]
if len(sys.argv)  >= 5:
    param1 = sys.argv[4]
if len(sys.argv)  >= 6:
    param2 = sys.argv[5]

cs = conn.cursor()

results = cs.execute('select current_timestamp()').fetchone()
datainicial = results[0]
#datainicial = '2025-01-31 11:00:59.466000-08:00'
time.sleep(10)

comando='EXECUTE TASK '+schema+'.'+task+' '+param1+' '+param2
results = cs.execute(comando)

time.sleep(10)
executou = 0
executar = 1
try:
    comando = "SELECT COUNT(1) FROM TABLE(INFORMATION_SCHEMA.TASK_HISTORY()) WHERE STATE = 'EXECUTING' AND NAME LIKE '%"+filtro+"%' AND query_start_time >= '"+str(datainicial)+"' "
    print(comando)
    # comando = "SELECT STATE, NAME , COMPLETED_TIME FROM TABLE(INFORMATION_SCHEMA.TASK_HISTORY()) WHERE STATE = 'EXECUTING' AND NAME LIKE '%TASK_TESTE%' AND query_start_time >= '"+str(datainicial)+"' ORDER BY query_start_time DESC"
    while executar == 1:
        print(". ")
        time.sleep(10)
        results = cs.execute(comando).fetchone()
        if int(results[0])==0:
            executou = executou + 1
        if executou > 5:
            executar = 0
finally:
    conn.close()

cs.close()
#dbo.task_gera_tabelao TASK_GERA_TABELAO
#dbo.task_tabelao_apaga_indevidos TASK_TABELAO_APAGA_INDEVIDOS;

#print('fechou')
