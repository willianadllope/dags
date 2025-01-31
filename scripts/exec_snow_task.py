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
param1 = ''
param2 = ''
param3 = ''

if len(sys.argv)  >= 2:
    schema = sys.argv[1]
if len(sys.argv)  >= 3:
    task = sys.argv[2]
if len(sys.argv)  >= 4:
    param1 = sys.argv[3]
if len(sys.argv)  >= 5:
    param2 = sys.argv[4]
if len(sys.argv)  >= 6:
    param3 = sys.argv[5]

cs = conn.cursor()

results = cs.execute('select current_timestamp()').fetchone()
datainicial = results[0]
#datainicial = '2025-01-31 11:00:59.466000-08:00'
time.sleep(10)

comando='EXECUTE TASK '+schema+'.'+task+' '+param1+' '+param2+' '+param3
results = cs.execute(comando)

time.sleep(5)

try:
    # comando = "SELECT STATE, NAME , COMPLETED_TIME FROM TABLE(INFORMATION_SCHEMA.TASK_HISTORY()) WHERE STATE = 'EXECUTING' AND NAME LIKE '%TASK_TESTE%' AND query_start_time >= '"+str(datainicial)+"' ORDER BY query_start_time DESC"
    executar = 1
    while executar == 1:
        executar = 0
        comando = "SELECT COUNT(1) FROM TABLE(INFORMATION_SCHEMA.TASK_HISTORY()) WHERE STATE = 'EXECUTING' AND NAME LIKE '%TASK_TESTE%' AND query_start_time >= '"+str(datainicial)+"' "
        print(comando)
        results = cs.execute(comando).fetchone()
        if int(results[0])==1:
            executar = 1
finally:
    conn.close()

cs.close()

#print('fechou')
