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

#comando='EXECUTE TASK '+schema+'.'+task+' '+param1+' '+param2+' '+param3
#results = cs.execute(comando)

time.sleep(5)

results = cs.execute('select current_timestamp()').fetchone()
datainicial = results[0]
print("data:"+datainicial)

try:
    # AND NAME LIKE '%TASK_TESTE%' 
    comando = "SELECT STATE, NAME , COMPLETED_TIME FROM TABLE(INFORMATION_SCHEMA.TASK_HISTORY()) WHERE STATE <> 'SUCCEEDED' AND NAME LIKE '%TASK_TESTE%' AND query_start_time >= '"+datainicial+"' ORDER BY query_start_time DESC"
    print(comando)
    #cs.execute(comando)
    #df = cs.fetch_pandas_all()
    #print("Total:"+df.size)
    
    #df.info()
    #print("__________")
    #print(df.to_string())
finally:
    conn.close()

cs.close()

#print('fechou')
