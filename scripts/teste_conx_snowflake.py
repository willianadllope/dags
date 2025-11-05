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
schema = schema.upper()    
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

cs.close()
#dbo.task_gera_tabelao TASK_GERA_TABELAO
#dbo.task_tabelao_apaga_indevidos TASK_TABELAO_APAGA_INDEVIDOS;

print('fechou')
