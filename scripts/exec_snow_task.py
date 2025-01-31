import snowflake as sf
from snowflake import connector
import config
import sys
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
task = 'ts_carga_inicial_carrega_ts_atual'
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

comando='EXECUTE TASK '+schema+'.'+task+' '+param1+' '+param2+' '+param3
print(comando)

results = cs.execute(comando).fetchone()
print(results)
cs.close()

#print('fechou')
