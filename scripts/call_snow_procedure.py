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
procedure = 'pr_carga_inicial_ts_atual'


cs = conn.cursor()

if len(sys.argv)  >= 2:
    schema = sys.argv[1]
if len(sys.argv)  >= 3:
    procedure = sys.argv[2]

comando='CALL '+schema+'.'+procedure+'();'
print(comando)

results = cs.execute(comando).fetchone()
print(results)
cs.close()

#print('fechou')
## python scripts/gera_parquet_ponteiros_pg.py
## python scripts/upload_snowflake.py ajusteponteirords FULL
## python scripts/call_snow_procedure.py FULL PR_CARGA_INICIAL_AJUSTEPONTEIRORDS
## 

