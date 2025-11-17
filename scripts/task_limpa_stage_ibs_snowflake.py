import snowflake as sf
from snowflake import connector
import config
import sys
#print('iniciou')

cfg = config.snowibs

conn = sf.connector.connect(
    user=cfg['user'],
    password=cfg['password'],
    account=cfg['account'],
    warehouse=cfg['warehouse'],
    database=cfg['database'],
    schema=cfg['schema']
)


cs = conn.cursor()
tipo = 'STAGING'

if len(sys.argv)  >= 2:
    tipo = sys.argv[1]
tipo = tipo.upper()

comando="CALL "+tipo+".pr_carga_inicial_limpa_arquivos();"
print(comando)

results = cs.execute(comando).fetchone()
print(results)
cs.close()

#print('fechou')
