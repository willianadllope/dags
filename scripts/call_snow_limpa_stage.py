import snowflake as sf
from snowflake import connector
import config
import sys
print('iniciou')

cfg = config.snowtabelao

conn = sf.connector.connect(
    user=cfg['user'],
    password=cfg['password'],
    account=cfg['account'],
    warehouse=cfg['warehouse'],
    database=cfg['database'],
    schema=cfg['schema']
)



comando="CALL full.pr_carga_inicial_limpa_arquivos();"
print(comando)
'''
results = cs.execute(comando).fetchone()
print(results)
cs.close()
'''
print('fechou')
