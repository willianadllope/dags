import snowflake as sf
from snowflake import connector
from .. import config
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
#print('conectou')

cs = conn.cursor()
print('conectou')

#results = cs.execute('select current_version()').fetchone()
#print(results[0]

#comando='select count(1) as total from DB_TABELAO.DBO.clientes;'
#results = cs.execute(comando).fetchone()
#print(results)


#comando='execute task full.task_teste_chamada01;'
comando="CALL full.pr_carga_inicial_limpa_arquivos();"
print(comando)
#results = cs.execute(comando).fetchone()
#print(results)


cs.close()

print('fechou')
