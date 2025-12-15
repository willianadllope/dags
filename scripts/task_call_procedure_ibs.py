import snowflake as sf
from snowflake import connector
import config
import sys
import ClassConnectSnowflake as ConxSnow
#print('iniciou')

cfg = config.snowibs

connectSnowflake = ConxSnow.ConnectSnowflake(cfg)
private_key_bytes = connectSnowflake.get_value_key()

conn = sf.connector.connect(
    user=cfg['user'],
    account=cfg['account'],
    private_key=private_key_bytes,
    warehouse=cfg['warehouse'],
    database=cfg['database'],
    schema=cfg['schema'],
    role=cfg['role']
)

cs = conn.cursor()
tipo = 'STAGING'
procedure = 'pr_carga_inicial_limpa_arquivos'

if len(sys.argv)  >= 2:
    tipo = sys.argv[1]
if len(sys.argv)  >= 3:
    procedure = sys.argv[2]

tipo = tipo.upper()

comando="CALL "+tipo+"."+procedure+"();"
print(comando)

results = cs.execute(comando).fetchone()
print(results)
cs.close()

#print('fechou')
