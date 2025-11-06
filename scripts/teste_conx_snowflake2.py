import snowflake as sf
import snowflake.connector
import os
import config 
import ClassConnectSnowflake as ConxSnow

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


print("Conectado com sucesso!")
