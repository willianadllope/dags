import snowflake.connector
import os
import config 

# Caminho da sua chave privada
private_key_path = os.path.expanduser("/keys/rsa_keyACCOUNTADMIN.p8")

# Ler e converter a chave para formato DER
with open(private_key_path, "rb") as key:
    private_key = key.read()

import cryptography.hazmat.primitives.serialization as serialization
from cryptography.hazmat.backends import default_backend

p_key = serialization.load_pem_private_key(
    private_key,
    password=None,  # se houver senha, use b"senha_aqui"
    backend=default_backend()
)

# Converter para formato DER para uso no Snowflake
from cryptography.hazmat.primitives import serialization as seri

private_key_bytes = p_key.private_bytes(
    encoding=seri.Encoding.DER,
    format=seri.PrivateFormat.PKCS8,
    encryption_algorithm=seri.NoEncryption()
)

cfg = config.snowibs
#print(cfg['user'])

conn = sf.connector.connect(
    user=cfg['user'],
#    password=cfg['password'],
    account=cfg['account'],
    private_key=private_key_bytes,
    warehouse=cfg['warehouse'],
    database=cfg['database'],
    schema=cfg['schema'],
    role=cfg['role']
)


print("✅ Conectado com sucesso!")
