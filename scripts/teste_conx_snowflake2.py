import snowflake as sf
import snowflake.connector
import os
import config 
import cryptography.hazmat.primitives.serialization as serialization
from cryptography.hazmat.backends import default_backend
# Converter para formato DER para uso no Snowflake
from cryptography.hazmat.primitives import serialization as seri

cfg = config.snowibs

class ConnectSnowflake:
    def __init__(self, cfg):
        self.cfg = cfg
    
    def get_value_key(self):
        # Caminho da sua chave privada
        self.private_key_path = os.path.expanduser(self.cfg['privatekey'])

        # Ler e converter a chave para formato DER
        with open(self.private_key_path, "rb") as key:
            self.private_key = key.read()

        self.p_key = serialization.load_pem_private_key(
            self.private_key,
            password=None,  # se houver senha, use b"senha_aqui"
            backend=default_backend()
        )

        self.private_key_bytes = self.p_key.private_bytes(
            encoding=seri.Encoding.DER,
            format=seri.PrivateFormat.PKCS8,
            encryption_algorithm=seri.NoEncryption()
        )

        return self.private_key_bytes

connectSnowflake = ConnectSnowflake(cfg)
private_key_bytes = connectSnowflake.get_value_key()

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
