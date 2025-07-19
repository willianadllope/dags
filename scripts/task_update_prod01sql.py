import sys
import os
import shutil
from time import time
from sqlalchemy import create_engine
from datetime import datetime
import pandas as pd
import config

db = config.prod01sql

tipo_carga = 'incremental' # ou full
tpCarga = 'I'
status_anterior_carga = '0'
status_carga = '0'

if len(sys.argv)  >= 2:
    tipo_carga = sys.argv[1]

if tipo_carga.lower() == 'incremental':
    tpCarga = 'I'

if tipo_carga.lower() == 'full':
    tpCarga = 'F'

if len(sys.argv)  >= 3:
    status_anterior_carga = sys.argv[2]

if len(sys.argv)  >= 4:
    status_carga = sys.argv[3]

engine = create_engine(f"mssql+pymssql://{db['UID']}:{db['PWD']}@{db['SERVER']}:{db['PORT']}/{db['DATABASE']}")

conx = engine.raw_connection()
cursor = conx.cursor()
comando = "snowflake.pr_update_carga @status = '"+status_carga+"', @carga='"+tpCarga+"', @status_anterior = "+status_anterior_carga
cursor.execute(comando)
conx.commit()
cursor.close()

