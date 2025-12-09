import sys
import os
import shutil
import pyodbc
import config
from time import time
from collections import namedtuple
from sqlalchemy import create_engine, text
from datetime import datetime
import pandas as pd
import urllib as ul

db = config.prod01sql

## ex: python task_call_procedure_prod01sql_pautas.py pr_preparar_tbl_registros_replicados full
## ex: python task_call_procedure_prod01sql_pautas.py pr_preparar_tbl_registros_replicados atualizacao
## ex: python task_call_procedure_prod01sql_pautas.py pr_criar_csv_completo full
## ex: python task_call_procedure_prod01sql_pautas.py pr_criar_csv_completo atualizacao

procedure = ''
tipo = ''

if len(sys.argv)  >= 2:
    procedure = sys.argv[1]

if len(sys.argv)  >= 3:
    tipo = sys.argv[2]

engine = create_engine(f"mssql+pymssql://{db['UID']}:{db['PWD']}@{db['SERVER']}:{db['PORT']}/{db['DBPAUTAS']}")

conx = engine.raw_connection()
cursor = conx.cursor()
command = "execute dbo."+procedure+" @tipo_geracao='"+tipo+"'"
print(command)

cursor.execute(command)
conx.commit()

cursor.close()
