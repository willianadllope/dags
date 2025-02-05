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

## ex: python call_procedure_prod01sql.py pr_preparar_carga_custom_prod full

procedure = 'pr_preparar_carga_custom_prod'
tipo = 'full'

if len(sys.argv)  >= 2:
    procedure = sys.argv[1]

if len(sys.argv)  >= 3:
    tipo = sys.argv[2]

engine = create_engine(f"mssql+pymssql://{db['UID']}:{db['PWD']}@{db['SERVER']}:{db['PORT']}/{db['DATABASE']}")

conx = engine.raw_connection()
cursor = conx.cursor()
command = "execute snowflake."+procedure+" @tipo='"+tipo+"'"
print(command)

cursor.execute(command)
#conx.commit()

cursor.close()
