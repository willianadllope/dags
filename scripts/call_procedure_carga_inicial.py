import sys
import os
import shutil
import pyodbc
from time import time
from collections import namedtuple
from sqlalchemy import create_engine, text
from datetime import datetime
import pandas as pd
import urllib as ul

db = {
    'DRIVER': '{ODBC Driver 17 for SQL Server}',
    'SERVER': '192.168.0.35',
    'DATABASE': 'systax_app',
    'PORT': '1433',
    'UID': 'willian',
    'PWD': 'billpoker13!'
}

## ex: python call_procedure_carga_inicial.py pr_preparar_carga_custom_prod full

procedure = 'pr_preparar_carga_custom_prod'
tipo = 'full'

if len(sys.argv)  >= 2:
    procedure = sys.argv[1]

if len(sys.argv)  >= 3:
    tipo = sys.argv[2]

engine = create_engine(f"mssql+pymssql://{db['UID']}:{db['PWD']}@{db['SERVER']}:{db['PORT']}/{db['DATABASE']}")

conx = engine.raw_connection()
cursor = conx.cursor()
command = "snowflake."+procedure+" @tipo='"+tipo+"'"
##print(command)
cursor.execute(command)
conx.commit()
cursor.close()
