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
configs = config.configs

print("Task Test:"+configs['scripts']['task_send_s3_rds'])
## ex: python call_procedure_prod01sql.py pr_preparar_carga_custom_prod full

numero = ''
tipo = ''

if len(sys.argv)  >= 2:
    numero = sys.argv[1]

if len(sys.argv)  >= 3:
    tipo = sys.argv[2]

print("Executando numero "+numero)


if numero == '2' and tipo == 'FULL':
    print("EXIT")
    sys.exit(0)


print("FIM")