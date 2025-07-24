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

procedure = ''
tipo = ''

if len(sys.argv)  >= 2:
    procedure = sys.argv[1]

if len(sys.argv)  >= 3:
    tipo = sys.argv[2]


if tipo != 'FULL':
    sys.exit(0)


print("fim")