import sys
import os
import shutil
from time import time
from sqlalchemy import create_engine
from datetime import datetime
import pandas as pd
import config

db = config.prod01sql

engine = create_engine(f"mssql+pymssql://{db['UID']}:{db['PWD']}@{db['SERVER']}:{db['PORT']}/{db['DATABASE']}")
con = engine.connect().execution_options(stream_results=True)

df = pd.read_sql("SELECT id, entidade from systax_app.dbo.clientes where id in (96201, 55982)", con)
print(df)

for index,row in df.iterrows():
    print("id = ",row['id'])
    print("nome = ",row['entidade'])

