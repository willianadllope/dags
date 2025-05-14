import psycopg2
import pandas as pd
import config
import sys
import os
import shutil
from time import time
from datetime import datetime

pgentrega = config.pgentrega
pastas = config.pastas 

con = psycopg2.connect(database=pgentrega['DATABASE'], user=pgentrega['UID'], password=pgentrega['PWD'], host=pgentrega['SERVER'], port=pgentrega['PORT'])

def delete_files_directory(directory_path):
    if os.path.exists(directory_path):
        shutil.rmtree(directory_path)
        print(f"The directory {directory_path} has been deleted.")
    else:
        print(f"The directory {directory_path} does not exist.")     
    os.makedirs(directory_path)

def export_query_to_parquet(sql,pasta, fileprefix, limit):
    time_step = time()
    print("Let's export", fileprefix)
    lines = 0
    print("SQL: "+sql)
    for i, df in enumerate(pd.read_sql(sql, con, chunksize=limit)):
		# by chunk of 1M rows if needed
        t_step = time()
        current_date = datetime.now()
        formatted_previous_day = current_date.strftime("%Y%m%d%H%M%S")
        file_name = pasta+fileprefix+ '_'+str(i) +'_'+ formatted_previous_day+'.parquet'
        df.to_parquet(file_name, index=False)
        lines += df.shape[0]
        print('  ', file_name, df.shape[0], f'lines ({round(time() - t_step, 2)}s)')
    print("  ", lines, f"lines exported {'' if i==0 else f' in {i} files'} ({round(time() - time_step, 2)}s)")

# cursor = connection.cursor()

# cursor.execute("SELECT * from tabelao_futuro_copia limit 10;")
# Fetch all rows from database
# record = cursor.fetchall()

#print("Data from Database:- ", record)

#df = pd.read_sql("SELECT id_cliente, id_config from tabelao_futuro_copia limit 10", con)
#for index,row in df.iterrows():
#    print("id_cliente = ",row['id_cliente'])
#    print("id_config = ",row['id_config'])

delete_files_directory(pastas['parquet']+'FULL/ajusteponteirords/')

export_query_to_parquet("Select id_cliente, id_config, cod_prod, origem_produto, menorts from public.tabelao limit 1000000", pastas['parquet']+'FULL/ajusteponteirords/', "regrasponteiros",200000)

