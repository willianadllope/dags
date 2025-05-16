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

def export_query_to_parquet(sql,pasta, fileprefix, limit, nrinicial):
    time_step = time()
    print("Let's export", fileprefix)
    lines = 0
    print("SQL: "+sql)
    lastID = -1
    for i, df in enumerate(pd.read_sql(sql, con, chunksize=limit)):
		# by chunk of 1M rows if needed
        t_step = time()
        linhas=len(df)
        if linhas>0:
            regID = df.loc[[(linhas-1)]].id
            lastID = regID[(linhas-1)]
            current_date = datetime.now()
            formatted_previous_day = current_date.strftime("%Y%m%d%H%M%S")
            file_name = pasta+fileprefix+ '_'+str(nrinicial)+'_'+str(i) +'_'+ formatted_previous_day+'.parquet'
            df.to_parquet(file_name, index=False)
            lines += df.shape[0]
            print('  ', file_name, df.shape[0], f'lines ({round(time() - t_step, 2)}s)')
    if i>0:
        print("  ", lines, f"lines exported in {(i+1)} files' ({round(time() - time_step, 2)}s)")
    if lines < limit:
        lastID = -1
    return lastID

# cursor = connection.cursor()

# cursor.execute("SELECT * from tabelao_futuro_copia limit 10;")
# Fetch all rows from database
# record = cursor.fetchall()

#print("Data from Database:- ", record)

#df = pd.read_sql("SELECT id_cliente, id_config from tabelao_futuro_copia limit 10", con)
#for index,row in df.iterrows():
#    print("id_cliente = ",row['id_cliente'])
#    print("id_config = ",row['id_config'])



apagararquivos = 0
corte = 5000000 #5Milhoes
paginacao = 1000000 #1milhao

if len(sys.argv)  >= 2:
    apagararquivos = sys.argv[1]
 
print("Inicio: ",datetime.now())

print("--------------------------------------------")
print("criacao de novo registro de controle de ponteiros: ", datetime.now())
cursor = con.cursor()
cursor.execute("insert into controle_carga_ponteiros_snowflake (ponteiro, status) select max(menorts), 0 from tabelao;")
con.commit()
cursor.close()

print("--------------------------------------------")
print("captura do ultimo ponteiro: ", datetime.now())
df = pd.read_sql("select id, ponteiro from controle_carga_ponteiros_snowflake where status = 2 order by datahora desc limit 1", con)
for index,row in df.iterrows():
    idponteiro = row['id']
    ponteiro = row['ponteiro']
    print("id = ",idponteiro)
    print("ponteiro = ",ponteiro)

if apagararquivos == '1':
    print("--------------------------------------------")
    print("apagando arquivos no diretorio", datetime.now())
    delete_files_directory(pastas['parquet']+'FULL/ajusteponteirords/')

print("--------------------------------------------")
print("download dos ponteiros", datetime.now())
id = 0
while id >= 0:
    comando = "Select id, id_cliente, idconfigprod, menorts from public.tabelao where id > "+str(id)+" and menorts > "+str(ponteiro)+" and id < 10000000 order by id limit "+str(corte)
    id = export_query_to_parquet(comando, pastas['parquet']+'FULL/ajusteponteirords/', "regrasponteiros", paginacao, id)


print("--------------------------------------------")
print("atualiza carga atual para status=2: ", datetime.now())
cursor = con.cursor()
comando = "update controle_carga_ponteiros_snowflake set status = 2 where status = 0 and id > "+str(idponteiro)+";"
cursor.execute(comando)
con.commit()
cursor.close()

print(" ")
print("Fim: ",datetime.now())