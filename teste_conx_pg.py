import psycopg2
import pandas as pd
import config

pgentrega = config.pgentrega

con = psycopg2.connect(database=pgentrega['DATABASE'], user=pgentrega['UID'], password=pgentrega['PWD'], host=pgentrega['SERVER'], port=pgentrega['PORT'])


# cursor = connection.cursor()

# cursor.execute("SELECT * from tabelao_futuro_copia limit 10;")
# Fetch all rows from database
# record = cursor.fetchall()

#print("Data from Database:- ", record)

df = pd.read_sql("SELECT id_cliente, id_config from tabelao_futuro_copia limit 10", con)
for index,row in df.iterrows():
    print("id_cliente = ",row['id_cliente'])
    print("id_config = ",row['id_config'])

#export_query_to_parquet("Select id_cliente, id_config from tabelao_futuro_copia limit 10", "full", "ajuste_ponteiro_rds")

