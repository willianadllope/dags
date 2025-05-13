import psycopg2
import pandas as pd

con = psycopg2.connect(database="systax", user="systax", password="SystX201406@psql", host="dbcentralizada.cwvlwwjgliab.us-east-1.rds.amazonaws.com", port=5432)

# cursor = connection.cursor()

# cursor.execute("SELECT * from tabelao_futuro_copia limit 10;")
# Fetch all rows from database
# record = cursor.fetchall()

print("Data from Database:- ", record)

df = pd.read_sql("SELECT id_cliente, id_config from tabelao_futuro_copia limit 10", con)

for index,row in df.iterrows():
    print("id_cliente = ",row['id_cliente'])
    print("id_config = ",row['id_config'])

