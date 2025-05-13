import psycopg2

connection = psycopg2.connect(database="systax", user="systax", password="SystX201406@psql", host="dbcentralizada.cwvlwwjgliab.us-east-1.rds.amazonaws.com", port=5432)

cursor = connection.cursor()

cursor.execute("SELECT * from tabelao_futuro_copia limit 10;")

# Fetch all rows from database
record = cursor.fetchall()

print("Data from Database:- ", record)

