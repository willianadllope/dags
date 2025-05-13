# Configurações do SqlServer
prod01sql = {
    'DRIVER': '{ODBC Driver 17 for SQL Server}',
    'SERVER': '192.168.0.35',
    'DATABASE': 'systax_app',
    'PORT': '1433',
    'UID': 'willian',
    'PWD': 'billpoker13!'
}

snowtabelao = {
    'user':'SYSTAXSNOW24',
    'password':'Dkjj$@8$g@hgsgj!!',
    'account':'DJDYJNY-ZK69750',
    'warehouse':'COMPUTE_WH',
    'database':'DB_TABELAO',
    'schema':'DBO'
}


pgentrega = {
    'SERVER': 'dbcentralizada.cwvlwwjgliab.us-east-1.rds.amazonaws.com',
    'DATABASE': 'systax',
    'PORT': '5432',
    'UID': 'systax',
    'PWD': 'SystX201406@psql'
}

pastas = {
    'dag':'/root/airflow/dags/',
    'scripts':'/root/airflow/dags/scripts/',
    'parquet':'/parquet2/tabelao/',
    'parquetfull':'/parquet2/tabelao/FULL/',
    'parquetincremental':'/parquet2/tabelao/INCREMENTAL/',
    'ajusteponteirords':'/parquet2/tabelao/ajuste_ponteiro_rds/',
    'tipoCarga':''
}