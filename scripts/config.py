# Configurações do SqlServer
prod01sql = {
    'DRIVER': '{ODBC Driver 17 for SQL Server}',
    'SERVER': '192.168.0.35',
    'DATABASE': 'systax_app',
    'PORT': '1433',
    'UID': 'willian',
    'PWD': 'billpoker13!'
}

pgentrega = {
    'SERVER': 'dbcentralizada.cwvlwwjgliab.us-east-1.rds.amazonaws.com',
    'DATABASE': 'systax',
    'PORT': '5432',
    'UID': 'systax',
    'PWD': 'SystX201406@psql'
}

snowtabelao = {
    'user':'SYSTAXSNOW24',
    'password':'Dkjj$@8$g@hgsgj!!',
    'account':'DJDYJNY-ZK69750',
    'warehouse':'COMPUTE_WH',
    'database':'DB_TABELAO',
    'schema':'DBO'
}

diretorios = {
    'dags':'/root/airflow/dags/',
    'tasks':'/root/airflow/dags/scripts/',
    'parquet':'/parquet2/tabelao/',
    'parquetfull':'/parquet2/tabelao/FULL/',
    'parquetincremental':'/parquet2/tabelao/INCREMENTAL/',
    'ajusteponteirords':'/parquet2/tabelao/ajuste_ponteiro_rds/'
}

files_python = {
    ## script que chama procedure na prod01sql para quebrar os ids em chunks e facilitar o load das tabelas
    'task_call_procedure_prod01sql_chunks':diretorios['tasks']+'task_call_procedure_prod01sql_chunks.py',
    'task_call_procedure_prod01sql':diretorios['tasks']+'task_call_procedure_prod01sql.py',
    'task_limpa_stage_snowflake':diretorios['tasks']+'task_limpa_stage_snowflake.py',
    'task_inicia_carga_updt_prod01sql':diretorios['tasks']+'task_inicia_carga_updt_prod01sql.py',    
    'task_parquet_geracao_envio_prod01sql_snowflake': diretorios['tasks']+'task_parquet_geracao_envio_prod01sql_snowflake.py',    
    'task_upload_snowflake': diretorios['tasks']+'task_upload_snowflake.py',    
    'task_execute_snowflake': diretorios['tasks']+'task_execute_snowflake.py',    
    'task_call_procedure_snowflake.py': diretorios['tasks']+'task_call_procedure_snowflake.py'
}

scripts = {
    'task_inicia_carga':files_python['task_inicia_carga_updt_prod01sql'],
    'task_limpa_stage':files_python['task_limpa_stage_snowflake'],
    'task_carrega_carga':files_python['task_call_procedure_prod01sql'], 
    'task_group_chunks':files_python['task_call_procedure_prod01sql_chunks'], 
    'task_group_carrega_carga_full':files_python['task_call_procedure_prod01sql'], 
    'task_group_gera_envia_parquet':files_python['task_parquet_geracao_envio_prod01sql_snowflake'],
    'task_group_gera_parquet':files_python['task_parquet_geracao_envio_prod01sql_snowflake'],
    'task_group_gera_parquet_caches':files_python['task_parquet_geracao_envio_prod01sql_snowflake'],
    'task_group_apaga_parquet':files_python['task_parquet_geracao_envio_prod01sql_snowflake'],
    'task_group_envia_parquet':files_python['task_upload_snowflake'],
    'task_snowflake_carga':files_python['task_execute_snowflake'],
    'task_snowflake_gera_tabelao':files_python['task_execute_snowflake'],
    'task_snowflake_tabelao_apaga_indevidos':files_python['task_execute_snowflake'],
    'task_apaga_csv_s3_tabelao':files_python['task_call_procedure_prod01sql'], 
    'task_carrega_carga':files_python['task_call_procedure_prod01sql'], 
    'task_carrega_csv_tabelao_prod01sql':files_python['task_call_procedure_prod01sql'], 
    'task_download_csvs_tabelao':files_python['task_call_procedure_prod01sql'], 
    'task_envia_tabelao_s3':files_python['task_call_procedure_snowflake']
 }

configs = {
    'diretorios':diretorios,
    'tipoCarga':'full',
    'scripts': scripts
}



