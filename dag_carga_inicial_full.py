from datetime import timedelta, datetime

# The DAG object; we'll need this to instantiate a DAG
from airflow import DAG
from airflow.models.baseoperator import chain

# Operators; we need this to operate!
from airflow.operators.bash import BashOperator
from airflow.utils.dates import days_ago



default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email': ['willian.lopes@systax.com.br'],
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=1),
}

dag = DAG(
    'carga_inicial_full',
    schedule="@daily",
    default_args=default_args,
    start_date=datetime(2025, 1, 21),
    tags=['example','mssql'],
    template_searchpath="/root/airflow/dags/",
    catchup=False,
)
'''
systax_app.snowflake.pr_preparar_carga_inicial_truncate
systax_app.snowflake.pr_preparar_carga_custom_prod
systax_app.snowflake.pr_preparar_carga_cean_relacionado
systax_app.snowflake.pr_preparar_carga_grupo
systax_app.snowflake.pr_preparar_carga_grupo_custom_prod
systax_app.snowflake.pr_preparar_carga_grupo_config
systax_app.snowflake.pr_preparar_carga_clientes
systax_app.snowflake.pr_preparar_carga_tributos_internos_cache_st
systax_app.snowflake.pr_preparar_carga_tributos_internos_cache
systax_app.snowflake.pr_preparar_carga_tributos_internos_cache_config
systax_app.snowflake.pr_preparar_carga_envio
systax_app.snowflake.pr_preparar_carga_schemafull
'''
carga_inicial_truncate = BashOperator(
    task_id="carga_inicial_truncate",
    bash_command="python /root/airflow/dags/scripts/call_procedure_carga_inicial.py 'pr_preparar_carga_inicial_truncate' 'full'",
    dag=dag,
)
carga_custom_prod = BashOperator(
    task_id="carga_custom_prod",
    bash_command="python /root/airflow/dags/scripts/call_procedure_carga_inicial.py 'pr_preparar_carga_custom_prod' 'full'",
    dag=dag,
)
carga_cean_relacionado = BashOperator(
    task_id="carga_cean_relacionado",
    bash_command="python /root/airflow/dags/scripts/call_procedure_carga_inicial.py 'pr_preparar_carga_cean_relacionado' 'full'",
    dag=dag,
)
carga_grupo = BashOperator(
    task_id="carga_grupo",
    bash_command="python /root/airflow/dags/scripts/call_procedure_carga_inicial.py 'pr_preparar_carga_grupo' 'full'",
    dag=dag,
)
carga_grupo_custom_prod = BashOperator(
    task_id="carga_grupo_custom_prod",
    bash_command="python /root/airflow/dags/scripts/call_procedure_carga_inicial.py 'pr_preparar_carga_grupo_custom_prod' 'full'",
    dag=dag,
)
carga_grupo_config = BashOperator(
    task_id="carga_grupo_config",
    bash_command="python /root/airflow/dags/scripts/call_procedure_carga_inicial.py 'pr_preparar_carga_grupo_config' 'full'",
    dag=dag,
)
carga_clientes = BashOperator(
    task_id="carga_clientes",
    bash_command="python /root/airflow/dags/scripts/call_procedure_carga_inicial.py 'pr_preparar_carga_clientes' 'full'",
    dag=dag,
)
carga_tributos_internos_cache_st = BashOperator(
    task_id="carga_tributos_internos_cache_st",
    bash_command="python /root/airflow/dags/scripts/call_procedure_carga_inicial.py 'pr_preparar_carga_tributos_internos_cache_st' 'full'",
    dag=dag,
)
carga_tributos_internos_cache = BashOperator(
    task_id="carga_tributos_internos_cache",
    bash_command="python /root/airflow/dags/scripts/call_procedure_carga_inicial.py 'pr_preparar_carga_tributos_internos_cache' 'full'",
    dag=dag,
)
carga_tributos_internos_cache_config = BashOperator(
    task_id="carga_tributos_internos_cache_config",
    bash_command="python /root/airflow/dags/scripts/call_procedure_carga_inicial.py 'pr_preparar_carga_tributos_internos_cache_config' 'full'",
    dag=dag,
)
carga_envio = BashOperator(
    task_id="carga_envio",
    bash_command="python /root/airflow/dags/scripts/call_procedure_carga_inicial.py 'pr_preparar_carga_envio' 'full'",
    dag=dag,
)
carga_schemafull = BashOperator(
    task_id="carga_schemafull",
    bash_command="python /root/airflow/dags/scripts/call_procedure_carga_inicial.py 'pr_preparar_carga_schemafull' 'full'",
    dag=dag,
)
'''
tsnow1 = BashOperator(
    task_id="bash_snow01",
    bash_command="python /root/airflow/dags/scripts/testesnow01.py",
    dag=dag,
)
'''
chain(carga_inicial_truncate, [
    carga_custom_prod,
    carga_cean_relacionado,
    carga_grupo,
    carga_grupo_config,
    carga_grupo_custom_prod,
    carga_tributos_internos_cache,
    carga_tributos_internos_cache_st,
    carga_clientes,
    carga_tributos_internos_cache_config
    ], carga_envio, carga_schemafull )

### teste de sobe um restore do DBCarrefourAtualizacao
### mudanca para o DBControle do 379 e 380
### change 263 para change normal 
