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
systax_app.snowflake.pr_preparar_carga_cean_relacinado
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
carga_custom_prod = BashOperator(
    task_id="carga_custom_prod",
    bash_command="python /root/airflow/dags/scripts/call_procedure_carga_inicial.py 'pr_preparar_carga_custom_prod', 'full'",
    dag=dag,
)

t2 = BashOperator(
    task_id="bash_example2",
    bash_command="python /root/airflow/dags/scripts/testes04c.py",
    dag=dag,
)

tsnow1 = BashOperator(
    task_id="bash_snow01",
    bash_command="python /root/airflow/dags/scripts/testesnow01.py",
    dag=dag,
)

t3 = BashOperator(
    task_id='print_date',
    bash_command='date',
    dag=dag,
)

t4 = BashOperator(
    task_id='sleep',
    depends_on_past=False,
    bash_command='sleep 2',
    retries=3,
    dag=dag,
)

t5 = BashOperator(
    task_id='print_date2',
    bash_command='date',
    dag=dag,
)

'''
def cadeia01():
    chain(t1, t4)

def cadeia02():
    chain(t3, cadeia04() )
'''
## chain(tsnow1, t1, [ t2, t3, t4 ], t5)

## chain(tsnow1, t1, [ t2, chain(t3, t4) ], t5)

chain(tsnow1, t1, t2, [t4, t5], t3)

### teste de sobe um restore do DBCarrefourAtualizacao
### mudanca para o DBControle do 379 e 380
### change 263 para change normal 
