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
    # 'queue': 'bash_queue',
    # 'pool': 'backfill',
    # 'priority_weight': 10,
    # 'end_date': datetime(2016, 1, 1),
    # 'wait_for_downstream': False,
    # 'dag': dag,
    # 'sla': timedelta(hours=2),
    # 'execution_timeout': timedelta(seconds=300),
    # 'on_failure_callback': some_function,
    # 'on_success_callback': some_other_function,
    # 'on_retry_callback': another_function,
    # 'sla_miss_callback': yet_another_function,
    # 'trigger_rule': 'all_success'
}

dag = DAG(
    'exemplo_mssql03',
    schedule="@daily",
    default_args=default_args,
    start_date=datetime(2025, 1, 21),
    tags=['example','mssql'],
    template_searchpath="/root/airflow/dags/",
    catchup=False,
)

t1 = BashOperator(
    task_id="bash_example1",
    bash_command="python /root/airflow/dags/scripts/testes04b.py",
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
