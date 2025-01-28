from datetime import timedelta, datetime
import scripts.config

# The DAG object; we'll need this to instantiate a DAG
from airflow import DAG
from airflow.models.baseoperator import chain

# Operators; we need this to operate!
from airflow.operators.bash import BashOperator
from airflow.operators.dummy import DummyOperator
from airflow.utils.dates import days_ago

from airflow.utils.task_group import TaskGroup
from airflow.decorators import dag, task_group, task

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email': ['willian.lopes@systax.com.br'],
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=1),
}

pastas = scripts.config.pastas;

with DAG(
    'carga_inicial_full',
    schedule="@daily",
    default_args=default_args,
    start_date=datetime(2025, 1, 21),
    tags=['tabelaoprod01'],
    params=pastas,
    concurrency=3,
    catchup=False,
) as dag:
    start_task = DummyOperator(
        task_id='start',
    )

    end_task = DummyOperator(
        task_id='end',
    )

    with TaskGroup(
            group_id="carrega_id_tabelas",
            ui_color="blue", 
            ui_fgcolor="green",
            tooltip="Carrega as tabelas de controle de ID do que sera enviado para o Snowflake",
        ) as carrega_ids:
        carga_inicial_truncate = BashOperator(
            task_id="carga_inicial_truncate",
            bash_command="python "+dag.params['scripts']+"call_procedure_carga_inicial.py 'pr_preparar_carga_inicial_truncate' 'full'",
        )
        carga_custom_prod = BashOperator(
            task_id="carga_custom_prod",
            bash_command="python /root/airflow/dags/scripts/call_procedure_carga_inicial.py 'pr_preparar_carga_custom_prod' 'full'",
        )
        carga_cean_relacionado = BashOperator(
            task_id="carga_cean_relacionado",
            bash_command="python /root/airflow/dags/scripts/call_procedure_carga_inicial.py 'pr_preparar_carga_cean_relacionado' 'full'",
        )
        carga_grupo = BashOperator(
            task_id="carga_grupo",
            bash_command="python /root/airflow/dags/scripts/call_procedure_carga_inicial.py 'pr_preparar_carga_grupo' 'full'",
        )
        carga_grupo_custom_prod = BashOperator(
            task_id="carga_grupo_custom_prod",
            bash_command="python /root/airflow/dags/scripts/call_procedure_carga_inicial.py 'pr_preparar_carga_grupo_custom_prod' 'full'",
        )
        carga_grupo_config = BashOperator(
            task_id="carga_grupo_config",
            bash_command="python /root/airflow/dags/scripts/call_procedure_carga_inicial.py 'pr_preparar_carga_grupo_config' 'full'",
        )
        carga_clientes = BashOperator(
            task_id="carga_clientes",
            bash_command="python /root/airflow/dags/scripts/call_procedure_carga_inicial.py 'pr_preparar_carga_clientes' 'full'",
        )
        carga_tributos_internos_cache_st = BashOperator(
            task_id="carga_tributos_internos_cache_st",
            bash_command="python /root/airflow/dags/scripts/call_procedure_carga_inicial.py 'pr_preparar_carga_tributos_internos_cache_st' 'full'",
        )
        carga_tributos_internos_cache = BashOperator(
            task_id="carga_tributos_internos_cache",
            bash_command="python /root/airflow/dags/scripts/call_procedure_carga_inicial.py 'pr_preparar_carga_tributos_internos_cache' 'full'",
        )
        carga_tributos_internos_cache_config = BashOperator(
            task_id="carga_tributos_internos_cache_config",
            bash_command="python /root/airflow/dags/scripts/call_procedure_carga_inicial.py 'pr_preparar_carga_tributos_internos_cache_config' 'full'",
        )
        carga_envio = BashOperator(
            task_id="carga_envio",
            bash_command="python /root/airflow/dags/scripts/call_procedure_carga_inicial.py 'pr_preparar_carga_envio' 'full'",
        )
        carga_schemafull = BashOperator(
            task_id="carga_schemafull",
            bash_command="python /root/airflow/dags/scripts/call_procedure_carga_inicial.py 'pr_preparar_carga_schemafull' 'full'",
        )
        chain(carga_inicial_truncate, [
            carga_custom_prod,
            carga_cean_relacionado,
            carga_grupo,
            carga_grupo_config,
            carga_clientes,
            carga_tributos_internos_cache_config,
            carga_grupo_custom_prod,
            carga_tributos_internos_cache_st,
            carga_tributos_internos_cache
            ], carga_envio, carga_schemafull )
    limpa_stage = BashOperator(
        task_id="limpa_stage",
        bash_command="python /root/airflow/dags/scripts/call_snow_limpa_stage.py 'full'",
    )   
    chain(start_task, carrega_ids, limpa_stage, end_task)
### teste de sobe um restore do DBCarrefourAtualizacao
### mudanca para o DBControle do 379 e 380
### change 263 para change normal 
