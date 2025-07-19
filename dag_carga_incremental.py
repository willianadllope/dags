import sys
import os
import shutil
from time import time
from datetime import timedelta, datetime
from sqlalchemy import create_engine
import pandas as pd

import scripts.config

# The DAG object; we'll need this to instantiate a DAG
from airflow import DAG
from airflow.models.baseoperator import chain

# Operators; we need this to operate!
from airflow.operators.bash import BashOperator
from airflow.operators.dummy import DummyOperator
from airflow.operators.python import BranchPythonOperator
from airflow.utils.trigger_rule import TriggerRule
from airflow.utils.dates import days_ago

from airflow.utils.task_group import TaskGroup
from airflow.decorators import dag, task_group, task
from airflow.operators.trigger_dagrun import TriggerDagRunOperator

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email': ['willian.lopes@systax.com.br'],
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=1),
}

pastas = scripts.config.pastas
prod01sql = scripts.config.prod01sql

## full, incremental ou test (nao executa nada dentro dos scripts)
pastas['tipoCarga'] = 'incremental'

def check_carga_em_execucao():
    engine = create_engine(f"mssql+pymssql://{prod01sql['UID']}:{prod01sql['PWD']}@{prod01sql['SERVER']}:{prod01sql['PORT']}/{prod01sql['DATABASE']}")
    con = engine.connect().execution_options(stream_results=True)
    df = pd.read_sql("SELECT carga from systax_app.snowflake.vw_carga_em_andamento", con)
    carga = ''
    for index,row in df.iterrows():
        carga = row['carga']
    return "inicia_carga_incremental" if carga == 'I' else "skip_execution"

with DAG(
    'carga_incremental',
    #schedule="@daily",
    schedule_interval=None,
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
        trigger_rule=TriggerRule.NONE_FAILED,
    )

    skip_execution = DummyOperator(
        task_id='skip_execution',
    )

    branching = BranchPythonOperator(
        task_id='branching',
        python_callable=check_carga_em_execucao,
    )

    inicia_carga_incremental = BashOperator(
        task_id="inicia_carga_incremental",
        bash_command="python "+dag.params['scripts']+"update_prod01sql.py '"+dag.params['tipoCarga']+"'"+" 0 1",
        #bash_command="echo 'carga_inicial_truncate'",
    )

    carga_incremental = BashOperator(
        task_id="carga_incremental",
        bash_command="python "+dag.params['scripts']+"call_procedure_prod01sql.py 'pr_preparar_atualizacao' '"+dag.params['tipoCarga']+"'",
        #bash_command="echo 'carga_inicial_truncate'",
    )

    limpa_stage = BashOperator(
        task_id="limpa_stage",
        bash_command="python "+dag.params['scripts']+"call_snow_limpa_stage.py '"+dag.params['tipoCarga']+"'",
        #bash_command="echo 'limpa_stage'",
    )   
        
    with TaskGroup(
            group_id="gera_envia_arquivos_parquet",
            ui_color="red", 
            ui_fgcolor="white",
            tooltip="Gera e envia os arquivos parquet das tabelas que participam da criação do Tabelao no Snowflake",
        ) as gera_envia_parquet:
        parquet_clientes = BashOperator(
            task_id="parquet_clientes",
            bash_command="python "+dag.params['scripts']+"parquet_geracao_envio.py clientes "+dag.params['tipoCarga'],
        )
        parquet_usuarios = BashOperator(
            task_id="parquet_usuarios",
            bash_command="python "+dag.params['scripts']+"parquet_geracao_envio.py usuarios "+dag.params['tipoCarga'],
        )        
        parquet_usuario_clientes = BashOperator(
            task_id="parquet_usuario_clientes",
            bash_command="python "+dag.params['scripts']+"parquet_geracao_envio.py usuario_clientes "+dag.params['tipoCarga'],
        )                
        parquet_custom_prod_rel_cigarros = BashOperator(
            task_id="parquet_custom_prod_rel_cigarros",
            bash_command="python "+dag.params['scripts']+"parquet_geracao_envio.py custom_prod_rel_cigarros "+dag.params['tipoCarga'],
        )                        
        parquet_licencas_controle = BashOperator(
            task_id="parquet_licencas_controle",
            bash_command="python "+dag.params['scripts']+"parquet_geracao_envio.py licencas_controle "+dag.params['tipoCarga'],
        )                        
        parquet_grupo_config = BashOperator(
            task_id="parquet_grupo_config",
            bash_command="python "+dag.params['scripts']+"parquet_geracao_envio.py grupo_config "+dag.params['tipoCarga'],
        )
        parquet_ts_diario = BashOperator(
            task_id="parquet_ts_diario",
            bash_command="python "+dag.params['scripts']+"parquet_geracao_envio.py ts_diario "+dag.params['tipoCarga'],
        )
        parquet_agrupamento_produtos = BashOperator(
            task_id="parquet_agrupamento_produtos",
            bash_command="python "+dag.params['scripts']+"parquet_geracao_envio.py agrupamento_produtos "+dag.params['tipoCarga'],
        )
        parquet_cean_relacionado = BashOperator(
            task_id="parquet_cean_relacionado",
            bash_command="python "+dag.params['scripts']+"parquet_geracao_envio.py cean_relacionado "+dag.params['tipoCarga'],
        )
        parquet_ex_origem_cache_familia = BashOperator(
            task_id="parquet_ex_origem_cache_familia",
            bash_command="python "+dag.params['scripts']+"parquet_geracao_envio.py ex_origem_cache_familia "+dag.params['tipoCarga'],
        )
        parquet_tributos_internos_cache_config = BashOperator(
            task_id="parquet_tributos_internos_cache_config",
            bash_command="python "+dag.params['scripts']+"parquet_geracao_envio.py tributos_internos_cache_config "+dag.params['tipoCarga'],
        )
        chain(parquet_clientes, 
              parquet_usuarios,
              parquet_usuario_clientes,
              parquet_licencas_controle,
              parquet_custom_prod_rel_cigarros,
              parquet_grupo_config, 
              parquet_cean_relacionado,
              parquet_agrupamento_produtos, 
              parquet_ex_origem_cache_familia, 
              parquet_ts_diario, 
              parquet_tributos_internos_cache_config 
        )
        
    with TaskGroup(
            group_id="gera_arquivos_parquet_custom_prod",
            ui_color="red", 
            ui_fgcolor="white",
            tooltip="Gera os arquivos parquet das tabelas custom_prod e grupo_custom_prod",
        ) as gera_parquet_custom_prod:
        parquet_custom_prod_001 = BashOperator(
            task_id="parquet_custom_prod_001",
            bash_command="python "+dag.params['scripts']+"parquet_geracao_envio.py custom_prod "+dag.params['tipoCarga']+" 1 10",
        )
        parquet_custom_prod_002 = BashOperator(
            task_id="parquet_custom_prod_002",
            bash_command="python "+dag.params['scripts']+"parquet_geracao_envio.py custom_prod "+dag.params['tipoCarga']+" 11 20",
        )
        parquet_custom_prod_003 = BashOperator(
            task_id="parquet_custom_prod_003",
            bash_command="python "+dag.params['scripts']+"parquet_geracao_envio.py custom_prod "+dag.params['tipoCarga']+" 21 30",
        )
        parquet_custom_prod_004 = BashOperator(
            task_id="parquet_custom_prod_004",
            bash_command="python "+dag.params['scripts']+"parquet_geracao_envio.py custom_prod "+dag.params['tipoCarga']+" 31 40",
        )
        parquet_custom_prod_005 = BashOperator(
            task_id="parquet_custom_prod_005",
            bash_command="python "+dag.params['scripts']+"parquet_geracao_envio.py custom_prod "+dag.params['tipoCarga']+" 41 50",
        )
        parquet_custom_prod_figuras_fiscais_001 = BashOperator(
            task_id="parquet_custom_prod_figuras_fiscais_001",
            bash_command="python "+dag.params['scripts']+"parquet_geracao_envio.py custom_prod_figuras_fiscais "+dag.params['tipoCarga']+" 1 10",
        )
        parquet_custom_prod_figuras_fiscais_002 = BashOperator(
            task_id="parquet_custom_prod_figuras_fiscais_002",
            bash_command="python "+dag.params['scripts']+"parquet_geracao_envio.py custom_prod_figuras_fiscais "+dag.params['tipoCarga']+" 11 20",
        )
        parquet_custom_prod_figuras_fiscais_003 = BashOperator(
            task_id="parquet_custom_prod_figuras_fiscais_003",
            bash_command="python "+dag.params['scripts']+"parquet_geracao_envio.py custom_prod_figuras_fiscais "+dag.params['tipoCarga']+" 21 30",
        )
        parquet_custom_prod_figuras_fiscais_004 = BashOperator(
            task_id="parquet_custom_prod_figuras_fiscais_004",
            bash_command="python "+dag.params['scripts']+"parquet_geracao_envio.py custom_prod_figuras_fiscais "+dag.params['tipoCarga']+" 31 40",
        )
        parquet_custom_prod_figuras_fiscais_005 = BashOperator(
            task_id="parquet_custom_prod_figuras_fiscais_005",
            bash_command="python "+dag.params['scripts']+"parquet_geracao_envio.py custom_prod_figuras_fiscais "+dag.params['tipoCarga']+" 41 50",
        )
        parquet_grupo_custom_prod_001 = BashOperator(
            task_id="parquet_grupo_custom_prod_001",
            bash_command="python "+dag.params['scripts']+"parquet_geracao_envio.py grupo_custom_prod "+dag.params['tipoCarga']+" 1 10",
        )
        parquet_grupo_custom_prod_002 = BashOperator(
            task_id="parquet_grupo_custom_prod_002",
            bash_command="python "+dag.params['scripts']+"parquet_geracao_envio.py grupo_custom_prod "+dag.params['tipoCarga']+" 11 20",
        )
        parquet_grupo_custom_prod_003 = BashOperator(
            task_id="parquet_grupo_custom_prod_003",
            bash_command="python "+dag.params['scripts']+"parquet_geracao_envio.py grupo_custom_prod "+dag.params['tipoCarga']+" 21 30",
        )
        parquet_grupo_custom_prod_004 = BashOperator(
            task_id="parquet_grupo_custom_prod_004",
            bash_command="python "+dag.params['scripts']+"parquet_geracao_envio.py grupo_custom_prod "+dag.params['tipoCarga']+" 31 40",
        )
        parquet_grupo_custom_prod_005 = BashOperator(
            task_id="parquet_grupo_custom_prod_005",
            bash_command="python "+dag.params['scripts']+"parquet_geracao_envio.py grupo_custom_prod "+dag.params['tipoCarga']+" 41 50",
        )
        chain(
            [ 
                parquet_custom_prod_001,
                parquet_custom_prod_002, 
                parquet_custom_prod_003, 
                parquet_custom_prod_004, 
                parquet_custom_prod_005
            ],
            [ 
                parquet_custom_prod_figuras_fiscais_001,
                parquet_custom_prod_figuras_fiscais_002, 
                parquet_custom_prod_figuras_fiscais_003, 
                parquet_custom_prod_figuras_fiscais_004, 
                parquet_custom_prod_figuras_fiscais_005
            ],
            [
                parquet_grupo_custom_prod_001, 
                parquet_grupo_custom_prod_002, 
                parquet_grupo_custom_prod_003, 
                parquet_grupo_custom_prod_004, 
                parquet_grupo_custom_prod_005
            ]
        )

    with TaskGroup(
            group_id="gera_arquivos_parquet_cache",
            ui_color="red", 
            ui_fgcolor="white",
            tooltip="Gera os arquivos parquet da tabela de tributos_internos_cache e cache_st",
        ) as gera_parquet_caches:
        parquet_tributos_internos_cache_001 = BashOperator(
            task_id="parquet_tributos_internos_cache_001",
            bash_command="python "+dag.params['scripts']+"parquet_geracao_envio.py tributos_internos_cache "+dag.params['tipoCarga']+" 1 100",
        )
        parquet_tributos_internos_cache_101 = BashOperator(
            task_id="parquet_tributos_internos_cache_101",
            bash_command="python "+dag.params['scripts']+"parquet_geracao_envio.py tributos_internos_cache "+dag.params['tipoCarga']+" 101 200",
        )
        parquet_tributos_internos_cache_201 = BashOperator(
            task_id="parquet_tributos_internos_cache_201",
            bash_command="python "+dag.params['scripts']+"parquet_geracao_envio.py tributos_internos_cache "+dag.params['tipoCarga']+" 201 300",
        )
        parquet_tributos_internos_cache_301 = BashOperator(
            task_id="parquet_tributos_internos_cache_301",
            bash_command="python "+dag.params['scripts']+"parquet_geracao_envio.py tributos_internos_cache "+dag.params['tipoCarga']+" 301 400",
        )
        parquet_tributos_internos_cache_401 = BashOperator(
            task_id="parquet_tributos_internos_cache_401",
            bash_command="python "+dag.params['scripts']+"parquet_geracao_envio.py tributos_internos_cache "+dag.params['tipoCarga']+" 401 500",
        )
        parquet_tributos_internos_cache_st_01 = BashOperator(
            task_id="parquet_tributos_internos_cache_st_01",
            bash_command="python "+dag.params['scripts']+"parquet_geracao_envio.py tributos_internos_cache_st "+dag.params['tipoCarga']+" 1 10",
        )
        parquet_tributos_internos_cache_st_11 = BashOperator(
            task_id="parquet_tributos_internos_cache_st_11",
            bash_command="python "+dag.params['scripts']+"parquet_geracao_envio.py tributos_internos_cache_st "+dag.params['tipoCarga']+" 11 20",
        )
        parquet_tributos_internos_cache_st_21 = BashOperator(
            task_id="parquet_tributos_internos_cache_st_21",
            bash_command="python "+dag.params['scripts']+"parquet_geracao_envio.py tributos_internos_cache_st "+dag.params['tipoCarga']+" 21 30",
        )
        parquet_tributos_internos_cache_st_31 = BashOperator(
            task_id="parquet_tributos_internos_cache_st_31",
            bash_command="python "+dag.params['scripts']+"parquet_geracao_envio.py tributos_internos_cache_st "+dag.params['tipoCarga']+" 31 40",
        )
        parquet_tributos_internos_cache_st_41 = BashOperator(
            task_id="parquet_tributos_internos_cache_st_41",
            bash_command="python "+dag.params['scripts']+"parquet_geracao_envio.py tributos_internos_cache_st "+dag.params['tipoCarga']+" 41 50",
        )
        chain(
            [ 
                parquet_tributos_internos_cache_st_01,
                parquet_tributos_internos_cache_st_11, 
                parquet_tributos_internos_cache_st_21, 
                parquet_tributos_internos_cache_st_31, 
                parquet_tributos_internos_cache_st_41
            ],
            [
                parquet_tributos_internos_cache_001, 
                parquet_tributos_internos_cache_101, 
                parquet_tributos_internos_cache_201, 
                parquet_tributos_internos_cache_301, 
                parquet_tributos_internos_cache_401
            ]
        )

    with TaskGroup(
            group_id="gera_deletados_parquet",
            ui_color="red", 
            ui_fgcolor="white",
            tooltip="Atualiza no snowflake os registros apagados na prod01sql",
        ) as gera_deletados_parquet:
        apagar_clientes = BashOperator(
            task_id="apagar_clientes",
            bash_command="python "+dag.params['scripts']+"parquet_geracao_envio.py apagar_clientes "+dag.params['tipoCarga'],
            #bash_command="echo 'parquet_clientes'",
        )
        apagar_grupo_config = BashOperator(
            task_id="apagar_grupo_config",
            bash_command="python "+dag.params['scripts']+"parquet_geracao_envio.py apagar_grupo_config "+dag.params['tipoCarga'],
            #bash_command="echo 'parquet_grupo_config'",
        )
        apagar_custom_prod = BashOperator(
            task_id="apagar_custom_prod",
            bash_command="python "+dag.params['scripts']+"parquet_geracao_envio.py apagar_custom_prod "+dag.params['tipoCarga'],
            #bash_command="echo 'apagar_custom_prod'",
        )
        apagar_custom_prod_figuras_fiscais = BashOperator(
            task_id="apagar_custom_prod_figuras_fiscais",
            bash_command="python "+dag.params['scripts']+"parquet_geracao_envio.py apagar_custom_prod_figuras_fiscais "+dag.params['tipoCarga'],
            #bash_command="echo 'apagar_custom_prod'",
        )
        apagar_grupo_custom_prod = BashOperator(
            task_id="apagar_grupo_custom_prod",
            bash_command="python "+dag.params['scripts']+"parquet_geracao_envio.py apagar_grupo_custom_prod "+dag.params['tipoCarga'],
            #bash_command="echo 'apagar_grupo_custom_prod'",
        )
        apagar_cean_relacionado = BashOperator(
            task_id="apagar_cean_relacionado",
            bash_command="python "+dag.params['scripts']+"parquet_geracao_envio.py apagar_cean_relacionado "+dag.params['tipoCarga'],
            #bash_command="echo 'apagar_cean_relacionado'",
        )
        apagar_tributos_internos_cache_config = BashOperator(
            task_id="apagar_tributos_internos_cache_config",
            bash_command="python "+dag.params['scripts']+"parquet_geracao_envio.py apagar_tributos_internos_cache_config "+dag.params['tipoCarga'],
            #bash_command="echo 'apagar_tributos_internos_cache_config'",
        )
        apagar_tributos_internos_cache = BashOperator(
            task_id="apagar_tributos_internos_cache",
            bash_command="python "+dag.params['scripts']+"parquet_geracao_envio.py apagar_tributos_internos_cache "+dag.params['tipoCarga'],
            #bash_command="echo 'apagar_tributos_internos_cache'",
        )
        apagar_tributos_internos_cache_st = BashOperator(
            task_id="apagar_tributos_internos_cache_st",
            bash_command="python "+dag.params['scripts']+"parquet_geracao_envio.py apagar_tributos_internos_cache_st "+dag.params['tipoCarga'],
            #bash_command="echo 'apagar_tributos_internos_cache_st'",
        )
        chain(apagar_clientes, 
              apagar_grupo_config, 
              apagar_custom_prod,
              apagar_grupo_custom_prod, 
              apagar_cean_relacionado, 
              apagar_tributos_internos_cache_config, 
              apagar_tributos_internos_cache,
              apagar_tributos_internos_cache_st 
        )

    with TaskGroup(
            group_id="envia_arquivos_parquet_caches_cp_gcp",
            ui_color="red", 
            ui_fgcolor="white",
            tooltip="Envia os arquivos parquet da tabela de tributos_internos_cache e cache_st",
        ) as envia_parquet_caches_cp_gcp:
        envia_parquet_tributos_internos_cache = BashOperator(
            task_id="envia_parquet_tributos_internos_cache",
            bash_command="python "+dag.params['scripts']+"upload_snowflake.py tributos_internos_cache "+dag.params['tipoCarga'],
        )
        envia_parquet_tributos_internos_cache_st = BashOperator(
            task_id="envia_parquet_tributos_internos_cache_st",
            bash_command="python "+dag.params['scripts']+"upload_snowflake.py tributos_internos_cache_st "+dag.params['tipoCarga'],
        )
        envia_parquet_custom_prod_figuras_fiscais = BashOperator(
            task_id="envia_parquet_custom_prod_figuras_fiscais",
            bash_command="python "+dag.params['scripts']+"upload_snowflake.py custom_prod_figuras_fiscais "+dag.params['tipoCarga'],
        ) 
        envia_parquet_custom_prod = BashOperator(
            task_id="envia_parquet_custom_prod",
            bash_command="python "+dag.params['scripts']+"upload_snowflake.py custom_prod "+dag.params['tipoCarga'],
        )
        envia_parquet_grupo_custom_prod = BashOperator(
            task_id="envia_parquet_grupo_custom_prod",
            bash_command="python "+dag.params['scripts']+"upload_snowflake.py grupo_custom_prod "+dag.params['tipoCarga'],
        )
        chain(
            [ 
                envia_parquet_custom_prod,
                envia_parquet_custom_prod_figuras_fiscais,
                envia_parquet_grupo_custom_prod,
                envia_parquet_tributos_internos_cache,
                envia_parquet_tributos_internos_cache_st
            ]
        )

    task_carga_snowflake = BashOperator(
        task_id="task_carga_snowflake",
        bash_command="python "+dag.params['scripts']+"exec_snow_task.py "+dag.params['tipoCarga']+" TASK_CARGA_INCREMENTAL TASK_CARGA",
    )   

    task_gera_tabelao = BashOperator(
        task_id="task_gera_tabelao",
        bash_command="python "+dag.params['scripts']+"exec_snow_task.py DBO TASK_GERA_TABELAO TASK_GERA_TABELAO",
    )   

    task_tabelao_apaga_indevidos = BashOperator(
        task_id="task_tabelao_apaga_indevidos",
        bash_command="python "+dag.params['scripts']+"exec_snow_task.py DBO TASK_TABELAO_APAGA_INDEVIDOS TASK_TABELAO_APAGA_INDEVIDOS",
    )   

    apaga_csv_s3_tabelao = BashOperator(
        task_id="apaga_csv_s3_tabelao",
        bash_command="python "+dag.params['scripts']+"call_procedure_prod01sql.py 'pr_apaga_csv_s3_tabelao' '"+dag.params['tipoCarga']+"'",
    )

    envia_tabelao_s3 = BashOperator(
        task_id="envia_tabelao_s3",
        bash_command="python "+dag.params['scripts']+"call_snow_procedure.py dbo pr_envia_tabelao_s3",
    )   
 
    download_csvs_tabelao = BashOperator(
        task_id="download_csvs_tabelao",
        bash_command="python "+dag.params['scripts']+"call_procedure_prod01sql.py 'pr_download_csvs_tabelao' '"+dag.params['tipoCarga']+"'",
        #bash_command="echo 'download_csvs_tabelao'",
    )

    carrega_csv_tabelao_prod01sql = BashOperator(
        task_id="carrega_csv_tabelao_prod01sql",
        bash_command="python "+dag.params['scripts']+"call_procedure_prod01sql.py 'pr_carrega_tabelao' '"+dag.params['tipoCarga']+"'",
        #bash_command="echo 'carrega_csv_tabelao_prod01sql'",
    )

    acionar_dag_generation_csv_to_rds = TriggerDagRunOperator(
        task_id="acionar_dag_generation_csv_to_rds",
        trigger_dag_id="dag_generation_csv_to_rds",  # ID da DAG a ser acionada
        wait_for_completion=True,  # Se True, espera a DAG terminar
        reset_dag_run=True,         # Se True, reinicia uma execução se já existir
    )

    acionar_dag_send_tabelao_prod01 = TriggerDagRunOperator(
        task_id="acionar_dag_send_tabelao_prod01",
        trigger_dag_id="dag_send_tabelao_prod01",  # ID da DAG a ser acionada
        wait_for_completion=True,  # Se True, espera a DAG terminar
        reset_dag_run=True,         # Se True, reinicia uma execução se já existir
    )

    chain(
        start_task, 
        branching,
        inicia_carga_incremental,        
        carga_incremental, 
        limpa_stage, 
        gera_envia_parquet, 
        gera_deletados_parquet, 
        gera_parquet_custom_prod,
        gera_parquet_caches,
        envia_parquet_caches_cp_gcp, 
        task_carga_snowflake, 
        task_gera_tabelao, 
        task_tabelao_apaga_indevidos, 
        apaga_csv_s3_tabelao,
        envia_tabelao_s3,
        download_csvs_tabelao,
        carrega_csv_tabelao_prod01sql,
        acionar_dag_send_tabelao_prod01,
        acionar_dag_generation_csv_to_rds,
        end_task
    )

    chain(
        start_task, 
        branching,
        skip_execution,
        end_task
    )    
### teste de sobe um restore do DBCarrefourAtualizacao
### mudanca para o DBControle do 379 e 380
### change 263 para change normal 
