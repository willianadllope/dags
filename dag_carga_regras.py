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
from airflow.operators.empty import EmptyOperator
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

configs = scripts.config.configs

## full || incremental || test(nao executa nada dentro dos scripts)
configs['tipoCarga'] = 'FULL'

prod01sql = scripts.config.prod01sql


def check_carga_em_execucao():
    engine = create_engine(f"mssql+pymssql://{prod01sql['UID']}:{prod01sql['PWD']}@{prod01sql['SERVER']}:{prod01sql['PORT']}/{prod01sql['DATABASE']}")
    con = engine.connect().execution_options(stream_results=True)
    df = pd.read_sql("SELECT carga from systax_app.snowflake.vw_carga_em_andamento", con)
    carga = ''
    for index,row in df.iterrows():
        carga = row['carga'].upper()
    print("Carga:",carga)
    return "group_carrega_carga_full" if carga == 'F' else "carrega_carga"

with DAG(
    'carga_regras',
    #schedule="@daily",
    schedule=None,
    default_args=default_args,
    start_date=datetime(2025, 1, 21),
    tags=['cargacsv'],
    params=configs,
    max_active_tasks=3,
    catchup=False,
) as dag:
    
    start_task = EmptyOperator(
        task_id='start',
    )

    end_task = EmptyOperator(
        task_id='end',
        trigger_rule=TriggerRule.NONE_FAILED,
    )

    skip_execution = EmptyOperator(
        task_id='skip_execution',
    )

    #branching = BranchPythonOperator(
    #    task_id='branching',
    #    python_callable=check_carga_em_execucao,
    #)

    inicia_carga = BashOperator(
        task_id="inicia_carga",
        bash_command="python "+dag.params['scripts']['task_inicia_carga']+" '"+dag.params['tipoCarga']+"'"+" 0 1",
    )

    ## chamar somente no incremental
    carrega_carga = BashOperator(
        task_id="carrega_carga",
        bash_command="python "+dag.params['scripts']['task_carrega_carga']+" 'pr_preparar_atualizacao' '"+dag.params['tipoCarga']+"'",
    )

    limpa_stage = BashOperator(
        task_id="limpa_stage",
        bash_command="python "+dag.params['scripts']['task_limpa_stage']+" '"+dag.params['tipoCarga']+"'",
    )   

    with TaskGroup(
            group_id="task_group_chunks",
            ui_color="blue", 
            ui_fgcolor="green",
            tooltip="Carrega os chunks das tabelas de controle de ID do que sera enviado para o Snowflake",
        ) as group_chunks:
        chunk_clientes = BashOperator(
            task_id="chunk_clientes",
            bash_command="python "+dag.params['scripts']['task_group_chunks']+" '"+dag.params['tipoCarga']+"' 'clientes'",
        )
        chunk_grupo = BashOperator(
            task_id="chunk_grupo",
            bash_command="python "+dag.params['scripts']['task_group_chunks']+" '"+dag.params['tipoCarga']+"' 'grupo'",
        )
        chunk_grupo_config = BashOperator(
            task_id="chunk_grupo_config",
            bash_command="python "+dag.params['scripts']['task_group_chunks']+" '"+dag.params['tipoCarga']+"' 'grupo_config'",
        )
        chunk_tributos_internos_cache_config = BashOperator(
            task_id="chunk_tributos_internos_cache_config",
            bash_command="python "+dag.params['scripts']['task_group_chunks']+" '"+dag.params['tipoCarga']+"' 'tributos_internos_cache_config'",
        )
        chunk_cean_relacionado = BashOperator(
            task_id="chunk_cean_relacionado",
            bash_command="python "+dag.params['scripts']['task_group_chunks']+" '"+dag.params['tipoCarga']+"' 'cean_relacionado'",
        )
        chunk_custom_prod = BashOperator(
            task_id="chunk_custom_prod",
            bash_command="python "+dag.params['scripts']['task_group_chunks']+" '"+dag.params['tipoCarga']+"' 'custom_prod'",
        )
        chunk_grupo_custom_prod = BashOperator(
            task_id="chunk_grupo_custom_prod",
            bash_command="python "+dag.params['scripts']['task_group_chunks']+" '"+dag.params['tipoCarga']+"' 'grupo_custom_prod'",
        )
        chunk_tributos_internos_cache_st = BashOperator(
            task_id="chunk_tributos_internos_cache_st",
            bash_command="python "+dag.params['scripts']['task_group_chunks']+" '"+dag.params['tipoCarga']+"' 'tributos_internos_cache_st'",
        )
        chunk_tributos_internos_cache = BashOperator(
            task_id="chunk_tributos_internos_cache",
            bash_command="python "+dag.params['scripts']['task_group_chunks']+" '"+dag.params['tipoCarga']+"' 'tributos_internos_cache'",
        )
        chunk_usuarios = BashOperator(
            task_id="chunk_usuarios",
            bash_command="python "+dag.params['scripts']['task_group_chunks']+" '"+dag.params['tipoCarga']+"' 'usuarios'",
        )
        chunk_usuario_clientes = BashOperator(
            task_id="chunk_usuario_clientes",
            bash_command="python "+dag.params['scripts']['task_group_chunks']+" '"+dag.params['tipoCarga']+"' 'usuario_clientes'",
        )
        chunk_licencas_controle = BashOperator(
            task_id="chunk_licencas_controle",
            bash_command="python "+dag.params['scripts']['task_group_chunks']+" '"+dag.params['tipoCarga']+"' 'licencas_controle'",
        )
        chunk_custom_prod_rel_cigarros = BashOperator(
            task_id="chunk_custom_prod_rel_cigarros",
            bash_command="python "+dag.params['scripts']['task_group_chunks']+" '"+dag.params['tipoCarga']+"' 'custom_prod_rel_cigarros'",
        )        
        chunk_custom_prod_figuras_fiscais = BashOperator(
            task_id="chunk_custom_prod_figuras_fiscais",
            bash_command="python "+dag.params['scripts']['task_group_chunks']+" '"+dag.params['tipoCarga']+"' 'custom_prod_figuras_fiscais'",
        )                
        chain(
            chunk_clientes,
            chunk_grupo,
            chunk_grupo_config,
            chunk_cean_relacionado,
            chunk_tributos_internos_cache_config,            
            chunk_tributos_internos_cache_st,
            chunk_custom_prod,
            chunk_custom_prod_rel_cigarros,
            chunk_custom_prod_figuras_fiscais,
            chunk_usuarios,
            chunk_usuario_clientes,
            chunk_licencas_controle,
            chunk_grupo_custom_prod,
            chunk_tributos_internos_cache
        )

    with TaskGroup(
            group_id="group_carrega_carga_full",
            ui_color="blue", 
            ui_fgcolor="green",
            tooltip="Carrega as tabelas de controle de ID do que sera enviado para o Snowflake",
        ) as group_carrega_carga_full:
        carga_inicial_truncate = BashOperator(
            task_id="carga_inicial_truncate",
            bash_command="python "+dag.params['scripts']['task_group_carrega_carga_full']+" 'pr_preparar_carga_inicial_truncate' '"+dag.params['tipoCarga']+"'",
        )
        carga_custom_prod = BashOperator(
            task_id="carga_custom_prod",
            bash_command="python "+dag.params['scripts']['task_group_carrega_carga_full']+" 'pr_preparar_carga_custom_prod' '"+dag.params['tipoCarga']+"'",
        )
        carga_custom_prod_rel_cigarros = BashOperator(
            task_id="carga_custom_prod_rel_cigarros",
            bash_command="python "+dag.params['scripts']['task_group_carrega_carga_full']+" 'pr_preparar_carga_custom_prod_rel_cigarros' '"+dag.params['tipoCarga']+"'",
        )
        carga_custom_prod_figuras_fiscais = BashOperator(
            task_id="carga_custom_prod_figuras_fiscais",
            bash_command="python "+dag.params['scripts']['task_group_carrega_carga_full']+" 'pr_preparar_carga_custom_prod_figuras_fiscais' '"+dag.params['tipoCarga']+"'",
        )
        carga_cean_relacionado = BashOperator(
            task_id="carga_cean_relacionado",
            bash_command="python "+dag.params['scripts']['task_group_carrega_carga_full']+" 'pr_preparar_carga_cean_relacionado' '"+dag.params['tipoCarga']+"'",
        )
        carga_grupo = BashOperator(
            task_id="carga_grupo",
            bash_command="python "+dag.params['scripts']['task_group_carrega_carga_full']+" 'pr_preparar_carga_grupo' '"+dag.params['tipoCarga']+"'",
        )
        carga_grupo_custom_prod = BashOperator(
            task_id="carga_grupo_custom_prod",
            bash_command="python "+dag.params['scripts']['task_group_carrega_carga_full']+" 'pr_preparar_carga_grupo_custom_prod' '"+dag.params['tipoCarga']+"'",
        )
        carga_grupo_config = BashOperator(
            task_id="carga_grupo_config",
            bash_command="python "+dag.params['scripts']['task_group_carrega_carga_full']+" 'pr_preparar_carga_grupo_config' '"+dag.params['tipoCarga']+"'",
        )
        carga_clientes = BashOperator(
            task_id="carga_clientes",
            bash_command="python "+dag.params['scripts']['task_group_carrega_carga_full']+" 'pr_preparar_carga_clientes' '"+dag.params['tipoCarga']+"'",
        )
        carga_usuarios = BashOperator(
            task_id="carga_usuarios",
            bash_command="python "+dag.params['scripts']['task_group_carrega_carga_full']+" 'pr_preparar_carga_usuarios' '"+dag.params['tipoCarga']+"'",
        )        
        carga_usuario_clientes = BashOperator(
            task_id="carga_usuario_clientes",
            bash_command="python "+dag.params['scripts']['task_group_carrega_carga_full']+" 'pr_preparar_carga_usuario_clientes' '"+dag.params['tipoCarga']+"'",
        )                
        carga_licencas_controle = BashOperator(
            task_id="carga_licencas_controle",
            bash_command="python "+dag.params['scripts']['task_group_carrega_carga_full']+" 'pr_preparar_carga_licencas_controle' '"+dag.params['tipoCarga']+"'",
        )        
        carga_tributos_internos_cache_st = BashOperator(
            task_id="carga_tributos_internos_cache_st",
            bash_command="python "+dag.params['scripts']['task_group_carrega_carga_full']+" 'pr_preparar_carga_tributos_internos_cache_st' '"+dag.params['tipoCarga']+"'",
        )
        carga_tributos_internos_cache = BashOperator(
            task_id="carga_tributos_internos_cache",
            bash_command="python "+dag.params['scripts']['task_group_carrega_carga_full']+" 'pr_preparar_carga_tributos_internos_cache' '"+dag.params['tipoCarga']+"'",
        )
        carga_tributos_internos_cache_config = BashOperator(
            task_id="carga_tributos_internos_cache_config",
            bash_command="python "+dag.params['scripts']['task_group_carrega_carga_full']+" 'pr_preparar_carga_tributos_internos_cache_config' '"+dag.params['tipoCarga']+"'",
        )
        carga_schemafull = BashOperator(
            task_id="carga_schemafull",
            bash_command="python "+dag.params['scripts']['task_group_carrega_carga_full']+" 'pr_preparar_carga_schemafull' '"+dag.params['tipoCarga']+"'",
        )
        chain(carga_inicial_truncate, [
            carga_custom_prod,
            carga_cean_relacionado,
            carga_grupo,
            carga_grupo_config,
            carga_clientes,
            carga_usuarios,
            carga_usuario_clientes,
            carga_custom_prod_figuras_fiscais,
            carga_custom_prod_rel_cigarros,
            carga_licencas_controle,
            carga_tributos_internos_cache_config,
            carga_grupo_custom_prod,
            carga_tributos_internos_cache_st,
            carga_tributos_internos_cache
            ], group_chunks, carga_schemafull )
    
    with TaskGroup(
            group_id="task_group_geraenvia_arquivos_parquet",
            ui_color="red", 
            ui_fgcolor="white",
            tooltip="Gera e envia os arquivos parquet das tabelas que participam da criação do Tabelao no Snowflake",
        ) as group_gera_envia_parquet:
        parquet_clientes = BashOperator(
            task_id="parquet_clientes",
            bash_command="python "+dag.params['scripts']['task_group_gera_envia_parquet']+" clientes "+dag.params['tipoCarga'],
        )
        parquet_usuarios = BashOperator(
            task_id="parquet_usuarios",
            bash_command="python "+dag.params['scripts']['task_group_gera_envia_parquet']+" usuarios "+dag.params['tipoCarga'],
        )        
        parquet_usuario_clientes = BashOperator(
            task_id="parquet_usuario_clientes",
            bash_command="python "+dag.params['scripts']['task_group_gera_envia_parquet']+" usuario_clientes "+dag.params['tipoCarga'],
        )                
        parquet_custom_prod_rel_cigarros = BashOperator(
            task_id="parquet_custom_prod_rel_cigarros",
            bash_command="python "+dag.params['scripts']['task_group_gera_envia_parquet']+" custom_prod_rel_cigarros "+dag.params['tipoCarga'],
        )                        
        parquet_licencas_controle = BashOperator(
            task_id="parquet_licencas_controle",
            bash_command="python "+dag.params['scripts']['task_group_gera_envia_parquet']+" licencas_controle "+dag.params['tipoCarga'],
        )                        
        parquet_grupo_config = BashOperator(
            task_id="parquet_grupo_config",
            bash_command="python "+dag.params['scripts']['task_group_gera_envia_parquet']+" grupo_config "+dag.params['tipoCarga'],
        )
        parquet_ts_diario = BashOperator(
            task_id="parquet_ts_diario",
            bash_command="python "+dag.params['scripts']['task_group_gera_envia_parquet']+" ts_diario "+dag.params['tipoCarga'],
        )
        parquet_agrupamento_produtos = BashOperator(
            task_id="parquet_agrupamento_produtos",
            bash_command="python "+dag.params['scripts']['task_group_gera_envia_parquet']+" agrupamento_produtos "+dag.params['tipoCarga'],
        )
        parquet_cean_relacionado = BashOperator(
            task_id="parquet_cean_relacionado",
            bash_command="python "+dag.params['scripts']['task_group_gera_envia_parquet']+" cean_relacionado "+dag.params['tipoCarga'],
        )
        parquet_ex_origem_cache_familia = BashOperator(
            task_id="parquet_ex_origem_cache_familia",
            bash_command="python "+dag.params['scripts']['task_group_gera_envia_parquet']+" ex_origem_cache_familia "+dag.params['tipoCarga'],
        )
        parquet_tributos_internos_cache_config = BashOperator(
            task_id="parquet_tributos_internos_cache_config",
            bash_command="python "+dag.params['scripts']['task_group_gera_envia_parquet']+" tributos_internos_cache_config "+dag.params['tipoCarga'],
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
            group_id="task_group_gera_arquivos_parquet_custom_prod",
            ui_color="red", 
            ui_fgcolor="white",
            tooltip="Gera os arquivos parquet das tabelas custom_prod e grupo_custom_prod",
        ) as group_gera_parquet:
        parquet_custom_prod_001 = BashOperator(
            task_id="parquet_custom_prod_001",
            bash_command="python "+dag.params['scripts']['task_group_gera_parquet']+" custom_prod "+dag.params['tipoCarga']+" 1 10",
        )
        parquet_custom_prod_002 = BashOperator(
            task_id="parquet_custom_prod_002",
            bash_command="python "+dag.params['scripts']['task_group_gera_parquet']+" custom_prod "+dag.params['tipoCarga']+" 11 20",
        )
        parquet_custom_prod_003 = BashOperator(
            task_id="parquet_custom_prod_003",
            bash_command="python "+dag.params['scripts']['task_group_gera_parquet']+" custom_prod "+dag.params['tipoCarga']+" 21 30",
        )
        parquet_custom_prod_004 = BashOperator(
            task_id="parquet_custom_prod_004",
            bash_command="python "+dag.params['scripts']['task_group_gera_parquet']+" custom_prod "+dag.params['tipoCarga']+" 31 40",
        )
        parquet_custom_prod_005 = BashOperator(
            task_id="parquet_custom_prod_005",
            bash_command="python "+dag.params['scripts']['task_group_gera_parquet']+" custom_prod "+dag.params['tipoCarga']+" 41 50",
        )
        parquet_custom_prod_figuras_fiscais_001 = BashOperator(
            task_id="parquet_custom_prod_figuras_fiscais_001",
            bash_command="python "+dag.params['scripts']['task_group_gera_parquet']+" custom_prod_figuras_fiscais "+dag.params['tipoCarga']+" 1 10",
        )
        parquet_custom_prod_figuras_fiscais_002 = BashOperator(
            task_id="parquet_custom_prod_figuras_fiscais_002",
            bash_command="python "+dag.params['scripts']['task_group_gera_parquet']+" custom_prod_figuras_fiscais "+dag.params['tipoCarga']+" 11 20",
        )
        parquet_custom_prod_figuras_fiscais_003 = BashOperator(
            task_id="parquet_custom_prod_figuras_fiscais_003",
            bash_command="python "+dag.params['scripts']['task_group_gera_parquet']+" custom_prod_figuras_fiscais "+dag.params['tipoCarga']+" 21 30",
        )
        parquet_custom_prod_figuras_fiscais_004 = BashOperator(
            task_id="parquet_custom_prod_figuras_fiscais_004",
            bash_command="python "+dag.params['scripts']['task_group_gera_parquet']+" custom_prod_figuras_fiscais "+dag.params['tipoCarga']+" 31 40",
        )
        parquet_custom_prod_figuras_fiscais_005 = BashOperator(
            task_id="parquet_custom_prod_figuras_fiscais_005",
            bash_command="python "+dag.params['scripts']['task_group_gera_parquet']+" custom_prod_figuras_fiscais "+dag.params['tipoCarga']+" 41 50",
        )
        parquet_grupo_custom_prod_001 = BashOperator(
            task_id="parquet_grupo_custom_prod_001",
            bash_command="python "+dag.params['scripts']['task_group_gera_parquet']+" grupo_custom_prod "+dag.params['tipoCarga']+" 1 10",
        )
        parquet_grupo_custom_prod_002 = BashOperator(
            task_id="parquet_grupo_custom_prod_002",
            bash_command="python "+dag.params['scripts']['task_group_gera_parquet']+" grupo_custom_prod "+dag.params['tipoCarga']+" 11 20",
        )
        parquet_grupo_custom_prod_003 = BashOperator(
            task_id="parquet_grupo_custom_prod_003",
            bash_command="python "+dag.params['scripts']['task_group_gera_parquet']+" grupo_custom_prod "+dag.params['tipoCarga']+" 21 30",
        )
        parquet_grupo_custom_prod_004 = BashOperator(
            task_id="parquet_grupo_custom_prod_004",
            bash_command="python "+dag.params['scripts']['task_group_gera_parquet']+" grupo_custom_prod "+dag.params['tipoCarga']+" 31 40",
        )
        parquet_grupo_custom_prod_005 = BashOperator(
            task_id="parquet_grupo_custom_prod_005",
            bash_command="python "+dag.params['scripts']['task_group_gera_parquet']+" grupo_custom_prod "+dag.params['tipoCarga']+" 41 50",
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
            group_id="task_gera_arquivos_parquet_cache",
            ui_color="red", 
            ui_fgcolor="white",
            tooltip="Gera os arquivos parquet da tabela de tributos_internos_cache e cache_st",
        ) as group_gera_parquet_caches:
        parquet_tributos_internos_cache_001 = BashOperator(
            task_id="parquet_tributos_internos_cache_001",
            bash_command="python "+dag.params['scripts']['task_group_gera_parquet_caches']+" tributos_internos_cache "+dag.params['tipoCarga']+" 1 100",
        )
        parquet_tributos_internos_cache_101 = BashOperator(
            task_id="parquet_tributos_internos_cache_101",
            bash_command="python "+dag.params['scripts']['task_group_gera_parquet_caches']+" tributos_internos_cache "+dag.params['tipoCarga']+" 101 200",
        )
        parquet_tributos_internos_cache_201 = BashOperator(
            task_id="parquet_tributos_internos_cache_201",
            bash_command="python "+dag.params['scripts']['task_group_gera_parquet_caches']+" tributos_internos_cache "+dag.params['tipoCarga']+" 201 300",
        )
        parquet_tributos_internos_cache_301 = BashOperator(
            task_id="parquet_tributos_internos_cache_301",
            bash_command="python "+dag.params['scripts']['task_group_gera_parquet_caches']+" tributos_internos_cache "+dag.params['tipoCarga']+" 301 400",
        )
        parquet_tributos_internos_cache_401 = BashOperator(
            task_id="parquet_tributos_internos_cache_401",
            bash_command="python "+dag.params['scripts']['task_group_gera_parquet_caches']+" tributos_internos_cache "+dag.params['tipoCarga']+" 401 500",
        )
        parquet_tributos_internos_cache_st_01 = BashOperator(
            task_id="parquet_tributos_internos_cache_st_01",
            bash_command="python "+dag.params['scripts']['task_group_gera_parquet_caches']+" tributos_internos_cache_st "+dag.params['tipoCarga']+" 1 10",
        )
        parquet_tributos_internos_cache_st_11 = BashOperator(
            task_id="parquet_tributos_internos_cache_st_11",
            bash_command="python "+dag.params['scripts']['task_group_gera_parquet_caches']+" tributos_internos_cache_st "+dag.params['tipoCarga']+" 11 20",
        )
        parquet_tributos_internos_cache_st_21 = BashOperator(
            task_id="parquet_tributos_internos_cache_st_21",
            bash_command="python "+dag.params['scripts']['task_group_gera_parquet_caches']+" tributos_internos_cache_st "+dag.params['tipoCarga']+" 21 30",
        )
        parquet_tributos_internos_cache_st_31 = BashOperator(
            task_id="parquet_tributos_internos_cache_st_31",
            bash_command="python "+dag.params['scripts']['task_group_gera_parquet_caches']+" tributos_internos_cache_st "+dag.params['tipoCarga']+" 31 40",
        )
        parquet_tributos_internos_cache_st_41 = BashOperator(
            task_id="parquet_tributos_internos_cache_st_41",
            bash_command="python "+dag.params['scripts']['task_group_gera_parquet_caches']+" tributos_internos_cache_st "+dag.params['tipoCarga']+" 41 50",
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
            group_id="task_group_apaga_parquet",
            ui_color="red", 
            ui_fgcolor="white",
            tooltip="Atualiza no snowflake os registros apagados na prod01sql",
        ) as group_apaga_parquet:
        apagar_clientes = BashOperator(
            task_id="apagar_clientes",
            bash_command="python "+dag.params['scripts']['task_group_apaga_parquet']+" apagar_clientes "+dag.params['tipoCarga'],
        )
        apagar_grupo_config = BashOperator(
            task_id="apagar_grupo_config",
            bash_command="python "+dag.params['scripts']['task_group_apaga_parquet']+" apagar_grupo_config "+dag.params['tipoCarga'],
        )
        apagar_custom_prod = BashOperator(
            task_id="apagar_custom_prod",
            bash_command="python "+dag.params['scripts']['task_group_apaga_parquet']+" apagar_custom_prod "+dag.params['tipoCarga'],
        )
        apagar_custom_prod_figuras_fiscais = BashOperator(
            task_id="apagar_custom_prod_figuras_fiscais",
            bash_command="python "+dag.params['scripts']['task_group_apaga_parquet']+" apagar_custom_prod_figuras_fiscais "+dag.params['tipoCarga'],
        )
        apagar_grupo_custom_prod = BashOperator(
            task_id="apagar_grupo_custom_prod",
            bash_command="python "+dag.params['scripts']['task_group_apaga_parquet']+" apagar_grupo_custom_prod "+dag.params['tipoCarga'],
        )
        apagar_cean_relacionado = BashOperator(
            task_id="apagar_cean_relacionado",
            bash_command="python "+dag.params['scripts']['task_group_apaga_parquet']+" apagar_cean_relacionado "+dag.params['tipoCarga'],
        )
        apagar_tributos_internos_cache_config = BashOperator(
            task_id="apagar_tributos_internos_cache_config",
            bash_command="python "+dag.params['scripts']['task_group_apaga_parquet']+" apagar_tributos_internos_cache_config "+dag.params['tipoCarga'],
        )
        apagar_tributos_internos_cache = BashOperator(
            task_id="apagar_tributos_internos_cache",
            bash_command="python "+dag.params['scripts']['task_group_apaga_parquet']+" apagar_tributos_internos_cache "+dag.params['tipoCarga'],
        )
        apagar_tributos_internos_cache_st = BashOperator(
            task_id="apagar_tributos_internos_cache_st",
            bash_command="python "+dag.params['scripts']['task_group_apaga_parquet']+" apagar_tributos_internos_cache_st "+dag.params['tipoCarga'],
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
            group_id="task_group_envia_parquet",
            ui_color="red", 
            ui_fgcolor="white",
            tooltip="Envia os arquivos parquet da tabela de tributos_internos_cache e cache_st",
        ) as group_envia_parquet:
        envia_parquet_tributos_internos_cache = BashOperator(
            task_id="envia_parquet_tributos_internos_cache",
            bash_command="python "+dag.params['scripts']['task_group_envia_parquet']+" tributos_internos_cache "+dag.params['tipoCarga'],
        )
        envia_parquet_tributos_internos_cache_st = BashOperator(
            task_id="envia_parquet_tributos_internos_cache_st",
            bash_command="python "+dag.params['scripts']['task_group_envia_parquet']+" tributos_internos_cache_st "+dag.params['tipoCarga'],
        )
        envia_parquet_custom_prod_figuras_fiscais = BashOperator(
            task_id="envia_parquet_custom_prod_figuras_fiscais",
            bash_command="python "+dag.params['scripts']['task_group_envia_parquet']+" custom_prod_figuras_fiscais "+dag.params['tipoCarga'],
        )        
        envia_parquet_custom_prod = BashOperator(
            task_id="envia_parquet_custom_prod",
            bash_command="python "+dag.params['scripts']['task_group_envia_parquet']+" custom_prod "+dag.params['tipoCarga'],
        )
        envia_parquet_grupo_custom_prod = BashOperator(
            task_id="envia_parquet_grupo_custom_prod",
            bash_command="python "+dag.params['scripts']['task_group_envia_parquet']+" grupo_custom_prod "+dag.params['tipoCarga'],
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

    task_snowflake_carga = BashOperator(
        task_id="task_snowflake_carga",
        bash_command="python "+dag.params['scripts']['task_snowflake_carga']+" "+dag.params['tipoCarga']+" TASK_CARGA_INICIAL TASK_CARGA",
    )   

    task_snowflake_gera_tabelao = BashOperator(
        task_id="task_snowflake_gera_tabelao",
        bash_command="python "+dag.params['scripts']['task_snowflake_carga']+" DBO TASK_GERA_TABELAO TASK_GERA_TABELAO",
    )   

    task_snowflake_tabelao_apaga_indevidos = BashOperator(
        task_id="task_snowflake_tabelao_apaga_indevidos",
        bash_command="python "+dag.params['scripts']['task_snowflake_tabelao_apaga_indevidos']+" DBO TASK_TABELAO_APAGA_INDEVIDOS TASK_TABELAO_APAGA_INDEVIDOS",
    )   

    task_apaga_csv_s3_tabelao = BashOperator(
        task_id="task_apaga_csv_s3_tabelao",
        bash_command="python "+dag.params['scripts']['task_apaga_csv_s3_tabelao']+" 'pr_apaga_csv_s3_tabelao' '"+dag.params['tipoCarga']+"'",
    )

    task_envia_tabelao_s3 = BashOperator(
        task_id="task_envia_tabelao_s3",
        bash_command="python "+dag.params['scripts']['task_envia_tabelao_s3']+" dbo pr_envia_tabelao_s3",
    )   

    task_execute_job_prod01sql = BashOperator(
            task_id="task_execute_job_prod01sql",
            bash_command="python "+dag.params['scripts']['task_execute_job_prod01sql']+" 'pr_execute_job_carga_tabelao' '"+dag.params['tipoCarga']+"'",
    )

    #task_download_csvs_tabelao = BashOperator(
    #        task_id="task_download_csvs_tabelao",
    #        bash_command="python "+dag.params['scripts']['task_download_csvs_tabelao']+" 'pr_download_csvs_tabelao' '"+dag.params['tipoCarga']+"'",
    #)

    #task_carrega_csv_tabelao_prod01sql = BashOperator(
    #        task_id="task_carrega_csv_tabelao_prod01sql",
    #        bash_command="python "+dag.params['scripts']['task_carrega_csv_tabelao_prod01sql']+" 'pr_carrega_tabelao' '"+dag.params['tipoCarga']+"'",
    #)

    #acionar_dag_generation_csv_to_rds = TriggerDagRunOperator(
    #    task_id="acionar_dag_generation_csv_to_rds",
    #    trigger_dag_id="dag_generation_csv_to_rds",  # ID da DAG a ser acionada
    #    wait_for_completion=False,  # Se True, espera a DAG terminar
    #    reset_dag_run=False,         # Se True, reinicia uma execução se já existir
    #)

    #acionar_dag_send_tabelao_prod01 = TriggerDagRunOperator(
    #    task_id="acionar_dag_send_tabelao_prod01",
    #    trigger_dag_id="dag_send_tabelao_prod01",  # ID da DAG a ser acionada
    #    wait_for_completion=False,  # Se True, espera a DAG terminar
    #    reset_dag_run=False,         # Se True, reinicia uma execução se já existir
    #)

    ### cadeia de eventos da carga full
    chain(
        start_task, 
        inicia_carga,
        carrega_carga,
        group_carrega_carga_full, 
        limpa_stage, 
        group_gera_envia_parquet, 
        group_gera_parquet,
        group_gera_parquet_caches, 
        group_envia_parquet, 
        task_snowflake_carga, 
        task_snowflake_gera_tabelao, 
        task_snowflake_tabelao_apaga_indevidos, 
        task_apaga_csv_s3_tabelao,
        task_envia_tabelao_s3,
        task_execute_job_prod01sql,
        #task_download_csvs_tabelao,
        #task_carrega_csv_tabelao_prod01sql,
        #acionar_dag_send_tabelao_prod01,
        #acionar_dag_generation_csv_to_rds,
        end_task
    )
    ### cadeia de eventos da carga incremental

