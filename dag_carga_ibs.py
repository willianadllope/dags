from airflow import DAG
from airflow.operators.bash_operator import BashOperator
from airflow.operators.empty import EmptyOperator
from airflow.operators.python import PythonOperator
from datetime import datetime
import scripts.config as cfg

class DAG_CARGA_IBS:
    def __init__(self, dag_id, schedule, start_date, params):
        self.dag = DAG(
            dag_id=dag_id,
            schedule=schedule,
            start_date=start_date,
            params=params,
            doc_md="""
                Essa DAG copia as tabelas de IBS do
                servidor PROD01SQL e enviado para o bucket no S3
                e depois faz as cargas no STAGING e depois na PRODUCAO
            """,
            catchup=False,
        )

    def limpa_arquivos_ibs(self):
        return BashOperator(
            task_id="limpa_arquivos_ibs",
            bash_command="python "+self.dag.params['scripts']['task_call_procedure_ibs']+' STAGING pr_carga_inicial_limpa_arquivos',
            dag=self.dag,
        )   

    def carga_csv_ibs(self):
        return BashOperator(
            task_id='carga_csv_ibs',
            bash_command="python "+self.dag.params['scripts']['task_carga_csv_ibs']+' ALL FULL',
            dag=self.dag,
        )

    def carga_staging_tabelas_ibs(self):
        return BashOperator(
            task_id="carga_staging_tabelas_ibs",
            bash_command="python "+self.dag.params['scripts']['task_call_procedure_ibs']+' STAGING pr_carregar_tabelas_ibs',
            dag=self.dag,
        )   

    def carga_producao_tabelas_ibs(self):
        return BashOperator(
            task_id="carga_producao_tabelas_ibs",
            bash_command="python "+self.dag.params['scripts']['task_call_procedure_ibs']+' STAGING pr_carregar_tabelas_ibs_producao',
            dag=self.dag,
        )   

    def wait_task(self):
        return BashOperator(
            task_id="wait_task",
            bash_command="sleep 10",
            doc_md=""" """,
            dag=self.dag,
        )
    
    def start_task(self):
        return EmptyOperator(
            task_id='start_task',
            dag=self.dag,
        )

    def end_task(self):
        return EmptyOperator(
            task_id='end_task',
            dag=self.dag,
        )    
    
    def create_dag(self):
        t0 = self.start_task()
        t1 = self.limpa_arquivos_ibs()
        t2 = self.carga_csv_ibs()
        t3 = self.carga_staging_tabelas_ibs()
        t4 = self.carga_producao_tabelas_ibs()
        tw = self.wait_task()
        tf = self.end_task()
        t0 >> t1 >> t2 >> t3 >> t4 >> tw >> tf
        return self.dag

# Instantiate the DAG class
dag_carga_ibs = DAG_CARGA_IBS(
    dag_id='dag_carga_ibs',
    schedule='0 */4 * * *', #roda diariamente, 1x a cada 4h
    start_date=datetime(2023, 1, 1),
    params=cfg.configs
)

# Get the DAG object
dag = dag_carga_ibs.create_dag()


# python task_call_procedure_ibs.py STAGING pr_carga_inicial_limpa_arquivos
# python task_parquet_geracao_envio_prod01sql_ibs_snowflake.py ALL FULL
# python task_call_procedure_ibs.py STAGING pr_carregar_tabelas_ibs
# python task_call_procedure_ibs.py STAGING pr_carregar_tabelas_ibs_producao

