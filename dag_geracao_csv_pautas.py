from airflow import DAG
from airflow.operators.bash_operator import BashOperator
from airflow.operators.empty import EmptyOperator
from airflow.operators.python import PythonOperator
from datetime import datetime
import scripts.config as cfg

class DAG_PAUTA_CSV:
    def __init__(self, dag_id, schedule, start_date, params):
        self.dag = DAG(
            dag_id=dag_id,
            schedule=schedule,
            start_date=start_date,
            params=params,
            doc_md="""
                Essa DAG envia o CSV de pautas gerado pelo
                servidor PROD01SQL e enviado para o bucket
                s3://csvvertex/ do lado Systax
                para o bucket do lado Vertex
                baixando o arquivo para a pasta /csvpautas/ local
            """,
            catchup=False,
        )

    ## faz o download do arquivo CSV gerado pela prod01sql e enviado para o bucket do S3
    def download_s3_pautas(self):
        return BashOperator(
            task_id="download_s3_pautas",
            bash_command="python "+self.dag.params['scripts']['task_download_s3_pautas'],
            doc_md="""
            Essa task faz o dowload do arquivo CSV que esta no bucket
            s3://csvvertex/
            da Systax, copia para a pasta /csvpautas local
            checando se tem necessidade de baixar algum arquivo , ou seja, se foi 
            gerado algum arquivo desde a ultima vez que rodou
            """,
            dag=self.dag,
        )   

    ## envia o arquivo CSV local para o bucket da Vertex, assumindo a role do lado Systax e depois a role do lado Vertex
    def send_csv_pautas_sandbox(self):
        return BashOperator(
            task_id='send_csv_pautas_sandbox',
            bash_command="python "+self.dag.params['scripts']['task_send_csv_pautas']+" 'SANDBOX'",
            doc_md="""
            envia o arquivo CSV local para o bucket da Vertex, assumindo a role do lado Systax e depois a role do lado Vertex
            """,
            dag=self.dag,
        )

    def send_csv_pautas_stage(self):
        return BashOperator(
            task_id='send_csv_pautas_stage',
            bash_command="python "+self.dag.params['scripts']['task_send_csv_pautas']+" 'STAGE'",
            doc_md="""
            envia o arquivo CSV local para o bucket da Vertex, assumindo a role do lado Systax e depois a role do lado Vertex
            """,
            dag=self.dag,
        )

    def send_csv_pautas_prod(self):
        return BashOperator(
            task_id='send_csv_pautas_prod',
            bash_command="python "+self.dag.params['scripts']['task_send_csv_pautas']+" 'PROD'",
            doc_md="""
            envia o arquivo CSV local para o bucket da Vertex, assumindo a role do lado Systax e depois a role do lado Vertex
            """,
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
        t1 = self.download_s3_pautas()
        t2sandbox = self.send_csv_pautas_sandbox()
        t2stage = self.send_csv_pautas_stage()
        t2prod = self.send_csv_pautas_prod()
        tw = self.wait_task()
        tf = self.end_task()
        t0 >> t1 >> t2sandbox >> t2stage >> t2prod >> tw >> tf
        return self.dag

# Instantiate the DAG class
dag_generation_pauta_csv = DAG_PAUTA_CSV(
    dag_id='dag_generation_pauta_csv',
    schedule='0 14-21 * * *', #roda diariamente, 1x por hora, entre 13h e 23h
    start_date=datetime(2023, 1, 1),
    params=cfg.configs
)

# Get the DAG object
dag = dag_generation_pauta_csv.create_dag()