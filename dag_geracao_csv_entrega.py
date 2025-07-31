from airflow import DAG
from airflow.operators.bash_operator import BashOperator
from airflow.operators.empty import EmptyOperator
from datetime import datetime
import scripts.config as cfg

class DAG_csv_to_rds:
    def __init__(self, dag_id, schedule, start_date, params):
        self.dag = DAG(
            dag_id=dag_id,
            schedule=schedule,
            start_date=start_date,
            params=params,
            doc_md="""
                Essa DAG envia os arquivos de ponteiro capturados do RDS de Entrega 
                para o Snowflake, para realizar a atualização dos ponteiros "ANTES" 
                da virada.
                Também envia os arquivos CSV para o AWS S3 e, na sequencia,
                chama as procedures do RDS para carregar esses arquivos.
            """,
            catchup=False,
        )

    ## apaga os arquivos parquet do snowflake
    def delete_parquet(self):
        return BashOperator(
            task_id="delete_parquet",
            #bash_command="python "+self.dag.params['scripts']['task_delete_parquet']+" entrega pr_apaga_arquivos_ajusteponteirosrds",
            bash_command="echo 'ok'",
            doc_md="""
            Essa task chama a procedure abaixo no Snowflake:
            DB_TABELAO.ENTREGA.PR_APAGA_ARQUIVOS_AJUSTEPONTEIROSRDS()
            Essa procedure apaga os arquivos parquet no storage do Snowflake, 
            referentes a tabela que guarda os ponteiros do tabelao que estão no RDS.
            Isso será usado para comparação e atualização dos ponteiros ANTES da virada.
            """,
            dag=self.dag,
        )   

    ## acessa o RDS para buscar os ultimos ponteiros do tabelao a partir da ultima replicacao
    def get_parquet(self):
        return BashOperator(
            task_id='get_parquet',
            #bash_command="python "+self.dag.params['scripts']['task_get_parquet']+" 1",
            bash_command="echo 'ok'",
            doc_md=""" """,
            dag=self.dag,
        )
    
    ## sobe arquivos parquet para o Snowflake
    def send_parquet(self):
        return BashOperator(
            task_id='send_parquet',
            #bash_command="python "+self.dag.params['scripts']['task_send_parquet']+" ajusteponteirords FULL",
            bash_command="echo 'ok'",
            doc_md=""" """,
            dag=self.dag,
        )    
    
    ## atualiza a tabela de ajuste de ponteiros no Snowflake, fazendo a carga com os novos registros desde a ultima replicacao
    def carga_ajuste_ponteiro_rds(self):
        return BashOperator(
            task_id="carga_ajuste_ponteiro_rds",
            #bash_command="python "+self.dag.params['scripts']['task_carga_ajuste_ponteiro_rds']+" full PR_CARGA_AJUSTE_PONTEIRO_RDS",
            bash_command="echo 'ok'",
            doc_md=""" """,
            dag=self.dag,
        )   
    
    ## executa uma TASK no Snowflake
    def preparar_enviar_csv(self):
        return BashOperator(
        task_id="preparar_enviar_csv",
        #bash_command="python "+self.dag.params['scripts']['task_preparar_enviar_csv']+" ENTREGA TASK_PREPARAR_ENVIO_RDS TASK_PREPARAR_ENVIO_RDS",
        bash_command="echo 'ok'",
        doc_md=""" """,
        dag=self.dag,
    )   

    ## chama procedure no RDS passando como parametro a tabela de _Copia que sera carregada e o arquivo do S3
    ## fc_carrega_csv_from_snowflake
    def send_s3_rds(self):
        return BashOperator(
            task_id="send_s3_rds",
            bash_command="python "+self.dag.params['scripts']['task_send_s3_rds'],
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
        t1 = self.delete_parquet()
        t2 = self.get_parquet()
        t3 = self.send_parquet()
        t4 = self.carga_ajuste_ponteiro_rds()
        t5 = self.preparar_enviar_csv()
        t6 = self.send_s3_rds()
        t7 = self.end_task()
        t0 >> t1 >> t2 >> t3 >> t4 >> t5 >> t6 >> t7
        return self.dag

# Instantiate the DAG class
dag_generation_csv_to_rds = DAG_csv_to_rds(
    dag_id='dag_generation_csv_to_rds',
    schedule=None,
    start_date=datetime(2023, 1, 1),
    params=cfg.configs
)

# Get the DAG object
dag = dag_generation_csv_to_rds.create_dag()