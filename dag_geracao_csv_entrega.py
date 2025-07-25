from airflow import DAG
from airflow.operators.bash_operator import BashOperator
from datetime import datetime
import scripts.config as cfg

class DAG_csv_to_rds:
    def __init__(self, dag_id, schedule, start_date, params):
        self.dag = DAG(
            dag_id=dag_id,
            schedule=schedule,
            start_date=start_date,
            params=params,
            catchup=False,
        )

    ## apaga os arquivos parquet do snowflake
    def delete_parquet(self):
        return BashOperator(
            task_id="delete_parquet",
            bash_command="python "+self.dag.params['scripts']['task_delete_parquet']+" entrega pr_apaga_arquivos_ajusteponteirosrds",
            dag=self.dag,
        )   

    ## acessa o RDS para buscar os ultimos ponteiros do tabelao a partir da ultima replicacao
    def get_parquet(self):
        return BashOperator(
            task_id='get_parquet',
            bash_command="python "+self.dag.params['scripts']['task_get_parquet']+" 1",
            dag=self.dag,
        )
    
    ## sobe arquivos parquet para o Snowflake
    def send_parquet(self):
        return BashOperator(
            task_id='send_parquet',
            bash_command="python "+self.dag.params['scripts']['task_send_parquet']+" ajusteponteirords FULL",
            dag=self.dag,
        )    
    
    ## atualiza a tabela de ajuste de ponteiros no Snowflake, fazendo a carga com os novos registros desde a ultima replicacao
    def carga_ajuste_ponteiro_rds(self):
        return BashOperator(
            task_id="carga_ajuste_ponteiro_rds",
            bash_command="python "+self.dag.params['scripts']['task_carga_ajuste_ponteiro_rds']+" full PR_CARGA_AJUSTE_PONTEIRO_RDS",
            dag=self.dag,
        )   
    
    ## executa uma TASK no Snowflake
    def preparar_enviar_csv(self):
        return BashOperator(
        task_id="preparar_enviar_csv",
        bash_command="python "+self.dag.params['scripts']['task_preparar_enviar_csv']+" ENTREGA TASK_PREPARAR_ENVIO_RDS TASK_PREPARAR_ENVIO_RDS",
        dag=self.dag,
        #bash_command="echo 'task_gera_tabelao' ",
    )   

    ## chama procedure no RDS passando como parametro a tabela de _Copia que sera carregada e o arquivo do S3
    ## fc_carrega_csv_from_snowflake
    def send_s3_rds(self):
        return BashOperator(
            task_id="send_s3_rds",
            bash_command="python "+self.dag.params['scripts']['task_send_s3_rds'],
            dag=self.dag,
        )
      
    def create_dag(self):
      t0 = self.delete_parquet()
      t1 = self.get_parquet()
      t2 = self.send_parquet()
      t3 = self.carga_ajuste_ponteiro_rds()
      t4 = self.preparar_enviar_csv()
      t5 = self.send_s3_rds()
      t0 >> t1 >> t2 >> t3 >> t4 >> t5
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