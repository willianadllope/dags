from airflow import DAG
from airflow.operators.bash_operator import BashOperator
from datetime import datetime
import scripts.config as cfg


class DAG_csv_to_rds:
    def __init__(self, dag_id, schedule_interval, start_date, params):
        self.dag = DAG(
            dag_id=dag_id,
            schedule_interval=schedule_interval,
            start_date=start_date,
            params=params,
            catchup=False,
        )

    def delete_parquet(self):
        return BashOperator(
            task_id="delete_parquet",
            bash_command="python "+self.dag.params['scripts']+"call_snow_procedure.py entrega pr_apaga_arquivos_ajusteponteirosrds",
            dag=self.dag,
            #bash_command="echo 'delete_parquet' ",
        )   

    def get_parquet(self):
        return BashOperator(
            task_id='get_parquet',
            bash_command="python "+self.dag.params['scripts']+"gera_parquet_ponteiros_pg.py 1",
            dag=self.dag,
        )
    
    def send_parquet(self):
        return BashOperator(
            task_id='send_parquet',
            bash_command="python "+self.dag.params['scripts']+"upload_snowflake.py ajusteponteirords FULL",
            dag=self.dag,
        )    

    def carga_ajuste_ponteiro_rds(self):
        return BashOperator(
            task_id="carga_ajuste_ponteiro_rds",
            bash_command="python "+self.dag.params['scripts']+"call_snow_procedure.py full PR_CARGA_AJUSTE_PONTEIRO_RDS",
            dag=self.dag,
        )   

    def send_s3_rds(self):
        return BashOperator(
            task_id="send_s3_rds",
            bash_command="python "+self.dag.params['scripts']+"send_s3_rds.py",
            dag=self.dag,
        )
       
    def task_preparar_enviar_csv(self):
        return BashOperator(
        task_id="task_preparar_enviar_csv",
        bash_command="python "+self.dag.params['scripts']+"exec_snow_task.py ENTREGA TASK_PREPARAR_ENVIO_RDS TASK_PREPARAR_ENVIO_RDS",
        dag=self.dag,
        #bash_command="echo 'task_gera_tabelao' ",
    )   
   
    def create_dag(self):
      t0 = self.delete_parquet()
      t1 = self.get_parquet()
      t2 = self.send_parquet()
      t3 = self.carga_ajuste_ponteiro_rds()
      t4 = self.task_preparar_enviar_csv()
      t5 = self.send_s3_rds()
      t0 >> t1 >> t2 >> t3 >> t4 >> t5
      return self.dag

# Instantiate the DAG class
dag_generation_csv_to_rds = DAG_csv_to_rds(
    dag_id='dag_generation_csv_to_rds',
    schedule_interval=None,
    start_date=datetime(2023, 1, 1),
    params=cfg.pastas
)

# Get the DAG object
dag = dag_generation_csv_to_rds.create_dag()