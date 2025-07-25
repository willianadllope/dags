from airflow import DAG
from airflow.operators.bash_operator import BashOperator
from airflow.operators.dummy import DummyOperator
from datetime import datetime
import scripts.config as cfg

class DAG_send_tabelao_prod01sql:
    def __init__(self, dag_id, schedule, start_date, params):
        self.dag = DAG(
            dag_id=dag_id,
            schedule=schedule,
            start_date=start_date,
            params=params,
            catchup=False,
        )

    def download_csvs_tabelao(self):
        return BashOperator(
            task_id="task_download_csvs_tabelao",
            bash_command="python "+self.dag.params['scripts']['task_download_csvs_tabelao']+" 'pr_download_csvs_tabelao' '"+self.dag.params['tipoCarga']+"'",
            dag=self.dag,
    )

    def carrega_csv_tabelao_prod01sql(self):
        return BashOperator(
            task_id="carrega_csv_tabelao_prod01sql",
            bash_command="python "+self.dag.params['scripts']['task_carrega_csv_tabelao_prod01sql']+" 'pr_carrega_tabelao' '"+self.dag.params['tipoCarga']+"'",
            dag=self.dag,
        )

    def finaliza_carga_full(self):
        return BashOperator(
            task_id="finaliza_carga_full",
            bash_command="python "+self.dag.params['scripts']['task_finaliza_carga_full']+" '"+self.dag.params['tipoCarga']+"'"+" 1 100",
            dag=self.dag,
        )

    def start_task(self):
        return DummyOperator(
            task_id='start_task',
            dag=self.dag,
        )

    def end_task(self):
        return DummyOperator(
            task_id='end_task',
            dag=self.dag,
        )
   
    def create_dag(self):
      t0 = self.start_task()
      t1 = self.download_csvs_tabelao()
      t2 = self.carrega_csv_tabelao_prod01sql()
      t3 = self.finaliza_carga_full()
      t4 = self.end_task()
      t0 >> t1 >> t2 >> t3 >> t4
      return self.dag

# Instantiate the DAG class
dag_send_tabelao_prod01 = DAG_send_tabelao_prod01sql(
    dag_id='dag_send_tabelao_prod01',
    schedule=None,
    start_date=datetime(2023, 1, 1),
    params=cfg.configs
)

# Get the DAG object
dag = dag_send_tabelao_prod01.create_dag()

    