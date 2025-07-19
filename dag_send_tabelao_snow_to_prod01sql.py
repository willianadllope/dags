from airflow import DAG
from airflow.operators.bash_operator import BashOperator
from airflow.operators.empty import EmptyOperator
from datetime import datetime
import scripts.config as cfg

class DAG_send_tabelao_prod01sql:
    def __init__(self, dag_id, schedule_interval, start_date, params):
        self.dag = DAG(
            dag_id=dag_id,
            schedule_interval=schedule_interval,
            start_date=start_date,
            params=params,
            catchup=False,
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
        return EmptyOperator(
            task_id='start',
            dag=self.dag,
        )

    def end_task(self):
        return EmptyOperator(
            task_id='end',
            dag=self.dag,
        )
   
    def create_dag(self):
      t0 = self.start_task()
      t1 = self.carrega_csv_tabelao_prod01sql()
      t2 = self.finaliza_carga_full()
      t3 = self.end_task()
      t0 >> t1 >> t2 >> t3
      return self.dag

# Instantiate the DAG class
dag_send_tabelao_prod01 = DAG_send_tabelao_prod01sql(
    dag_id='dag_send_tabelao_prod01',
    schedule_interval=None,
    start_date=datetime(2023, 1, 1),
    params=cfg.configs
)

# Get the DAG object
dag = dag_send_tabelao_prod01.create_dag()

    