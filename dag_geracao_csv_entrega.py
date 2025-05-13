from airflow import DAG
from airflow.operators.bash_operator import BashOperator
from datetime import datetime

class MyDAG:
    def __init__(self, dag_id, schedule_interval, start_date):
        self.dag = DAG(
            dag_id=dag_id,
            schedule_interval=schedule_interval,
            start_date=start_date,
            catchup=False,
        )

    def task_1(self):
        return BashOperator(
            task_id='task_1',
            bash_command='echo "Task 1"',
            dag=self.dag,
        )

    def task_2(self):
        return BashOperator(
            task_id='task_2',
            bash_command='echo "Task 2"',
            dag=self.dag,
        )
    
    def create_dag(self):
      t1 = self.task_1()
      t2 = self.task_2()
      t1 >> t2
      return self.dag

# Instantiate the DAG class
my_dag_instance = MyDAG(
    dag_id='my_object_oriented_dag',
    schedule_interval=None,
    start_date=datetime(2023, 1, 1),
)

# Get the DAG object
dag = my_dag_instance.create_dag()