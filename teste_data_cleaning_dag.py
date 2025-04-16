from datetime import timedelta, datetime
import scripts.config


# The DAG object; we'll need this to instantiate a DAG
from airflow import DAG
from airflow.models.baseoperator import chain

# Operators; we need this to operate!

from airflow.operators.bash import BashOperator
from airflow.operators.dummy import DummyOperator
from airflow.operators.trigger_dagrun import TriggerDagRunOperator
from airflow.utils.dates import days_ago

from airflow.utils.task_group import TaskGroup
from airflow.decorators import dag, task_group, task


@dag(start_date=datetime(2025, 3, 6), schedule_interval=None)
def data_cleaning_dag():

    @task()
    def extract_data():
        # Code to extract data
        return "Raw data extracted"

    @task()
    def clean_data():
        # Code to clean the extracted data
        return "Data cleaned"

    @task()
    def trigger_next_dag():
        # Trigger DAG 2 (Data Processing)
        trigger = TriggerDagRunOperator(
            task_id='trigger_data_processing',
            trigger_dag_id='data_processing_dag',
            dag=data_cleaning_dag
        )
        return trigger

    extract = extract_data()
    clean = clean_data()
    trigger = trigger_next_dag()

    extract >> clean >> trigger

data_cleaning_instance = data_cleaning_dag()