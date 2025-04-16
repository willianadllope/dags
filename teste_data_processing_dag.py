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
def data_processing_dag():

    @task()
    def process_data(cleaned_data):
        # Code to process the cleaned data
        return f"Data processed with {cleaned_data}"

    @task()
    def generate_report(processed_data):
        # Code to generate a report
        return f"Report generated for {processed_data}"

    clean_data = "{{ task_instance.xcom_pull(task_ids='trigger_data_processing') }}"

    processed = process_data(clean_data)
    report = generate_report(processed)

    processed >> report

data_processing_instance = data_processing_dag()