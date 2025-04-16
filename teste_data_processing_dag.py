from datetime import timedelta, datetime
from airflow import DAG
from airflow.models.baseoperator import chain
from airflow.operators.trigger_dagrun import TriggerDagRunOperator
from airflow.utils.dates import days_ago

from airflow.decorators import dag, task


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