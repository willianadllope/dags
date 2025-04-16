from datetime import timedelta, datetime
from airflow import DAG
from airflow.models.baseoperator import chain
from airflow.operators.trigger_dagrun import TriggerDagRunOperator
from airflow.utils.dates import days_ago

from airflow.decorators import dag, task


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
            trigger_dag_id='data_processing_dag2',
            conf={"clean_data": "processed"},
            dag=data_cleaning_dag
        )
        return trigger

    extract = extract_data()
    clean = clean_data()
    trigger = trigger_next_dag()

    extract >> clean >> trigger

data_cleaning_instance = data_cleaning_dag()


@dag(start_date=datetime(2025, 3, 6), schedule_interval=None)
def data_processing_dag2():

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

data_processing_instance = data_processing_dag2()