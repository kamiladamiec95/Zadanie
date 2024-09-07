from airflow.operators.python_operator import PythonOperator
from airflow import DAG, settings
from datetime import datetime, timedelta
from utilities.load import load_data_to_sql
from utilities.extract import get_xml_data_from_api_to_file
from utilities.transform import save_transformed_data_to_files
from utilities.connection import create_conn_airflow
from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator
import json
from airflow.operators.python import BranchPythonOperator
from airflow.models.baseoperator import chain


# def get_last_import_date(ti):
#     with open("/opt/airflow/dags/utilities/params.json", "r") as f:
#         file_data = json.load(f)
#     last_import_date = file_data['last_import_date']
#     ti.xcom_push(key='last_import_date', value=last_import_date)


def get_schedule_interval():
    """
    Funkcja pobierająca co ile minut wykonywać DAG
    """
    with open("/opt/airflow/dags/utilities/params.json", "r") as f:
        file_data = json.load(f)
    schedule_interval = file_data['schedule_interval_minutes']
    return schedule_interval


default_args = {
    'owner': 'kamil',
    'retries': 0,
    'retry_delay': timedelta(minutes=1)
}


with DAG(
        default_args=default_args,
        dag_id='etl',
        description='etl_dag',
        start_date=datetime.now(),
        schedule_interval=timedelta(minutes=get_schedule_interval())
        #template_searchpath=['/opt/airflow/dags/utilities/sql']
) as dag:
    create_conn = PythonOperator(
        task_id='create_airflow_connection',
        python_callable=create_conn_airflow,
        provide_context=True
    )
    extract = PythonOperator(
        task_id='extract_data',
        python_callable=get_xml_data_from_api_to_file,
        provide_context=True
    )
    transform = PythonOperator(
        task_id='transform_data',
        python_callable=save_transformed_data_to_files,
        do_xcom_push=False,
        provide_context=True
    )
    load = PythonOperator(
        task_id='load_data',
        python_callable=load_data_to_sql,
        do_xcom_push=False,
        provide_context=True
    )
    create_conn >> extract >> transform >> load


