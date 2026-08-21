from airflow import DAG
from airflow.providers.standard.operators.python import PythonOperator   # import baru
from datetime import datetime, timedelta
import logging

logger = logging.getLogger(__name__)

default_args = {
    'owner': 'data_engineer',
    'retries': 1,
    'retry_delay': timedelta(minutes=1),
}

def print_hello():
    logger.info("Halo dari PythonOperator!")
    logger.info("Belajar Airflow bareng Python 🐍")

def print_nama(nama, umur):
    logger.info(f"Nama  : {nama}")
    logger.info(f"Umur  : {umur} tahun")
    logger.info("Task berhasil dijalankan!")

with DAG(
    dag_id='python_operator_basic',
    default_args=default_args,
    description='Contoh dasar PythonOperator (fixed)',
    start_date=datetime(2024, 1, 1),
    schedule=None,
    catchup=False,
    tags=['belajar', 'python'],
) as dag:

    task_hello = PythonOperator(
        task_id='print_hello',
        python_callable=print_hello
    )

    task_nama = PythonOperator(
        task_id='print_nama',
        python_callable=print_nama,
        op_kwargs={
            'nama': 'Budi',
            'umur': 25
        }
    )

    task_hello >> task_nama