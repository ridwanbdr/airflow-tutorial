from airflow.decorators import dag, task
from datetime import datetime, timedelta
import logging
import random

logger = logging.getLogger(__name__)

default_args = {
    'owner': 'data_engineer',
    'retries': 1,
    'retry_delay': timedelta(minutes=1),
}

@dag(
    dag_id='taskflow_api_modern',
    default_args=default_args,
    description='Contoh modern pakai TaskFlow API (@task)',
    start_date=datetime(2024, 1, 1),
    schedule=None,                    # None = manual trigger
    catchup=False,
    tags=['belajar', 'taskflow', 'modern'],
)
def taskflow_example():

    @task
    def generate_data():
        """Generate data dan otomatis push ke XCom (cukup return)"""
        value = f"Data_{random.randint(1000, 9999)}"
        logger.info(f"Generated value: {value}")
        return value                  # otomatis masuk XCom

    @task
    def process_data(value: str):
        """Terima data dari task sebelumnya (otomatis dari XCom)"""
        logger.info(f"Received value: {value}")
        processed = f"Processed_{value}"
        logger.info(f"Processed value: {processed}")
        return processed

    @task
    def final_report(original: str, processed: str):
        """Bisa menerima multiple return value dari task lain"""
        logger.info("=" * 40)
        logger.info("FINAL REPORT")
        logger.info("=" * 40)
        logger.info(f"Original  : {original}")
        logger.info(f"Processed : {processed}")
        logger.info("Selesai!")

    # Cara menulis dependency jauh lebih bersih
    generated = generate_data()
    processed = process_data(generated)
    final_report(generated, processed)


# Panggil fungsi DAG-nya
taskflow_example()