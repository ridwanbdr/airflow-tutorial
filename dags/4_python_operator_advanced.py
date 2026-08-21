from airflow import DAG
from airflow.providers.standard.operators.python import PythonOperator   # import baru
from datetime import datetime, timedelta
import random
import logging

logger = logging.getLogger(__name__)

default_args = {
    'owner': 'data_engineer',
    'retries': 1,
    'retry_delay': timedelta(minutes=1),
}

def generate_data(**context):
    """Generate data dan push ke XCom"""
    ds = context['ds']                    # execution date (YYYY-MM-DD)
    ti = context['ti']                    # task instance
    
    value = f"Data_{ds}_{random.randint(1000, 9999)}"
    
    logger.info(f"Execution Date : {ds}")
    logger.info(f"Nilai yang di-generate: {value}")
    
    # Cara 1: return → otomatis masuk XCom (key = return_value)
    return value


def process_data(**context):
    """Ambil data dari task sebelumnya via XCom"""
    ti = context['ti']
    
    # Ambil nilai dari task generate_data
    received = ti.xcom_pull(task_ids='generate_data')
    
    logger.info(f"Nilai yang diterima: {received}")
    
    # Proses data
    processed = f"Processed_{received}"
    logger.info(f"Hasil proses: {processed}")

    # Data baru untuk metode key custom
    new_value = f"Custom_{received}"
    logger.info(f"Nilai custom: {new_value}")
    
    # Push manual dengan key custom
    ti.xcom_push(key='hasil_custom', value=new_value)
    
    return processed


def final_report(**context):
    """Ambil beberapa XCom sekaligus"""
    ti = context['ti']
    
    nilai_asli = ti.xcom_pull(task_ids='generate_data')
    nilai_proses = ti.xcom_pull(task_ids='process_data')
    nilai_custom = ti.xcom_pull(task_ids='process_data', key='hasil_custom')
    
    logger.info("=" * 40)
    logger.info("LAPORAN AKHIR")
    logger.info("=" * 40)
    logger.info(f"Nilai Asli     : {nilai_asli}")
    logger.info(f"Nilai Proses   : {nilai_proses}")
    logger.info(f"Nilai Custom   : {nilai_custom}")
    logger.info(f"Logical Date   : {context['logical_date']}")
    logger.info("=" * 40)

with DAG(
    dag_id='python_operator_advanced',
    default_args=default_args,
    description='PythonOperator Advanced - XCom & Context',
    start_date=datetime(2024, 1, 1),
    schedule=None,
    catchup=False,
    tags=['belajar', 'python', 'xcom'],
) as dag:

    t1 = PythonOperator(
        task_id='generate_data',
        python_callable=generate_data
    )

    t2 = PythonOperator(
        task_id='process_data',
        python_callable=process_data
    )

    t3 = PythonOperator(
        task_id='final_report',
        python_callable=final_report
    )

    t1 >> t2 >> t3