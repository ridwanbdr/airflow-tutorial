from airflow import DAG
from airflow.operators.bash import BashOperator
from datetime import datetime, timedelta

# Default arguments yang akan dipakai semua task di dalam DAG ini
default_args = {
    'owner': 'ridwan_data_engineer',
    'retries': 1,
    'retry_delay': timedelta(minutes=1),
}

with DAG(
    dag_id='bash_operator_basic',          # Nama unik DAG
    default_args=default_args,
    description='Contoh sederhana menggunakan BashOperator',
    start_date=datetime(2026, 1, 1),
    schedule=None,                       # None = hanya jalan manual (trigger)
    catchup=False,                           # Jangan jalankan run-run yang terlewat
    tags=['belajar', 'bash'],
) as dag:

    # Task 1: print hello
    task_hello = BashOperator(
        task_id='print_hello',
        bash_command='echo "Halo dari BashOperator! Belajar Airflow yuk 🚀"'
    )

    # Task 2: print tanggal hari ini
    task_date = BashOperator(
        task_id='print_date',
        bash_command='date'
    )

    # Task 3: buat file sederhana
    task_create_file = BashOperator(
        task_id='create_file',
        bash_command='echo "File dibuat oleh Airflow pada $(date)" > /tmp/airflow_test.txt && cat /tmp/airflow_test.txt'
    )

    # Task 4: list isi folder /tmp (opsional, untuk cek)
    task_list = BashOperator(
        task_id='list_tmp',
        bash_command='ls -la /tmp | head -10'
    )

    # Urutan eksekusi (dependency)
    task_hello >> task_date >> task_create_file >> task_list