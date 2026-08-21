from airflow.decorators import dag, task
from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator
from datetime import datetime, timedelta
import logging

logger = logging.getLogger(__name__)

default_args = {
    'owner': 'data_engineer',
    'retries': 1,
    'retry_delay': timedelta(minutes=1),
}

@dag(
    dag_id='postgres_operator_basic',
    default_args=default_args,
    description='Contoh SQLExecuteQueryOperator - simpan data ke PostgreSQL',
    start_date=datetime(2024, 1, 1),
    schedule=None,
    catchup=False,
    tags=['belajar', 'postgres', 'database'],
)
def postgres_operator_basic():

    # -------------------------------------------------
    # 1. Buat tabel (jika belum ada)
    # -------------------------------------------------
    create_table = SQLExecuteQueryOperator(
        task_id='create_table',
        postgres_conn_id='postgres_dbeaver',          # nama connection yang kamu buat
        sql="""
            CREATE TABLE IF NOT EXISTS belajar_airflow (
                id              SERIAL PRIMARY KEY,
                nama            VARCHAR(100),
                kota            VARCHAR(50),
                nilai           INTEGER,
                created_at      TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            );
        """
    )

    # -------------------------------------------------
    # 2. Insert data (contoh statis)
    # -------------------------------------------------
    insert_data = SQLExecuteQueryOperator(
        task_id='insert_data',
        postgres_conn_id='postgres_dbeaver',
        sql="""
            INSERT INTO belajar_airflow (nama, kota, nilai)
            VALUES
                ('Budi', 'Jakarta', 85),
                ('Siti', 'Bandung', 90),
                ('Andi', 'Surabaya', 78),
                ('Dewi', 'Yogyakarta', 92);
        """
    )

    # -------------------------------------------------
    # 3. Insert data dinamis pakai Jinja (opsional)
    # -------------------------------------------------
    insert_dynamic = SQLExecuteQueryOperator(
        task_id='insert_dynamic',
        postgres_conn_id='postgres_dbeaver',
        sql="""
            INSERT INTO belajar_airflow (nama, kota, nilai)
            VALUES ('Airflow_{{ ds_nodash }}', 'Automated', {{ 70 + 5 }});
        """
    )

    # -------------------------------------------------
    # 4. Cek data yang baru masuk
    # -------------------------------------------------
    check_data = SQLExecuteQueryOperator(
        task_id='check_data',
        postgres_conn_id='postgres_dbeaver',
        sql="""
            SELECT * FROM belajar_airflow
            ORDER BY id DESC
            LIMIT 10;
        """
    )

    # Dependency
    create_table >> insert_data >> insert_dynamic >> check_data


postgres_operator_basic()