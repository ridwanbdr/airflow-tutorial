from airflow.decorators import dag, task
from datetime import datetime, timedelta
import logging

logger = logging.getLogger(__name__)

@dag(
    dag_id='catchup_arguments',
    start_date=datetime(2026, 8, 15),     # Mulai dari tanggal ini
    schedule='@daily',                    # Jalan setiap hari
    catchup=True,                         # ← PENTING: True = jalankan yang terlewat
    tags=['belajar', 'catchup'],
)
def catchup_arguments():

    @task
    def print_date(**context):
        ds = context['ds']
        logger.info(f"Menjalankan task untuk tanggal: {ds}")
        logger.info(f"Logical Date: {context['logical_date']}")

    print_date()


catchup_arguments()


# Kalau True, Airflow akan otomatis menjalankan semua interval yang terlewat sejak start_date sampai sekarang saat DAG diaktifkan.

# Cara kerja catchup=True:

# start_date = 15 Agustus 2026
# Hari ini 20 Agustus 2026
# Begitu aktifkan DAG → Airflow akan otomatis membuat run untuk tanggal 15, 16, 17, 18, 19 (yang terlewat).