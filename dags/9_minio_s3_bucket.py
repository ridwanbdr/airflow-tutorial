from airflow.decorators import dag, task
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from datetime import datetime, timedelta
import logging
import os

logger = logging.getLogger(__name__)

default_args = {
    'owner': 'data_engineer',
    'retries': 1,
    'retry_delay': timedelta(minutes=1),
}

@dag(
    dag_id='minio_s3_example',
    default_args=default_args,
    description='Contoh interaksi dengan MinIO (S3)',
    start_date=datetime(2024, 1, 1),
    schedule=None,
    catchup=False,
    tags=['belajar', 'minio', 's3'],
)
def minio_s3_example():

    @task
    def upload_csv_to_minio():
        """Upload file CSV ke MinIO bucket"""
        hook = S3Hook(aws_conn_id='minio_bucket')
        
        bucket_name = 'airflow'          # ganti sesuai nama bucket kamu
        local_file_path = '/opt/airflow/data/customers.csv'   # sesuaikan path file CSV kamu
        s3_key = f'raw/sample_{datetime.now().strftime("%Y%m%d_%H%M%S")}.csv'

        # Cek apakah file lokal ada
        if not os.path.exists(local_file_path):
            raise FileNotFoundError(f"File tidak ditemukan: {local_file_path}")

        # Upload file
        hook.load_file(
            filename=local_file_path,
            key=s3_key,
            bucket_name=bucket_name,
            replace=True
        )
        
        logger.info(f"Berhasil upload: s3://{bucket_name}/{s3_key}")
        return s3_key

    @task
    def list_objects_in_bucket(uploaded_key: str):
        """List object di dalam bucket"""
        hook = S3Hook(aws_conn_id='minio_bucket')
        bucket_name = 'airflow'

        keys = hook.list_keys(bucket_name=bucket_name, prefix='raw/')
        
        logger.info(f"Isi folder raw/ di bucket {bucket_name}:")
        if keys:
            for key in keys:
                logger.info(f"  - {key}")
        else:
            logger.info("  (kosong)")

        logger.info(f"File yang baru diupload: {uploaded_key}")
        return keys

    @task
    def download_file_from_minio(s3_key: str):
        """Download file dari MinIO (contoh)"""
        hook = S3Hook(aws_conn_id='minio_bucket')
        bucket_name = 'airflow'
        
        local_download_path = f"/tmp/{os.path.basename(s3_key)}"
        
        # Download
        file_name = hook.download_file(
            key=s3_key,
            bucket_name=bucket_name,
            local_path='/tmp'
        )
        
        logger.info(f"File berhasil didownload ke: {file_name}")
        return file_name

    # Flow
    uploaded = upload_csv_to_minio()
    listed = list_objects_in_bucket(uploaded)
    downloaded = download_file_from_minio(uploaded)

    uploaded >> listed >> downloaded


minio_s3_example()