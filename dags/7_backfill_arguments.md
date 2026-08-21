# Backfill : Menjalankan ulang DAG untuk rentang tanggal tertentu secara manual (biasanya lewat CLI).

## 1. Masuk ke scheduler / webserver airflow
"docker exec -it <nama_container_airflow> bash"

## 2. Jalankan Backfill

>> Backfill untuk rentang tanggal tertentu
"
airflow dags backfill catchup_example \
    --start-date 2026-08-10 \
    --end-date 2026-08-14
"

>> Contoh lain

"
>>> Backfill + reset yang sudah ada
airflow dags backfill catchup_example \
    --start-date 2026-08-10 \
    --end-date 2026-08-14 \
    --reset-dagruns

>>> Backfill hanya 1 hari
airflow dags backfill catchup_example \
    --start-date 2026-08-18 \
    --end-date 2026-08-18
"