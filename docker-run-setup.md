## RUN & SET UP FROM SCRATCH ##
docker --version (cek docker nyala)
docker compose --version (cek compose nyala)
curl -LfO 'https://airflow.apache.org/docs/apache-airflow/3.3.1/docker-compose.yaml'  (untuk ambil config yaml file airflow)

>> **set up docker-compose.yaml file""

>> mkdir -p ./dags ./logs ./plugins ./config *(create folder project)*

docker compose up airflow-init
docker compose up -d (build & run container)
docker ps (cek container running)

>> jika ada perubahan di .yaml file, ulangi jalankan dari line 11



## STOP PROJECT
docker compose down >> (Stop + hapus container (paling sering dipakai), gak berat di laptop)
docker compose stop >> (Hanya stop (container masih ada, data aman))
docker compose down -v >> (Stop + hapus container + hapus volume (hati-hati, logs & database ikut terhapus))


## CONTINUE PROJECT

1. Masuk ke folder project (tempat file docker-compose.yaml berada)
2. Jalankan: docker compose up -d
3. Akses localhost:8080 (Airflow)