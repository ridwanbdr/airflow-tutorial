# Airflow Tutorial

Kumpulan contoh Apache Airflow untuk mempelajari operator dasar, dependency antar-task, templating Jinja, XCom, TaskFlow API, `catchup`, dan `backfill`, PostgreSQL connection, fault logs monitoring.

Project ini dijalankan secara lokal menggunakan Docker Compose dan Apache Airflow `3.3.1`. Seluruh file di folder `dags/` otomatis di-mount ke folder DAG Airflow di dalam container.

## Daftar Isi

- [Prasyarat](#prasyarat)
- [Environment Variables & Security](#environment-variables--security)
- [Menjalankan Project](#menjalankan-project)
- [Daftar DAG](#daftar-dag)
- [Materi dan Operasi](#materi-dan-operasi)
- [Menjalankan DAG](#menjalankan-dag)
- [Catchup dan Backfill](#catchup-dan-backfill)
- [Menghentikan dan Melanjutkan Project](#menghentikan-dan-melanjutkan-project)
- [Konfigurasi Docker Compose](#konfigurasi-docker-compose)
- [Struktur Project](#struktur-project)
- [Troubleshooting](#troubleshooting)
- [Keamanan & Best Practices](#keamanan--best-practices)

## Prasyarat

- Docker Desktop sudah terpasang dan sedang berjalan.
- Docker Compose tersedia melalui perintah `docker compose`.
- Minimal sekitar 4 GB memori Docker, 2 CPU, dan 10 GB ruang disk disarankan oleh konfigurasi Airflow.

Periksa instalasi:

```bash
docker --version
docker compose version
```

## Environment Variables & Security

Project ini menggunakan file `.env` untuk mengelola variabel sensitif seperti password, username, dan security keys. **Jangan pernah commit `.env` ke repository publik.**

### Variabel Sensitif yang Dikelola

| Variabel | Nilai Default | Keterangan |
| --- | --- | --- |
| `POSTGRES_USER` | `airflow` | Username PostgreSQL |
| `POSTGRES_PASSWORD` | `airflow` | Password PostgreSQL |
| `POSTGRES_DB` | `airflow` | Nama database PostgreSQL |
| `MINIO_ROOT_USER` | `admin` | Username MinIO root |
| `MINIO_ROOT_PASSWORD` | `password123` | Password MinIO root |
| `_AIRFLOW_WWW_USER_USERNAME` | `airflow` | Username Airflow Web UI |
| `_AIRFLOW_WWW_USER_PASSWORD` | `airflow` | Password Airflow Web UI |
| `AIRFLOW__API_AUTH__JWT_SECRET` | `airflow_jwt_secret` | Secret key untuk API authentication |
| `AIRFLOW__API_AUTH__JWT_ISSUER` | `airflow` | JWT issuer identifier |

### Mengubah Credentials Default

Sebelum menjalankan project, **sangat disarankan** untuk mengubah credentials default, terutama untuk password:

```bash
# Edit file .env
nano .env

# Atau gunakan editor pilihan Anda
# Ubah password dan credentials sesuai kebutuhan
```

Contoh `.env` yang lebih aman:

```env
AIRFLOW_UID=50000

_AIRFLOW_WWW_USER_USERNAME=airflow_admin
_AIRFLOW_WWW_USER_PASSWORD=YourSecurePassword123!

AIRFLOW__API_AUTH__JWT_SECRET=your_secure_jwt_secret_key_here
AIRFLOW__API_AUTH__JWT_ISSUER=airflow

POSTGRES_USER=airflow_user
POSTGRES_PASSWORD=PostgresSecurePassword456!
POSTGRES_DB=airflow_prod

MINIO_ROOT_USER=minioadmin
MINIO_ROOT_PASSWORD=MinIOSecurePassword789!

MINIO_API_PORT=9000
MINIO_CONSOLE_PORT=9001
```

### .gitignore

Pastikan `.env` sudah ada di `.gitignore` untuk mencegah commit credentials:

```bash
# Lihat atau buat .gitignore
echo ".env" >> .gitignore
```

## Menjalankan Project

Jalankan perintah dari folder yang berisi `docker-compose.yaml`.

### 0. Setup Environment Variables (Penting!)

Sebelum memulai, setup file `.env` dengan credentials yang aman:

```bash
# File .env sudah tersedia di project
# Review dan ubah credentials default jika diperlukan
nano .env
```

Minimal ubah password untuk security yang lebih baik:
- `POSTGRES_PASSWORD`
- `MINIO_ROOT_PASSWORD`
- `_AIRFLOW_WWW_USER_PASSWORD`
- `AIRFLOW__API_AUTH__JWT_SECRET`

### 1. Inisialisasi database dan user

```bash
docker compose up airflow-init
```

Perintah ini menjalankan migrasi database dan membuat user default berdasarkan variabel di `.env`:

| Item | Diambil dari |
| --- | --- |
| URL Airflow | http://localhost:8080 |
| Username | `_AIRFLOW_WWW_USER_USERNAME` di `.env` |
| Password | `_AIRFLOW_WWW_USER_PASSWORD` di `.env` |
| Database | PostgreSQL (variabel `POSTGRES_DB` di `.env`) |
| Database User | `POSTGRES_USER` di `.env` |
| Database Password | `POSTGRES_PASSWORD` di `.env` |

**Catatan:** Jika credentials di `.env` diubah, pastikan perubahan dilakukan sebelum menjalankan `docker compose up airflow-init`.

### 2. Menyalakan seluruh service

```bash
docker compose up -d
docker compose ps
```

Buka [http://localhost:8080](http://localhost:8080), lalu login menggunakan kredensial di atas. DAG dibuat dalam keadaan paused oleh konfigurasi project, sehingga DAG perlu di-unpause sebelum dijalankan melalui scheduler.

### 3. Setup MinIO (opsional, untuk DAG S3)

Akses MinIO console di [http://localhost:9001](http://localhost:9001) menggunakan credentials dari `.env`:
- Username: nilai `MINIO_ROOT_USER`
- Password: nilai `MINIO_ROOT_PASSWORD`

Buat bucket baru dengan nama `airflow` jika belum ada.

Buat folder `data/` di root project untuk menyimpan file yang akan diupload ke MinIO:

```bash
mkdir -p data
# Taruh file CSV atau file lain di folder ini
```

### 4. Membuat koneksi database dan MinIO di Airflow

Koneksi perlu dibuat di Web UI Admin → Connections atau melalui CLI. **Gunakan credentials dari `.env`:**

**Koneksi PostgreSQL:**
```bash
# Ganti POSTGRES_USER, POSTGRES_PASSWORD, POSTGRES_DB dengan nilai dari .env
docker compose run --rm airflow-cli connections add \
  --conn-uri postgresql://${POSTGRES_USER}:${POSTGRES_PASSWORD}@postgres:5432/${POSTGRES_DB} \
  postgres_dbeaver
```

Contoh dengan nilai aktual dari `.env`:
```bash
docker compose run --rm airflow-cli connections add \
  --conn-uri postgresql://airflow_user:PostgresSecurePassword456!@postgres:5432/airflow_prod \
  postgres_dbeaver
```

**Koneksi MinIO:**
```bash
# Ganti MINIO_ROOT_USER dan MINIO_ROOT_PASSWORD dengan nilai dari .env
docker compose run --rm airflow-cli connections add \
  --conn-type aws \
  --conn-login ${MINIO_ROOT_USER} \
  --conn-password ${MINIO_ROOT_PASSWORD} \
  --conn-extra '{"endpoint_url":"http://minio:9000"}' \
  minio_bucket
```

Contoh dengan nilai aktual dari `.env`:
```bash
docker compose run --rm airflow-cli connections add \
  --conn-type aws \
  --conn-login minioadmin \
  --conn-password MinIOSecurePassword789! \
  --conn-extra '{"endpoint_url":"http://minio:9000"}' \
  minio_bucket
```

### 5. Memantau log container

```bash
docker compose logs -f airflow-scheduler
docker compose logs -f airflow-dag-processor
```

Scheduler memeriksa perubahan file DAG secara berkala. Setelah file ditambahkan atau diubah, tunggu beberapa detik lalu refresh halaman Airflow.

## Daftar DAG

| File | DAG ID | Jadwal | Fokus pembelajaran |
| --- | --- | --- | --- |
| `1_bash_operator_basic.py` | `bash_operator_basic` | Manual | `BashOperator`, command shell, dan dependency berurutan |
| `2_bash_operator_advanced.py` | `bash_operator_advanced` | Manual | Bash multi-line, Jinja template, `params`, dan XCom |
| `3_python_operator_basic.py` | `python_operator_basic` | Manual | `PythonOperator`, callable, dan `op_kwargs` |
| `4_python_operator_advanced.py` | `python_operator_advanced` | Manual | Context Airflow, XCom return value, dan custom key |
| `5_taskflow_api_modern.py` | `taskflow_api_modern` | Manual | Decorator `@dag`, `@task`, dan dependency otomatis |
| `6_catchup_arguments.py` | `catchup_arguments` | `@daily` | `start_date`, logical date, dan `catchup=True` |
| `8_postgresql_operator_basic.py` | `postgres_operator_basic` | Manual | `SQLExecuteQueryOperator`, membuat tabel, insert, dan query PostgreSQL |
| `9_minio_s3_bucket.py` | `minio_s3_example` | Manual | `S3Hook`, upload, list, dan download file dari MinIO |

File `7_backfill_arguments.md` berisi contoh perintah CLI untuk menjalankan backfill pada rentang tanggal tertentu.

## Materi dan Operasi

### 1. BashOperator dasar

`bash_operator_basic` menjalankan empat task secara berurutan:

```text
print_hello -> print_date -> create_file -> list_tmp
```

Operasi yang dipraktikkan:

- Mencetak pesan dengan `echo`.
- Mencetak tanggal menggunakan `date`.
- Membuat `/tmp/airflow_test.txt` dan menampilkan isinya.
- Memeriksa isi folder `/tmp` menggunakan `ls`.

DAG ini hanya dapat dijalankan manual karena memiliki `schedule=None` dan tidak melakukan run yang terlewat karena `catchup=False`.

### 2. BashOperator lanjutan

`bash_operator_advanced` memperagakan alur pemrosesan data melalui XCom:

```text
generate_data -> process_data -> final_check
```

- `generate_data` membuat nilai berbasis waktu dan random, lalu mengirim baris terakhir output ke XCom.
- `process_data` mengambil nilai tersebut dengan `ti.xcom_pull()`, membuat `/tmp/hasil_proses.txt`, lalu mengirim nilai hasil proses.
- `final_check` membaca XCom dari task sebelumnya dan memeriksa isi file hasil.
- Template Jinja seperti `{{ ds }}`, `{{ logical_date }}`, dan `{{ params.nama }}` dirender saat task berjalan.
- Parameter default yang tersedia adalah `nama=Data Engineer` dan `environment=development`.

### 3. PythonOperator dasar

`python_operator_basic` menjalankan:

```text
print_hello -> print_nama
```

Contoh ini menunjukkan cara menghubungkan fungsi Python ke `PythonOperator`. Task `print_nama` menerima argumen melalui `op_kwargs`:

```python
nama = "Budi"
umur = 25
```

Output dicatat menggunakan modul `logging` agar dapat dibaca dari log task Airflow.

### 4. PythonOperator lanjutan dan XCom

`python_operator_advanced` menjalankan:

```text
generate_data -> process_data -> final_report
```

- `generate_data` menerima context Airflow, membaca `ds`, dan mengembalikan nilai. Return value otomatis disimpan ke XCom dengan key `return_value`.
- `process_data` mengambil return value, mengolahnya, lalu mengirim nilai tambahan dengan key custom `hasil_custom`.
- `final_report` mengambil nilai asli, nilai hasil proses, dan nilai custom untuk membuat laporan akhir.

### 5. TaskFlow API modern

`taskflow_api_modern` menggunakan `@dag` dan `@task` sehingga dependency terbentuk dari aliran nilai antar fungsi:

```text
generate_data() -> process_data(generated) -> final_report(generated, processed)
```

Return value dari task otomatis menjadi XCom dan dapat diteruskan sebagai argumen Python biasa. Pola ini mengurangi boilerplate dibandingkan membuat setiap task dengan `PythonOperator` secara eksplisit.

### 6. Catchup

`catchup_arguments` memiliki konfigurasi:

```python
start_date = 2026-08-15
schedule = "@daily"
catchup = True
```

Dengan `catchup=True`, scheduler membuat DAG run untuk interval harian yang terlewat sejak `start_date` ketika DAG diaktifkan. Contohnya, jika DAG aktif pada 20 Agustus 2026, Airflow dapat membuat run untuk interval tanggal 15 sampai 19 Agustus 2026.

Task `print_date` menampilkan `ds` dan `logical_date` untuk memperlihatkan tanggal logis setiap DAG run.

### 7. PostgreSQL dengan SQLExecuteQueryOperator

`postgres_operator_basic` menjalankan operasi database PostgreSQL:

```text
create_table -> insert_data -> insert_dynamic -> check_data
```

- `create_table` membuat tabel `airflow_tutorial` dengan kolom id, nama, kota, nilai, dan created_at.
- `insert_data` memasukkan data statis ke dalam tabel.
- `insert_dynamic` memasukkan data dengan template Jinja, menggunakan `{{ ds_nodash }}` untuk tanggal eksekusi.
- `check_data` menampilkan 10 data terakhir dari tabel.

DAG ini memerlukan koneksi PostgreSQL bernama `postgres_dbeaver` yang perlu dikonfigurasi di Web UI Airflow Admin → Connections.

**Konfigurasi koneksi (ambil dari `.env`):**
- Connection ID: `postgres_dbeaver`
- Connection Type: `PostgreSQL`
- Host: `postgres` (hostname container PostgreSQL)
- Database: nilai dari `POSTGRES_DB` di `.env`
- User: nilai dari `POSTGRES_USER` di `.env`
- Password: nilai dari `POSTGRES_PASSWORD` di `.env`
- Port: `5432`

### 8. MinIO S3 dengan S3Hook

`minio_s3_example` menjalankan operasi file storage menggunakan MinIO:

```text
upload_csv_to_minio -> list_objects_in_bucket -> download_file_from_minio
```

- `upload_csv_to_minio` mengupload file CSV lokal ke bucket MinIO dengan timestamp.
- `list_objects_in_bucket` menampilkan daftar object di folder `raw/` di dalam bucket.
- `download_file_from_minio` mendownload file yang telah diupload ke folder `/tmp`.

DAG ini memerlukan koneksi AWS/MinIO bernama `minio_bucket` yang perlu dikonfigurasi di Web UI Airflow Admin → Connections.

**Konfigurasi koneksi MinIO (ambil dari `.env`):**
- Connection ID: `minio_bucket`
- Connection Type: `Amazon S3`
- Access Key ID: nilai dari `MINIO_ROOT_USER` di `.env`
- Secret Access Key: nilai dari `MINIO_ROOT_PASSWORD` di `.env`
- Extra (JSON):
  ```json
  {
    "endpoint_url": "http://minio:9000"
  }
  ```

Pastikan folder `/opt/airflow/data/` di container Airflow memiliki file CSV yang akan diupload.

## Menjalankan DAG

### Melalui Web UI

1. Buka [http://localhost:8080](http://localhost:8080).
2. Cari DAG berdasarkan DAG ID pada tabel di atas.
3. Klik toggle pause agar DAG aktif.
4. Pilih **Trigger DAG** untuk DAG dengan jadwal manual.
5. Buka halaman DAG run atau task instance untuk melihat status dan log.

### Melalui CLI di container

Daftar DAG yang terdeteksi:

```bash
docker compose run --rm airflow-cli dags list
```

Trigger DAG manual:

```bash
docker compose run --rm airflow-cli dags trigger bash_operator_basic
docker compose run --rm airflow-cli dags trigger bash_operator_advanced
docker compose run --rm airflow-cli dags trigger python_operator_basic
docker compose run --rm airflow-cli dags trigger python_operator_advanced
docker compose run --rm airflow-cli dags trigger taskflow_api_modern
docker compose run --rm airflow-cli dags trigger postgres_operator_basic
docker compose run --rm airflow-cli dags trigger minio_s3_example
```

Melihat task pada sebuah DAG:

```bash
docker compose run --rm airflow-cli tasks list bash_operator_basic
```

Melihat daftar run dan menguji satu task:

```bash
docker compose run --rm airflow-cli dags list-runs -d bash_operator_basic
docker compose run --rm airflow-cli tasks test bash_operator_basic print_hello 2026-08-20
```

`tasks test` berguna untuk pengujian lokal satu task dan tidak membuat DAG run normal.

## Catchup dan Backfill

### Catchup otomatis

Untuk mengamati catchup, aktifkan `catchup_arguments` di Web UI. Scheduler akan membuat run harian yang terlewat berdasarkan `start_date` dan `schedule`.

### Backfill manual

Backfill menjalankan ulang DAG untuk rentang tanggal yang dipilih melalui CLI. Masuk ke container Airflow:

```bash
docker compose run --rm airflow-cli bash
```

Lalu jalankan:

```bash
airflow dags backfill catchup_arguments --start-date 2026-08-10 --end-date 2026-08-14
```

Contoh lain:

```bash
# Backfill dan reset DAG run yang sudah ada
airflow dags backfill catchup_arguments --start-date 2026-08-10 --end-date 2026-08-14 --reset-dagruns

# Backfill hanya satu hari
airflow dags backfill catchup_arguments --start-date 2026-08-18 --end-date 2026-08-18
```

Gunakan DAG ID `catchup_arguments`, sesuai file `6_catchup_arguments.py`. Perintah pada catatan lama yang menggunakan `catchup_example` perlu disesuaikan dengan ID tersebut.

## Menghentikan dan Melanjutkan Project

```bash
# Stop dan hapus container, volume tetap dipertahankan
docker compose down

# Hanya menghentikan container
docker compose stop

# Menyalakan kembali project
docker compose up -d
```

Hapus volume database hanya jika memang ingin mengulang inisialisasi dari awal:

```bash
docker compose down -v
```

Perintah `down -v` menghapus volume PostgreSQL sehingga metadata Airflow dan histori DAG run ikut hilang.

## Konfigurasi Docker Compose

Project ini menggunakan `docker-compose.yaml` dengan service berikut:

### PostgreSQL

```yaml
postgres:
  image: postgres:16
  environment:
    POSTGRES_USER: ${POSTGRES_USER}
    POSTGRES_PASSWORD: ${POSTGRES_PASSWORD}
    POSTGRES_DB: ${POSTGRES_DB}
  ports:
    - 5432:5432
  volumes:
    - postgres-db-volume:/var/lib/postgresql/data
```

Database PostgreSQL untuk metadata Airflow dan tempat menyimpan data dari DAG yang menggunakan `SQLExecuteQueryOperator`. Credentials diambil dari variabel `.env`.

### MinIO (S3-compatible Object Storage)

```yaml
minio:
  image: quay.io/minio/minio
  ports:
    - "${MINIO_API_PORT}:9000"      # API endpoint
    - "${MINIO_CONSOLE_PORT}:9001"  # Web console
  environment:
    MINIO_ROOT_USER: ${MINIO_ROOT_USER}
    MINIO_ROOT_PASSWORD: ${MINIO_ROOT_PASSWORD}
  volumes:
    - ./minio/data:/data
  command: server /data --console-address ":9001"
```

MinIO menyediakan storage S3-compatible untuk operasi upload/download file. Credentials dan port diambil dari variabel `.env`.

### Airflow Services

- **airflow-apiserver**: Web UI dan API server di port 8080
- **airflow-scheduler**: Scheduler untuk menjalankan DAG sesuai jadwal
- **airflow-dag-processor**: Processor untuk membaca dan memvalidasi DAG

### Provider Packages

Konfigurasi `.env` atau environment variable `_PIP_ADDITIONAL_REQUIREMENTS` pada docker-compose.yaml:

```yaml
_PIP_ADDITIONAL_REQUIREMENTS: 'apache-airflow-providers-postgres'
```

Untuk menggunakan MinIO/S3, provider AWS juga dapat ditambahkan:

```yaml
_PIP_ADDITIONAL_REQUIREMENTS: 'apache-airflow-providers-postgres apache-airflow-providers-amazon'
```

## Struktur Project

```text
.
├── dags/                   # Contoh DAG dan materi backfill
├── config/                 # Konfigurasi Airflow yang di-mount ke container
├── logs/                   # Log task dan komponen Airflow
├── data/                   # Dataset sample
├── plugins/                # Lokasi plugin lokal
├── docker-compose.yaml     # Service Airflow dan PostgreSQL
├── docker-run-setup.md     # Catatan setup Docker ringkas
└── README.md               # Dokumentasi project
```

## Troubleshooting

### .env tidak ditemukan atau belum dikonfigurasi

File `.env` sudah disediakan di project. Jika belum ada, Anda perlu membuatnya dari template atau menggunakan nilai default. Pastikan file `.env` ada di root project sebelum menjalankan `docker compose up`.

### Credentials tidak cocok setelah mengubah .env

Jika Anda mengubah `.env` setelah container sudah berjalan, Anda perlu **restart container** dan kemungkinan **re-initialize database**:

```bash
# Stop container
docker compose down

# Edit .env dengan credentials baru
nano .env

# Jalankan init ulang (hapus data lama)
docker compose down -v

# Start kembali dengan .env baru
docker compose up airflow-init
docker compose up -d
```

**PENTING**: Ini akan menghapus semua data Airflow dan database yang sudah ada.

### DAG belum muncul di Web UI

- Pastikan `docker compose up -d` sudah berjalan.
- Periksa log `airflow-dag-processor`.
- Pastikan file berada di folder `dags/` dan berekstensi `.py`.
- Tunggu interval pemindaian DAG, lalu refresh Web UI.

### Task gagal karena file di `/tmp` tidak ditemukan

Task yang membaca file harus dijalankan setelah task pembuat file pada DAG run yang sama. Untuk contoh lanjutan, urutannya adalah `generate_data -> process_data -> final_check`.

### Ingin melihat output `echo` atau `logger.info`

Buka task instance di Web UI dan pilih tab **Log**, atau gunakan perintah CLI task test untuk percobaan satu task.

### PostgreSQL DAG gagal karena koneksi tidak ditemukan

Pastikan koneksi `postgres_dbeaver` sudah dibuat di Admin → Connections dengan konfigurasi dari `.env`:
- Host: `postgres` (hostname container PostgreSQL)
- Database: nilai `POSTGRES_DB` dari `.env`
- User: nilai `POSTGRES_USER` dari `.env`
- Password: nilai `POSTGRES_PASSWORD` dari `.env`
- Port: `5432`

Untuk membuat koneksi melalui CLI (ganti placeholder dengan nilai dari `.env`):

```bash
docker compose run --rm airflow-cli connections add \
  --conn-uri postgresql://airflow_user:PostgresSecurePassword456!@postgres:5432/airflow_prod \
  postgres_dbeaver
```

### MinIO/S3 DAG gagal karena koneksi tidak ditemukan

Pastikan koneksi `minio_bucket` sudah dibuat di Admin → Connections dengan konfigurasi dari `.env`:
- Connection Type: Amazon S3
- Access Key ID: nilai `MINIO_ROOT_USER` dari `.env`
- Secret Access Key: nilai `MINIO_ROOT_PASSWORD` dari `.env`
- Extra: `{"endpoint_url": "http://minio:9000"}`

Untuk membuat koneksi melalui CLI (ganti placeholder dengan nilai dari `.env`):

```bash
docker compose run --rm airflow-cli connections add \
  --conn-type aws \
  --conn-login minioadmin \
  --conn-password MinIOSecurePassword789! \
  --conn-extra '{"endpoint_url":"http://minio:9000"}' \
  minio_bucket
```

### File CSV untuk MinIO tidak ditemukan

Buat folder `data/` di root project dan taruh file CSV di sana. Folder ini sudah di-mount ke container di `/opt/airflow/data/`. Ubah `local_file_path` di `9_minio_s3_bucket.py` sesuai dengan file CSV yang ada.

### MinIO console tidak bisa diakses

Pastikan service MinIO sudah running:

```bash
docker compose ps
```

Akses console di port yang dikonfigurasi di `.env` (default: [http://localhost:9001](http://localhost:9001)) dengan username dan password dari `MINIO_ROOT_USER` dan `MINIO_ROOT_PASSWORD` di `.env`. Buat bucket bernama `airflow` jika belum ada.

### Login Airflow Web UI gagal

Gunakan username dan password dari `.env`:
- Username: `_AIRFLOW_WWW_USER_USERNAME`
- Password: `_AIRFLOW_WWW_USER_PASSWORD`

Jika lupa, restart dengan re-init database menggunakan `.env` baru.

## Keamanan & Best Practices

### Untuk Development

File `.env` default sudah cukup untuk pembelajaran lokal. Jika ingin testing security:

```env
# Ubah minimal password
POSTGRES_PASSWORD=dev_password_123
MINIO_ROOT_PASSWORD=dev_minio_password_456
_AIRFLOW_WWW_USER_PASSWORD=dev_airflow_password_789
AIRFLOW__API_AUTH__JWT_SECRET=dev_jwt_secret_key_here
```

### Untuk Production

**JANGAN PERNAH** gunakan credentials default untuk production. Implementasikan:

1. **Strong passwords**: Minimal 16 karakter, kombinasi uppercase, lowercase, number, special character
2. **Secret management**: Gunakan secrets manager (HashiCorp Vault, AWS Secrets Manager, etc)
3. **Environment separation**: Pisahkan `.env` per environment (dev, staging, prod)
4. **.gitignore**: Pastikan `.env` tidak pernah di-commit ke repository
5. **Credential rotation**: Ubah password secara berkala
6. **SSL/TLS**: Aktifkan untuk semua komunikasi
7. **Firewall**: Batasi akses ke port (9000, 9001, 5432, 8080)

Contoh `.gitignore`:
```
.env
.env.local
.env.*.local
```

> Konfigurasi Docker Compose pada project ini ditujukan untuk pembelajaran dan pengembangan lokal, bukan deployment production.