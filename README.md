# Airflow Tutorial

Kumpulan contoh Apache Airflow untuk mempelajari operator dasar, dependency antar-task, templating Jinja, XCom, TaskFlow API, `catchup`, dan `backfill`.

Project ini dijalankan secara lokal menggunakan Docker Compose dan Apache Airflow `3.3.1`. Seluruh file di folder `dags/` otomatis di-mount ke folder DAG Airflow di dalam container.

## Daftar Isi

- [Prasyarat](#prasyarat)
- [Menjalankan Project](#menjalankan-project)
- [Daftar DAG](#daftar-dag)
- [Materi dan Operasi](#materi-dan-operasi)
- [Menjalankan DAG](#menjalankan-dag)
- [Catchup dan Backfill](#catchup-dan-backfill)
- [Menghentikan dan Melanjutkan Project](#menghentikan-dan-melanjutkan-project)
- [Struktur Project](#struktur-project)
- [Troubleshooting](#troubleshooting)

## Prasyarat

- Docker Desktop sudah terpasang dan sedang berjalan.
- Docker Compose tersedia melalui perintah `docker compose`.
- Minimal sekitar 4 GB memori Docker, 2 CPU, dan 10 GB ruang disk disarankan oleh konfigurasi Airflow.

Periksa instalasi:

```bash
docker --version
docker compose version
```

## Menjalankan Project

Jalankan perintah dari folder yang berisi `docker-compose.yaml`.

### 1. Inisialisasi database dan user

```bash
docker compose up airflow-init
```

Perintah ini menjalankan migrasi database dan membuat user default:

| Item | Nilai default |
| --- | --- |
| URL Airflow | http://localhost:8080 |
| Username | `airflow` |
| Password | `airflow` |
| Database | PostgreSQL (`airflow`) |

### 2. Menyalakan seluruh service

```bash
docker compose up -d
docker compose ps
```

Buka [http://localhost:8080](http://localhost:8080), lalu login menggunakan kredensial di atas. DAG dibuat dalam keadaan paused oleh konfigurasi project, sehingga DAG perlu di-unpause sebelum dijalankan melalui scheduler.

### 3. Memantau log container

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

## Struktur Project

```text
.
├── dags/                   # Contoh DAG dan materi backfill
├── config/                 # Konfigurasi Airflow yang di-mount ke container
├── logs/                   # Log task dan komponen Airflow
├── plugins/                # Lokasi plugin lokal
├── docker-compose.yaml     # Service Airflow dan PostgreSQL
├── docker-run-setup.md     # Catatan setup Docker ringkas
└── README.md               # Dokumentasi project
```

## Troubleshooting

### DAG belum muncul di Web UI

- Pastikan `docker compose up -d` sudah berjalan.
- Periksa log `airflow-dag-processor`.
- Pastikan file berada di folder `dags/` dan berekstensi `.py`.
- Tunggu interval pemindaian DAG, lalu refresh Web UI.

### Task gagal karena file di `/tmp` tidak ditemukan

Task yang membaca file harus dijalankan setelah task pembuat file pada DAG run yang sama. Untuk contoh lanjutan, urutannya adalah `generate_data -> process_data -> final_check`.

### Ingin melihat output `echo` atau `logger.info`

Buka task instance di Web UI dan pilih tab **Log**, atau gunakan perintah CLI task test untuk percobaan satu task.

> Konfigurasi Docker Compose pada project ini ditujukan untuk pembelajaran dan pengembangan lokal, bukan deployment production.