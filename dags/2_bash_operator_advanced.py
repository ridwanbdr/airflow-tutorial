from airflow import DAG
from airflow.operators.bash import BashOperator
from datetime import datetime, timedelta

default_args = {
    'owner': 'data_engineer',
    'retries': 1,
    'retry_delay': timedelta(minutes=1),
}

with DAG(
    dag_id='bash_operator_advanced',
    default_args=default_args,
    description='Contoh BashOperator: Jinja + Multi-line + XCom',
    start_date=datetime(2024, 1, 1),
    schedule=None,          # Hanya trigger manual
    catchup=False,
    tags=['belajar', 'bash', 'jinja', 'xcom'],
    params={                         # Parameter default yang bisa dipakai via Jinja
        'nama': 'Data Engineer',
        'environment': 'development'
    }
) as dag:

    # -------------------------------------------------
    # Task 1: Generate data + push ke XCom
    # -------------------------------------------------
    # do_xcom_push=True (default) → baris terakhir dari stdout akan di-push ke XCom
    generate_data = BashOperator(
        task_id='generate_data',
        bash_command="""
            echo "=== Task Generate Data ==="
            echo "Hari ini: {{ ds }}"
            echo "Nama dari params: {{ params.nama }}"
            echo "Environment: {{ params.environment }}"
            
            # Kita generate sebuah nilai (misalnya timestamp + random)
            VALUE="Airflow_$(date +%Y%m%d_%H%M%S)_$RANDOM"
            
            echo "Nilai yang akan dikirim ke task berikutnya: $VALUE"
            
            # Baris terakhir inilah yang akan di-push ke XCom
            echo "$VALUE"
        """
    )

    # -------------------------------------------------
    # Task 2: Terima data dari XCom + multi-line command
    # -------------------------------------------------
    process_data = BashOperator(
        task_id='process_data',
        bash_command="""
            echo "=== Task Process Data ==="
            
            # Ambil nilai dari task sebelumnya menggunakan Jinja + XCom
            RECEIVED_VALUE="{{ ti.xcom_pull(task_ids='generate_data') }}"
            
            echo "Nilai yang diterima dari generate_data: $RECEIVED_VALUE"
            
            # Multi-line bash command
            echo "Membuat file hasil proses..."
            cat > /tmp/hasil_proses.txt << EOF
================================
Hasil Proses Airflow
================================
Execution Date : {{ ds }}
Logical Date   : {{ logical_date }}
Nilai dari XCom: $RECEIVED_VALUE
Nama           : {{ params.nama }}
Environment    : {{ params.environment }}
Generated at   : $(date)
================================
EOF
            
            echo "Isi file hasil:"
            cat /tmp/hasil_proses.txt
            
            # Kita push lagi nilai baru ke XCom (opsional)
            echo "Processed_$RECEIVED_VALUE"
        """
    )

    # -------------------------------------------------
    # Task 3: Final check + pakai XCom dari task 2
    # -------------------------------------------------
    final_check = BashOperator(
        task_id='final_check',
        bash_command="""
            echo "=== Final Check ==="
            
            FINAL_VALUE="{{ ti.xcom_pull(task_ids='process_data') }}"
            
            echo "Nilai final dari process_data: $FINAL_VALUE"
            echo "File yang dibuat sebelumnya:"
            ls -la /tmp/hasil_proses.txt
            
            echo ""
            echo "Isi file:"
            cat /tmp/hasil_proses.txt
            
            echo ""
            echo "Selesai! Semua task berhasil."
        """
    )

    # Dependency
    generate_data >> process_data >> final_check